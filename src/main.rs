/*
 * Copyright (c) 2026 Antonio Alvarado Hernández
 * A high-performance HTTP benchmarking tool written in Rust (ruk)
 */

use clap::Parser;
use flume::Receiver;
use hdrhistogram::Histogram;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::rt::TokioIo;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "ruk", version, about = "High-performance HTTP Benchmark Tool", long_about = None)]
struct Args {
    /// Target URL to benchmark
    #[arg(required = true)]
    url: String,

    /// Number of concurrent workers (connections)
    #[arg(short, long, default_value_t = 50)]
    connections: usize,

    /// Number of OS threads for Tokio runtime (0 = all cores)
    #[arg(short, long, default_value_t = 0)]
    threads: usize,

    /// Duration of the benchmark run in seconds
    #[arg(short, long, default_value_t = 10)]
    duration: u64,

    /// Warm-up duration in seconds before recording metrics
    #[arg(short, long, default_value_t = 2)]
    warmup: u64,
}

struct WorkerStats {
    total_requests: u64,
    successful_requests: u64,
    total_bytes: usize,
    latency_hist: Histogram<u64>,
    status_codes: std::collections::HashMap<u16, u64>,
}

impl Default for WorkerStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            total_bytes: 0,
            latency_hist: Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).unwrap(),
            status_codes: std::collections::HashMap::new(),
        }
    }
}

impl WorkerStats {
    fn merge(&mut self, other: WorkerStats) {
        self.total_requests += other.total_requests;
        self.successful_requests += other.successful_requests;
        self.total_bytes += other.total_bytes;
        self.latency_hist.add(other.latency_hist).expect("Histogram merge failed");
        for (code, count) in other.status_codes {
            *self.status_codes.entry(code).or_insert(0) += count;
        }
    }
}

async fn aggregator_task(rx: Receiver<WorkerStats>) -> WorkerStats {
    let mut total_stats = WorkerStats::default();

    while let Ok(worker_stats) = rx.recv_async().await {
        total_stats.merge(worker_stats);
    }

    total_stats
}

/// Establish a raw TCP connection and perform the hyper HTTP/1.1 handshake.
/// Returns a SendRequest handle for issuing requests on this connection.
/// The connection driver is spawned as a background task.
async fn connect_http1(
    addr: SocketAddr,
) -> Result<hyper::client::conn::http1::SendRequest<http_body_util::Empty<Bytes>>, Box<dyn std::error::Error + Send + Sync>>
{
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?; // Disable Nagle for low-latency benchmarking
    let io = TokioIo::new(stream);

    let (sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    // Drive the connection in the background. When it drops, so does the TCP socket.
    tokio::spawn(async move {
        let _ = conn.await;
    });

    Ok(sender)
}

/// Resolve the host:port pair to a SocketAddr, preferring IPv4.
fn resolve_addr(host: &str, port: u16) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let addrs: Vec<SocketAddr> = format!("{}:{}", host, port).to_socket_addrs()?.collect();

    // Prefer IPv4 to avoid issues with NGINX only listening on 127.0.0.1
    if let Some(v4) = addrs.iter().find(|a| a.is_ipv4()) {
        return Ok(*v4);
    }
    addrs.into_iter().next().ok_or_else(|| "DNS resolution failed".into())
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // Always use multi_thread runtime. The work-stealing scheduler handles
    // the (N workers + N conn drivers) task topology correctly, even with
    // worker_threads(1). The current_thread scheduler starves conn drivers
    // under high concurrency due to its cooperative FIFO queue.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if args.threads > 0 {
        builder.worker_threads(args.threads);
    }
    builder.enable_all();

    let rt = builder.build()?;
    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Parse the URI and resolve the socket address ONCE upfront.
    let uri: hyper::Uri = args.url.parse()?;
    let host = uri.host().ok_or("URL must have a host")?;
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("https") { 443 } else { 80 });
    let authority = uri.authority().cloned().ok_or("URL must have an authority")?;

    let addr = resolve_addr(host, port)?;
    eprintln!("Resolved {} -> {}", host, addr);

    let path = uri.path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| "/".to_owned());

    // 2. Setup channels and synchronization primitives
    let (tx, rx) = flume::unbounded();
    let recording = Arc::new(AtomicBool::new(false));
    let running = Arc::new(AtomicBool::new(true));

    // 3. Start Aggregator Task
    let aggregator_handle = tokio::spawn(aggregator_task(rx));

    // 4. Start Workers — each owns a single persistent TCP + HTTP/1.1 connection
    let mut worker_handles = Vec::with_capacity(args.connections);

    for worker_id in 0..args.connections {
        let tx = tx.clone();
        let recording = recording.clone();
        let running = running.clone();
        let path = path.clone();
        let authority = authority.clone();

        worker_handles.push(tokio::spawn(async move {
            let mut local_stats = WorkerStats::default();

            // Establish the initial connection
            let mut sender = match connect_http1(addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Worker {} failed initial connection: {}", worker_id, e);
                    // Still send (empty) stats so the aggregator doesn't hang
                    let _ = tx.send_async(local_stats).await;
                    return;
                }
            };

            while running.load(Ordering::Relaxed) {
                // Reconnect if the connection has been closed by the server
                if sender.is_closed() {
                    sender = match connect_http1(addr).await {
                        Ok(s) => s,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            continue;
                        }
                    };
                }

                let start = Instant::now();
                let is_rec = recording.load(Ordering::Relaxed);

                let req = Request::builder()
                    .uri(&path)
                    .header(hyper::header::HOST, authority.as_str())
                    .body(http_body_util::Empty::<Bytes>::new())
                    .expect("Failed to build request");

                match sender.send_request(req).await {
                    Ok(resp) => {
                        let status_code = resp.status().as_u16();
                        let success = status_code >= 200 && status_code < 400;

                        // Flat header size estimation (matches wrk's L7 accounting)
                        let mut bytes_this_req: usize = 215;

                        // Consume body as fast as possible using collect()
                        match resp.into_body().collect().await {
                            Ok(collected) => {
                                bytes_this_req += collected.to_bytes().len();
                            }
                            Err(_) => {}
                        }

                        if is_rec {
                            local_stats.total_requests += 1;
                            if success {
                                local_stats.successful_requests += 1;
                            }
                            local_stats.total_bytes += bytes_this_req;
                            *local_stats.status_codes.entry(status_code).or_insert(0) += 1;
                            let elapsed = start.elapsed().as_micros() as u64;
                            let _ = local_stats.latency_hist.record(elapsed);
                        }
                    }
                    Err(_) => {
                        if is_rec {
                            local_stats.total_requests += 1;
                            *local_stats.status_codes.entry(0).or_insert(0) += 1;
                            let elapsed = start.elapsed().as_micros() as u64;
                            let _ = local_stats.latency_hist.record(elapsed);
                        }
                    }
                }
            }
            // Send final results once per worker
            let _ = tx.send_async(local_stats).await;
        }));
    }

    // Phase 1: Warmup
    println!("Warming up for {} seconds...", args.warmup);
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(args.warmup)) => {}
        _ = tokio::signal::ctrl_c() => {
            println!("\nInterrupted during warmup.");
            running.store(false, Ordering::Relaxed);
            return Ok(());
        }
    }

    // Phase 2: Execution
    println!("Running benchmark for {} seconds...", args.duration);
    recording.store(true, Ordering::Relaxed);

    let exec_start = Instant::now();
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(args.duration)) => {}
        _ = tokio::signal::ctrl_c() => {
            println!("\nInterrupted during benchmark.");
        }
    }

    // Stop workers
    running.store(false, Ordering::Relaxed);
    let actual_duration = exec_start.elapsed();

    // Release resources and wait for graceful shutdown
    for handle in worker_handles {
        let _ = handle.await;
    }

    // Drop sender so aggregator loop terminates
    drop(tx);

    let stats = aggregator_handle.await.unwrap();

    // Summary
    println!("\n=== Benchmark Summary ===");
    println!("Target URL:          {}", args.url);
    println!("Concurrency Level:   {}", args.connections);
    println!("Time Taken:          {:.2}s", actual_duration.as_secs_f64());
    println!("Total Requests:      {}", stats.total_requests);
    println!("Successful Reqs:     {}", stats.successful_requests);
    println!("Failed Requests:     {}", stats.total_requests - stats.successful_requests);
    println!("Data Transferred:    {:.2} MB", stats.total_bytes as f64 / 1_048_576.0);
    println!("Requests/sec:        {:.2}", stats.total_requests as f64 / actual_duration.as_secs_f64());

    println!("\nStatus Codes:");
    let mut codes: Vec<_> = stats.status_codes.iter().collect();
    codes.sort_by_key(|&(k, _)| *k);
    for (code, count) in codes {
        if *code == 0 {
            println!("  [Network Error]:     {}", count);
        } else {
            println!("  [HTTP {}]:           {}", code, count);
        }
    }

    if stats.total_requests > 0 {
        let hist = stats.latency_hist;
        println!("\nLatency Distribution:");
        println!("  Min:         {:.2} ms", hist.min() as f64 / 1000.0);
        println!("  Mean:        {:.2} ms", hist.mean() / 1000.0);
        println!("  p50:         {:.2} ms", hist.value_at_quantile(0.50) as f64 / 1000.0);
        println!("  p90:         {:.2} ms", hist.value_at_quantile(0.90) as f64 / 1000.0);
        println!("  p99:         {:.2} ms", hist.value_at_quantile(0.99) as f64 / 1000.0);
        println!("  Max:         {:.2} ms", hist.max() as f64 / 1000.0);
    }

    Ok(())
}
