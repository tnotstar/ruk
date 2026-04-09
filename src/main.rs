/*
 * Copyright 2020-2026, Antonio Alvarado <tnotstar+copyright@gmail.com>
 *
 * A high-performance HTTP benchmarking tool written in Rust (ruk)
 */

use clap::Parser;
use flume::Receiver;
use hdrhistogram::Histogram;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RECVBUF: usize = 16384;
const MAX_HEADERS: usize = 64;
const STATUS_SLOTS: usize = 600;

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
    total_bytes: u64,
    latency_hist: Histogram<u64>,
    status_codes: [u64; STATUS_SLOTS],
}

impl Default for WorkerStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            total_bytes: 0,
            latency_hist: Histogram::<u64>::new_with_bounds(1, 600_000_000, 3).unwrap(),
            status_codes: [0; STATUS_SLOTS],
        }
    }
}

impl WorkerStats {
    fn merge(&mut self, other: WorkerStats) {
        self.total_requests += other.total_requests;
        self.successful_requests += other.successful_requests;
        self.total_bytes += other.total_bytes;
        self.latency_hist.add(other.latency_hist).expect("Histogram merge failed");
        for i in 0..STATUS_SLOTS {
            self.status_codes[i] += other.status_codes[i];
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

fn resolve_addr(host: &str, port: u16) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let addrs: Vec<SocketAddr> = format!("{}:{}", host, port).to_socket_addrs()?.collect();
    if let Some(v4) = addrs.iter().find(|a| a.is_ipv4()) {
        return Ok(*v4);
    }
    addrs.into_iter().next().ok_or_else(|| "DNS resolution failed".into())
}

struct UrlParts {
    host: String,
    authority: String,
    port: u16,
    path: String,
}

fn parse_url(url: &str) -> Result<UrlParts, Box<dyn std::error::Error + Send + Sync>> {
    let (scheme, rest) = url.split_once("://").ok_or("Invalid URL: missing scheme")?;

    let default_port = match scheme {
        "http" => 80,
        "https" => 443,
        _ => return Err("Unsupported scheme".into()),
    };

    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };

    let (host, port) = match authority.rfind(':') {
        Some(i) => (&authority[..i], authority[i + 1..].parse().unwrap_or(default_port)),
        None => (authority, default_port),
    };

    Ok(UrlParts {
        host: host.to_string(),
        authority: authority.to_string(),
        port,
        path: path.to_string(),
    })
}

fn build_request(path: &str, authority: &str) -> Vec<u8> {
    format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
        path, authority
    )
    .into_bytes()
}

enum BodyInfo {
    ContentLength(usize),
    Chunked,
    UntilClose,
}

fn classify_body(headers: &[httparse::Header]) -> BodyInfo {
    let mut content_length = None;
    let mut chunked = false;
    let mut connection_close = false;

    for h in headers {
        if h.name.eq_ignore_ascii_case("content-length") {
            content_length = std::str::from_utf8(h.value).ok().and_then(|v| v.parse().ok());
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = std::str::from_utf8(h.value)
                .map(|v| v.contains("chunked"))
                .unwrap_or(false);
        } else if h.name.eq_ignore_ascii_case("connection") {
            connection_close = std::str::from_utf8(h.value)
                .map(|v| v.eq_ignore_ascii_case("close"))
                .unwrap_or(false);
        }
    }

    if let Some(cl) = content_length {
        BodyInfo::ContentLength(cl)
    } else if chunked {
        BodyInfo::Chunked
    } else if connection_close {
        BodyInfo::UntilClose
    } else {
        BodyInfo::UntilClose
    }
}

async fn discard_bytes(
    stream: &mut TcpStream,
    buf: &mut [u8],
    mut remaining: usize,
) -> Result<usize, ()> {
    let mut total = 0;
    while remaining > 0 {
        let to_read = remaining.min(buf.len());
        let n = stream.read(&mut buf[..to_read]).await.map_err(|_| ())?;
        if n == 0 {
            return Err(());
        }
        remaining -= n;
        total += n;
    }
    Ok(total)
}

async fn discard_chunked(
    stream: &mut TcpStream,
    buf: &mut [u8],
    initial: &[u8],
) -> Result<usize, ()> {
    let mut extra = 0;
    let mut pending: Vec<u8> = initial.to_vec();
    let mut pos = 0;

    loop {
        while pos < pending.len() && pending[pos] == b'\r' {
            pos += 1;
        }
        if pos < pending.len() && pending[pos] == b'\n' {
            pos += 1;
        }

        loop {
            if pos + 1 >= pending.len() {
                let n = stream.read(buf).await.map_err(|_| ())?;
                if n == 0 {
                    return Err(());
                }
                pending.extend_from_slice(&buf[..n]);
                extra += n;
                continue;
            }

            let line_end = pending[pos..].windows(2).position(|w| w == b"\r\n");
            if let Some(i) = line_end {
                let size_str = std::str::from_utf8(&pending[pos..pos + i]).map_err(|_| ())?;
                let size_str = size_str.split(';').next().unwrap_or(size_str).trim();
                let chunk_size = usize::from_str_radix(size_str, 16).map_err(|_| ())?;
                pos += i + 2;

                if chunk_size == 0 {
                    return Ok(extra);
                }

                let needed = chunk_size + 2;
                while pos + needed > pending.len() {
                    let n = stream.read(buf).await.map_err(|_| ())?;
                    if n == 0 {
                        return Err(());
                    }
                    pending.extend_from_slice(&buf[..n]);
                    extra += n;
                }
                pos += needed;
                break;
            } else if pending.len() - pos > 16 {
                return Err(());
            } else {
                let n = stream.read(buf).await.map_err(|_| ())?;
                if n == 0 {
                    return Err(());
                }
                pending.extend_from_slice(&buf[..n]);
                extra += n;
            }
        }
    }
}

enum ExecuteError {
    ConnectionClosed,
    ProtocolError,
}

struct ResponseResult {
    status: u16,
    total_bytes: usize,
    connection_close: bool,
}

async fn execute_request(
    stream: &mut TcpStream,
    request: &[u8],
    buf: &mut [u8],
) -> Result<ResponseResult, ExecuteError> {
    stream.write_all(request).await.map_err(|_| ExecuteError::ConnectionClosed)?;

    let mut total_read = 0;

    let (status, header_len, body_info) = loop {
        if total_read >= buf.len() {
            return Err(ExecuteError::ProtocolError);
        }

        let n = stream.read(&mut buf[total_read..]).await.map_err(|_| ExecuteError::ConnectionClosed)?;
        if n == 0 {
            return Err(ExecuteError::ConnectionClosed);
        }
        total_read += n;

        let parsed = {
            let mut hdr = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut resp = httparse::Response::new(&mut hdr);
            match resp.parse(&buf[..total_read]) {
                Ok(httparse::Status::Complete(hlen)) => {
                    Some((resp.code.ok_or(ExecuteError::ProtocolError)?, hlen, classify_body(resp.headers)))
                }
                Ok(httparse::Status::Partial) => None,
                Err(_) => return Err(ExecuteError::ProtocolError),
            }
        };

        if let Some((s, h, b)) = parsed {
            break (s, h, b);
        }
    };

    let body_in_buffer = total_read - header_len;
    let mut total_bytes = total_read;

    match body_info {
        BodyInfo::ContentLength(cl) => {
            let remaining = cl.saturating_sub(body_in_buffer);
            total_bytes += discard_bytes(stream, buf, remaining).await.map_err(|_| ExecuteError::ConnectionClosed)?;
        }
        BodyInfo::Chunked => {
            let initial_data = buf[header_len..total_read].to_vec();
            total_bytes += discard_chunked(stream, buf, &initial_data).await.map_err(|_| ExecuteError::ConnectionClosed)?;
        }
        BodyInfo::UntilClose => {
            loop {
                let n = stream.read(buf).await.map_err(|_| ExecuteError::ConnectionClosed)?;
                if n == 0 {
                    break;
                }
                total_bytes += n;
            }
            return Ok(ResponseResult {
                status,
                total_bytes,
                connection_close: true,
            });
        }
    }

    Ok(ResponseResult {
        status,
        total_bytes,
        connection_close: false,
    })
}

async fn connect(addr: SocketAddr) -> Result<TcpStream, ()> {
    let stream = TcpStream::connect(addr).await.map_err(|_| ())?;
    stream.set_nodelay(true).map_err(|_| ())?;
    Ok(stream)
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if args.threads > 0 {
        builder.worker_threads(args.threads);
    }
    builder.enable_all();

    let rt = builder.build()?;
    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = parse_url(&args.url)?;
    let addr = resolve_addr(&url.host, url.port)?;
    eprintln!("Resolved {} -> {}", url.host, addr);

    let request = build_request(&url.path, &url.authority);

    let (tx, rx) = flume::unbounded();
    let recording = Arc::new(AtomicBool::new(false));
    let running = Arc::new(AtomicBool::new(true));

    let aggregator_handle = tokio::spawn(aggregator_task(rx));

    let mut worker_handles = Vec::with_capacity(args.connections);

    for worker_id in 0..args.connections {
        let tx = tx.clone();
        let recording = recording.clone();
        let running = running.clone();
        let request = request.clone();

        worker_handles.push(tokio::spawn(async move {
            let mut stats = WorkerStats::default();
            let mut buf = [0u8; RECVBUF];

            let mut stream = match connect(addr).await {
                Ok(s) => s,
                Err(()) => {
                    eprintln!("Worker {} failed to connect", worker_id);
                    let _ = tx.send_async(stats).await;
                    return;
                }
            };

            while running.load(Ordering::Relaxed) {
                let mut is_retry = false;
                loop {
                    let start = Instant::now();
                    let is_rec = recording.load(Ordering::Relaxed);

                    match execute_request(&mut stream, &request, &mut buf).await {
                        Ok(result) => {
                            if is_rec {
                                stats.total_requests += 1;
                                if result.status >= 200 && result.status < 400 {
                                    stats.successful_requests += 1;
                                }
                                stats.total_bytes += result.total_bytes as u64;
                                let idx = result.status as usize;
                                if idx < STATUS_SLOTS {
                                    stats.status_codes[idx] += 1;
                                }
                                let elapsed = start.elapsed().as_micros() as u64;
                                let _ = stats.latency_hist.record(elapsed);
                            }

                            if result.connection_close {
                                if let Ok(s) = connect(addr).await {
                                    stream = s;
                                }
                            }
                            break;
                        }
                        Err(ExecuteError::ConnectionClosed) if !is_retry => {
                            // Silent reconnect and retry once
                            if let Ok(s) = connect(addr).await {
                                stream = s;
                                is_retry = true;
                                continue;
                            } else {
                                if is_rec {
                                    stats.total_requests += 1;
                                    stats.status_codes[0] += 1;
                                }
                                break;
                            }
                        }
                        Err(_) => {
                            if is_rec {
                                stats.total_requests += 1;
                                stats.status_codes[0] += 1;
                                let elapsed = start.elapsed().as_micros() as u64;
                                let _ = stats.latency_hist.record(elapsed);
                            }

                            // Try to reconnect for the next iteration
                            if let Ok(s) = connect(addr).await {
                                stream = s;
                            }
                            break;
                        }
                    }
                }
            }

            let _ = tx.send_async(stats).await;
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

    running.store(false, Ordering::Relaxed);
    let actual_duration = exec_start.elapsed();

    for handle in worker_handles {
        let _ = handle.await;
    }

    drop(tx);
    let stats = aggregator_handle.await.unwrap();

    // Summary
    println!("\n=== Benchmark Summary ===");
    println!("Target URL:          {}", args.url);
    println!("Concurrency Level:   {}", args.connections);
    println!("Time Taken:          {:.2}s", actual_duration.as_secs_f64());
    println!("Total Requests:      {}", stats.total_requests);
    println!("Successful Reqs:     {}", stats.successful_requests);
    println!(
        "Failed Requests:     {}",
        stats.total_requests - stats.successful_requests
    );
    println!(
        "Data Transferred:    {:.2} MB",
        stats.total_bytes as f64 / 1_048_576.0
    );
    println!(
        "Requests/sec:        {:.2}",
        stats.total_requests as f64 / actual_duration.as_secs_f64()
    );

    println!("\nStatus Codes:");
    for (code, &count) in stats.status_codes.iter().enumerate() {
        if count > 0 {
            if code == 0 {
                println!("  [Network Error]:     {}", count);
            } else {
                println!("  [HTTP {}]:           {}", code, count);
            }
        }
    }

    if stats.total_requests > 0 {
        let hist = stats.latency_hist;
        println!("\nLatency Distribution:");
        println!("  Min:         {:.2} ms", hist.min() as f64 / 1000.0);
        println!("  Mean:        {:.2} ms", hist.mean() / 1000.0);
        println!(
            "  p50:         {:.2} ms",
            hist.value_at_quantile(0.50) as f64 / 1000.0
        );
        println!(
            "  p90:         {:.2} ms",
            hist.value_at_quantile(0.90) as f64 / 1000.0
        );
        println!(
            "  p99:         {:.2} ms",
            hist.value_at_quantile(0.99) as f64 / 1000.0
        );
        println!("  Max:         {:.2} ms", hist.max() as f64 / 1000.0);
    }

    Ok(())
}
