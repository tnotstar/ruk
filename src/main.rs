/*
 * Copyright 2020-2026, Antonio Alvarado <tnotstar+copyright@gmail.com>
 *
 * A high-performance HTTP benchmarking tool written in Rust (ruk)
 */

use clap::Parser;
use hdrhistogram::Histogram;
use std::io::{Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RECVBUF: usize = 16384;
const MAX_HEADERS: usize = 16; // Optimized from 64 to 16 to save stack initialization time
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
        let _ = self.latency_hist.add(other.latency_hist);
        for i in 0..STATUS_SLOTS {
            self.status_codes[i] += other.status_codes[i];
        }
    }
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

fn resolve_addr(host: &str, port: u16) -> Result<SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let addrs: Vec<SocketAddr> = format!("{}:{}", host, port).to_socket_addrs()?.collect();
    if let Some(v4) = addrs.iter().find(|a| a.is_ipv4()) {
        return Ok(*v4);
    }
    addrs.into_iter().next().ok_or_else(|| "DNS resolution failed".into())
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

struct HeaderInfo {
    body_info: BodyInfo,
    connection_close: bool,
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    let mut val = 0;
    let mut has_digits = false;
    for &b in bytes {
        if b >= b'0' && b <= b'9' {
            val = val * 10 + (b - b'0') as usize;
            has_digits = true;
        } else if b == b' ' || b == b'\t' {
            continue;
        } else {
            break;
        }
    }
    if has_digits { Some(val) } else { None }
}

fn bytes_contains_chunked(bytes: &[u8]) -> bool {
    bytes.windows(7).any(|w| {
        w[0].to_ascii_lowercase() == b'c'
            && w[1].to_ascii_lowercase() == b'h'
            && w[2].to_ascii_lowercase() == b'u'
            && w[3].to_ascii_lowercase() == b'n'
            && w[4].to_ascii_lowercase() == b'k'
            && w[5].to_ascii_lowercase() == b'e'
            && w[6].to_ascii_lowercase() == b'd'
    })
}

fn bytes_eq_close(bytes: &[u8]) -> bool {
    let mut start = 0;
    while start < bytes.len() && (bytes[start] == b' ' || bytes[start] == b'\t') {
        start += 1;
    }
    let mut end = bytes.len();
    while end > start && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\t' || bytes[end - 1] == b'\r' || bytes[end - 1] == b'\n') {
        end -= 1;
    }
    let len = end - start;
    if len != 5 {
        return false;
    }
    let slice = &bytes[start..end];
    slice[0].to_ascii_lowercase() == b'c'
        && slice[1].to_ascii_lowercase() == b'l'
        && slice[2].to_ascii_lowercase() == b'o'
        && slice[3].to_ascii_lowercase() == b's'
        && slice[4].to_ascii_lowercase() == b'e'
}

fn classify_headers(headers: &[httparse::Header]) -> HeaderInfo {
    let mut content_length = None;
    let mut chunked = false;
    let mut connection_close = false;

    for h in headers {
        let name = h.name;
        if name.eq_ignore_ascii_case("content-length") {
            content_length = parse_usize(h.value);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = bytes_contains_chunked(h.value);
        } else if name.eq_ignore_ascii_case("connection") {
            connection_close = bytes_eq_close(h.value);
        }
    }

    let body_info = if let Some(cl) = content_length {
        BodyInfo::ContentLength(cl)
    } else if chunked {
        BodyInfo::Chunked
    } else if connection_close {
        BodyInfo::UntilClose
    } else {
        BodyInfo::UntilClose
    };

    HeaderInfo {
        body_info,
        connection_close,
    }
}

#[derive(Clone, Copy)]
enum ConnState {
    Connecting,
    Reconnecting,
    Writing,
    ReadingHeaders,
    ReadingBody {
        remaining: usize,
    },
    ReadingChunked {
        chunk_state: ChunkState,
        parsed_offset: usize,
    },
    ReadingUntilClose,
}

#[derive(Clone, Copy)]
enum ChunkState {
    Size,
    Data { remaining: usize },
    DataCRLF,
    Trailer,
}

struct Connection {
    token: Token,
    socket: TcpStream,
    state: ConnState,
    start_time: Option<Instant>,
    buf: Vec<u8>,
    read_len: usize,
    written_len: usize,
    close_connection: bool,
    addr: SocketAddr,
}

fn connect_socket(addr: SocketAddr) -> Result<TcpStream, std::io::Error> {
    let socket = TcpStream::connect(addr)?;
    socket.set_nodelay(true)?;
    Ok(socket)
}

fn reconnect_conn(
    conn: &mut Connection,
    poll: &Poll,
    _stats: &mut WorkerStats,
    _recording: &AtomicBool,
) {
    let _ = poll.registry().deregister(&mut conn.socket);

    match connect_socket(conn.addr) {
        Ok(mut socket) => {
            if let Ok(()) = poll.registry().register(
                &mut socket,
                conn.token,
                Interest::WRITABLE,
            ) {
                conn.socket = socket;
                conn.state = ConnState::Connecting;
            } else {
                conn.state = ConnState::Reconnecting;
            }
        }
        Err(_) => {
            conn.state = ConnState::Reconnecting;
        }
    }

    conn.read_len = 0;
    conn.written_len = 0;
    conn.close_connection = false;
    conn.start_time = None;
}

fn complete_request(
    conn: &mut Connection,
    poll: &Poll,
    stats: &mut WorkerStats,
    recording: &AtomicBool,
) {
    let is_rec = recording.load(Ordering::Relaxed);
    if is_rec {
        if let Some(start) = conn.start_time {
            let elapsed = start.elapsed().as_micros() as u64;
            let _ = stats.latency_hist.record(elapsed);
        }
    }

    if conn.close_connection {
        reconnect_conn(conn, poll, stats, recording);
    } else {
        conn.read_len = 0;
        conn.written_len = 0;
        conn.start_time = Some(Instant::now());
        conn.state = ConnState::Writing;
        let _ = poll.registry().reregister(
            &mut conn.socket,
            conn.token,
            Interest::WRITABLE,
        );
    }
}

fn parse_chunked_body(
    conn: &mut Connection,
    poll: &Poll,
    stats: &mut WorkerStats,
    recording: &AtomicBool,
) -> bool {
    if let ConnState::ReadingChunked { ref mut chunk_state, ref mut parsed_offset } = conn.state {
        let mut offset = *parsed_offset;
        let mut state = *chunk_state;

        loop {
            match state {
                ChunkState::Size => {
                    let slice = &conn.buf[offset..conn.read_len];
                    if let Some(pos) = slice.windows(2).position(|w| w == b"\r\n") {
                        let size_str = match std::str::from_utf8(&slice[..pos]) {
                            Ok(s) => s,
                            Err(_) => {
                                reconnect_conn(conn, poll, stats, recording);
                                return true;
                            }
                        };
                        let size_str = size_str.split(';').next().unwrap_or(size_str).trim();
                        let chunk_size = match usize::from_str_radix(size_str, 16) {
                            Ok(v) => v,
                            Err(_) => {
                                reconnect_conn(conn, poll, stats, recording);
                                return true;
                            }
                        };

                        offset += pos + 2;

                        if chunk_size == 0 {
                            state = ChunkState::Trailer;
                        } else {
                            state = ChunkState::Data { remaining: chunk_size };
                        }
                    } else {
                        break;
                    }
                }
                ChunkState::Data { remaining } => {
                    let available = conn.read_len - offset;
                    if available >= remaining {
                        offset += remaining;
                        state = ChunkState::DataCRLF;
                    } else {
                        offset += available;
                        state = ChunkState::Data { remaining: remaining - available };
                        break;
                    }
                }
                ChunkState::DataCRLF => {
                    let available = conn.read_len - offset;
                    if available >= 2 {
                        if &conn.buf[offset..offset + 2] == b"\r\n" {
                            offset += 2;
                            state = ChunkState::Size;
                        } else {
                            reconnect_conn(conn, poll, stats, recording);
                            return true;
                        }
                    } else {
                        break;
                    }
                }
                ChunkState::Trailer => {
                    let slice = &conn.buf[offset..conn.read_len];
                    if slice.starts_with(b"\r\n") {
                        offset += 2;
                        *chunk_state = state;
                        *parsed_offset = offset;
                        complete_request(conn, poll, stats, recording);
                        return true;
                    } else if let Some(pos) = slice.windows(2).position(|w| w == b"\r\n") {
                        offset += pos + 2;
                    } else {
                        break;
                    }
                }
            }
        }

        *chunk_state = state;
        *parsed_offset = offset;

        if offset > 0 {
            if offset >= conn.buf.len() - 1024 || offset > 8192 {
                let unparsed = conn.read_len - offset;
                conn.buf.copy_within(offset..conn.read_len, 0);
                conn.read_len = unparsed;
                *parsed_offset = 0;
            }
        }
    }
    false
}

fn handle_read(
    conn: &mut Connection,
    _request: &[u8],
    recording: &AtomicBool,
    stats: &mut WorkerStats,
    poll: &Poll,
) {
    loop {
        if conn.read_len >= conn.buf.len() {
            reconnect_conn(conn, poll, stats, recording);
            return;
        }

        match conn.socket.read(&mut conn.buf[conn.read_len..]) {
            Ok(0) => {
                match conn.state {
                    ConnState::ReadingUntilClose => {
                        complete_request(conn, poll, stats, recording);
                    }
                    _ => {
                        let is_rec = recording.load(Ordering::Relaxed);
                        if is_rec {
                            stats.total_requests += 1;
                            stats.status_codes[0] += 1;
                        }
                        reconnect_conn(conn, poll, stats, recording);
                    }
                }
                return;
            }
            Ok(n) => {
                conn.read_len += n;
                match conn.state {
                    ConnState::ReadingHeaders => {
                        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
                        let mut resp = httparse::Response::new(&mut headers);
                        match resp.parse(&conn.buf[..conn.read_len]) {
                            Ok(httparse::Status::Complete(header_len)) => {
                                let status = resp.code.unwrap_or(0);
                                let is_rec = recording.load(Ordering::Relaxed);
                                if is_rec {
                                    stats.total_requests += 1;
                                    if status >= 200 && status < 400 {
                                        stats.successful_requests += 1;
                                    }
                                    let idx = status as usize;
                                    if idx < STATUS_SLOTS {
                                        stats.status_codes[idx] += 1;
                                    }
                                    stats.total_bytes += conn.read_len as u64;
                                }

                                let info = classify_headers(&headers);
                                conn.close_connection = info.connection_close;

                                let body_in_buffer = conn.read_len - header_len;

                                match info.body_info {
                                    BodyInfo::ContentLength(cl) => {
                                        if body_in_buffer >= cl {
                                            complete_request(conn, poll, stats, recording);
                                            return;
                                        } else {
                                            conn.state = ConnState::ReadingBody {
                                                remaining: cl - body_in_buffer,
                                            };
                                            conn.read_len = 0;
                                        }
                                    }
                                    BodyInfo::Chunked => {
                                        conn.state = ConnState::ReadingChunked {
                                            chunk_state: ChunkState::Size,
                                            parsed_offset: header_len,
                                        };
                                        if parse_chunked_body(conn, poll, stats, recording) {
                                            return;
                                        }
                                    }
                                    BodyInfo::UntilClose => {
                                        conn.state = ConnState::ReadingUntilClose;
                                        conn.read_len = 0;
                                    }
                                }
                            }
                            Ok(httparse::Status::Partial) => {
                                break;
                            }
                            Err(_) => {
                                reconnect_conn(conn, poll, stats, recording);
                                return;
                            }
                        }
                    }
                    ConnState::ReadingBody { remaining } => {
                        let bytes_read = n;
                        let is_rec = recording.load(Ordering::Relaxed);
                        if is_rec {
                            stats.total_bytes += bytes_read as u64;
                        }

                        if bytes_read >= remaining {
                            complete_request(conn, poll, stats, recording);
                            return;
                        } else {
                            conn.state = ConnState::ReadingBody { remaining: remaining - bytes_read };
                            conn.read_len = 0;
                        }
                    }
                    ConnState::ReadingChunked { .. } => {
                        let is_rec = recording.load(Ordering::Relaxed);
                        if is_rec {
                            stats.total_bytes += n as u64;
                        }
                        if parse_chunked_body(conn, poll, stats, recording) {
                            return;
                        }
                        break;
                    }
                    ConnState::ReadingUntilClose => {
                        let is_rec = recording.load(Ordering::Relaxed);
                        if is_rec {
                            stats.total_bytes += n as u64;
                        }
                        conn.read_len = 0;
                    }
                    _ => {}
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                break;
            }
            Err(_) => {
                reconnect_conn(conn, poll, stats, recording);
                return;
            }
        }
    }
}

fn handle_write(
    conn: &mut Connection,
    request: &[u8],
    recording: &AtomicBool,
    stats: &mut WorkerStats,
    poll: &Poll,
) {
    loop {
        match conn.state {
            ConnState::Connecting => {
                match conn.socket.take_error() {
                    Ok(None) => {
                        conn.state = ConnState::Writing;
                        conn.start_time = Some(Instant::now());
                        conn.written_len = 0;
                    }
                    _ => {
                        let is_rec = recording.load(Ordering::Relaxed);
                        if is_rec {
                            stats.total_requests += 1;
                            stats.status_codes[0] += 1;
                        }
                        reconnect_conn(conn, poll, stats, recording);
                        return;
                    }
                }
            }
            ConnState::Writing => {
                let to_write = &request[conn.written_len..];
                match conn.socket.write(to_write) {
                    Ok(n) => {
                        conn.written_len += n;
                        if conn.written_len >= request.len() {
                            conn.state = ConnState::ReadingHeaders;
                            conn.read_len = 0;
                            let _ = poll.registry().reregister(
                                &mut conn.socket,
                                conn.token,
                                Interest::READABLE,
                            );
                            return;
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        return;
                    }
                    Err(_) => {
                        let is_rec = recording.load(Ordering::Relaxed);
                        if is_rec {
                            stats.total_requests += 1;
                            stats.status_codes[0] += 1;
                        }
                        reconnect_conn(conn, poll, stats, recording);
                        return;
                    }
                }
            }
            _ => {
                let _ = poll.registry().reregister(
                    &mut conn.socket,
                    conn.token,
                    Interest::READABLE,
                );
                return;
            }
        }
    }
}

fn worker_thread(
    thread_id: usize,
    num_conns: usize,
    addr: SocketAddr,
    request: Vec<u8>,
    running: Arc<AtomicBool>,
    recording: Arc<AtomicBool>,
) -> WorkerStats {
    let mut stats = WorkerStats::default();
    if num_conns == 0 {
        return stats;
    }

    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Thread {} failed to create Poll instance", thread_id);
            return stats;
        }
    };

    let mut events = Events::with_capacity(1024);
    let mut connections = Vec::with_capacity(num_conns);

    for i in 0..num_conns {
        let token = Token(i);
        let mut retry_count = 0;
        let socket = loop {
            match connect_socket(addr) {
                Ok(s) => break s,
                Err(e) => {
                    retry_count += 1;
                    if retry_count > 5 {
                        eprintln!(
                            "Thread {} connection {} failed to connect after 5 attempts: {}",
                            thread_id, i, e
                        );
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        };

        let mut socket = socket;
        let _ = poll.registry().register(
            &mut socket,
            token,
            Interest::WRITABLE,
        );

        connections.push(Connection {
            token,
            socket,
            state: ConnState::Connecting,
            start_time: None,
            buf: vec![0u8; RECVBUF],
            read_len: 0,
            written_len: 0,
            close_connection: false,
            addr,
        });
    }

    while running.load(Ordering::Relaxed) {
        for conn in &mut connections {
            if let ConnState::Reconnecting = conn.state {
                if let Ok(mut socket) = connect_socket(addr) {
                    if let Ok(()) = poll.registry().register(
                        &mut socket,
                        conn.token,
                        Interest::WRITABLE,
                    ) {
                        conn.socket = socket;
                        conn.state = ConnState::Connecting;
                    }
                }
            }
        }

        match poll.poll(&mut events, Some(Duration::from_millis(100))) {
            Ok(_) => {
                for event in events.iter() {
                    let token = event.token();
                    let conn_idx = token.0;
                    if conn_idx < connections.len() {
                        let mut conn = &mut connections[conn_idx];

                        if event.is_writable() {
                            handle_write(&mut conn, &request, &recording, &mut stats, &poll);
                        }
                        if event.is_readable() {
                            handle_read(&mut conn, &request, &recording, &mut stats, &poll);
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::Interrupted {
                    eprintln!("Thread {} poll error: {}", thread_id, e);
                }
            }
        }
    }

    for conn in &mut connections {
        let _ = poll.registry().deregister(&mut conn.socket);
    }

    stats
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let url = parse_url(&args.url)?;
    let addr = resolve_addr(&url.host, url.port)?;
    eprintln!("Resolved {} -> {}", url.host, addr);

    let request = build_request(&url.path, &url.authority);

    let recording = Arc::new(AtomicBool::new(false));
    let running = Arc::new(AtomicBool::new(true));

    let num_threads = if args.threads > 0 {
        args.threads
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    };

    let conns_per_thread = args.connections / num_threads;
    let extra_conns = args.connections % num_threads;

    let mut thread_handles = Vec::with_capacity(num_threads);

    for thread_id in 0..num_threads {
        let recording = recording.clone();
        let running = running.clone();
        let request = request.clone();
        let num_conns = conns_per_thread + if thread_id < extra_conns { 1 } else { 0 };

        thread_handles.push(std::thread::spawn(move || {
            worker_thread(thread_id, num_conns, addr, request, running, recording)
        }));
    }

    let running_ctrlc = running.clone();
    ctrlc::set_handler(move || {
        running_ctrlc.store(false, Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    // Phase 1: Warmup
    println!("Warming up for {} seconds...", args.warmup);
    let warmup_start = Instant::now();
    let warmup_duration = Duration::from_secs(args.warmup);
    while Instant::now().duration_since(warmup_start) < warmup_duration {
        if !running.load(Ordering::Relaxed) {
            println!("Interrupted during warmup.");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Phase 2: Execution
    println!("Running benchmark for {} seconds...", args.duration);
    recording.store(true, Ordering::Relaxed);
    let exec_start = Instant::now();
    let exec_duration = Duration::from_secs(args.duration);
    while Instant::now().duration_since(exec_start) < exec_duration {
        if !running.load(Ordering::Relaxed) {
            println!("Interrupted during benchmark.");
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    running.store(false, Ordering::Relaxed);
    recording.store(false, Ordering::Relaxed);

    let actual_duration = exec_start.elapsed();

    let mut total_stats = WorkerStats::default();
    for handle in thread_handles {
        if let Ok(stats) = handle.join() {
            total_stats.merge(stats);
        }
    }

    // Summary
    println!("\n=== Benchmark Summary ===");
    println!("Target URL:          {}", args.url);
    println!("Concurrency Level:   {}", args.connections);
    println!("Time Taken:          {:.2}s", actual_duration.as_secs_f64());
    println!("Total Requests:      {}", total_stats.total_requests);
    println!("Successful Reqs:     {}", total_stats.successful_requests);
    println!(
        "Failed Requests:     {}",
        total_stats.total_requests - total_stats.successful_requests
    );
    println!(
        "Data Transferred:    {:.2} MB",
        total_stats.total_bytes as f64 / 1_048_576.0
    );
    println!(
        "Requests/sec:        {:.2}",
        total_stats.total_requests as f64 / actual_duration.as_secs_f64()
    );

    println!("\nStatus Codes:");
    for (code, &count) in total_stats.status_codes.iter().enumerate() {
        if count > 0 {
            if code == 0 {
                println!("  [Network Error]:     {}", count);
            } else {
                println!("  [HTTP {}]:           {}", code, count);
            }
        }
    }

    if total_stats.total_requests > 0 {
        let hist = total_stats.latency_hist;
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
