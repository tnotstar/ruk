# AGENTS.md — Ruk Project

## Overview

**Ruk** is a high-performance HTTP/1.1 benchmarking tool written in Rust. Inspired by `wrk`, it spawns N concurrent workers, each maintaining a persistent raw TCP connection, and aggregates latency/throughput statistics via a channel-based pipeline. It prioritizes maximum performance by eschewing high-level HTTP clients in favor of low-level `httparse` buffering directly over `tokio::net::TcpStream`.

## Commands

```bash
cargo build                    # Debug build
cargo build --release          # Optimized release build (LTO, single codegen unit, stripped)
cargo run -- <url>             # Run with defaults (50 connections, 10s duration, 2s warmup)
cargo run -- --help            # CLI usage
cargo test                     # No tests exist yet
```

### Example invocation

```bash
cargo run --release -- http://127.0.0.1:8080/ -c 100 -t 1 -d 30 -w 5
```

## Architecture

Everything lives in **`src/main.rs`** — a single-file binary. No library crate, no modules, no tests yet.

### Control flow

```text
main() → parses CLI args (clap derive)
  └→ builds Tokio runtime (current_thread or multi_thread based on `-t` flag)
      └→ async_main()
           ├→ parses target URL and resolves host to SocketAddr (prefers IPv4)
           ├→ spawns aggregator_task (reads WorkerStats from flume channel)
           ├→ spawns N worker tasks (each owns one persistent raw TCP stream)
           ├→ Phase 1: warmup (recording=false, workers run but don't collect stats)
           ├→ Phase 2: benchmark (recording=true, workers collect stats)
           ├→ sets running=false → workers exit loops, each sends final WorkerStats
           ├→ drop(tx) → aggregator channel closes
           └→ aggregator returns merged stats → prints summary
```

### Data flow

- Workers accumulate `WorkerStats` locally during the recording phase to avoid channel contention.
- Request/Response parsing is handled manually. The worker writes a pre-formatted `GET` string to the `TcpStream` and reads back the response using `httparse`. 
- Responses are consumed according to `Content-Length`, `Transfer-Encoding: chunked`, or `Connection: close` rules.
- Each worker sends its `WorkerStats` **once** at the end of its loop via a `flume::unbounded` channel.
- The aggregator task merges histograms and counters into a single global `WorkerStats`.
- `hdrhistogram::Histogram<u64>` stores latencies in **microseconds** (1–600,000,000 μs range, 3 significant figures).

### Key synchronization

- `running: Arc<AtomicBool>` — signals all workers to stop the work loops.
- `recording: Arc<AtomicBool>` — gates stat collection (warmup vs benchmark).
- `flume::unbounded` channel — workers → aggregator (single producer per worker, single consumer).

## Non-obvious patterns and gotchas

- **Uses Raw `TcpStream` and `httparse`**: Instead of relying on `hyper`, `ruk` manages the TCP connection manually. It implements its own buffering and state machine for chunked encoding and content-length reading. This bypasses the overhead of standard HTTP clients locking connections or spawning background driver tasks.
- **`status_codes` as a fixed array**: To minimize allocation overhead in the Hot Path, `WorkerStats` tracks status codes using a `[u64; 600]` array rather than a `HashMap`. 
- **Thread scaling (`-t` flag)**: If `--threads 0` is set, Tokio builds a `multi_thread` runtime using all cores. If `--threads 1` or higher is set, it specifically scales the `worker_threads()`. This helps avoid cross-thread context switching when targeting single-core performance optimizations (emulating `wrk -t 1`).
- **IPv4 preference during DNS resolution** (`resolve_addr`). This avoids issues with servers (e.g., NGINX) listening only on `127.0.0.1` while DNS resolves `localhost` to `::1`.
- **Automatic Reconnection & Retry**: To handle server-side keep-alive limits (like NGINX's `keepalive_requests`), workers distinguish between protocol errors and connection closures. On a closed connection, the worker silently reconnects and retries the request once before incrementing the error counter. This ensures clean statistics and prevents expected connection recycling from appearing as network failures.

- **Global allocator is `mimalloc`** — not the system allocator. Defined via `#[global_allocator]` at module scope to handle the high frequency of futures allocation efficiently.

## Dependencies and their roles

| Crate | Role |
|---|---|
| `tokio` | Async runtime (multi-thread/current_thread), TCP Streams, timers |
| `httparse` | Zero-copy, high-performance HTTP/1.x parser for decoding headers |
| `flume` | MPSC channel between workers and aggregator |
| `clap` (derive) | CLI argument parsing |
| `hdrhistogram` | High-fidelity latency histogram with configurable precision |
| `mimalloc` | Alternative global allocator for performance |

*(Note: `hyper`, `hyper-util`, and `rustls` were removed during optimizations to achieve maximum throughput directly over TCP).*

## Release profile

The `Cargo.toml` `[profile.release]` is tuned for maximum performance:
- `opt-level = 3`, `codegen-units = 1`, `lto = true`, `panic = "abort"`, `strip = true`

## Current limitations

- **No test suite.**
- **No HTTPS support**: Because the stack was moved directly to raw TCP streams to boost performance, HTTPS capability has been lost.
- **Single file** — no module separation yet.
- **No request method customization** (GET only).
- **No request body support.**
