<p align="center">
  <img src="assets/logo.svg" alt="rate-li logo" width="600" style="background:#1a1a2e; border-radius:12px; padding:16px"/>
</p>

# rate-li

A production-grade **distributed rate limiting core engine** written in Rust. Built with a clean trait-based architecture, it supports three battle-tested algorithms, two storage backends, and a full resilience layer — designed to slot into any Rust service as a library.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Algorithms](#algorithms)
- [Storage Backends](#storage-backends)
- [Resilience Layer](#resilience-layer)
- [Configuration](#configuration)
- [Usage](#usage)
- [CLI Tool](#cli-tool)
- [Benchmarking](#benchmarking)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)

---

## Overview

`rate-li` is a Rust library crate (`rate_limiter`) that provides async, pluggable rate limiting. The core design is built around two traits:

- **`RateLimitAlgorithm`** — defines the limiting logic (token bucket, sliding window, fixed window)
- **`StorageBackend`** — defines where state lives (in-memory or Redis)

The top-level `RateLimiter<A, S>` struct wires these together and exposes three primary operations: `is_allowed`, `record_request`, and `check_and_record`.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│              RateLimiter<A, S>               │
│   check_and_record / is_allowed / reset      │
└──────────────────┬───────────────────────────┘
                   │
       ┌───────────┴────────────┐
       ▼                        ▼
┌─────────────────┐    ┌────────────────────┐
│  RateLimitAlgorithm  │    │  StorageBackend    │
│  (trait)        │    │  (trait)           │
├─────────────────┤    ├────────────────────┤
│ TokenBucket     │    │ MemoryStorage      │
│ FixedWindow     │    │ RedisStorage       │
│ SlidingWindow   │    │ ResilientStorage   │
└─────────────────┘    └────────────────────┘
                                │
                       ┌────────┴────────────┐
                       ▼                     ▼
              ┌──────────────┐    ┌──────────────────┐
              │ CircuitBreaker│    │  HealthChecker   │
              └──────────────┘    └──────────────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ ExponentialBackoff│
              └──────────────────┘
```

All algorithms and storage backends are generic over each other — any algorithm can be paired with any storage backend at compile time.

---

## Algorithms

All three implement the `RateLimitAlgorithm` trait and return a `RateLimitStatus` on every check:

```rust
pub struct RateLimitStatus {
    pub allowed: bool,
    pub remaining: u64,
    pub limit: u64,
    pub reset_after: Duration,
    pub details: Option<String>,
}
```

### Token Bucket

Maintains a bucket of tokens replenished at a constant rate. Each request consumes one token; requests are rejected when the bucket is empty. Handles burst traffic naturally — if the service is idle, tokens accumulate up to `capacity`, allowing a burst when traffic resumes.

**Config:**

```rust
TokenBucketConfig {
    capacity: 100,               // max tokens in bucket
    refill_rate: 10.0,           // tokens added per second
    initial_tokens: None,        // defaults to capacity
}
```

**State keys per client key:**
- `{prefix}:{key}:tokens` — current token count (big-endian u64)
- `{prefix}:{key}:last_refill` — timestamp of last refill (millis since epoch)

Both are written atomically via a storage pipeline on each request.

---

### Fixed Window

Divides time into discrete windows (e.g., every 60 seconds) and rejects requests once `max_requests` is hit within the current window. Counter resets at the start of each new window.

**Config:**

```rust
FixedWindowConfig {
    max_requests: 1000,
    window: Duration::from_secs(60),
}
```

**State keys per client key:**
- `{prefix}:{key}:window:{window_start_ts}` — counter for the current window, with TTL set to `window + 60s`

Simple and cheap, but has an edge case: a client can send `max_requests` at the end of one window and `max_requests` again at the start of the next, briefly doubling throughput.

---

### Sliding Window

Approximates a continuous sliding window using sub-buckets (controlled by `precision`). Eliminates the boundary burst problem of fixed windows while keeping storage costs bounded.

**Config:**

```rust
SlidingWindowConfig {
    max_requests: 1000,
    window: Duration::from_secs(60),
    precision: 10,    // number of sub-buckets; higher = smoother limiting
}
```

**How it works:**
- The window is divided into `precision` equally-sized buckets
- `bucket_size = window / precision` (minimum 1 second)
- On each request, all buckets within the current window are summed
- Only the current bucket is incremented
- Expired buckets are evicted by TTL

**State keys per client key:**
- `{prefix}:{key}:bucket:{bucket_index}` — counter per time bucket

Uses a pipeline to fetch all relevant bucket counts in a single round-trip.

---

## Storage Backends

All backends implement `StorageBackend`, including a `pipeline()` / `execute_pipeline()` API for batching operations.

### MemoryStorage

An in-memory `HashMap` backed by `Arc<RwLock<...>>`. Suitable for single-instance deployments, testing, and local development. Supports optional background TTL cleanup via a Tokio task.

```rust
InMemoryConfig {
    max_entries: 10_000,
    use_background_task: true,
    cleanup_interval: Duration::from_secs(60),
}
```

### RedisStorage

Async Redis backend using `redis-rs` with `ConnectionManager` for automatic reconnection. Uses `SETEX`, `INCRBY`, `EXPIRE`, and pipelined commands. Connection timeout is enforced via `tokio::time::timeout`.

```rust
RedisConfig {
    url: "redis://127.0.0.1:6379".to_string(),
    pool_size: 10,
    connection_timeout: Duration::from_secs(2),
}
```

### ResilientStorage

Wraps `RedisStorage` with automatic fallback to `MemoryStorage`. Composes all three resilience components (circuit breaker, health checker, exponential backoff) into a single `StorageBackend` implementation. Redis is tried first on every operation; on failure, the fallback is used transparently.

---

## Resilience Layer

Three components in `src/resilience/`:

### Circuit Breaker

Three-state FSM: `Closed → Open → HalfOpen → Closed`.

| State | Behavior |
|---|---|
| `Closed` | Requests flow normally; failure count tracked |
| `Open` | Requests immediately rejected; entered after `failure_threshold` consecutive failures |
| `HalfOpen` | Limited requests allowed through after `reset_timeout` elapses; closes again after `success_threshold` consecutive successes |

```rust
CircuitBreakerConfig {
    failure_threshold: 5,
    reset_timeout: Duration::from_secs(30),
    success_threshold: 3,
}
```

Uses `AtomicUsize` for lock-free failure/success counting and `tokio::sync::RwLock` for state transitions.

### Health Checker

Background Tokio task that PINGs Redis on a configurable interval. Updates an `AtomicBool` (`is_healthy`) visible to `ResilientStorage` for zero-lock-cost reads on the hot path.

```rust
HealthCheckConfig {
    check_interval: Duration::from_secs(5),
    check_timeout: Duration::from_secs(1),
}
```

### Exponential Backoff

Iterator-style retry scheduler. On each call to `next_backoff()`, returns the next sleep duration or `None` when `max_attempts` is exhausted. Supports jitter (random 50–100% of calculated backoff) to prevent thundering herd.

```rust
RetryConfig {
    max_attempts: 3,
    initial_backoff: Duration::from_millis(100),
    max_backoff: Duration::from_secs(5),
    backoff_multiplier: 2.0,
    use_jitter: true,
}
```

---

## Configuration

All config structs implement `serde::Serialize` / `Deserialize` with sensible defaults. Durations serialize as milliseconds (u64).

Full resilience config:

```rust
ResilienceConfig {
    circuit_breaker: CircuitBreakerConfig::default(),
    retry: RetryConfig::default(),
    health_check: HealthCheckConfig::default(),
    memory_config: InMemoryConfig::default(),
}
```

---

## Usage

### With in-memory storage (no Redis required)

```rust
use rate_limiter::{
    RateLimiter,
    algorithms::FixedWindow,
    config::{FixedWindowConfig, InMemoryConfig},
    storage::MemoryStorage,
};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let storage = MemoryStorage::new(InMemoryConfig::default());
    let algorithm = FixedWindow::new(storage, FixedWindowConfig {
        max_requests: 100,
        window: Duration::from_secs(60),
    });

    let limiter = RateLimiter::new(algorithm, MemoryStorage::new(InMemoryConfig::default()), "myapp".into());

    let status = limiter.check_and_record("user:42").await.unwrap();
    if status.allowed {
        println!("Request allowed. {} remaining.", status.remaining);
    } else {
        println!("Rate limited. Resets in {:?}.", status.reset_after);
    }
}
```

### With Redis

```rust
use rate_limiter::{
    RateLimiter,
    algorithms::TokenBucket,
    config::{TokenBucketConfig, RedisConfig},
    storage::{RedisStorage, StorageBackend},
};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let storage = RedisStorage::new(RedisConfig {
        url: "redis://127.0.0.1:6379".into(),
        pool_size: 10,
        connection_timeout: Duration::from_secs(2),
    }).await.unwrap();

    let algorithm = TokenBucket::new(storage.clone(), TokenBucketConfig {
        capacity: 500,
        refill_rate: 50.0,
        initial_tokens: None,
    });

    let limiter = RateLimiter::new(algorithm, storage, "api".into());
    let status = limiter.check_and_record("tenant:abc").await.unwrap();
    println!("Allowed: {}", status.allowed);
}
```

### With full resilience (Redis + memory fallback)

```rust
use rate_limiter::{
    RateLimiter,
    algorithms::SlidingWindow,
    config::{SlidingWindowConfig, RedisConfig},
    resilience::resilient_storage::{ResilientStorage, ResilienceConfig},
};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let storage = ResilientStorage::new(
        RedisConfig { url: "redis://127.0.0.1:6379".into(), ..Default::default() },
        ResilienceConfig::default(),
    ).await.unwrap();

    let algorithm = SlidingWindow::new(storage.clone(), SlidingWindowConfig {
        max_requests: 1000,
        window: Duration::from_secs(60),
        precision: 10,
    });

    let limiter = RateLimiter::new(algorithm, storage, "prod".into());
    let status = limiter.check_and_record("user:xyz").await.unwrap();
    println!("Allowed: {}, Remaining: {}", status.allowed, status.remaining);
}
```

---

## CLI Tool

A `structopt`-based CLI for manual testing and simulation. Runs against in-memory storage only.

```bash
cd rate_limiter
cargo run --bin rate_limiter_cli -- --help
```

**Key flags:**

| Flag | Description | Default |
|---|---|---|
| `-a, --algorithm` | `fixed_window`, `sliding_window`, `token_bucket` | `fixed_window` |
| `-k, --key` | Client key to rate limit | `default_user` |
| `-m, --max-requests` | Request limit | `10` |
| `-w, --window-seconds` | Window size in seconds | `60` |
| `-p, --precision` | Buckets for sliding window | `6` |
| `--refill-rate` | Tokens/sec for token bucket | `0.0` |
| `--simulation` | `burst`, `steady`, `sine_wave`, `custom` | `burst` |
| `-n, --num-requests` | Total requests to simulate | `20` |
| `-t, --request-interval-ms` | Ms between requests | `100` |
| `-v` | Increase verbosity (repeat for more) | — |

**Examples:**

```bash
# Simulate 30 burst requests with token bucket, capacity 15, refill at 2/sec
cargo run --bin rate_limiter_cli -- \
  -a token_bucket --max-requests 15 --refill-rate 2.0 \
  --simulation burst -n 30

# Steady traffic against sliding window with high precision
cargo run --bin rate_limiter_cli -- \
  -a sliding_window -m 20 -w 30 -p 10 \
  --simulation steady -n 40 -t 200
```

---

## Benchmarking

A concurrent benchmarking binary that measures throughput, latency, and denial rates across algorithms and storage backends.

```bash
cargo run --bin rate_limiter_bench -- --help
```

**Key flags:**

| Flag | Description | Default |
|---|---|---|
| `-a, --algorithm` | `fixed_window`, `sliding_window`, `token_bucket`, `all` | `all` |
| `-s, --storage` | `memory`, `redis` | `memory` |
| `--redis-url` | Redis connection URL | `redis://localhost:6379` |
| `-u, --num-users` | Concurrent users | `10` |
| `-r, --requests-per-user` | Requests per user | `100` |
| `-c, --concurrency` | Max concurrency level | `100` |
| `-i, --iterations` | Benchmark iterations | `3` |

**Examples:**

```bash
# Benchmark all algorithms in memory, 50 users × 200 requests
cargo run --bin rate_limiter_bench -- \
  -a all -s memory -u 50 -r 200 -c 200

# Benchmark token bucket against Redis
cargo run --bin rate_limiter_bench -- \
  -a token_bucket -s redis --redis-url redis://localhost:6379 \
  -u 20 -r 100
```

Benchmark results and a summary report format are documented in `src/scripts/README.md`.

---

## Running Tests

```bash
cd rate_limiter

# All tests
cargo test

# Specific module
cargo test algorithms
cargo test storage
cargo test resilience

# With logs
RUST_LOG=debug cargo test -- --nocapture
```

Redis-dependent tests require a local Redis instance:

```bash
docker run -d -p 6379:6379 redis:alpine
cargo test storage::redis
```

Test coverage spans: all three algorithms (unit + integration), `MemoryStorage`, `RedisStorage`, `CircuitBreaker`, `ExponentialBackoff`, `HealthChecker`, `ResilientStorage`, and end-to-end `RateLimiter` coordination tests.

---

## Project Structure

```
rate_limiter/
├── Cargo.toml
└── src/
    ├── lib.rs                      # Public API surface; RateLimiter<A, S>
    ├── main.rs                     # Minimal startup entrypoint
    ├── error.rs                    # RateLimiterError, StorageError, Result<T>
    ├── logging.rs                  # tracing setup; rate_limit_event! macro
    ├── config/
    │   └── mod.rs                  # All config structs (TokenBucket, FixedWindow, etc.)
    ├── algorithms/
    │   ├── mod.rs                  # RateLimitAlgorithm trait, RateLimitStatus
    │   ├── token_bucket.rs
    │   ├── fixed_window.rs
    │   ├── sliding_window.rs
    │   └── tests/                  # Per-algorithm unit tests
    ├── storage/
    │   ├── mod.rs                  # StorageBackend + StoragePipeline traits
    │   ├── memory.rs               # MemoryStorage (Arc<RwLock<HashMap>>)
    │   ├── redis.rs                # RedisStorage (ConnectionManager)
    │   └── tests/
    ├── resilience/
    │   ├── mod.rs
    │   ├── circuit_breaker.rs      # 3-state FSM (Closed/Open/HalfOpen)
    │   ├── exponential_backoff.rs  # Jittered retry scheduler
    │   ├── health_checker.rs       # Background Redis PING task
    │   ├── resilient_storage.rs    # ResilientStorage (Redis + memory fallback)
    │   └── tests/
    ├── tests/
    │   └── rate_limiter_tests.rs   # Integration tests for RateLimiter<A, S>
    ├── test_utils.rs               # MockStorage, helpers for tests
    └── bin/
        ├── rate_limiter_cli.rs     # Interactive CLI (simulation modes)
        └── rate_limiter_bench.rs   # Concurrent throughput benchmarker
```

---

## Dependencies

| Crate | Purpose |
|---|---|
| `tokio` | Async runtime |
| `async-trait` | Async methods in traits |
| `redis` | Redis client with `ConnectionManager` |
| `serde` / `serde_json` | Config serialization |
| `thiserror` | Typed error definitions |
| `tracing` / `tracing-subscriber` | Structured logging |
| `structopt` | CLI argument parsing |
| `rand` | Jitter in exponential backoff |
| `uuid` | Key generation utilities |

---

## License

MIT
