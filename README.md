# reliability-toolkit

[![CI](https://github.com/mizcausevic-dev/reliability-toolkit-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/mizcausevic-dev/reliability-toolkit-rs/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Async reliability primitives for Tokio-based Rust services**: token-bucket rate limiter, 3-state circuit breaker, exponential backoff with full jitter, and a semaphore-backed bulkhead. Small, composable, no surprises.

```rust
use std::time::Duration;
use reliability_toolkit::{RateLimiter, CircuitBreaker, Retry, Bulkhead, RetryConfig};

let limiter = RateLimiter::new(100.0, 100);           // 100 rps, burst 100
let breaker = CircuitBreaker::builder()
    .failure_threshold(5)
    .cool_down(Duration::from_secs(10))
    .build();
let retry: Retry<std::io::Error> = Retry::new(RetryConfig::default());
let pool = Bulkhead::new(20);

let result = retry.run(|| async {
    limiter.acquire().await;
    let _permit = pool.acquire().await?;
    breaker
        .call(async {
            // ... your downstream call here
            Ok::<_, std::io::Error>("ok")
        })
        .await
        .map_err(|open| std::io::Error::other(open.to_string()))
        .and_then(|inner| inner)
}).await;
```

---

## Why another reliability crate

Most of the ecosystem ships these primitives as separate crates with subtly incompatible APIs. This crate's design rules:

1. **One async runtime assumption** — Tokio. Everything is `async fn`. No "executor-agnostic" weasel words that mean "doesn't compose with anything you'd actually use."
2. **No `rand` dep.** Jitter uses an inline xorshift PRNG. That's one less surface for advisory updates.
3. **Cheap clones.** Every primitive is `Arc`-shaped under the hood, so you can hand it across tasks without lifetime gymnastics.
4. **Compose by stacking calls, not by trait gymnastics.** No middleware traits, no `tower::Service` opt-in — those are great, but they push complexity into the wrong place when all you want is "retry around this expensive thing."

---

## Primitives

### `RateLimiter` — token bucket

```rust
let limiter = reliability_toolkit::RateLimiter::new(rps, burst);
limiter.acquire().await;        // blocks until a token is free
limiter.acquire_n(5).await;     // five at once
limiter.try_acquire().await;    // returns bool — no waiting
```

- O(1) per call (`Mutex` over a small `State`)
- Burst is enforced as a hard ceiling
- Refill is computed lazily on each call — no background timer

### `CircuitBreaker` — Closed → Open → HalfOpen

```rust
let cb = reliability_toolkit::CircuitBreaker::builder()
    .failure_threshold(5)              // 5 consecutive failures trips it
    .cool_down(Duration::from_secs(30)) // then stays open for 30s
    .half_open_max_calls(1)             // admit one trial call before reclosing
    .build();

match cb.call(some_fallible_future).await {
    Ok(Ok(value)) => { /* call ran and succeeded */ }
    Ok(Err(err))  => { /* call ran and returned an error; breaker counted it */ }
    Err(reliability_toolkit::ToolkitError::CircuitOpen { retry_after }) => {
        // call was rejected without being invoked
    }
    _ => {}
}
```

- Failure counting is on **consecutive** failures inside `Closed`; a single success resets it
- `HalfOpen` admits up to `half_open_max_calls` and waits for all of them to succeed before reclosing — one failure flips back to `Open`
- `trip()` and `reset()` are exposed for kill switches

### `Retry` — exponential backoff with full jitter

```rust
let retry: reliability_toolkit::Retry<std::io::Error> =
    reliability_toolkit::Retry::new(reliability_toolkit::RetryConfig {
        max_attempts: 4,
        base_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(5),
        retry_if: Some(std::sync::Arc::new(|e: &std::io::Error| {
            // Don't retry on PermissionDenied; treat it as fatal.
            e.kind() != std::io::ErrorKind::PermissionDenied
        })),
    });

let result = retry.run(|| async { some_call().await }).await;
```

- Backoff is `min(max_delay, base * 2^(attempt - 1))` with [full jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) applied
- `retry_if` is a `&E -> bool` predicate; absent means "retry all errors"
- The closure is `FnMut() -> Future` (not a single future), so the next attempt gets a fresh future

### `Bulkhead` — concurrency cap

```rust
let pool = reliability_toolkit::Bulkhead::new(20);

let _permit = pool.acquire().await?;
// ... up to 20 of these can be in flight; further callers wait
```

- Backed by `tokio::sync::Semaphore`
- `try_acquire()` for non-blocking attempts
- `close()` to drain on shutdown

---

## Composition

The layering you usually want is **rate-limit → bulkhead → circuit-breaker → retry** (with `retry` outermost), so a transient failure inside `breaker.call()` doesn't bypass the budget the rate limiter is enforcing. See [`tests/composition.rs`](tests/composition.rs) and [`examples/compose.rs`](examples/compose.rs).

Run the example:

```bash
cargo run --example compose
```

---

## Benchmarks

```bash
cargo bench
```

Single-threaded hot-bucket throughput on an M-class workstation typically runs in the tens of millions of ops/sec for `try_acquire()`. The numbers exist mostly to catch regressions in the token math — the goal is "negligible vs. the call you're wrapping," and it is.

---

## Tests

```bash
cargo test            # unit + integration
cargo test --doc      # doctest
cargo clippy --all-targets -- -Dwarnings
cargo fmt --all -- --check
```

CI runs the matrix `stable`, `beta`, and `1.75.0` (MSRV).

---

## Related work in this ecosystem

This is part of the [**Platform Reliability Stack**](https://github.com/mizcausevic-dev) — small, focused libraries that compose into a production reliability story:

- **[slo-budget-tracker](https://github.com/mizcausevic-dev/slo-budget-tracker)** — Python SLO + error-budget library + Prometheus exporter.
- **[procurement-decision-api](https://github.com/mizcausevic-dev/procurement-decision-api)** — drafts AI Procurement Decision Cards from vendor Suite documents.
- More at [kineticgain.com](https://kineticgain.com/).

---

## License

MIT. See [LICENSE](LICENSE).
