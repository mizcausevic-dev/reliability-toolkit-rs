//! Throughput micro-benchmark for [`RateLimiter`].
//!
//! Single-threaded happy path: bucket is always primed, so every call hits the
//! fast path (lock + refill + decrement). Helps catch regressions in the inner
//! token math. Bench it with `cargo bench`.

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use reliability_toolkit::RateLimiter;
use tokio::runtime::Builder;

fn bench_try_acquire(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let limiter = RateLimiter::new(1_000_000.0, 1_000_000);

    c.bench_function("rate_limiter_try_acquire_hot_bucket", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = limiter.try_acquire().await;
        });
    });
}

fn bench_acquire_n(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let limiter = RateLimiter::new(1_000_000.0, 1_000_000);

    c.bench_function("rate_limiter_acquire_n_5", |b| {
        b.to_async(&rt).iter(|| async {
            limiter.acquire_n(5).await;
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(50).measurement_time(Duration::from_secs(3));
    targets = bench_try_acquire, bench_acquire_n
}
criterion_main!(benches);
