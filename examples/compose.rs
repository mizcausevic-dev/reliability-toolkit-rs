//! Runnable example showing the four primitives composed around a fake call.
//!
//!     cargo run --example compose

use std::time::Duration;

use reliability_toolkit::{Bulkhead, CircuitBreaker, RateLimiter, Retry, RetryConfig};

async fn call_downstream(attempt: u32) -> Result<&'static str, std::io::Error> {
    if attempt < 2 {
        Err(std::io::Error::other("downstream flaked"))
    } else {
        Ok("downstream OK")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limiter = RateLimiter::new(50.0, 50);
    let breaker = CircuitBreaker::builder()
        .failure_threshold(5)
        .cool_down(Duration::from_secs(10))
        .build();
    let pool = Bulkhead::new(8);
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig {
        max_attempts: 4,
        base_delay: Duration::from_millis(50),
        max_delay: Duration::from_secs(2),
        retry_if: None,
    });

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

    let result: Result<&'static str, std::io::Error> = retry
        .run(|| {
            let limiter = limiter.clone();
            let breaker = breaker.clone();
            let pool = pool.clone();
            let attempts = attempts.clone();
            async move {
                limiter.acquire().await;
                let _permit = pool.acquire().await.map_err(std::io::Error::other)?;
                let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                match breaker.call(call_downstream(n)).await {
                    Ok(inner) => inner,
                    Err(open) => Err(std::io::Error::other(open.to_string())),
                }
            }
        })
        .await;

    println!("final result: {result:?}");
    Ok(())
}
