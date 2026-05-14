//! End-to-end test showing the four primitives layered together.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reliability_toolkit::{Bulkhead, CircuitBreaker, RateLimiter, Retry, RetryConfig};
use tokio::time::pause;

#[tokio::test]
async fn retry_circuit_bulkhead_rate_limit_compose() {
    pause();
    let limiter = RateLimiter::new(100.0, 100);
    let breaker = CircuitBreaker::builder()
        .failure_threshold(5)
        .cool_down(Duration::from_secs(10))
        .build();
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig {
        max_attempts: 3,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
        retry_if: None,
    });
    let pool = Bulkhead::new(10);
    let calls = Arc::new(AtomicU32::new(0));

    let result: Result<u32, std::io::Error> = retry
        .run(|| {
            let limiter = limiter.clone();
            let breaker = breaker.clone();
            let pool = pool.clone();
            let calls = calls.clone();
            async move {
                limiter.acquire().await;
                let _permit = pool.acquire().await.expect("bulkhead open");
                let inner: Result<Result<u32, std::io::Error>, _> = breaker
                    .call(async {
                        let n = calls.fetch_add(1, Ordering::SeqCst);
                        if n < 1 {
                            Err(std::io::Error::other("transient"))
                        } else {
                            Ok(123_u32)
                        }
                    })
                    .await;
                match inner {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(e),
                    Err(_open) => Err(std::io::Error::other("circuit open")),
                }
            }
        })
        .await;

    assert!(matches!(result, Ok(123)));
    // First call failed, second succeeded.
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
