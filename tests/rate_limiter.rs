use std::time::Duration;

use reliability_toolkit::RateLimiter;
use tokio::time::{advance, pause};

#[tokio::test]
async fn fresh_limiter_allows_burst_immediately() {
    pause();
    let limiter = RateLimiter::new(10.0, 5);
    for _ in 0..5 {
        assert!(limiter.try_acquire().await);
    }
    assert!(!limiter.try_acquire().await);
}

#[tokio::test]
async fn refills_at_configured_rate() {
    pause();
    let limiter = RateLimiter::new(10.0, 5);
    for _ in 0..5 {
        assert!(limiter.try_acquire().await);
    }
    assert!(!limiter.try_acquire().await);

    // Advance one second; we should regain a full burst (capped at 5).
    advance(Duration::from_secs(1)).await;
    for _ in 0..5 {
        assert!(limiter.try_acquire().await);
    }
}

#[tokio::test]
async fn acquire_blocks_until_token_available() {
    pause();
    let limiter = RateLimiter::new(2.0, 1); // 2 rps, burst 1
    assert!(limiter.try_acquire().await);

    // Bucket is empty; acquire() should suspend.
    let limiter_clone = limiter.clone();
    let task = tokio::spawn(async move { limiter_clone.acquire().await });

    // Move the virtual clock forward 600ms — long enough to refill one token
    // at 2 rps (refill cadence: 500ms).
    advance(Duration::from_millis(600)).await;
    // Give the task a chance to wake up.
    tokio::task::yield_now().await;
    task.await.expect("acquire task should complete");
}

#[tokio::test]
async fn acquire_n_consumes_multiple_tokens() {
    pause();
    let limiter = RateLimiter::new(10.0, 5);
    assert!(limiter.try_acquire_n(3).await);
    let tokens = limiter.tokens().await;
    assert!((tokens - 2.0).abs() < 0.0001);
}

#[tokio::test]
#[should_panic(expected = "rate_per_second must be positive")]
async fn rejects_non_positive_rate() {
    let _ = RateLimiter::new(0.0, 5);
}

#[tokio::test]
#[should_panic(expected = "burst must be non-zero")]
async fn rejects_zero_burst() {
    let _ = RateLimiter::new(10.0, 0);
}

#[tokio::test]
#[should_panic(expected = "requested 10 tokens but burst is 5")]
async fn acquire_n_rejects_more_than_burst() {
    let limiter = RateLimiter::new(10.0, 5);
    limiter.acquire_n(10).await;
}
