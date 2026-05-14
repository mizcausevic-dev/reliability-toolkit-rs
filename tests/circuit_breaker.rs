use std::time::Duration;

use reliability_toolkit::{CircuitBreaker, CircuitState, ToolkitError};
use tokio::time::{advance, pause};

#[tokio::test]
async fn starts_closed() {
    let cb = CircuitBreaker::new();
    assert_eq!(cb.state().await, CircuitState::Closed);
}

#[tokio::test]
async fn successes_keep_it_closed() {
    let cb = CircuitBreaker::new();
    for _ in 0..10 {
        let r: Result<Result<u32, std::io::Error>, ToolkitError> =
            cb.call(async { Ok(42_u32) }).await;
        assert!(matches!(r, Ok(Ok(42))));
    }
    assert_eq!(cb.state().await, CircuitState::Closed);
}

#[tokio::test]
async fn trips_after_threshold_consecutive_failures() {
    let cb = CircuitBreaker::builder()
        .failure_threshold(3)
        .cool_down(Duration::from_secs(60))
        .build();
    for _ in 0..3 {
        let r: Result<Result<u32, std::io::Error>, ToolkitError> =
            cb.call(async { Err(std::io::Error::other("boom")) }).await;
        assert!(matches!(r, Ok(Err(_))));
    }
    assert_eq!(cb.state().await, CircuitState::Open);
}

#[tokio::test]
async fn open_breaker_rejects_calls_without_invoking_them() {
    let cb = CircuitBreaker::builder()
        .failure_threshold(1)
        .cool_down(Duration::from_secs(60))
        .build();
    let _: Result<Result<u32, std::io::Error>, _> =
        cb.call(async { Err(std::io::Error::other("boom")) }).await;
    assert_eq!(cb.state().await, CircuitState::Open);

    let invocations = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let inv = invocations.clone();
    let r: Result<Result<u32, std::io::Error>, ToolkitError> = cb
        .call(async move {
            inv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(99_u32)
        })
        .await;
    assert!(matches!(r, Err(ToolkitError::CircuitOpen { .. })));
    assert_eq!(invocations.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cool_down_transitions_to_half_open() {
    pause();
    let cb = CircuitBreaker::builder()
        .failure_threshold(1)
        .cool_down(Duration::from_secs(30))
        .half_open_max_calls(2)
        .build();
    let _: Result<Result<u32, std::io::Error>, _> =
        cb.call(async { Err(std::io::Error::other("boom")) }).await;
    assert_eq!(cb.state().await, CircuitState::Open);

    advance(Duration::from_secs(31)).await;
    assert_eq!(cb.state().await, CircuitState::HalfOpen);
}

#[tokio::test]
async fn half_open_success_closes_breaker() {
    pause();
    let cb = CircuitBreaker::builder()
        .failure_threshold(1)
        .cool_down(Duration::from_secs(30))
        .half_open_max_calls(1)
        .build();
    let _: Result<Result<u32, std::io::Error>, _> =
        cb.call(async { Err(std::io::Error::other("boom")) }).await;
    advance(Duration::from_secs(31)).await;
    assert_eq!(cb.state().await, CircuitState::HalfOpen);

    let r: Result<Result<u32, std::io::Error>, ToolkitError> = cb.call(async { Ok(7_u32) }).await;
    assert!(matches!(r, Ok(Ok(7))));
    assert_eq!(cb.state().await, CircuitState::Closed);
}

#[tokio::test]
async fn half_open_failure_re_opens() {
    pause();
    let cb = CircuitBreaker::builder()
        .failure_threshold(1)
        .cool_down(Duration::from_secs(30))
        .half_open_max_calls(1)
        .build();
    let _: Result<Result<u32, std::io::Error>, _> =
        cb.call(async { Err(std::io::Error::other("boom")) }).await;
    advance(Duration::from_secs(31)).await;
    assert_eq!(cb.state().await, CircuitState::HalfOpen);

    let _: Result<Result<u32, std::io::Error>, _> = cb
        .call(async { Err(std::io::Error::other("boom again")) })
        .await;
    assert_eq!(cb.state().await, CircuitState::Open);
}

#[tokio::test]
async fn manual_trip_and_reset() {
    let cb = CircuitBreaker::new();
    cb.trip().await;
    assert_eq!(cb.state().await, CircuitState::Open);
    cb.reset().await;
    assert_eq!(cb.state().await, CircuitState::Closed);
}

#[tokio::test]
async fn single_failure_below_threshold_stays_closed() {
    let cb = CircuitBreaker::builder().failure_threshold(3).build();
    for _ in 0..2 {
        let _: Result<Result<u32, std::io::Error>, _> =
            cb.call(async { Err(std::io::Error::other("boom")) }).await;
    }
    assert_eq!(cb.state().await, CircuitState::Closed);
    // One success resets the consecutive count.
    let _: Result<Result<u32, std::io::Error>, _> = cb.call(async { Ok(1_u32) }).await;
    // Now two more failures shouldn't trip it.
    for _ in 0..2 {
        let _: Result<Result<u32, std::io::Error>, _> =
            cb.call(async { Err(std::io::Error::other("boom")) }).await;
    }
    assert_eq!(cb.state().await, CircuitState::Closed);
}
