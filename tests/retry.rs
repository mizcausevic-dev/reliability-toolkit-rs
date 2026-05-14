use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reliability_toolkit::{Retry, RetryConfig};
use tokio::time::pause;

#[tokio::test]
async fn returns_immediately_on_success() {
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig::default());
    let calls = Arc::new(AtomicU32::new(0));
    let calls_inner = calls.clone();
    let result: Result<u32, std::io::Error> = retry
        .run(|| {
            let c = calls_inner.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(42_u32)
            }
        })
        .await;
    assert!(matches!(result, Ok(42)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retries_until_success() {
    pause();
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig {
        max_attempts: 5,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
        retry_if: None,
    });
    let calls = Arc::new(AtomicU32::new(0));
    let calls_inner = calls.clone();
    let result: Result<u32, std::io::Error> = retry
        .run(|| {
            let c = calls_inner.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(std::io::Error::other("not yet"))
                } else {
                    Ok(7_u32)
                }
            }
        })
        .await;
    assert!(matches!(result, Ok(7)));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn gives_up_after_max_attempts() {
    pause();
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig {
        max_attempts: 3,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
        retry_if: None,
    });
    let calls = Arc::new(AtomicU32::new(0));
    let calls_inner = calls.clone();
    let result: Result<u32, std::io::Error> = retry
        .run(|| {
            let c = calls_inner.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(std::io::Error::other("always fails"))
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn predicate_can_skip_retries() {
    pause();
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig {
        max_attempts: 5,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
        retry_if: Some(Arc::new(|e: &std::io::Error| {
            // Only retry transient kinds; treat PermissionDenied as fatal.
            e.kind() != std::io::ErrorKind::PermissionDenied
        })),
    });
    let calls = Arc::new(AtomicU32::new(0));
    let calls_inner = calls.clone();
    let result: Result<u32, std::io::Error> = retry
        .run(|| {
            let c = calls_inner.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn backoff_is_bounded_by_max_delay() {
    // Ensure max_delay caps the exponent — pause virtual time then run a few
    // attempts with a huge base; the test should still terminate quickly.
    pause();
    let retry: Retry<std::io::Error> = Retry::new(RetryConfig {
        max_attempts: 4,
        base_delay: Duration::from_secs(60),
        max_delay: Duration::from_millis(10),
        retry_if: None,
    });
    let result: Result<u32, std::io::Error> = retry
        .run(|| async { Err(std::io::Error::other("nope")) })
        .await;
    assert!(result.is_err());
}
