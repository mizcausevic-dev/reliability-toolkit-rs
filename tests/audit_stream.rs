//! Integration tests for the optional `audit-stream` feature.

#![cfg(feature = "audit-stream")]

use std::sync::Mutex;
use std::time::Duration;

use reliability_toolkit::{AuditingBreaker, CircuitBreaker};
use serde_json::Value;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

static ENV_GUARD: Mutex<()> = Mutex::new(());

struct EnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn lock() -> Self {
        let lock = ENV_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::remove_var("AUDIT_STREAM_URL");
        std::env::remove_var("AUDIT_STREAM_TIMEOUT_S");
        EnvGuard { _lock: lock }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("AUDIT_STREAM_URL");
        std::env::remove_var("AUDIT_STREAM_TIMEOUT_S");
    }
}

fn breaker_with_threshold(n: u32) -> CircuitBreaker {
    CircuitBreaker::builder()
        .failure_threshold(n)
        .cool_down(Duration::from_secs(60))
        .build()
}

#[tokio::test]
async fn manual_trip_emits_breaker_opened() {
    let _guard = EnvGuard::lock();
    let server = MockServer::start().await;
    std::env::set_var("AUDIT_STREAM_URL", server.uri());

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&server)
        .await;

    let ab = AuditingBreaker::new(
        CircuitBreaker::new(),
        "downstream-billing",
        reqwest::Client::new(),
    );
    ab.trip().await;

    let recvd = server.received_requests().await.unwrap();
    assert_eq!(recvd.len(), 1);
    let body: Value = serde_json::from_slice(&recvd[0].body).unwrap();
    assert_eq!(body["kind"], "breaker_opened");
    assert_eq!(body["source"], "reliability-toolkit");
    assert_eq!(body["payload"]["name"], "downstream-billing");
    assert_eq!(body["payload"]["previous_state"], "closed");
    assert_eq!(body["payload"]["cause"], "trip");
}

#[tokio::test]
async fn manual_reset_after_trip_emits_breaker_recovered() {
    let _guard = EnvGuard::lock();
    let server = MockServer::start().await;
    std::env::set_var("AUDIT_STREAM_URL", server.uri());

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(201))
        // 2 events: trip (opens) + reset (recovers)
        .expect(2)
        .mount(&server)
        .await;

    let ab = AuditingBreaker::new(
        CircuitBreaker::new(),
        "downstream-billing",
        reqwest::Client::new(),
    );
    ab.trip().await;
    ab.reset().await;

    let recvd = server.received_requests().await.unwrap();
    assert_eq!(recvd.len(), 2);
    let last: Value = serde_json::from_slice(&recvd[1].body).unwrap();
    assert_eq!(last["kind"], "breaker_recovered");
    assert_eq!(last["source"], "reliability-toolkit");
    assert_eq!(last["payload"]["name"], "downstream-billing");
    assert_eq!(last["payload"]["previous_state"], "open");
    assert_eq!(last["payload"]["cause"], "reset");
}

#[tokio::test]
async fn failure_streak_via_call_opens_breaker() {
    let _guard = EnvGuard::lock();
    let server = MockServer::start().await;
    std::env::set_var("AUDIT_STREAM_URL", server.uri());

    // Threshold 3 → 3 errored calls before the breaker opens. Expect 1 event.
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&server)
        .await;

    let ab = AuditingBreaker::new(
        breaker_with_threshold(3),
        "downstream-search",
        reqwest::Client::new(),
    );
    for _ in 0..3 {
        let _ = ab
            .call(async { Err::<(), std::io::Error>(std::io::Error::other("nope")) })
            .await;
    }

    let recvd = server.received_requests().await.unwrap();
    assert_eq!(recvd.len(), 1);
    let body: Value = serde_json::from_slice(&recvd[0].body).unwrap();
    assert_eq!(body["kind"], "breaker_opened");
    assert_eq!(body["payload"]["name"], "downstream-search");
    assert_eq!(body["payload"]["cause"], "call");
}

#[tokio::test]
async fn trip_when_already_open_is_silent() {
    let _guard = EnvGuard::lock();
    let server = MockServer::start().await;
    std::env::set_var("AUDIT_STREAM_URL", server.uri());

    // Only one breaker_opened event — the second trip is a no-op transition.
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&server)
        .await;

    let ab = AuditingBreaker::new(
        CircuitBreaker::new(),
        "downstream-cache",
        reqwest::Client::new(),
    );
    ab.trip().await;
    ab.trip().await; // already open — no event

    let recvd = server.received_requests().await.unwrap();
    assert_eq!(recvd.len(), 1);
}

#[tokio::test]
async fn silent_when_env_var_unset() {
    let _guard = EnvGuard::lock();
    // No URL set; trip + reset are functionally fine, just silent.
    let server = MockServer::start().await;
    let ab = AuditingBreaker::new(
        CircuitBreaker::new(),
        "downstream-x",
        reqwest::Client::new(),
    );
    ab.trip().await;
    ab.reset().await;
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn audit_outage_does_not_break_trip() {
    let _guard = EnvGuard::lock();
    std::env::set_var("AUDIT_STREAM_URL", "http://127.0.0.1:1");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let ab = AuditingBreaker::new(CircuitBreaker::new(), "any", client);
    // Should not panic / return early. State must still flip.
    ab.trip().await;
    assert_eq!(
        ab.inner().state().await,
        reliability_toolkit::CircuitState::Open
    );
}
