//! Optional audit-stream-py producer.
//!
//! When the `audit-stream` Cargo feature is enabled **and** the
//! `AUDIT_STREAM_URL` env var is set, an [`AuditingBreaker`] fires
//! governance events on every circuit-breaker state transition worth
//! recording:
//!
//! - `breaker_opened`     — closed/half-open → open. The "we just stopped
//!                          trusting a downstream" signal.
//! - `breaker_recovered`  — half-open → closed. The "downstream looks
//!                          healthy again" signal.
//!
//! Manual [`AuditingBreaker::trip`] / [`AuditingBreaker::reset`] also
//! fire when they cause a real state change.
//!
//! Same opt-in pattern as the other Rust producers (hash-attestation,
//! incident-correlation, aeo-graph-explorer). Identical env-var contract:
//!
//! - `AUDIT_STREAM_URL`        — base URL, e.g. `http://audit.local:8093`
//! - `AUDIT_STREAM_TIMEOUT_S`  — per-call timeout, default 2.5s
//!
//! Best-effort. Failures are logged to stderr and swallowed — an
//! audit-stream outage must never block a call gated by the breaker.

use std::env;
use std::time::Duration;

use serde_json::json;

use crate::circuit_breaker::{CircuitBreaker, CircuitState};
use crate::error::ToolkitError;

/// Default per-call timeout when `AUDIT_STREAM_TIMEOUT_S` is unset.
pub const DEFAULT_TIMEOUT_S: f64 = 2.5;

/// True when `AUDIT_STREAM_URL` is set to a non-empty value.
#[must_use]
pub fn is_enabled() -> bool {
    base_url().is_some()
}

/// Stripped audit-stream base URL, or `None` when disabled.
#[must_use]
pub fn base_url() -> Option<String> {
    let raw = env::var("AUDIT_STREAM_URL").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.trim_end_matches('/').to_string())
}

/// Configured per-call timeout. Defaults to 2.5 seconds.
#[must_use]
pub fn timeout() -> Duration {
    let secs = env::var("AUDIT_STREAM_TIMEOUT_S")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .map_or(DEFAULT_TIMEOUT_S, |v| v.max(0.1));
    Duration::from_secs_f64(secs)
}

/// Fire one event. Silent no-op when `AUDIT_STREAM_URL` is unset.
pub async fn emit(client: &reqwest::Client, kind: &str, payload: serde_json::Value) {
    let Some(url) = base_url() else {
        return;
    };
    let body = json!({
        "kind": kind,
        "source": "reliability-toolkit",
        "payload": payload,
    });
    let endpoint = format!("{url}/events");
    let result = client
        .post(&endpoint)
        .json(&body)
        .timeout(timeout())
        .send()
        .await;
    match result {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            eprintln!(
                "audit-stream emit failed (kind={kind}): HTTP {}",
                resp.status()
            );
        }
        Err(err) => {
            eprintln!("audit-stream emit failed (kind={kind}): {err}");
        }
    }
}

/// Wraps a [`CircuitBreaker`] with a name + audit-stream client so that
/// state transitions fan out as governance events.
///
/// The wrapper is cheap to clone — both the inner breaker and the
/// reqwest client are reference-counted.
#[derive(Clone)]
pub struct AuditingBreaker {
    inner: CircuitBreaker,
    name: String,
    client: reqwest::Client,
}

impl AuditingBreaker {
    /// Wrap a breaker with a name (identifies which breaker in the audit
    /// log — e.g. `"downstream-billing"`) and an HTTP client.
    pub fn new(breaker: CircuitBreaker, name: impl Into<String>, client: reqwest::Client) -> Self {
        Self {
            inner: breaker,
            name: name.into(),
            client,
        }
    }

    /// The wrapped breaker. Use this for non-audited calls or to read
    /// state directly.
    #[must_use]
    pub fn inner(&self) -> &CircuitBreaker {
        &self.inner
    }

    /// Name this breaker emits under.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Execute `fut` through the breaker, firing transition events on
    /// open/recover. Semantics match [`CircuitBreaker::call`].
    pub async fn call<F, T, E>(&self, fut: F) -> Result<Result<T, E>, ToolkitError>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        let before = self.inner.state().await;
        let out = self.inner.call(fut).await;
        let after = self.inner.state().await;
        self.emit_transition(before, after, "call").await;
        out
    }

    /// Manually trip the breaker, emitting `breaker_opened` if the
    /// transition was real (was-not-already-open → open).
    pub async fn trip(&self) {
        let before = self.inner.state().await;
        self.inner.trip().await;
        let after = self.inner.state().await;
        self.emit_transition(before, after, "trip").await;
    }

    /// Manually reset the breaker, emitting `breaker_recovered` if the
    /// transition was real (was-not-already-closed → closed).
    pub async fn reset(&self) {
        let before = self.inner.state().await;
        self.inner.reset().await;
        let after = self.inner.state().await;
        self.emit_transition(before, after, "reset").await;
    }

    async fn emit_transition(&self, before: CircuitState, after: CircuitState, cause: &str) {
        if before == after {
            return;
        }
        // Opened: anything → Open
        if after == CircuitState::Open {
            emit(
                &self.client,
                "breaker_opened",
                json!({
                    "name": self.name,
                    "previous_state": state_label(before),
                    "cause": cause,
                }),
            )
            .await;
        }
        // Recovered: HalfOpen → Closed, or manual reset Open/HalfOpen → Closed
        else if after == CircuitState::Closed && before != CircuitState::Closed {
            emit(
                &self.client,
                "breaker_recovered",
                json!({
                    "name": self.name,
                    "previous_state": state_label(before),
                    "cause": cause,
                }),
            )
            .await;
        }
        // Closed → HalfOpen and HalfOpen ↔ HalfOpen are not audit-worthy on
        // their own — they're internal cool-down transitions. The "real"
        // governance events are opened (we lost trust) and recovered (we
        // re-gained it).
    }
}

fn state_label(s: CircuitState) -> &'static str {
    match s {
        CircuitState::Closed => "closed",
        CircuitState::Open => "open",
        CircuitState::HalfOpen => "half_open",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_GUARD: Mutex<()> = Mutex::new(());

    fn reset_env() {
        env::remove_var("AUDIT_STREAM_URL");
        env::remove_var("AUDIT_STREAM_TIMEOUT_S");
    }

    #[test]
    fn disabled_when_unset() {
        let _l = ENV_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_env();
        assert!(!is_enabled());
    }

    #[test]
    fn enabled_with_value() {
        let _l = ENV_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_env();
        env::set_var("AUDIT_STREAM_URL", "http://audit.local:8093/");
        assert!(is_enabled());
        assert_eq!(base_url().unwrap(), "http://audit.local:8093");
        env::remove_var("AUDIT_STREAM_URL");
    }

    #[test]
    fn timeout_default() {
        let _l = ENV_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_env();
        assert_eq!(timeout(), Duration::from_secs_f64(DEFAULT_TIMEOUT_S));
    }

    #[test]
    fn timeout_override() {
        let _l = ENV_GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_env();
        env::set_var("AUDIT_STREAM_TIMEOUT_S", "0.75");
        assert_eq!(timeout(), Duration::from_secs_f64(0.75));
        env::remove_var("AUDIT_STREAM_TIMEOUT_S");
    }
}
