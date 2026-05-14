//! Circuit breaker.
//!
//! Three states:
//!
//! * **Closed** — calls go through; consecutive failures are counted.
//!   When the count hits `failure_threshold`, the breaker trips to `Open`.
//! * **Open** — calls are rejected with [`ToolkitError::CircuitOpen`] until
//!   `cool_down` has elapsed since the trip. Then the breaker moves to
//!   `HalfOpen`.
//! * **HalfOpen** — only `half_open_max_calls` calls are admitted. If they all
//!   succeed, the breaker returns to `Closed`. A single failure trips it back
//!   to `Open`.
//!
//! The implementation deliberately uses a single Mutex for the state machine.
//! Breakers protect expensive downstream calls — the cost of locking is dwarfed
//! by the network/IO cost of the call itself.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::error::ToolkitError;

/// Current state of the breaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    /// Calls flow through. Failures are being counted.
    Closed,
    /// Calls are rejected fast. Waiting out the cool-down.
    Open,
    /// Trial state — a limited number of calls are allowed.
    HalfOpen,
}

/// Builder for [`CircuitBreaker`].
#[derive(Clone, Debug)]
pub struct CircuitBreakerBuilder {
    failure_threshold: u32,
    cool_down: Duration,
    half_open_max_calls: u32,
}

impl Default for CircuitBreakerBuilder {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cool_down: Duration::from_secs(30),
            half_open_max_calls: 1,
        }
    }
}

impl CircuitBreakerBuilder {
    /// Number of consecutive failures before the breaker trips. Default: 5.
    #[must_use]
    pub fn failure_threshold(mut self, n: u32) -> Self {
        assert!(n > 0, "failure_threshold must be > 0");
        self.failure_threshold = n;
        self
    }

    /// How long the breaker stays open before allowing a trial. Default: 30s.
    #[must_use]
    pub fn cool_down(mut self, d: Duration) -> Self {
        self.cool_down = d;
        self
    }

    /// How many calls to admit in `HalfOpen`. Default: 1.
    #[must_use]
    pub fn half_open_max_calls(mut self, n: u32) -> Self {
        assert!(n > 0, "half_open_max_calls must be > 0");
        self.half_open_max_calls = n;
        self
    }

    /// Finalize the breaker.
    #[must_use]
    pub fn build(self) -> CircuitBreaker {
        CircuitBreaker {
            inner: Arc::new(Inner {
                cfg: self,
                state: Mutex::new(StateMachine {
                    state: CircuitState::Closed,
                    consecutive_failures: 0,
                    opened_at: None,
                    half_open_inflight: 0,
                    half_open_successes: 0,
                }),
            }),
        }
    }
}

/// A circuit breaker. Cheap to `clone`; the inner state is `Arc<Mutex<_>>`.
#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    cfg: CircuitBreakerBuilder,
    state: Mutex<StateMachine>,
}

#[derive(Debug)]
struct StateMachine {
    state: CircuitState,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    half_open_inflight: u32,
    half_open_successes: u32,
}

impl CircuitBreaker {
    /// Default-configured breaker.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Start configuring a new breaker.
    pub fn builder() -> CircuitBreakerBuilder {
        CircuitBreakerBuilder::default()
    }

    /// Current breaker state. Mostly for telemetry / tests.
    pub async fn state(&self) -> CircuitState {
        let mut sm = self.inner.state.lock().await;
        self.tick(&mut sm);
        sm.state
    }

    /// Execute `fut`, gating it on the breaker state.
    ///
    /// Returns:
    ///
    /// * `Ok(Ok(value))` — call ran and succeeded.
    /// * `Ok(Err(e))` — call ran and the wrapped future returned `Err(e)`.
    ///   The breaker counts this as a failure.
    /// * `Err(ToolkitError::CircuitOpen { .. })` — call was rejected without
    ///   being invoked.
    pub async fn call<F, T, E>(&self, fut: F) -> Result<Result<T, E>, ToolkitError>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        // Phase 1: gate
        let admitted = {
            let mut sm = self.inner.state.lock().await;
            self.tick(&mut sm);
            match sm.state {
                CircuitState::Closed => true,
                CircuitState::HalfOpen => {
                    if sm.half_open_inflight < self.inner.cfg.half_open_max_calls {
                        sm.half_open_inflight += 1;
                        true
                    } else {
                        false
                    }
                }
                CircuitState::Open => false,
            }
        };

        if !admitted {
            let retry_after = self.retry_after().await;
            return Err(ToolkitError::CircuitOpen { retry_after });
        }

        // Phase 2: run the call
        let result = fut.await;

        // Phase 3: record
        {
            let mut sm = self.inner.state.lock().await;
            match (&result, sm.state) {
                (Ok(_), CircuitState::Closed) => {
                    sm.consecutive_failures = 0;
                }
                (Ok(_), CircuitState::HalfOpen) => {
                    sm.half_open_inflight = sm.half_open_inflight.saturating_sub(1);
                    sm.half_open_successes += 1;
                    if sm.half_open_successes >= self.inner.cfg.half_open_max_calls {
                        // promote back to Closed
                        sm.state = CircuitState::Closed;
                        sm.consecutive_failures = 0;
                        sm.opened_at = None;
                        sm.half_open_inflight = 0;
                        sm.half_open_successes = 0;
                    }
                }
                (Err(_), CircuitState::Closed) => {
                    sm.consecutive_failures += 1;
                    if sm.consecutive_failures >= self.inner.cfg.failure_threshold {
                        sm.state = CircuitState::Open;
                        sm.opened_at = Some(Instant::now());
                    }
                }
                (Err(_), CircuitState::HalfOpen) => {
                    sm.state = CircuitState::Open;
                    sm.opened_at = Some(Instant::now());
                    sm.half_open_inflight = 0;
                    sm.half_open_successes = 0;
                }
                (_, CircuitState::Open) => {
                    // We shouldn't get here — we only run the call when admitted —
                    // but if we did the conservative choice is to do nothing.
                }
            }
        }

        Ok(result)
    }

    /// Manually trip the breaker open. Useful for kill-switches.
    pub async fn trip(&self) {
        let mut sm = self.inner.state.lock().await;
        sm.state = CircuitState::Open;
        sm.opened_at = Some(Instant::now());
        sm.half_open_inflight = 0;
        sm.half_open_successes = 0;
    }

    /// Manually reset the breaker to closed. Useful after a known-good deploy.
    pub async fn reset(&self) {
        let mut sm = self.inner.state.lock().await;
        sm.state = CircuitState::Closed;
        sm.consecutive_failures = 0;
        sm.opened_at = None;
        sm.half_open_inflight = 0;
        sm.half_open_successes = 0;
    }

    fn tick(&self, sm: &mut StateMachine) {
        if sm.state == CircuitState::Open {
            if let Some(t) = sm.opened_at {
                if Instant::now().duration_since(t) >= self.inner.cfg.cool_down {
                    sm.state = CircuitState::HalfOpen;
                    sm.half_open_inflight = 0;
                    sm.half_open_successes = 0;
                }
            }
        }
    }

    async fn retry_after(&self) -> Duration {
        let sm = self.inner.state.lock().await;
        match sm.opened_at {
            Some(t) => self
                .inner
                .cfg
                .cool_down
                .checked_sub(Instant::now().duration_since(t))
                .unwrap_or_else(|| Duration::from_secs(0)),
            None => Duration::from_secs(0),
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}
