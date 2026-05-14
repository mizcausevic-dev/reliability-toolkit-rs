//! Token-bucket rate limiter.
//!
//! Tokens accrue at `rate_per_second` up to `burst`. Acquiring a token blocks
//! until one is available. Bursts are absorbed up to the bucket capacity, which
//! is the right shape for protecting downstreams that can briefly tolerate
//! traffic spikes but reject sustained overload.
//!
//! The implementation is lock-free for the read path (one `Mutex` guarding a
//! tiny token count + last-refill timestamp). For workloads >> 10k rps consider
//! a sharded variant.
//!
//! ```
//! # use std::time::Duration;
//! # use reliability_toolkit::RateLimiter;
//! # async fn demo() {
//! let limiter = RateLimiter::new(10.0, 5); // 10 rps, burst 5
//! for _ in 0..3 {
//!     limiter.acquire().await;
//! }
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

/// Token-bucket rate limiter. Cheap to `clone`; the inner state is `Arc<Mutex<_>>`.
#[derive(Clone, Debug)]
pub struct RateLimiter {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    rate_per_second: f64,
    burst: f64,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    /// Build a limiter that produces `rate_per_second` tokens, capped at `burst`.
    ///
    /// # Panics
    ///
    /// Panics if `rate_per_second` is not positive or `burst` is zero.
    pub fn new(rate_per_second: f64, burst: u32) -> Self {
        assert!(rate_per_second > 0.0, "rate_per_second must be positive");
        assert!(burst > 0, "burst must be non-zero");
        let burst_f = f64::from(burst);
        Self {
            inner: Arc::new(Inner {
                rate_per_second,
                burst: burst_f,
                state: Mutex::new(State {
                    tokens: burst_f,
                    last_refill: Instant::now(),
                }),
            }),
        }
    }

    /// Wait until a token is available, then consume it.
    pub async fn acquire(&self) {
        self.acquire_n(1).await;
    }

    /// Wait until `n` tokens are available, then consume them.
    ///
    /// # Panics
    ///
    /// Panics if `n` exceeds the configured burst (it could never be granted).
    pub async fn acquire_n(&self, n: u32) {
        let needed = f64::from(n);
        assert!(
            needed <= self.inner.burst,
            "requested {n} tokens but burst is {}",
            self.inner.burst
        );

        loop {
            let wait = {
                let mut state = self.inner.state.lock().await;
                self.refill(&mut state);
                if state.tokens >= needed {
                    state.tokens -= needed;
                    return;
                }
                let deficit = needed - state.tokens;
                let seconds = deficit / self.inner.rate_per_second;
                Duration::from_secs_f64(seconds)
            };
            sleep(wait).await;
        }
    }

    /// Try to consume one token without waiting. Returns `true` on success.
    pub async fn try_acquire(&self) -> bool {
        self.try_acquire_n(1).await
    }

    /// Try to consume `n` tokens without waiting. Returns `true` on success.
    pub async fn try_acquire_n(&self, n: u32) -> bool {
        let needed = f64::from(n);
        let mut state = self.inner.state.lock().await;
        self.refill(&mut state);
        if state.tokens >= needed {
            state.tokens -= needed;
            true
        } else {
            false
        }
    }

    /// Current bucket level (mostly useful for tests + telemetry).
    pub async fn tokens(&self) -> f64 {
        let mut state = self.inner.state.lock().await;
        self.refill(&mut state);
        state.tokens
    }

    fn refill(&self, state: &mut State) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            state.tokens =
                (state.tokens + elapsed * self.inner.rate_per_second).min(self.inner.burst);
            state.last_refill = now;
        }
    }
}
