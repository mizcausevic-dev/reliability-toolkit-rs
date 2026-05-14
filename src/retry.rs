//! Exponential backoff with full jitter.
//!
//! `Retry::run` re-invokes the supplied closure until either it succeeds or
//! the attempt budget is exhausted. Backoff is `min(max_delay, base * 2^attempt)`
//! with [full jitter] applied to the result.
//!
//! Pass a custom retry predicate via [`RetryConfig::retry_if`] to skip
//! re-execution for errors that aren't transient (auth failures, 4xx, …).
//!
//! [full jitter]: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

/// Decides whether a given error should trigger another retry attempt.
pub type RetryPredicate<E> = Arc<dyn Fn(&E) -> bool + Send + Sync>;

/// Configuration for [`Retry`].
#[derive(Clone)]
pub struct RetryConfig<E = std::io::Error> {
    /// Maximum total attempts (including the first). Default: 3.
    pub max_attempts: u32,
    /// Initial backoff. Default: 100ms.
    pub base_delay: Duration,
    /// Cap on any single backoff. Default: 5s.
    pub max_delay: Duration,
    /// Predicate that decides if a given error should be retried.
    /// Default: retry every error.
    pub retry_if: Option<RetryPredicate<E>>,
}

impl<E> Default for RetryConfig<E> {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            retry_if: None,
        }
    }
}

impl<E> std::fmt::Debug for RetryConfig<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryConfig")
            .field("max_attempts", &self.max_attempts)
            .field("base_delay", &self.base_delay)
            .field("max_delay", &self.max_delay)
            .field("retry_if", &self.retry_if.as_ref().map(|_| "<predicate>"))
            .finish()
    }
}

/// Exponential-backoff retry helper.
#[derive(Clone, Debug)]
pub struct Retry<E = std::io::Error> {
    cfg: RetryConfig<E>,
    seed: Arc<AtomicU64>,
}

impl<E> Retry<E> {
    /// Build a `Retry` with the given config.
    pub fn new(cfg: RetryConfig<E>) -> Self {
        // Seed our tiny PRNG from the wall clock at construction time.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0xdead_beef, |d| d.as_nanos() as u64);
        Self {
            cfg,
            seed: Arc::new(AtomicU64::new(seed.wrapping_add(1))),
        }
    }

    /// Execute `make_fut` up to `max_attempts` times, backing off between tries.
    ///
    /// `make_fut` is a closure (not a single future) so we can re-poll a fresh
    /// future on each attempt — most futures aren't `Clone`.
    pub async fn run<F, Fut, T>(&self, mut make_fut: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match make_fut().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt >= self.cfg.max_attempts {
                        return Err(e);
                    }
                    if let Some(pred) = &self.cfg.retry_if {
                        if !pred(&e) {
                            return Err(e);
                        }
                    }
                    let delay = self.backoff(attempt);
                    sleep(delay).await;
                }
            }
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        // attempt is 1-based here; exponent should grow as 0,1,2,...
        let exp = attempt.saturating_sub(1).min(30);
        let raw = self.cfg.base_delay.saturating_mul(1u32 << exp);
        let capped = raw.min(self.cfg.max_delay);
        // Full jitter: pick a random duration in [0, capped].
        let max_ms = capped.as_millis().min(u128::from(u64::MAX)) as u64;
        let jitter_ms = self.next_rand() % (max_ms + 1);
        Duration::from_millis(jitter_ms)
    }

    /// XorShift64* PRNG — good enough for jitter, no `rand` dep needed.
    fn next_rand(&self) -> u64 {
        let mut x = self.seed.load(Ordering::Relaxed);
        if x == 0 {
            x = 0xdead_beef;
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.seed.store(x, Ordering::Relaxed);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}
