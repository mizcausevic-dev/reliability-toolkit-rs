//! # reliability-toolkit
//!
//! Async reliability primitives for Tokio-based Rust services. The crate is
//! deliberately small: each primitive is independent, composable, and avoids
//! pulling in unrelated dependencies.
//!
//! ## Primitives
//!
//! - [`RateLimiter`] — token-bucket rate limiter with configurable burst.
//! - [`CircuitBreaker`] — closed → open → half-open with failure-rate trip and cool-down.
//! - [`Retry`] — exponential backoff with full jitter and per-error predicates.
//! - [`Bulkhead`] — semaphore-backed concurrency cap.
//!
//! ## Composition
//!
//! Each primitive exposes an `execute()` (or `run()` / `acquire()`) that wraps
//! a user-supplied future. They are designed so the typical layering pattern —
//! `Retry(CircuitBreaker(Bulkhead(RateLimit(call))))` — falls out naturally.
//!
//! ```no_run
//! use std::time::Duration;
//! use reliability_toolkit::{RateLimiter, CircuitBreaker, Retry, Bulkhead, RetryConfig};
//!
//! # async fn outer() -> Result<(), Box<dyn std::error::Error>> {
//! let limiter = RateLimiter::new(100.0, 100);            // 100 rps, burst 100
//! let breaker = CircuitBreaker::builder()
//!     .failure_threshold(5)
//!     .cool_down(Duration::from_secs(10))
//!     .build();
//! let retry = Retry::new(RetryConfig::default());
//! let pool = Bulkhead::new(20);
//!
//! let result: Result<(), std::io::Error> = retry
//!     .run(|| async {
//!         limiter.acquire().await;
//!         let _permit = pool
//!             .acquire()
//!             .await
//!             .map_err(std::io::Error::other)?;
//!         match breaker.call(async { Ok::<_, std::io::Error>(()) }).await {
//!             Ok(inner) => inner,
//!             Err(open) => Err(std::io::Error::other(open.to_string())),
//!         }
//!     })
//!     .await;
//! # let _ = result;
//! # Ok(()) }
//! ```

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::doc_markdown)]

pub mod bulkhead;
pub mod circuit_breaker;
pub mod error;
pub mod rate_limiter;
pub mod retry;

/// Optional audit-stream-py producer. Gated behind the `audit-stream`
/// Cargo feature so the core toolkit stays HTTP-free.
#[cfg(feature = "audit-stream")]
pub mod audit_stream;

pub use bulkhead::{Bulkhead, BulkheadPermit};
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerBuilder, CircuitState};
pub use error::ToolkitError;
pub use rate_limiter::RateLimiter;
pub use retry::{Retry, RetryConfig};

#[cfg(feature = "audit-stream")]
pub use audit_stream::AuditingBreaker;
