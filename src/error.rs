//! Error types shared across the crate.

use std::time::Duration;

use thiserror::Error;

/// Errors that can be returned by reliability primitives themselves
/// (as opposed to errors from the wrapped operation).
#[derive(Debug, Error)]
pub enum ToolkitError {
    /// The circuit breaker is currently open and rejected the call without invoking it.
    #[error("circuit breaker is open (will retry after {retry_after:?})")]
    CircuitOpen {
        /// How long the breaker expects to remain open.
        retry_after: Duration,
    },

    /// The bulkhead has been closed and is no longer accepting permits.
    #[error("bulkhead is closed")]
    BulkheadClosed,

    /// Retry exhausted its attempt budget without a success.
    #[error("retry exhausted after {attempts} attempts")]
    RetryExhausted {
        /// How many attempts were made before giving up.
        attempts: u32,
    },
}
