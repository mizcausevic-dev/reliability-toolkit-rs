//! Bulkhead — semaphore-backed concurrency cap.
//!
//! Wraps `tokio::sync::Semaphore`. Useful for isolating a downstream resource
//! ("at most N in-flight calls to this database") so one runaway caller can't
//! starve the rest of the system.
//!
//! Holding a [`BulkheadPermit`] keeps a slot reserved; dropping it releases.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ToolkitError;

/// A concurrency cap.
#[derive(Clone, Debug)]
pub struct Bulkhead {
    sem: Arc<Semaphore>,
    capacity: usize,
}

/// Active permit. Dropping it releases the slot.
pub struct BulkheadPermit(#[allow(dead_code)] OwnedSemaphorePermit);

impl std::fmt::Debug for BulkheadPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkheadPermit").finish()
    }
}

impl Bulkhead {
    /// Build a bulkhead allowing up to `capacity` concurrent permits.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        Self {
            sem: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    /// Wait until a slot is free, then take it.
    pub async fn acquire(&self) -> Result<BulkheadPermit, ToolkitError> {
        match self.sem.clone().acquire_owned().await {
            Ok(p) => Ok(BulkheadPermit(p)),
            Err(_) => Err(ToolkitError::BulkheadClosed),
        }
    }

    /// Try to take a slot without waiting. Returns `None` if all slots are busy.
    pub fn try_acquire(&self) -> Option<BulkheadPermit> {
        self.sem
            .clone()
            .try_acquire_owned()
            .ok()
            .map(BulkheadPermit)
    }

    /// Number of currently free slots.
    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    /// Total capacity (slots when fully idle).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Close the bulkhead — pending acquires will return [`ToolkitError::BulkheadClosed`].
    pub fn close(&self) {
        self.sem.close();
    }
}
