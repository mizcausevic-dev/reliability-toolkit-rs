use reliability_toolkit::{Bulkhead, ToolkitError};

#[tokio::test]
async fn capacity_caps_in_flight_count() {
    let pool = Bulkhead::new(2);
    assert_eq!(pool.capacity(), 2);
    assert_eq!(pool.available_permits(), 2);

    let p1 = pool.acquire().await.unwrap();
    let p2 = pool.acquire().await.unwrap();
    assert_eq!(pool.available_permits(), 0);
    assert!(pool.try_acquire().is_none());

    drop(p1);
    assert_eq!(pool.available_permits(), 1);
    assert!(pool.try_acquire().is_some());
    drop(p2);
}

#[tokio::test]
async fn try_acquire_returns_none_when_full() {
    let pool = Bulkhead::new(1);
    let p = pool.try_acquire().expect("first take should succeed");
    assert!(pool.try_acquire().is_none());
    drop(p);
    assert!(pool.try_acquire().is_some());
}

#[tokio::test]
async fn closed_bulkhead_rejects_acquires() {
    let pool = Bulkhead::new(1);
    pool.close();
    let r = pool.acquire().await;
    assert!(matches!(r, Err(ToolkitError::BulkheadClosed)));
}

#[tokio::test]
#[should_panic(expected = "capacity must be > 0")]
async fn zero_capacity_panics() {
    let _ = Bulkhead::new(0);
}
