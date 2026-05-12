use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{SchedulerError, SchedulerResult};

/// A handle returned when a consumer registers for refresh signals.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefreshSignalHandle(pub String);

/// Callback signature for refresh signal consumers.
///
/// Consumers receive the new tick and generation, discard old snapshot
/// references, clear caches, and return `Ok()` to acknowledge.
pub type RefreshCallback = Arc<dyn Fn(u64, u64) -> SchedulerResult<()> + Send + Sync>;

/// The tick-boundary refresh signal protocol.
///
/// After `journal.commit` completes, the scheduler broadcasts
/// `REFRESH_SIGNAL` to all registered consumers. Consumers must
/// acknowledge before the scheduler proceeds to the next tick.
pub struct RefreshSignalBus {
    consumers: Arc<Mutex<HashMap<String, RefreshCallback>>>,
}

impl std::fmt::Debug for RefreshSignalBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self
            .consumers
            .lock()
            .map(|m| m.len())
            .unwrap_or(0);
        f.debug_struct("RefreshSignalBus")
            .field("consumer_count", &count)
            .finish()
    }
}

impl Clone for RefreshSignalBus {
    fn clone(&self) -> Self {
        Self {
            consumers: Arc::clone(&self.consumers),
        }
    }
}

impl Default for RefreshSignalBus {
    fn default() -> Self {
        Self::new()
    }
}

impl RefreshSignalBus {
 /// Create a new empty signal bus.
    pub fn new() -> Self {
        Self {
            consumers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

 /// Register a consumer with a unique name and callback.
    pub fn register(
        &self,
        name: impl Into<String>,
        callback: RefreshCallback,
    ) -> SchedulerResult<RefreshSignalHandle> {
        let name = name.into();
        let mut consumers = self
            .consumers
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("refresh signal lock poisoned: {e}")))?;
        consumers.insert(name.clone(), callback);
        Ok(RefreshSignalHandle(name))
    }

 /// Unregister a consumer by handle.
    pub fn unregister(&self, handle: &RefreshSignalHandle) -> SchedulerResult<()> {
        let mut consumers = self
            .consumers
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("refresh signal lock poisoned: {e}")))?;
        consumers.remove(&handle.0);
        Ok(())
    }

 /// Broadcast the refresh signal to all registered consumers.
 ///
 /// Returns `Ok()` only if every consumer acknowledges successfully.
    pub fn broadcast(&self, tick: u64, generation: u64) -> SchedulerResult<()> {
        let consumers = self
            .consumers
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("refresh signal lock poisoned: {e}")))?;
        consumers
            .iter()
            .map(|(name, cb)| {
                cb(tick, generation)
                    .map_err(|e| SchedulerError::RefreshSignalFailed {
                    consumer: name.clone(),
                    source: Box::new(e),
                })
            })
            .collect::<SchedulerResult<Vec<()>>>()
            .map(|_| ())
    }

 /// Returns the names of all currently registered consumers.
    pub fn consumer_names(&self) -> SchedulerResult<Vec<String>> {
        let consumers = self
            .consumers
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("refresh signal lock poisoned: {e}")))?;
        Ok(consumers.keys().cloned().collect())
    }

 /// Returns the number of registered consumers.
    pub fn consumer_count(&self) -> SchedulerResult<usize> {
        let consumers = self
            .consumers
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("refresh signal lock poisoned: {e}")))?;
        Ok(consumers.len())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn register_and_broadcast() {
        let bus = RefreshSignalBus::new();
        let counter = Arc::new(AtomicU64::new(0));
        let cb: RefreshCallback = {
            let c = counter.clone();
            Arc::new(move |_tick, _gen| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        };
        bus.register("c1", cb).unwrap();
        bus.broadcast(5, 10).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn broadcast_receives_correct_params() {
        let bus = RefreshSignalBus::new();
        let received = Arc::new(std::sync::Mutex::new((0u64, 0u64)));
        let cb: RefreshCallback = {
            let r = received.clone();
            Arc::new(move |tick, gen| {
                let mut guard = r.lock().map_err(|_| SchedulerError::Generic("poisoned".into()))?;
                *guard = (tick, gen);
                Ok(())
            })
        };
        bus.register("c1", cb).unwrap();
        bus.broadcast(7, 3).unwrap();
        let vals = *received.lock().unwrap();
        assert_eq!(vals, (7, 3));
    }

    #[test]
    fn broadcast_error_propagates() {
        let bus = RefreshSignalBus::new();
        let cb: RefreshCallback = Arc::new(|_, _| Err(SchedulerError::Generic("fail".into())));
        bus.register("bad", cb).unwrap();
        let err = bus.broadcast(0, 0).unwrap_err();
        assert!(
            matches!(
                err,
                SchedulerError::RefreshSignalFailed {
                    consumer: ref n,
                    ..
                } if n == "bad"
            ),
            "expected RefreshSignalFailed, got {err:?}"
        );
    }

    #[test]
    fn unregister_stops_broadcast() {
        let bus = RefreshSignalBus::new();
        let counter = Arc::new(AtomicU64::new(0));
        let cb: RefreshCallback = {
            let c = counter.clone();
            Arc::new(move |_tick, _gen| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        };
        let handle = bus.register("tmp", cb).unwrap();
        bus.broadcast(1, 1).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        bus.unregister(&handle).unwrap();
        bus.broadcast(2, 2).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn consumer_names_returns_all() {
        let bus = RefreshSignalBus::new();
        let noop: RefreshCallback = Arc::new(|_, _| Ok(()));
        bus.register("a", noop.clone()).unwrap();
        bus.register("b", noop).unwrap();
        let names = bus.consumer_names().unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"a".to_owned()));
        assert!(names.contains(&"b".to_owned()));
    }

    #[test]
    fn consumer_count_updates() {
        let bus = RefreshSignalBus::new();
        assert_eq!(bus.consumer_count().unwrap(), 0);
        let noop: RefreshCallback = Arc::new(|_, _| Ok(()));
        let h = bus.register("x", noop).unwrap();
        assert_eq!(bus.consumer_count().unwrap(), 1);
        bus.unregister(&h).unwrap();
        assert_eq!(bus.consumer_count().unwrap(), 0);
    }

    #[test]
    fn empty_broadcast_succeeds() {
        let bus = RefreshSignalBus::new();
        bus.broadcast(0, 0).unwrap();
    }

    #[test]
    fn bus_is_cloneable() {
        let bus = RefreshSignalBus::new();
        let counter = Arc::new(AtomicU64::new(0));
        let cb: RefreshCallback = {
            let c = counter.clone();
            Arc::new(move |_tick, _gen| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        };
        bus.register("shared", cb).unwrap();
        let bus2 = bus.clone();
        bus2.broadcast(1, 1).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
