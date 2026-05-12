use scharnhorst_arrow_store::{ArrowStore, WorldSnapshot};
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_scheduler::refresh_signal::{RefreshCallback, RefreshSignalHandle};
use scharnhorst_scheduler::Scheduler;
use std::sync::{Arc, Mutex};

use crate::error::{BevyBridgeError, BevyBridgeResult};
use crate::sync::ViewModel;

#[derive(Debug)]
struct RefreshHandlerState {
    signal_received: bool,
    last_snapshot_generation: u64,
}

#[derive(Debug, Clone)]
pub struct SnapshotRefreshHandler {
    view_model: Arc<ViewModel>,
    query_engine: Arc<QueryEngine>,
    #[allow(dead_code)]
    arrow_store: Arc<ArrowStore>,
    state: Arc<Mutex<RefreshHandlerState>>,
}

impl SnapshotRefreshHandler {
    pub fn new(
        view_model: Arc<ViewModel>,
        query_engine: Arc<QueryEngine>,
        arrow_store: Arc<ArrowStore>,
    ) -> Self {
        Self {
            view_model,
            query_engine,
            arrow_store,
            state: Arc::new(Mutex::new(RefreshHandlerState {
                signal_received: false,
                last_snapshot_generation: 0,
            })),
        }
    }

    pub fn on_refresh_signal(&self) -> BevyBridgeResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        state.signal_received = true;
        Ok(())
    }

    pub fn should_refresh(&self) -> BevyBridgeResult<bool> {
        self.state
            .lock()
            .map(|s| s.signal_received)
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))
    }

    pub fn refresh_snapshot(
        &self,
        new_snapshot: WorldSnapshot,
    ) -> BevyBridgeResult<()> {
        let generation = {
            let mut state = self
                .state
                .lock()
                .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
            state.last_snapshot_generation += 1;
            state.last_snapshot_generation
        };

        self.view_model
            .refresh(Arc::new(new_snapshot), generation)?;

        {
            let mut state = self
                .state
                .lock()
                .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
            state.signal_received = false;
        }

        Ok(())
    }

    pub fn current_generation(&self) -> BevyBridgeResult<u64> {
        self.state
            .lock()
            .map(|s| s.last_snapshot_generation)
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))
    }

    pub fn callback(&self) -> RefreshCallback {
        let vm = Arc::clone(&self.view_model);
        let qe = Arc::clone(&self.query_engine);
        let state = Arc::clone(&self.state);
        Arc::new(move |tick, _generation| {
            // Obtain WorldSnapshot through query-engine (DC-10 compliant path).
            // The snapshot was pushed into query-engine during journal.commit()
            // via store_world_snapshot(). The tick parameter is ignored since
            // query_engine.snapshot() returns whatever was most recently stored.
            let _ = tick;
            let snapshot = qe
                .snapshot()
                .map_err(|e| {
                    scharnhorst_scheduler::error::SchedulerError::Generic(e.to_string())
                })?;

            let new_gen = {
                let mut s = state.lock().map_err(|e| {
                    scharnhorst_scheduler::error::SchedulerError::Generic(e.to_string())
                })?;
                s.last_snapshot_generation += 1;
                s.last_snapshot_generation
            };

            vm.refresh(snapshot, new_gen)
                .map_err(|e| {
                    scharnhorst_scheduler::error::SchedulerError::Generic(e.to_string())
                })?;

            let mut s = state.lock().map_err(|e| {
                scharnhorst_scheduler::error::SchedulerError::Generic(e.to_string())
            })?;
            s.signal_received = false;
            Ok(())
        })
    }

    pub fn register_with_scheduler(
        &self,
        scheduler: &Scheduler,
        name: impl Into<String>,
    ) -> BevyBridgeResult<RefreshSignalHandle> {
        let handle = scheduler
            .register_consumer(name, self.callback())
            .map_err(|e| BevyBridgeError::Scheduler(e.to_string()))?;
        Ok(handle)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RefreshHandlerConfig {
    pub consumer_name: String,
}

impl RefreshHandlerConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            consumer_name: name.into(),
        }
    }

    pub fn build_and_register(
        &self,
        view_model: Arc<ViewModel>,
        query_engine: Arc<QueryEngine>,
        arrow_store: Arc<ArrowStore>,
        scheduler: &Scheduler,
    ) -> BevyBridgeResult<RefreshSignalHandle> {
        let handler = SnapshotRefreshHandler::new(view_model, query_engine, arrow_store);
        handler.register_with_scheduler(scheduler, self.consumer_name.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_core::Tick;

    fn make_handler() -> (Arc<ViewModel>, Arc<QueryEngine>, Arc<ArrowStore>) {
        let vm = Arc::new(ViewModel::new());
        let registry = scharnhorst_schema::SchemaRegistry::new();
        let qe = Arc::new(QueryEngine::new(registry));
        let store = Arc::new(ArrowStore::default());
        (vm, qe, store)
    }

    #[test]
    fn refresh_handler_initial_state() -> BevyBridgeResult<()> {
        let (vm, qe, store) = make_handler();
        let handler = SnapshotRefreshHandler::new(vm, qe, store);
        assert!(!handler.should_refresh()?);
        assert_eq!(handler.current_generation()?, 0);
        Ok(())
    }

    #[test]
    fn refresh_handler_on_signal_and_should_refresh() -> BevyBridgeResult<()> {
        let (vm, qe, store) = make_handler();
        let handler = SnapshotRefreshHandler::new(vm, qe, store);
        handler.on_refresh_signal()?;
        assert!(handler.should_refresh()?);
        Ok(())
    }

    #[test]
    fn refresh_handler_refresh_snapshot() -> BevyBridgeResult<()> {
        let (vm, qe, store) = make_handler();
        let handler = SnapshotRefreshHandler::new(vm.clone(), qe, store);
        handler.on_refresh_signal()?;
        assert!(handler.should_refresh()?);

        let snapshot = WorldSnapshot::new(Tick(7));
        handler.refresh_snapshot(snapshot)?;

        assert!(!handler.should_refresh()?);
        assert_eq!(handler.current_generation()?, 1);
        assert_eq!(vm.generation()?, 1);
        assert_eq!(vm.latest_tick()?, Some(Tick(7)));
        Ok(())
    }

    #[test]
    fn refresh_handler_callback_runs_without_panic() {
        let (vm, qe, store) = make_handler();
        let handler = SnapshotRefreshHandler::new(vm, qe, store);
        let cb = handler.callback();
        let result = cb(1, 1);
        assert!(result.is_err());
    }

    #[test]
    fn refresh_handler_config_builds() {
        let config = RefreshHandlerConfig::new("bevy_bridge");
        assert_eq!(config.consumer_name, "bevy_bridge");
    }
}