use scharnhorst_arrow_store::{ArrowStore, WorldView};
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_scheduler::refresh_signal::{RefreshCallback, RefreshSignalHandle};
use scharnhorst_scheduler::Scheduler;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{BevyBridgeError, BevyBridgeResult};
use crate::sync::ViewModel;

#[derive(Debug, Clone)]
pub struct SnapshotRefreshHandler {
    view_model: Arc<ViewModel>,
    query_engine: Arc<QueryEngine>,
    #[allow(dead_code)]
    arrow_store: Arc<ArrowStore>,
    signal_received: Arc<AtomicBool>,
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
            signal_received: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn on_refresh_signal(&self) -> BevyBridgeResult<()> {
        self.signal_received.store(true, Ordering::Release);
        Ok(())
    }

    pub fn should_refresh(&self) -> BevyBridgeResult<bool> {
        Ok(self.signal_received.load(Ordering::Acquire))
    }

    pub fn refresh_snapshot(&self, new_view: WorldView) -> BevyBridgeResult<()> {
        let generation = self.view_model.generation()?.saturating_add(1);

        self.view_model.refresh(new_view, generation)?;

        self.signal_received.store(false, Ordering::Release);

        Ok(())
    }

    pub fn current_generation(&self) -> BevyBridgeResult<u64> {
        self.view_model.generation()
    }

    pub fn callback(&self) -> RefreshCallback {
        let vm = Arc::clone(&self.view_model);
        let qe = Arc::clone(&self.query_engine);
        let sig = Arc::clone(&self.signal_received);
        Arc::new(move |_tick, generation| {
            let snapshot = qe.snapshot().map_err(|e| {
                scharnhorst_scheduler::error::SchedulerError::Generic(e.to_string())
            })?;

            vm.refresh(snapshot, generation).map_err(|e| {
                scharnhorst_scheduler::error::SchedulerError::Generic(e.to_string())
            })?;

            sig.store(false, Ordering::Release);
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

        let view = WorldView::new(Arc::new(scharnhorst_arrow_store::WorldSnapshot::new(Tick(
            7,
        ))));
        handler.refresh_snapshot(view)?;

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
