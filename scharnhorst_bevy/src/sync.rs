use bevy::prelude::{Entity, Query, Resource};
use scharnhorst_arrow_store::WorldView;
use scharnhorst_core::{RowId, Tick};
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_query::unified_read::ReadRequest;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{BevyBridgeError, BevyBridgeResult};
use crate::view_of::ViewOf;

#[derive(Debug, Clone, Default)]
struct ViewModelState {
    snapshot: Option<WorldView>,
    latest_tick: Option<Tick>,
}

#[derive(Debug, Clone, Resource)]
pub struct ViewModel {
    state: Arc<Mutex<ViewModelState>>,
    generation: Arc<AtomicU64>,
}

impl Default for ViewModel {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(ViewModelState::default())),
            generation: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ViewModel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn refresh(&self, snapshot: WorldView, generation: u64) -> BevyBridgeResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;

        let snap_tick = snapshot.tick();
        state.snapshot = Some(snapshot);
        state.latest_tick = Some(snap_tick);
        self.generation.store(generation, Ordering::Relaxed);
        Ok(())
    }

    pub fn snapshot(&self) -> BevyBridgeResult<Option<WorldView>> {
        let state = self
            .state
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(state.snapshot.clone())
    }

    pub fn generation(&self) -> BevyBridgeResult<u64> {
        Ok(self.generation.load(Ordering::Relaxed))
    }

    pub fn latest_tick(&self) -> BevyBridgeResult<Option<Tick>> {
        let state = self
            .state
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(state.latest_tick)
    }

    pub fn read_table_via_query_engine(
        &self,
        query_engine: &QueryEngine,
        table_name: &str,
    ) -> BevyBridgeResult<Option<scharnhorst_query::unified_read::TableReadView>> {
        let tick = self
            .latest_tick()?
            .ok_or(BevyBridgeError::SnapshotNotAvailable(0))?;

        let request = ReadRequest::new(tick).with_table(table_name);
        let response = query_engine
            .read(request)
            .map_err(|e| BevyBridgeError::QueryEngine(e.to_string()))?;

        Ok(response.get(table_name).ok().cloned())
    }
}

#[derive(Debug, Clone, Default, Resource)]
pub struct SyncState {
    view_models: Arc<Mutex<HashMap<Entity, SyncEntry>>>,
    dirty_entities: Arc<Mutex<HashSet<Entity>>>,
}

/// Tracks synchronization state for a materialized entity.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SyncEntry {
    table: String,
    generation: u64,
    last_sync_tick: Tick,
}

impl SyncState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_entity(
        &self,
        entity: Entity,
        table: impl Into<String>,
        generation: u64,
        last_sync_tick: Tick,
    ) -> BevyBridgeResult<()> {
        let mut models = self
            .view_models
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        models.insert(
            entity,
            SyncEntry {
                table: table.into(),
                generation,
                last_sync_tick,
            },
        );
        Ok(())
    }

    pub fn unregister_entity(&self, entity: Entity) -> BevyBridgeResult<()> {
        let mut models = self
            .view_models
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let mut dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        models.remove(&entity);
        dirty.remove(&entity);
        Ok(())
    }

    pub fn mark_dirty(&self, entity: Entity) -> BevyBridgeResult<()> {
        let mut dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        dirty.insert(entity);
        Ok(())
    }

    pub fn mark_all_dirty(&self) -> BevyBridgeResult<()> {
        let models = self
            .view_models
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let mut dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        dirty.extend(models.keys().copied());
        Ok(())
    }

    pub fn is_dirty(&self, entity: Entity) -> BevyBridgeResult<bool> {
        let dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(dirty.contains(&entity))
    }

    pub fn dirty_count(&self) -> BevyBridgeResult<usize> {
        let dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(dirty.len())
    }

    pub fn entity_count(&self) -> BevyBridgeResult<usize> {
        let models = self
            .view_models
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(models.len())
    }

    pub fn sync_all(
        &self,
        view_model: &ViewModel,
        query_engine: &QueryEngine,
    ) -> BevyBridgeResult<usize> {
        let models = self
            .view_models
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let mut dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;

        let mut synced = 0usize;
        let mut errors: Vec<String> = Vec::new();
        let entities_to_sync: Vec<Entity> = dirty.iter().copied().collect();

        for entity in entities_to_sync {
            if let Some(entry) = models.get(&entity) {
                match view_model.read_table_via_query_engine(query_engine, &entry.table) {
                    Ok(_) => {
                        // In full implementation, component-specific sync fields
                        // would be applied here. For now, we mark the sync complete.
                        synced = synced.saturating_add(1);
                    }
                    Err(e) => {
                        errors.push(e.to_string());
                    }
                }
            }
        }

        dirty.clear();

        if errors.is_empty() {
            Ok(synced)
        } else {
            Err(BevyBridgeError::SyncFailed(errors.join("; ")))
        }
    }

    pub fn clear_dirty(&self) -> BevyBridgeResult<()> {
        let mut dirty = self
            .dirty_entities
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        dirty.clear();
        Ok(())
    }
}

pub trait SyncField<C>: Send + Sync {
    fn fetch(
        &self,
        view_model: &ViewModel,
        query_engine: &QueryEngine,
        row_id: RowId,
    ) -> BevyBridgeResult<Option<C>>;
}

pub fn sync_table_components<C, S>(
    view_model: &ViewModel,
    query_engine: &QueryEngine,
    sync_field: &S,
    query: &mut Query<(Entity, &ViewOf, &mut C)>,
    table_name: &str,
) -> BevyBridgeResult<usize>
where
    C: bevy::prelude::Component<Mutability = bevy::ecs::component::Mutable>,
    S: SyncField<C>,
{
    let mut updated = 0usize;

    for (_entity, view, mut component) in query.iter_mut() {
        if view.table_name() != table_name {
            continue;
        }

        if let Some(new_value) = sync_field.fetch(view_model, query_engine, view.row_id())? {
            *component = new_value;
            updated = updated.saturating_add(1);
        }
    }

    Ok(updated)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NullSyncField;

impl<C: Default + bevy::prelude::Component> SyncField<C> for NullSyncField {
    fn fetch(
        &self,
        _view_model: &ViewModel,
        _query_engine: &QueryEngine,
        _row_id: RowId,
    ) -> BevyBridgeResult<Option<C>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_model_initially_empty() -> BevyBridgeResult<()> {
        let vm = ViewModel::new();
        assert_eq!(vm.generation()?, 0);
        assert_eq!(vm.latest_tick()?, None);
        assert!(vm.snapshot()?.is_none());
        Ok(())
    }

    #[test]
    fn view_model_refresh_updates_state() -> BevyBridgeResult<()> {
        let vm = ViewModel::new();
        let snapshot = Arc::new(scharnhorst_arrow_store::WorldSnapshot::new(Tick(7)));
        let view = WorldView::new(snapshot);
        vm.refresh(view, 3)?;
        assert_eq!(vm.generation()?, 3);
        assert_eq!(vm.latest_tick()?, Some(Tick(7)));
        assert!(vm.snapshot()?.is_some());
        Ok(())
    }

    #[test]
    fn view_model_read_table_without_snapshot_fails() {
        let vm = ViewModel::new();
        let registry = scharnhorst_schema::SchemaRegistry::new();
        let qe = QueryEngine::new(registry);
        let result = vm.read_table_via_query_engine(&qe, "provinces");
        assert!(
            matches!(result, Err(BevyBridgeError::SnapshotNotAvailable(0))),
            "expected SnapshotNotAvailable, got {:?}",
            result
        );
    }

    #[test]
    fn sync_state_register_and_count() -> BevyBridgeResult<()> {
        let state = SyncState::new();
        let entity = Entity::from_raw_u32(1).expect("Entity index must be valid");
        state.register_entity(entity, "provinces", 0, Tick(0))?;
        assert_eq!(state.entity_count()?, 1);
        Ok(())
    }

    #[test]
    fn sync_state_unregister_removes() -> BevyBridgeResult<()> {
        let state = SyncState::new();
        let entity = Entity::from_raw_u32(1).expect("Entity index must be valid");
        state.register_entity(entity, "provinces", 0, Tick(0))?;
        state.unregister_entity(entity)?;
        assert_eq!(state.entity_count()?, 0);
        Ok(())
    }

    #[test]
    fn sync_state_mark_dirty() -> BevyBridgeResult<()> {
        let state = SyncState::new();
        let entity = Entity::from_raw_u32(1).expect("Entity index must be valid");
        state.register_entity(entity, "provinces", 0, Tick(0))?;
        state.mark_dirty(entity)?;
        assert!(state.is_dirty(entity)?);
        assert_eq!(state.dirty_count()?, 1);
        Ok(())
    }

    #[test]
    fn sync_state_mark_all_dirty() -> BevyBridgeResult<()> {
        let state = SyncState::new();
        let e1 = Entity::from_raw_u32(1).expect("Entity index must be valid");
        let e2 = Entity::from_raw_u32(2).expect("Entity index must be valid");
        state.register_entity(e1, "a", 0, Tick(0))?;
        state.register_entity(e2, "b", 0, Tick(0))?;
        state.mark_all_dirty()?;
        assert!(state.is_dirty(e1)?);
        assert!(state.is_dirty(e2)?);
        assert_eq!(state.dirty_count()?, 2);
        Ok(())
    }

    #[test]
    fn sync_state_clear_dirty() -> BevyBridgeResult<()> {
        let state = SyncState::new();
        let entity = Entity::from_raw_u32(1).expect("Entity index must be valid");
        state.register_entity(entity, "provinces", 0, Tick(0))?;
        state.mark_dirty(entity)?;
        state.clear_dirty()?;
        assert!(!state.is_dirty(entity)?);
        assert_eq!(state.dirty_count()?, 0);
        Ok(())
    }

    #[test]
    fn null_sync_field_returns_none() -> BevyBridgeResult<()> {
        let vm = ViewModel::new();
        let registry = scharnhorst_schema::SchemaRegistry::new();
        let qe = QueryEngine::new(registry);
        let result: Option<bevy::prelude::Transform> =
            NullSyncField.fetch(&vm, &qe, RowId::new(1))?;
        assert_eq!(result, None);
        Ok(())
    }
}
