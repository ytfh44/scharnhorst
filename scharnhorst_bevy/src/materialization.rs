use bevy::prelude::{Commands, Entity, Query, Res, Resource};
use scharnhorst_core::RowId;
use scharnhorst_query::engine::QueryEngine;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{BevyBridgeError, BevyBridgeResult};
use crate::view_of::ViewOf;

#[derive(Debug, Clone, Resource)]
pub struct MaterializationConfig {
    pub table: String,
    pub filter: Option<MaterializationFilter>,
    pub max_entities: Option<usize>,
}

impl MaterializationConfig {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filter: None,
            max_entities: None,
        }
    }

    pub fn with_filter(mut self, filter: MaterializationFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_max_entities(mut self, max: usize) -> Self {
        self.max_entities = Some(max);
        self
    }
}

#[derive(Debug, Clone)]
pub enum MaterializationFilter {
    ColumnEquals {
        column: String,
        value: serde_json::Value,
    },
    RowIdRange {
        start: RowId,
        end: RowId,
    },
    AllRows,
}

impl MaterializationFilter {
    pub fn matches_row_id(&self, row_id: RowId) -> bool {
        match self {
            MaterializationFilter::AllRows => true,
            MaterializationFilter::RowIdRange { start, end } => {
                row_id.as_u64() >= start.as_u64() && row_id.as_u64() <= end.as_u64()
            }
            MaterializationFilter::ColumnEquals { .. } => true,
        }
    }
}

#[derive(Debug, Clone, Default, Resource)]
pub struct EntityMaterializationRegistry {
    inner: Arc<Mutex<HashMap<(String, RowId), Entity>>>,
    configs: Arc<Mutex<Vec<MaterializationConfig>>>,
}

impl EntityMaterializationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_config(&self, config: MaterializationConfig) -> BevyBridgeResult<()> {
        let mut configs = self
            .configs
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        configs.push(config);
        Ok(())
    }

    pub fn configs(&self) -> BevyBridgeResult<Vec<MaterializationConfig>> {
        let configs = self
            .configs
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(configs.clone())
    }

    pub fn register(
        &self,
        table_name: impl Into<String>,
        row_id: RowId,
        entity: Entity,
    ) -> BevyBridgeResult<()> {
        let mut map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        map.insert((table_name.into(), row_id), entity);
        Ok(())
    }

    pub fn lookup(
        &self,
        table_name: &str,
        row_id: RowId,
    ) -> BevyBridgeResult<Option<Entity>> {
        let map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(map.get(&(table_name.to_owned(), row_id)).copied())
    }

    pub fn unregister(
        &self,
        table_name: &str,
        row_id: RowId,
    ) -> BevyBridgeResult<Option<Entity>> {
        let mut map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(map.remove(&(table_name.to_owned(), row_id)))
    }

    pub fn len(&self) -> BevyBridgeResult<usize> {
        let map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(map.len())
    }

    pub fn is_empty(&self) -> BevyBridgeResult<bool> {
        self.len().map(|n| n == 0)
    }

    pub fn table_names(&self) -> BevyBridgeResult<Vec<String>> {
        let map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let names: Vec<String> = map
            .keys()
            .map(|(table, _)| table.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        Ok(names)
    }

    pub fn materialize(
        &self,
        commands: &mut Commands,
        query_engine: &QueryEngine,
    ) -> BevyBridgeResult<Vec<Entity>> {
        let configs = self.configs()?;
        let mut spawned = Vec::new();

        for config in configs {
            let tick = query_engine
                .latest_tick()
                .map_err(|e| BevyBridgeError::QueryEngine(e.to_string()))?
                .ok_or(BevyBridgeError::SnapshotNotAvailable(0))?;

            let view = query_engine
                .read_single_table(tick, &config.table)
                .map_err(|e| BevyBridgeError::QueryEngine(e.to_string()))?;

            let max = config.max_entities.unwrap_or(usize::MAX);
            let pm = view.position_map();
            let mut spawned_count: usize = 0;

            for row_id in pm.row_ids() {
                if spawned_count >= max {
                    break;
                }

                if let Some(ref filter) = config.filter {
                    if !filter.matches_row_id(row_id) {
                        continue;
                    }
                }

                if self.lookup(&config.table, row_id)?.is_some() {
                    continue;
                }

                let gen = tick.as_u64();
                let entity = commands
                    .spawn(ViewOf::with_generation(row_id, config.table.clone(), gen))
                    .id();

                self.register(config.table.as_str(), row_id, entity)?;
                spawned.push(entity);
                spawned_count += 1;
            }
        }

        Ok(spawned)
    }

    pub fn dematerialize(
        &self,
        commands: &mut Commands,
        entity: Entity,
    ) -> BevyBridgeResult<()> {
        let map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;

        let key = map
            .iter()
            .find(|(_, e)| **e == entity)
            .map(|(k, _)| k.clone());

        drop(map);

        match key {
            Some((table, row_id)) => {
                self.unregister(&table, row_id)?;
                commands.entity(entity).despawn();
                Ok(())
            }
            None => Err(BevyBridgeError::EntityNotFound(entity.index().index() as u64)),
        }
    }

    pub fn dematerialize_all(&self, commands: &mut Commands) -> BevyBridgeResult<()> {
        let mut map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let entities: Vec<Entity> = map.values().copied().collect();
        map.clear();
        drop(map);

        for entity in entities {
            commands.entity(entity).despawn();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializeRequest {
    pub row_id: RowId,
    pub table_name: String,
}

impl MaterializeRequest {
    pub fn new(row_id: RowId, table_name: impl Into<String>) -> Self {
        Self {
            row_id,
            table_name: table_name.into(),
        }
    }
}

pub fn materialize_entity(
    commands: &mut Commands,
    registry: &Res<EntityMaterializationRegistry>,
    request: MaterializeRequest,
) -> BevyBridgeResult<Entity> {
    if let Some(existing) = registry.lookup(&request.table_name, request.row_id)? {
        return Ok(existing);
    }

    let entity = commands
        .spawn(ViewOf::new(request.row_id, request.table_name.clone()))
        .id();

    registry.register(request.table_name, request.row_id, entity)?;
    Ok(entity)
}

pub fn dematerialize_entity(
    commands: &mut Commands,
    registry: &Res<EntityMaterializationRegistry>,
    request: MaterializeRequest,
) -> BevyBridgeResult<Entity> {
    let entity = registry
        .unregister(&request.table_name, request.row_id)?
        .ok_or(BevyBridgeError::EntityNotFound(request.row_id.as_u64()))?;

    commands.entity(entity).despawn();
    Ok(entity)
}

pub fn entities_for_table(
    query: &Query<(Entity, &ViewOf)>,
    table_name: &str,
) -> BevyBridgeResult<Vec<(Entity, RowId)>> {
    Ok(query
        .iter()
        .filter(|(_, view)| view.table_name() == table_name)
        .map(|(entity, view)| (entity, view.row_id()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialization_config_new() {
        let config = MaterializationConfig::new("provinces");
        assert_eq!(config.table, "provinces");
        assert!(config.filter.is_none());
        assert!(config.max_entities.is_none());
    }

    #[test]
    fn materialization_config_with_filter() {
        let config = MaterializationConfig::new("provinces")
            .with_filter(MaterializationFilter::AllRows);
        assert!(config.filter.is_some());
    }

    #[test]
    fn materialization_config_with_max_entities() {
        let config = MaterializationConfig::new("provinces").with_max_entities(10);
        assert_eq!(config.max_entities, Some(10));
    }

    #[test]
    fn filter_all_rows_matches_any() {
        let filter = MaterializationFilter::AllRows;
        assert!(filter.matches_row_id(RowId::new(0)));
        assert!(filter.matches_row_id(RowId::new(999)));
    }

    #[test]
    fn filter_row_id_range_matches() {
        let filter = MaterializationFilter::RowIdRange {
            start: RowId::new(5),
            end: RowId::new(10),
        };
        assert!(filter.matches_row_id(RowId::new(5)));
        assert!(filter.matches_row_id(RowId::new(7)));
        assert!(filter.matches_row_id(RowId::new(10)));
    }

    #[test]
    fn filter_row_id_range_out_of_range() {
        let filter = MaterializationFilter::RowIdRange {
            start: RowId::new(5),
            end: RowId::new(10),
        };
        assert!(!filter.matches_row_id(RowId::new(3)));
        assert!(!filter.matches_row_id(RowId::new(11)));
    }

    #[test]
    fn registry_round_trip() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        let entity = bevy::prelude::Entity::from_raw_u32(7).expect("Entity index must be valid");
        reg.register("provinces", RowId::new(99), entity)?;
        let looked_up = reg.lookup("provinces", RowId::new(99))?;
        assert_eq!(looked_up, Some(entity));
        Ok(())
    }

    #[test]
    fn registry_lookup_missing_returns_none() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        let result = reg.lookup("provinces", RowId::new(1))?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn registry_unregister_removes_mapping() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        let entity = bevy::prelude::Entity::from_raw_u32(3).expect("Entity index must be valid");
        reg.register("actors", RowId::new(5), entity)?;
        let removed = reg.unregister("actors", RowId::new(5))?;
        assert_eq!(removed, Some(entity));
        let missing = reg.lookup("actors", RowId::new(5))?;
        assert_eq!(missing, None);
        Ok(())
    }

    #[test]
    fn registry_len_and_is_empty() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        assert!(reg.is_empty()?);
        reg.register("t", RowId::new(1), bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid"))?;
        assert_eq!(reg.len()?, 1);
        assert!(!reg.is_empty()?);
        Ok(())
    }

    #[test]
    fn registry_table_names() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        reg.register("a", RowId::new(1), bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid"))?;
        reg.register("a", RowId::new(2), bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid"))?;
        reg.register("b", RowId::new(3), bevy::prelude::Entity::from_raw_u32(3).expect("Entity index must be valid"))?;
        let mut names = reg.table_names()?;
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn registry_register_config() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        reg.register_config(MaterializationConfig::new("provinces"))?;
        reg.register_config(MaterializationConfig::new("units"))?;
        let configs = reg.configs()?;
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].table, "provinces");
        assert_eq!(configs[1].table, "units");
        Ok(())
    }

    #[test]
    fn materialize_request_new() {
        let req = MaterializeRequest::new(RowId::new(5), "units");
        assert_eq!(req.row_id, RowId::new(5));
        assert_eq!(req.table_name, "units");
    }
}