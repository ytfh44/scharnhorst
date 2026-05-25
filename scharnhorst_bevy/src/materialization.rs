use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::DataType;
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

    /// Returns `true` when the filter allows a row based on its column values.
    ///
    /// `AllRows` and `RowIdRange` always return `true` (they are row-id-based
    /// filters). `ColumnEquals` extracts the named column from the batch at
    /// `row_offset` and compares it to the stored value.
    pub fn matches_value(&self, batch: &RecordBatch, row_offset: usize) -> bool {
        match self {
            MaterializationFilter::AllRows => true,
            MaterializationFilter::RowIdRange { .. } => true,
            MaterializationFilter::ColumnEquals { column, value } => {
                arrow_value_at(batch, column, row_offset)
                    .map(|v| &v == value)
                    .unwrap_or(false)
            }
        }
    }
}

/// Extracts the value at `(column, row_offset)` from a [`RecordBatch`] as
/// a `serde_json::Value`. Returns `None` when the column is missing, the
/// offset is out of bounds, or the Arrow type is unsupported.
fn arrow_value_at(
    batch: &RecordBatch,
    column: &str,
    row_offset: usize,
) -> Option<serde_json::Value> {
    let col_idx = batch.schema().index_of(column).ok()?;
    let array = batch.column(col_idx);
    if row_offset >= array.len() {
        return None;
    }
    if array.is_null(row_offset) {
        return Some(serde_json::Value::Null);
    }
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| serde_json::json!(arr.value(row_offset))),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|arr| serde_json::json!(arr.value(row_offset))),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|arr| serde_json::json!(arr.value(row_offset))),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|arr| serde_json::json!(arr.value(row_offset))),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| serde_json::json!(arr.value(row_offset))),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .map(|arr| serde_json::json!(arr.value(row_offset))),
        _ => None,
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

    pub fn lookup(&self, table_name: &str, row_id: RowId) -> BevyBridgeResult<Option<Entity>> {
        let map = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(map.get(&(table_name.to_owned(), row_id)).copied())
    }

    pub fn unregister(&self, table_name: &str, row_id: RowId) -> BevyBridgeResult<Option<Entity>> {
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
            let mut row_ids: Vec<RowId> = pm.row_ids().collect();
            row_ids.sort_by_key(|row_id| row_id.as_u64());

            for row_id in row_ids {
                if spawned_count >= max {
                    break;
                }

                if let Some(ref filter) = config.filter {
                    if !filter.matches_row_id(row_id) {
                        continue;
                    }
                    if let MaterializationFilter::ColumnEquals { .. } = filter {
                        let (batch_idx, row_offset) = match pm.position_of(row_id) {
                            Some(pos) => pos,
                            None => continue,
                        };
                        let batch = match view.batches().nth(batch_idx) {
                            Some(b) => b,
                            None => continue,
                        };
                        if !filter.matches_value(batch, row_offset) {
                            continue;
                        }
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

    pub fn dematerialize(&self, commands: &mut Commands, entity: Entity) -> BevyBridgeResult<()> {
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
            None => Err(BevyBridgeError::EntityNotFound(
                entity.index().index() as u64
            )),
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
    use bevy::prelude::World;
    use scharnhorst_core::{RowPositionMap, Tick};
    use scharnhorst_query::engine::QueryEngine;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
    use std::sync::Arc;

    #[test]
    fn materialization_config_new() {
        let config = MaterializationConfig::new("provinces");
        assert_eq!(config.table, "provinces");
        assert!(config.filter.is_none());
        assert!(config.max_entities.is_none());
    }

    #[test]
    fn materialization_config_with_filter() {
        let config =
            MaterializationConfig::new("provinces").with_filter(MaterializationFilter::AllRows);
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
        reg.register(
            "t",
            RowId::new(1),
            bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid"),
        )?;
        assert_eq!(reg.len()?, 1);
        assert!(!reg.is_empty()?);
        Ok(())
    }

    #[test]
    fn registry_table_names() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        reg.register(
            "a",
            RowId::new(1),
            bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid"),
        )?;
        reg.register(
            "a",
            RowId::new(2),
            bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid"),
        )?;
        reg.register(
            "b",
            RowId::new(3),
            bevy::prelude::Entity::from_raw_u32(3).expect("Entity index must be valid"),
        )?;
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

    // ------------------------------------------------------------------
    // arrow_value_at uncovered-type tests
    // ------------------------------------------------------------------

    #[test]
    fn arrow_value_at_uint64_extracts_value() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(UInt64Array::from(vec![42u64, 99u64]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "cnt",
                DataType::UInt64,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        let v = arrow_value_at(&batch, "cnt", 0);
        assert_eq!(v, Some(serde_json::json!(42u64)));
        let v2 = arrow_value_at(&batch, "cnt", 1);
        assert_eq!(v2, Some(serde_json::json!(99u64)));
    }

    #[test]
    fn arrow_value_at_large_utf8_extracts_value() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(arrow_array::LargeStringArray::from(vec!["hello", "world"]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "text",
                DataType::LargeUtf8,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        let v = arrow_value_at(&batch, "text", 0);
        assert_eq!(v, Some(serde_json::json!("hello")));
        let v2 = arrow_value_at(&batch, "text", 1);
        assert_eq!(v2, Some(serde_json::json!("world")));
    }

    #[test]
    fn arrow_value_at_null_value_returns_null() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(Int64Array::from(vec![None::<i64>, Some(7i64)]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "x",
                DataType::Int64,
                true,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        let v_null = arrow_value_at(&batch, "x", 0);
        assert_eq!(v_null, Some(serde_json::Value::Null));
        let v_ok = arrow_value_at(&batch, "x", 1);
        assert_eq!(v_ok, Some(serde_json::json!(7)));
    }

    #[test]
    fn arrow_value_at_unsupported_type_returns_none() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(arrow_array::Int32Array::from(vec![1i32, 2i32]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "small",
                DataType::Int32,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        let v = arrow_value_at(&batch, "small", 0);
        assert_eq!(v, None);
    }

    #[test]
    fn arrow_value_at_out_of_bounds_returns_none() {
        let batch = int_batch(&[1, 2]);
        let v = arrow_value_at(&batch, "id", 99);
        assert_eq!(v, None);
    }

    #[test]
    fn arrow_value_at_missing_column_returns_none() {
        let batch = int_batch(&[1]);
        let v = arrow_value_at(&batch, "does_not_exist", 0);
        assert_eq!(v, None);
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn arrow_value_at_float64_extracts_value() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(Float64Array::from(vec![1.5f64, 3.14]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "score",
                DataType::Float64,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        assert_eq!(
            arrow_value_at(&batch, "score", 0),
            Some(serde_json::json!(1.5))
        );
        assert_eq!(
            arrow_value_at(&batch, "score", 1),
            Some(serde_json::json!(3.14))
        );
    }

    #[test]
    fn arrow_value_at_boolean_extracts_value() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(BooleanArray::from(vec![true, false]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "active",
                DataType::Boolean,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        assert_eq!(
            arrow_value_at(&batch, "active", 0),
            Some(serde_json::json!(true))
        );
        assert_eq!(
            arrow_value_at(&batch, "active", 1),
            Some(serde_json::json!(false))
        );
    }

    #[test]
    fn arrow_value_at_utf8_extracts_value() {
        let array: arrow_array::ArrayRef =
            std::sync::Arc::new(StringArray::from(vec!["hello", "world"]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "text",
                DataType::Utf8,
                false,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        assert_eq!(
            arrow_value_at(&batch, "text", 0),
            Some(serde_json::json!("hello"))
        );
        assert_eq!(
            arrow_value_at(&batch, "text", 1),
            Some(serde_json::json!("world"))
        );
    }

    #[test]
    fn matches_value_column_equals_null_value_is_compared() {
        // When the arrow column contains a null, arrow_value_at returns Null.
        // ColumnEquals with value=Null should match.
        let filter = MaterializationFilter::ColumnEquals {
            column: "x".to_string(),
            value: serde_json::Value::Null,
        };
        let array: arrow_array::ArrayRef = std::sync::Arc::new(Int64Array::from(vec![None::<i64>]));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "x",
                DataType::Int64,
                true,
            )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("RecordBatch::try_new");
        assert!(filter.matches_value(&batch, 0));
    }

    // ------------------------------------------------------------------
    // Path: dematerialize with entity-not-found
    // ------------------------------------------------------------------

    #[test]
    fn dematerialize_nonexistent_entity_returns_entity_not_found() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let entity = bevy::prelude::Entity::from_raw_u32(99).expect("Entity index must be valid");
        let result = reg.dematerialize(&mut world.commands(), entity);
        assert!(
            matches!(result, Err(BevyBridgeError::EntityNotFound(_))),
            "expected EntityNotFound, got {:?}",
            result
        );
    }

    #[test]
    fn unregister_nonexistent_mapping_returns_none() -> BevyBridgeResult<()> {
        let reg = EntityMaterializationRegistry::new();
        let removed = reg.unregister("ghost", RowId::new(0))?;
        assert_eq!(removed, None);
        Ok(())
    }

    #[test]
    fn dematerialize_all_clears_registry_and_despawns() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let e1 = bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
        let e2 = bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid");
        reg.register("t", RowId::new(1), e1).unwrap();
        reg.register("t", RowId::new(2), e2).unwrap();
        assert_eq!(reg.len().unwrap(), 2);

        reg.dematerialize_all(&mut world.commands()).unwrap();
        assert_eq!(reg.len().unwrap(), 0);
        assert!(reg.is_empty().unwrap());
    }

    #[test]
    fn materialize_request_row_id_equality() {
        let req1 = MaterializeRequest::new(RowId::new(1), "t");
        let req2 = MaterializeRequest::new(RowId::new(1), "t");
        assert_eq!(req1, req2);
        let req3 = MaterializeRequest::new(RowId::new(2), "t");
        assert_ne!(req1, req3);
    }

    fn string_batch(data: &[&str]) -> RecordBatch {
        let array = StringArray::from(Vec::from(data));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "name",
                DataType::Utf8,
                false,
            )]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(array)])
            .expect("RecordBatch::try_new")
    }

    fn int_batch(data: &[i64]) -> RecordBatch {
        let array = Int64Array::from(Vec::from(data));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                DataType::Int64,
                false,
            )]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(array)])
            .expect("RecordBatch::try_new")
    }

    fn bool_batch(data: &[bool]) -> RecordBatch {
        let array = BooleanArray::from(Vec::from(data));
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "active",
                DataType::Boolean,
                false,
            )]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(array)])
            .expect("RecordBatch::try_new")
    }

    #[test]
    fn matches_value_all_rows_always_true() {
        let filter = MaterializationFilter::AllRows;
        let batch = string_batch(&["Alice"]);
        assert!(filter.matches_value(&batch, 0));
    }

    #[test]
    fn matches_value_row_id_range_always_true() {
        let filter = MaterializationFilter::RowIdRange {
            start: RowId::new(0),
            end: RowId::new(10),
        };
        let batch = string_batch(&["Bob"]);
        assert!(filter.matches_value(&batch, 0));
    }

    #[test]
    fn matches_value_column_equals_string_match() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "name".to_string(),
            value: serde_json::json!("Alice"),
        };
        let batch = string_batch(&["Alice", "Bob"]);
        assert!(filter.matches_value(&batch, 0));
    }

    #[test]
    fn matches_value_column_equals_string_mismatch() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "name".to_string(),
            value: serde_json::json!("Alice"),
        };
        let batch = string_batch(&["Bob", "Alice"]);
        assert!(!filter.matches_value(&batch, 0));
    }

    #[test]
    fn matches_value_column_equals_int_match() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "id".to_string(),
            value: serde_json::json!(42),
        };
        let batch = int_batch(&[10, 42, 99]);
        assert!(filter.matches_value(&batch, 1));
    }

    #[test]
    fn matches_value_column_equals_int_mismatch() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "id".to_string(),
            value: serde_json::json!(7),
        };
        let batch = int_batch(&[10, 42]);
        assert!(!filter.matches_value(&batch, 1));
    }

    #[test]
    fn matches_value_column_equals_bool_match() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "active".to_string(),
            value: serde_json::json!(true),
        };
        let batch = bool_batch(&[true, false]);
        assert!(filter.matches_value(&batch, 0));
        assert!(!filter.matches_value(&batch, 1));
    }

    #[test]
    fn matches_value_column_missing_returns_false() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "missing".to_string(),
            value: serde_json::json!("x"),
        };
        let batch = string_batch(&["hello"]);
        assert!(!filter.matches_value(&batch, 0));
    }

    #[test]
    fn matches_value_out_of_bounds_returns_false() {
        let filter = MaterializationFilter::ColumnEquals {
            column: "name".to_string(),
            value: serde_json::json!("Alice"),
        };
        let batch = string_batch(&["Alice"]);
        assert!(!filter.matches_value(&batch, 99));
    }

    // ------------------------------------------------------------------
    // materialize() integration tests (require QueryEngine)
    // ------------------------------------------------------------------

    /// Builds a QueryEngine with a table containing an `id` (Int64) and
    /// `name` (Utf8) column, populated with the given strings.
    fn make_engine_with_rows(table_name: &str, names: &[&str]) -> QueryEngine {
        let spec = TableSpec::new(table_name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .expect("table spec")
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .expect("table spec");

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("name", DataType::Utf8, false),
        ]));

        let ids: Vec<i64> = (0..names.len() as i64).collect();
        let id_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(ids));
        let name_arr: arrow_array::ArrayRef = Arc::new(StringArray::from(Vec::from(names)));
        let batch =
            RecordBatch::try_new(schema, vec![id_arr, name_arr]).expect("RecordBatch::try_new");

        let mut pm = RowPositionMap::new();
        for i in 0..names.len() {
            pm.insert(RowId::new(i as u64), 0, i);
        }

        let engine = QueryEngine::new(SchemaRegistry::new());
        engine.register_table_schema(spec).expect("register schema");
        engine
            .ingest_snapshot(Tick(1), table_name, vec![batch], pm)
            .expect("ingest snapshot");
        engine
    }

    #[test]
    fn materialize_column_equals_no_match_creates_no_entities() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice", "Bob"]);

        let config =
            MaterializationConfig::new("units").with_filter(MaterializationFilter::ColumnEquals {
                column: "name".to_string(),
                value: serde_json::json!("NonExistent"),
            });
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert!(
            spawned.is_empty(),
            "no rows should match a non-existent ColumnEquals value"
        );
        assert!(reg.is_empty().unwrap());
    }

    #[test]
    fn materialize_all_rows_creates_entities() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice", "Bob", "Charlie"]);

        let config =
            MaterializationConfig::new("units").with_filter(MaterializationFilter::AllRows);
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(spawned.len(), 3);
        assert_eq!(reg.len().unwrap(), 3);
    }

    #[test]
    fn materialize_twice_no_duplicates() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice", "Bob"]);

        let config =
            MaterializationConfig::new("units").with_filter(MaterializationFilter::AllRows);
        reg.register_config(config).unwrap();

        let first = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(reg.len().unwrap(), 2);

        // second materialize should skip already-existing entities
        let second = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert!(
            second.is_empty(),
            "second materialize must not create duplicates"
        );
        assert_eq!(reg.len().unwrap(), 2);
    }

    #[test]
    fn materialize_respects_max_entities() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice", "Bob", "Charlie", "Dave"]);

        let config = MaterializationConfig::new("units")
            .with_filter(MaterializationFilter::AllRows)
            .with_max_entities(2);
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(spawned.len(), 2);
        assert_eq!(reg.len().unwrap(), 2);
    }

    /// May-fail: max_entities must pick a deterministic RowId prefix.
    #[test]
    fn materialize_max_entities_uses_row_id_order() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let spec = TableSpec::new("units")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .expect("table spec")
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .expect("table spec");
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("name", DataType::Utf8, false),
        ]));
        let row_ids = [100_u64, 1, 50, 2, 75, 3];
        let id_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(
            row_ids.iter().map(|id| *id as i64).collect::<Vec<_>>(),
        ));
        let name_arr: arrow_array::ArrayRef =
            Arc::new(StringArray::from(vec!["A", "B", "C", "D", "E", "F"]));
        let batch =
            RecordBatch::try_new(schema, vec![id_arr, name_arr]).expect("RecordBatch::try_new");
        let mut pm = RowPositionMap::new();
        for (offset, row_id) in row_ids.iter().enumerate() {
            pm.insert(RowId::new(*row_id), 0, offset);
        }
        let engine = QueryEngine::new(SchemaRegistry::new());
        engine.register_table_schema(spec).expect("register schema");
        engine
            .ingest_snapshot(Tick(1), "units", vec![batch], pm)
            .expect("ingest snapshot");

        let config = MaterializationConfig::new("units")
            .with_filter(MaterializationFilter::AllRows)
            .with_max_entities(3);
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(spawned.len(), 3);
        assert!(reg.lookup("units", RowId::new(1)).unwrap().is_some());
        assert!(reg.lookup("units", RowId::new(2)).unwrap().is_some());
        assert!(reg.lookup("units", RowId::new(3)).unwrap().is_some());
        assert!(reg.lookup("units", RowId::new(50)).unwrap().is_none());
        assert!(reg.lookup("units", RowId::new(75)).unwrap().is_none());
        assert!(reg.lookup("units", RowId::new(100)).unwrap().is_none());
    }

    #[test]
    fn materialize_row_id_range_filter() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice", "Bob", "Charlie", "Dave"]);

        let config =
            MaterializationConfig::new("units").with_filter(MaterializationFilter::RowIdRange {
                start: RowId::new(1),
                end: RowId::new(2),
            });
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(spawned.len(), 2); // RowId 1 and 2
        assert!(reg.lookup("units", RowId::new(0)).unwrap().is_none());
        assert!(reg.lookup("units", RowId::new(1)).unwrap().is_some());
        assert!(reg.lookup("units", RowId::new(2)).unwrap().is_some());
        assert!(reg.lookup("units", RowId::new(3)).unwrap().is_none());
    }

    #[test]
    fn materialize_column_equals_match_creates_entities() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice", "Bob", "Alice"]);

        let config =
            MaterializationConfig::new("units").with_filter(MaterializationFilter::ColumnEquals {
                column: "name".to_string(),
                value: serde_json::json!("Alice"),
            });
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(spawned.len(), 2);
        assert_eq!(reg.len().unwrap(), 2);
    }

    #[test]
    fn dematerialize_removes_entity_from_registry() {
        let reg = EntityMaterializationRegistry::new();
        let mut world = World::default();
        let engine = make_engine_with_rows("units", &["Alice"]);

        let config =
            MaterializationConfig::new("units").with_filter(MaterializationFilter::AllRows);
        reg.register_config(config).unwrap();

        let spawned = reg.materialize(&mut world.commands(), &engine).unwrap();
        assert_eq!(spawned.len(), 1);
        let entity = spawned[0];

        reg.dematerialize(&mut world.commands(), entity).unwrap();
        assert!(reg.is_empty().unwrap());
        assert!(reg.lookup("units", RowId::new(0)).unwrap().is_none());
    }
}
