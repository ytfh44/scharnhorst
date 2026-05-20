use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use scharnhorst_core::Tick;

use crate::error::ArrowStoreResult;
use crate::partition::PartitionSnapshot;
use crate::versioned_table::VersionedTable;

/// An immutable snapshot of the entire world state at a specific tick.
///
/// This is the only structure exposed to read-only consumers (e.g. the query
/// engine). It does not permit mutation and hides direct access to the
/// underlying Arrow tables.
#[derive(Debug, Clone)]
pub struct WorldSnapshot {
    tick: Tick,
    /// Table name -> reference to the versioned table at this tick.
    tables: HashMap<String, Arc<VersionedTable>>,
    /// Cached view of partition snapshots per table.
    partition_views: HashMap<String, HashMap<String, PartitionSnapshot>>,
}

impl WorldSnapshot {
    pub fn new(tick: Tick) -> Self {
        Self {
            tick,
            tables: HashMap::new(),
            partition_views: HashMap::new(),
        }
    }

    pub fn tick(&self) -> Tick {
        self.tick
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn register_table(&mut self, name: impl Into<String>, table: Arc<VersionedTable>) {
        self.tables.insert(name.into(), table);
    }

    pub fn get_table(&self, name: &str) -> ArrowStoreResult<Arc<VersionedTable>> {
        self.tables
            .get(name)
            .cloned()
            .ok_or_else(|| crate::error::ArrowStoreError::TableNotFound(name.to_owned()))
    }

    /// Returns the record batches for a table at this snapshot's tick, if any.
    pub fn table_batches(&self, name: &str) -> ArrowStoreResult<Vec<RecordBatch>> {
        let table = self.get_table(name)?;
        let batches = table.get_version(self.tick).cloned().unwrap_or_default();
        Ok(batches)
    }

    /// Returns a partition snapshot for the given table and region.
    pub fn partition_snapshot(
        &self,
        table_name: &str,
        region_id: &str,
    ) -> ArrowStoreResult<PartitionSnapshot> {
        self.partition_views
            .get(table_name)
            .and_then(|m| m.get(region_id))
            .cloned()
            .ok_or_else(|| {
                crate::error::ArrowStoreError::PartitionNotFound(format!(
                    "{}:{}",
                    table_name, region_id
                ))
            })
    }

    /// Registers a partition snapshot for a table.
    pub fn register_partition_snapshot(
        &mut self,
        table_name: impl Into<String>,
        snapshot: PartitionSnapshot,
    ) {
        self.partition_views
            .entry(table_name.into())
            .or_default()
            .insert(snapshot.region_id.clone(), snapshot);
    }

    /// Returns true if the snapshot contains data for the given table.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Returns true if the snapshot contains the specified partition.
    pub fn has_partition(&self, table_name: &str, region_id: &str) -> bool {
        self.partition_views
            .get(table_name)
            .map(|m| m.contains_key(region_id))
            .unwrap_or(false)
    }
}
