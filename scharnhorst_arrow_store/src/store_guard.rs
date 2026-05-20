use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use scharnhorst_core::{CommitToken, Diff, InitToken, TableId, Tick};
use scharnhorst_schema::TableSpec;

use crate::error::ArrowStoreResult;
use crate::index::RowLocation;
use crate::partition::PartitionMap;
use crate::snapshot::WorldSnapshot;
use crate::store::{ArrowStore, JsonToArrayFn, NullArrayFn};
use crate::versioned_table::MutationMode;

#[derive(Clone, Copy)]
pub enum WriteCheckpointToken {
    Init,
    Commit,
}

/// Wraps ArrowStore for initialization-phase mutations.
///
/// Holds an `InitToken` and delegates all init-phase operations through it.
/// The token ensures callers possess compile-time proof of authorization.
pub struct InitStore {
    store: Arc<ArrowStore>,
    token: InitToken,
}

impl InitStore {
    pub fn new(store: Arc<ArrowStore>) -> Self {
        Self {
            store,
            token: InitToken::new(),
        }
    }

    pub fn register_type(
        &self,
        name: impl Into<String>,
        data_type: DataType,
        json_to_array: JsonToArrayFn,
        null_array: NullArrayFn,
    ) -> ArrowStoreResult<()> {
        self.store
            .register_type(&self.token, name, data_type, json_to_array, null_array)
    }

    pub fn create_table(&self, spec: &TableSpec, mode: MutationMode) -> ArrowStoreResult<TableId> {
        self.store.create_table(&self.token, spec, mode)
    }

    pub fn drop_table(&self, name: &str) -> ArrowStoreResult<()> {
        self.store.drop_table(&self.token, name)
    }

    pub fn set_mutation_mode(&self, name: &str, mode: MutationMode) -> ArrowStoreResult<()> {
        self.store.set_mutation_mode(&self.token, name, mode)
    }

    pub fn append_batches(
        &self,
        name: &str,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        self.store.append_batches(&self.token, name, tick, batches)
    }

    pub fn rebuild_table(
        &self,
        name: &str,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        self.store.rebuild_table(&self.token, name, tick, batches)
    }

    pub fn write_checkpoint(&self, tick: Tick) -> ArrowStoreResult<()> {
        self.store.write_checkpoint(tick, WriteCheckpointToken::Init)
    }

    pub fn load_checkpoint(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        self.store.load_checkpoint(&self.token, tick)
    }

    pub fn build_primary_key_index(&self, name: &str) -> ArrowStoreResult<()> {
        self.store.build_primary_key_index(&self.token, name)
    }

    pub fn build_foreign_key_index(
        &self,
        name: &str,
        column: &str,
        target_table: &str,
    ) -> ArrowStoreResult<()> {
        self.store
            .build_foreign_key_index(&self.token, name, column, target_table)
    }

    pub fn partition_map_mut<F, R>(&self, name: &str, f: F) -> ArrowStoreResult<R>
    where
        F: FnOnce(&mut PartitionMap) -> R,
    {
        self.store.partition_map_mut(&self.token, name, f)
    }

    /// Consume this `InitStore`, advance to simulation, and return a `CommitStore`.
    pub fn into_simulation(self) -> ArrowStoreResult<CommitStore> {
        self.store.advance_to_simulation()?;
        Ok(CommitStore::new(Arc::clone(&self.store)))
    }
}

/// Wraps ArrowStore for commit-phase mutations.
///
/// Holds a `CommitToken` and delegates all commit-phase operations through it.
/// The token ensures callers possess compile-time proof of authorization.
pub struct CommitStore {
    store: Arc<ArrowStore>,
    token: CommitToken,
}

impl CommitStore {
    pub fn new(store: Arc<ArrowStore>) -> Self {
        Self {
            store,
            token: CommitToken::new(),
        }
    }

    pub fn apply_diffs(&self, tick: Tick, diffs: &[Diff]) -> ArrowStoreResult<()> {
        self.store.apply_diffs(&self.token, tick, diffs)
    }

    pub fn generate_snapshot(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        self.store.generate_snapshot(&self.token, tick)
    }

    pub fn truncate_before(&self, tick: Tick) -> ArrowStoreResult<()> {
        self.store.truncate_before(&self.token, tick)
    }

    pub fn patch_rows(
        &self,
        name: &str,
        tick: Tick,
        locations: &[RowLocation],
        batch: RecordBatch,
    ) -> ArrowStoreResult<()> {
        self.store
            .patch_rows(&self.token, name, tick, locations, batch)
    }

    pub fn write_checkpoint(&self, tick: Tick) -> ArrowStoreResult<()> {
        self.store.write_checkpoint(tick, WriteCheckpointToken::Commit)
    }

    pub fn save_table_versions(
        &self,
        table_names: &[&str],
    ) -> ArrowStoreResult<HashMap<String, HashMap<Tick, Vec<RecordBatch>>>> {
        self.store.save_table_versions(table_names)
    }

    pub fn restore_table_versions(
        &self,
        saved: HashMap<String, HashMap<Tick, Vec<RecordBatch>>>,
    ) -> ArrowStoreResult<()> {
        self.store.restore_table_versions(saved)
    }
}
