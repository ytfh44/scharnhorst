use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::BufWriter;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, LargeStringBuilder, StringArray, StringBuilder,
    UInt64Array, UInt64Builder,
};
use arrow::datatypes::DataType;
use arrow::ipc::writer::FileWriter;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, UInt64Type};
use arrow_array::RecordBatch;
use arrow_schema::Field;
use dashmap::DashMap;
use scharnhorst_core::{
    CommitToken, Diff, InitToken, LifecycleGuard, RowId, RowPositionMap, TableId, Tick,
};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

use crate::error::{ArrowStoreError, ArrowStoreResult};
use crate::index::{ForeignKeyIndex, PrimaryKeyIndex, RowLocation};
use crate::partition::{PartitionMap, PartitionSnapshot};
use crate::snapshot::WorldSnapshot;
use crate::versioned_table::{MutationMode, VersionedTable};

// ------------------------------------------------------------------
// Type Registry extensible Arrow data-type mapping
// ------------------------------------------------------------------

/// Factory that converts a JSON value to a single-element Arrow array.
pub type JsonToArrayFn =
    Arc<dyn Fn(&serde_json::Value) -> ArrowStoreResult<ArrayRef> + Send + Sync>;

/// Factory that creates a single-element null/zero Arrow array for a type.
pub type NullArrayFn = Arc<dyn Fn() -> ArrowStoreResult<ArrayRef> + Send + Sync>;

/// A registered storage type entry carrying its Arrow `DataType`,
/// JSON-deserialiser factory and null-value factory.
#[derive(Clone)]
pub struct TypeEntry {
    pub data_type: DataType,
    pub json_to_array: JsonToArrayFn,
    pub null_array: NullArrayFn,
}

impl std::fmt::Debug for TypeEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeEntry")
            .field("data_type", &self.data_type)
            .field("json_to_array", &"<fn>")
            .field("null_array", &"<fn>")
            .finish()
    }
}

/// Registry of storage-type names to their Arrow type mappings.
///
/// The default registry is initialised lazily with the seven built-in
/// types (`i64`, `i32`, `u64`, `f64`, `utf8`, `large_utf8`, `bool`).
/// Additional types can be registered via [`TypeRegistry::register`].
#[derive(Default, Clone)]
pub struct TypeRegistry {
    entries: HashMap<String, TypeEntry>,
}

impl std::fmt::Debug for TypeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeRegistry")
            .field("type_names", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new storage type with all three factories.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        data_type: DataType,
        json_to_array: JsonToArrayFn,
        null_array: NullArrayFn,
    ) {
        self.entries.insert(
            name.into(),
            TypeEntry {
                data_type,
                json_to_array,
                null_array,
            },
        );
    }

    /// Look up a [`TypeEntry`] by its storage-type name.
    pub fn get(&self, name: &str) -> Option<&TypeEntry> {
        self.entries.get(name)
    }

    /// Returns `true` if the storage-type name has been registered.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Resolve a storage-type name to its Arrow [`DataType`].
    pub fn resolve_data_type(&self, name: &str) -> ArrowStoreResult<DataType> {
        self.entries
            .get(name)
            .map(|e| e.data_type.clone())
            .ok_or_else(|| ArrowStoreError::Schema(format!("unsupported storage type: {}", name)))
    }
}

fn default_type_registry() -> TypeRegistry {
    let mut reg = TypeRegistry::new();

    reg.register(
        "i64",
        DataType::Int64,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_i64(value)?;
            Ok(Arc::new(Int64Array::from(vec![v])))
        }),
        Arc::new(|| Ok(Arc::new(Int64Array::from(vec![0i64])))),
    );

    reg.register(
        "i32",
        DataType::Int32,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_i32(value)?;
            Ok(Arc::new(Int32Array::from(vec![v])))
        }),
        Arc::new(|| Ok(Arc::new(Int32Array::from(vec![0i32])))),
    );

    reg.register(
        "u64",
        DataType::UInt64,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_u64(value)?;
            Ok(Arc::new(UInt64Array::from(vec![v])))
        }),
        Arc::new(|| Ok(Arc::new(UInt64Array::from(vec![0u64])))),
    );

    reg.register(
        "f64",
        DataType::Float64,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_f64(value)?;
            Ok(Arc::new(Float64Array::from(vec![v])))
        }),
        Arc::new(|| Ok(Arc::new(Float64Array::from(vec![0f64])))),
    );

    reg.register(
        "utf8",
        DataType::Utf8,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_string(value)?;
            Ok(Arc::new(StringArray::from(vec![v.as_str()])))
        }),
        Arc::new(|| Ok(Arc::new(StringArray::from(vec![""])))),
    );

    reg.register(
        "large_utf8",
        DataType::LargeUtf8,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_string(value)?;
            Ok(Arc::new(arrow::array::LargeStringArray::from(vec![
                v.as_str()
            ])))
        }),
        Arc::new(|| Ok(Arc::new(arrow::array::LargeStringArray::from(vec![""])))),
    );

    reg.register(
        "bool",
        DataType::Boolean,
        Arc::new(|value: &serde_json::Value| {
            let v = json_to_bool(value)?;
            Ok(Arc::new(BooleanArray::from(vec![v])))
        }),
        Arc::new(|| Ok(Arc::new(BooleanArray::from(vec![false])))),
    );

    reg
}

/// Concurrent Arrow store with per-table locking via DashMap.
///
/// Tables are stored in a `DashMap` with per-entry `Arc<RwLock<VersionedTable>>`,
/// enabling concurrent read/write access to different tables without a global lock.
#[derive(Debug, Clone)]
pub struct ArrowStore {
    tables: Arc<DashMap<String, Arc<RwLock<VersionedTable>>>>,
    table_id_map: Arc<DashMap<TableId, String>>,
    snapshots: Arc<DashMap<Tick, Arc<WorldSnapshot>>>,
    next_table_id: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    type_registry: Arc<RwLock<TypeRegistry>>,
    create_drop_lock: Arc<Mutex<()>>,
    lifecycle: Arc<RwLock<LifecycleGuard>>,
}

impl Default for ArrowStore {
    fn default() -> Self {
        Self {
            tables: Arc::new(DashMap::new()),
            table_id_map: Arc::new(DashMap::new()),
            snapshots: Arc::new(DashMap::new()),
            next_table_id: Arc::new(AtomicU64::new(0)),
            generation: Arc::new(AtomicU64::new(0)),
            type_registry: Arc::new(RwLock::new(default_type_registry())),
            create_drop_lock: Arc::new(Mutex::new(())),
            lifecycle: Arc::new(RwLock::new(LifecycleGuard::new())),
        }
    }
}

impl ArrowStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an additional storage type at runtime.
    ///
    /// The type is added to this store's [`TypeRegistry`] so that subsequent
    /// table operations recognise it. This is intended for use before any
    /// table operations that reference the custom type.
    pub fn register_type(
        &self,
        _token: &InitToken,
        name: impl Into<String>,
        data_type: DataType,
        json_to_array: JsonToArrayFn,
        null_array: NullArrayFn,
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let mut reg = self
            .type_registry
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        reg.register(name, data_type, json_to_array, null_array);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Table management
    // ------------------------------------------------------------------

    pub fn create_table(
        &self,
        _token: &InitToken,
        spec: &TableSpec,
        mode: MutationMode,
    ) -> ArrowStoreResult<TableId> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let _guard = self
            .create_drop_lock
            .lock()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if self.tables.contains_key(&spec.name) {
            return Err(ArrowStoreError::TableAlreadyExists(spec.name.clone()));
        }

        let id_val = self.next_table_id.fetch_add(1, Ordering::Relaxed);
        let id = TableId::new(id_val);

        let mut table = VersionedTable::new(&spec.name, mode);
        table.set_spec(spec.clone());

        self.tables
            .insert(spec.name.clone(), Arc::new(RwLock::new(table)));
        self.table_id_map.insert(id, spec.name.clone());
        Ok(id)
    }

    pub fn drop_table(&self, _token: &InitToken, name: &str) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let _guard = self
            .create_drop_lock
            .lock()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        let id_to_remove = self
            .table_id_map
            .iter()
            .find(|entry| entry.value() == name)
            .map(|entry| *entry.key());

        if let Some(id) = id_to_remove {
            self.table_id_map.remove(&id);
        }

        self.tables
            .remove(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> ArrowStoreResult<VersionedTable> {
        let lock = self
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let table = lock
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        Ok(table.clone())
    }

    /// Returns the names of all tables currently in the store.
    ///
    /// NOTE: Returns a best-effort snapshot. Under concurrent mutations
    /// (rare), the result may include recently-created tables or omit
    /// recently-dropped tables.
    pub fn table_names(&self) -> ArrowStoreResult<Vec<String>> {
        Ok(self
            .tables
            .iter()
            .map(|entry| entry.key().clone())
            .collect())
    }

    pub fn table_count(&self) -> ArrowStoreResult<usize> {
        Ok(self.tables.len())
    }

    // ------------------------------------------------------------------
    // Mutation modes
    // ------------------------------------------------------------------

    pub fn set_mutation_mode(
        &self,
        _token: &InitToken,
        name: &str,
        mode: MutationMode,
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        table.mutation_mode = mode;
        Ok(())
    }

    pub fn mutation_mode(&self, name: &str) -> ArrowStoreResult<MutationMode> {
        let lock = self
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let table = lock
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        Ok(table.mutation_mode())
    }

    // ------------------------------------------------------------------
    // Data ingestion
    // ------------------------------------------------------------------

    pub fn append_batches(
        &self,
        _token: &InitToken,
        name: &str,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        let entry = table.versions.entry(tick).or_default();
        entry.extend(batches);
        Ok(())
    }

    pub fn patch_rows(
        &self,
        _token: &CommitToken,
        name: &str,
        tick: Tick,
        locations: &[RowLocation],
        batch: RecordBatch,
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_initialization() {
            return Err(ArrowStoreError::Lifecycle(
                "commit operation not allowed during initialization".to_owned(),
            ));
        }
        if locations.is_empty() {
            return Ok(());
        }
        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        let versions =
            table
                .versions
                .get_mut(&tick)
                .ok_or_else(|| ArrowStoreError::TickNotFound {
                    table: name.to_owned(),
                    tick: tick.as_u64(),
                })?;

        let mut grouped: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
        for (patch_row_idx, loc) in locations.iter().enumerate() {
            grouped
                .entry(loc.batch_index)
                .or_default()
                .push((loc.row_index, patch_row_idx));
        }

        for (batch_idx, patches) in &grouped {
            if *batch_idx >= versions.len() {
                continue;
            }
            let patched = patch_record_batch(&versions[*batch_idx], &batch, patches)?;
            versions[*batch_idx] = patched;
        }

        Ok(())
    }

    pub fn rebuild_table(
        &self,
        _token: &InitToken,
        name: &str,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;

        table.position_map = RowPositionMap::new();
        let mut per_table_counter: u64 = 0;
        for (batch_idx, batch) in batches.iter().enumerate() {
            for offset in 0..batch.num_rows() {
                table
                    .position_map
                    .insert(RowId::new(per_table_counter), batch_idx, offset);
                per_table_counter += 1;
            }
        }

        table.versions.insert(tick, batches);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Snapshot generation & retrieval
    // ------------------------------------------------------------------

    /// Generate a snapshot at the given tick.
    ///
    /// IMPORTANT: Cross-table consistency is not guaranteed when called
    /// concurrently with mutating operations (`create_table`, `drop_table`,
    /// `apply_diffs`). Within the sequential scheduler commit path, this
    /// constraint is trivially satisfied.
    pub fn generate_snapshot(
        &self,
        _token: &CommitToken,
        tick: Tick,
    ) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_initialization() {
            return Err(ArrowStoreError::Lifecycle(
                "commit operation not allowed during initialization".to_owned(),
            ));
        }
        self.do_generate_snapshot(tick)
    }

    fn do_generate_snapshot(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        self.generation.fetch_add(1, Ordering::Relaxed);
        let mut snapshot = WorldSnapshot::new(tick);

        for entry in self.tables.iter() {
            let table = entry
                .value()
                .read()
                .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
            let name = entry.key().clone();
            snapshot.register_table(&name, Arc::new(table.clone()));

            for region_id in table.partitions().region_ids() {
                let partition = table.partitions().get(region_id)?;
                let ps = PartitionSnapshot::new(region_id, tick, partition.batches().to_vec());
                snapshot.register_partition_snapshot(&name, ps);
            }
        }

        let arc = Arc::new(snapshot);
        self.snapshots.insert(tick, arc.clone());
        Ok(arc)
    }

    pub fn get_snapshot(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        self.snapshots
            .get(&tick)
            .map(|r| r.value().clone())
            .ok_or(ArrowStoreError::SnapshotNotFound(tick.as_u64()))
    }

    /// Returns the latest snapshot if any exists.
    ///
    /// NOTE: Returns a best-effort snapshot. Under concurrent mutations,
    /// the result may not reflect the most recently committed snapshot.
    pub fn latest_snapshot(&self) -> ArrowStoreResult<Option<Arc<WorldSnapshot>>> {
        let latest = self
            .snapshots
            .iter()
            .map(|entry| *entry.key())
            .max()
            .and_then(|tick| self.snapshots.get(&tick).map(|r| r.value().clone()));
        Ok(latest)
    }

    /// Returns the ticks of all stored snapshots.
    ///
    /// NOTE: Returns a best-effort snapshot. Under concurrent mutations,
    /// the result may be inconsistent (e.g. include a snapshot created
    /// concurrently or omit one being created/destroyed).
    pub fn snapshot_ticks(&self) -> ArrowStoreResult<Vec<Tick>> {
        Ok(self.snapshots.iter().map(|entry| *entry.key()).collect())
    }

    // ------------------------------------------------------------------
    // Indexing
    // ------------------------------------------------------------------

    pub fn build_primary_key_index(&self, _token: &InitToken, name: &str) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let pk_name = {
            let lock = self
                .tables
                .get(name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
            let table = lock
                .read()
                .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
            let spec = table
                .spec()
                .ok_or_else(|| ArrowStoreError::Schema("no table spec for PK index".into()))?;
            let pk_col = spec
                .primary_key_column()
                .ok_or_else(|| ArrowStoreError::Schema("no primary key column defined".into()))?;
            pk_col.name.clone()
        };

        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        table.primary_key_index = PrimaryKeyIndex::new();

        let versions: Vec<(Tick, Vec<RecordBatch>)> = table
            .versions
            .iter()
            .map(|(&t, b)| (t, b.clone()))
            .collect();

        for (tick, batches) in &versions {
            for (batch_idx, batch) in batches.iter().enumerate() {
                let col_idx = batch.schema().index_of(&pk_name).map_err(|e| {
                    ArrowStoreError::Schema(format!("column {} not found: {}", pk_name, e))
                })?;
                let array = batch.column(col_idx);
                for row_idx in 0..batch.num_rows() {
                    let key = array_value_to_string(array, row_idx);
                    table
                        .primary_key_index
                        .insert(key, *tick, batch_idx, row_idx);
                }
            }
        }
        Ok(())
    }

    pub fn build_foreign_key_index(
        &self,
        _token: &InitToken,
        name: &str,
        column: &str,
        target_table: &str,
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        let mut fk_idx = ForeignKeyIndex::new(column, target_table);

        for (&tick, batches) in &table.versions {
            for (batch_idx, batch) in batches.iter().enumerate() {
                let col_idx = batch.schema().index_of(column).map_err(|e| {
                    ArrowStoreError::Schema(format!("column {} not found: {}", column, e))
                })?;
                let array = batch.column(col_idx);
                for row_idx in 0..batch.num_rows() {
                    let key = array_value_to_string(array, row_idx);
                    fk_idx.insert(key, tick, batch_idx, row_idx);
                }
            }
        }
        table.add_foreign_key_index(fk_idx);
        Ok(())
    }

    pub fn primary_key_index(&self, name: &str) -> ArrowStoreResult<PrimaryKeyIndex> {
        let lock = self
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let table = lock
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        Ok(table.primary_key_index().clone())
    }

    pub fn foreign_key_indices(&self, name: &str) -> ArrowStoreResult<Vec<ForeignKeyIndex>> {
        let lock = self
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let table = lock
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        Ok(table.foreign_key_indices().to_vec())
    }

    // ------------------------------------------------------------------
    // Partitioned access
    // ------------------------------------------------------------------

    pub fn partition_map(&self, name: &str) -> ArrowStoreResult<PartitionMap> {
        let lock = self
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let table = lock
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        Ok(table.partitions().clone())
    }

    pub fn partition_map_mut<F, R>(
        &self,
        _token: &InitToken,
        name: &str,
        f: F,
    ) -> ArrowStoreResult<R>
    where
        F: FnOnce(&mut PartitionMap) -> R,
    {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        let lock = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        Ok(f(table.partitions_mut()))
    }

    pub fn get_partition(
        &self,
        table_name: &str,
        region_id: &str,
    ) -> ArrowStoreResult<crate::partition::Partition> {
        let lock = self
            .tables
            .get(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        let table = lock
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        table.partitions().get(region_id).cloned()
    }

    // ------------------------------------------------------------------
    // Checkpoint / diff helpers
    // ------------------------------------------------------------------

    pub(crate) fn write_checkpoint(&self, tick: Tick, token: crate::store_guard::WriteCheckpointToken) -> ArrowStoreResult<()> {
        {
            let lifecycle = self
                .lifecycle
                .read()
                .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
            match token {
                crate::store_guard::WriteCheckpointToken::Init => {
                    if lifecycle.is_simulation() {
                        return Err(ArrowStoreError::Lifecycle(
                            "write_checkpoint requires Initialization, but lifecycle is Simulation"
                                .to_owned(),
                        ));
                    }
                }
                crate::store_guard::WriteCheckpointToken::Commit => {
                    if lifecycle.is_initialization() {
                        return Err(ArrowStoreError::Lifecycle(
                            "write_checkpoint requires Simulation, but lifecycle is Initialization"
                                .to_owned(),
                        ));
                    }
                }
            }
        }
        self.do_write_checkpoint(tick)
    }

    fn do_write_checkpoint(&self, tick: Tick) -> ArrowStoreResult<()> {
        let snapshot = match self.snapshots.get(&tick) {
            Some(s) => s.value().clone(),
            None => {
                let latest = self
                    .snapshots
                    .iter()
                    .map(|entry| *entry.key())
                    .max()
                    .and_then(|k| self.snapshots.get(&k).map(|r| r.value().clone()));
                match latest {
                    Some(s) => s,
                    None => return Ok(()),
                }
            }
        };

        let dir = format!("checkpoint_{}", tick.as_u64());
        fs::create_dir_all(&dir)
            .map_err(|e| ArrowStoreError::Io(format!("create checkpoint dir: {}", e)))?;

        for table_name in snapshot.table_names() {
            let batches = snapshot.table_batches(table_name).unwrap_or_default();
            if batches.is_empty() {
                continue;
            }
            let path = format!("{}/{}.arrow", dir, table_name);
            let file = fs::File::create(&path)
                .map_err(|e| ArrowStoreError::Io(format!("create file {}: {}", path, e)))?;
            let writer = BufWriter::new(file);
            let schema = batches[0].schema();
            let mut ipc_writer = FileWriter::try_new(writer, &schema)
                .map_err(|e| ArrowStoreError::Arrow(format!("ipc writer: {}", e)))?;
            for batch in &batches {
                ipc_writer
                    .write(batch)
                    .map_err(|e| ArrowStoreError::Arrow(format!("ipc write: {}", e)))?;
            }
            ipc_writer
                .finish()
                .map_err(|e| ArrowStoreError::Arrow(format!("ipc finish: {}", e)))?;
        }
        Ok(())
    }

    pub fn load_checkpoint(
        &self,
        _token: &InitToken,
        tick: Tick,
    ) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_simulation() {
            return Err(ArrowStoreError::Lifecycle(
                "operation not allowed during simulation".to_owned(),
            ));
        }
        use arrow::ipc::reader::FileReader;
        use std::io::BufReader;

        let dir = format!("checkpoint_{}", tick.as_u64());
        let dir_path = std::path::Path::new(&dir);
        if !dir_path.is_dir() {
            return Err(ArrowStoreError::Io(format!(
                "checkpoint directory not found: {}",
                dir
            )));
        }

        // Step 1: Read all.arrow files without holding any lock
        let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let entries = fs::read_dir(&dir)
            .map_err(|e| ArrowStoreError::Io(format!("read checkpoint dir {}: {}", dir, e)))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| ArrowStoreError::Io(format!("read dir entry in {}: {}", dir, e)))?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "arrow") {
                let table_name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| {
                        ArrowStoreError::Io(format!(
                            "invalid checkpoint file name: {}",
                            path.display()
                        ))
                    })?
                    .to_owned();

                let file = fs::File::open(&path)
                    .map_err(|e| ArrowStoreError::Io(format!("open {}: {}", path.display(), e)))?;
                let reader = BufReader::new(file);
                let ipc_reader = FileReader::try_new(reader, None).map_err(|e| {
                    ArrowStoreError::Arrow(format!("ipc reader for table {}: {}", table_name, e))
                })?;

                let mut batches: Vec<RecordBatch> = Vec::new();
                for batch_result in ipc_reader {
                    let batch = batch_result.map_err(|e| {
                        ArrowStoreError::Arrow(format!(
                            "read batch from table {}: {}",
                            table_name, e
                        ))
                    })?;
                    batches.push(batch);
                }
                table_batches.insert(table_name, batches);
            }
        }

        if table_batches.is_empty() {
            return Err(ArrowStoreError::Io(format!(
                "no .arrow files found in checkpoint directory: {}",
                dir
            )));
        }

        // Step 2: Insert data per-table and generate snapshot.
        // Acquire create_drop_lock to prevent concurrent create/drop during
        // table and table_id_map insertion.
        {
            let _create_drop_guard = self
                .create_drop_lock
                .lock()
                .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;

            for (name, batches) in table_batches {
                let is_new = !self.tables.contains_key(&name);

                self.tables.entry(name.clone()).or_insert_with(|| {
                    Arc::new(RwLock::new(VersionedTable::new(
                        &name,
                        MutationMode::AppendOnly,
                    )))
                });

                if is_new {
                    let id_val = self.next_table_id.fetch_add(1, Ordering::Relaxed);
                    let id = TableId::new(id_val);
                    self.table_id_map.insert(id, name.clone());
                }

                let lock = self
                    .tables
                    .get_mut(&name)
                    .ok_or_else(|| ArrowStoreError::TableNotFound(name.clone()))?;
                let mut table = lock
                    .write()
                    .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;

                if is_new {
                    if let Some(first_batch) = batches.first() {
                        let mut spec = TableSpec::new(&name);
                        for field in first_batch.schema().fields() {
                            let col = ColumnSpec::new(
                                field.name(),
                                FieldSemantic::Raw,
                                field.data_type().to_string().as_str(),
                            )
                            .with_nullable(field.is_nullable());
                            if let Ok(s) = spec.clone().with_column(col) {
                                spec = s;
                            }
                        }
                        table.set_spec(spec);
                    }
                }

                table.versions.insert(tick, batches);
            }
        }

        self.do_generate_snapshot(tick)
    }

    /// Truncate all snapshots and table versions before the given tick.
    ///
    /// IMPORTANT: Must not run concurrently with `generate_snapshot()`.
    /// Running both concurrently may produce corrupted snapshots referencing
    /// deleted versions. Within the sequential scheduler commit path, this
    /// constraint is trivially satisfied.
    pub fn truncate_before(&self, _token: &CommitToken, tick: Tick) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_initialization() {
            return Err(ArrowStoreError::Lifecycle(
                "commit operation not allowed during initialization".to_owned(),
            ));
        }
        self.snapshots.retain(|&t, _| t >= tick);

        for entry in self.tables.iter_mut() {
            let mut table = entry
                .value()
                .write()
                .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
            table.versions.retain(|&t, _| t >= tick);
        }
        Ok(())
    }

    pub fn generation(&self) -> ArrowStoreResult<u64> {
        Ok(self.generation.load(Ordering::Relaxed))
    }

    /// Advance the lifecycle guard from Initialization to Simulation.
    ///
    /// Once called, all init-phase mutation methods will reject further calls.
    /// Commit-phase methods (apply_diffs, generate_snapshot, truncate_before,
    /// patch_rows) become available.
    pub(crate) fn advance_to_simulation(&self) -> ArrowStoreResult<()> {
        let mut lifecycle = self
            .lifecycle
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        lifecycle
            .advance_to_simulation()
            .map_err(|e| ArrowStoreError::Lifecycle(e.to_string()))
    }

    // ------------------------------------------------------------------
    // Diff application
    // ------------------------------------------------------------------

    pub fn apply_diffs(
        &self,
        _token: &CommitToken,
        tick: Tick,
        diffs: &[Diff],
    ) -> ArrowStoreResult<()> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        if lifecycle.is_initialization() {
            return Err(ArrowStoreError::Lifecycle(
                "commit operation not allowed during initialization".to_owned(),
            ));
        }

        // Collect unique table names and save pre-diff versions snapshot
        let touched_tables: HashSet<&str> = diffs.iter().map(|d| d.table()).collect();
        let mut saved_versions: HashMap<String, HashMap<Tick, Vec<RecordBatch>>> =
            HashMap::new();
        for table_name in &touched_tables {
            if let Some(lock) = self.tables.get(*table_name) {
                let table = lock
                    .read()
                    .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
                saved_versions.insert(table_name.to_string(), table.versions.clone());
            }
        }

        for diff in diffs {
            let result = match diff {
                Diff::Update {
                    table,
                    row,
                    column,
                    value,
                } => self.apply_diff_update(_token, tick, table, row, column, value),
                Diff::Insert { table, row, values } => {
                    self.apply_diff_insert(_token, tick, table, row, values)
                }
                Diff::Delete { table, row } => self.apply_diff_delete(_token, tick, table, row),
                Diff::ReplaceTable { table, rows } => {
                    self.apply_diff_replace(_token, tick, table, rows)
                }
            };

            if let Err(e) = result {
                // Rollback: restore saved versions for all touched tables
                for (table_name, saved) in &saved_versions {
                    if let Some(lock) = self.tables.get_mut(table_name) {
                        if let Ok(mut table) = lock
                            .write()
                            .map_err(|le| ArrowStoreError::LockPoisoned(le.to_string()))
                        {
                            table.versions = saved.clone();
                        }
                    }
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Save the versions HashMap for the given table names, used for rollback.
    pub fn save_table_versions(
        &self,
        table_names: &[&str],
    ) -> ArrowStoreResult<HashMap<String, HashMap<Tick, Vec<RecordBatch>>>> {
        let mut saved = HashMap::new();
        for name in table_names {
            if let Some(lock) = self.tables.get(*name) {
                let table = lock
                    .read()
                    .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
                saved.insert(name.to_string(), table.versions.clone());
            }
        }
        Ok(saved)
    }

    /// Restore previously saved versions to the given tables.
    pub fn restore_table_versions(
        &self,
        saved: HashMap<String, HashMap<Tick, Vec<RecordBatch>>>,
    ) -> ArrowStoreResult<()> {
        for (name, versions) in saved {
            if let Some(lock) = self.tables.get_mut(&name) {
                let mut table = lock
                    .write()
                    .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
                table.versions = versions;
            }
        }
        Ok(())
    }
}

// ------------------------------------------------------------------
// Diff application dispatchers — acquire per-table write lock once,
// then delegate to free functions that operate on &mut VersionedTable
// ------------------------------------------------------------------

impl ArrowStore {
    fn apply_diff_update(
        &self,
        _token: &CommitToken,
        tick: Tick,
        table_name: &str,
        row: &RowId,
        column: &str,
        value: &serde_json::Value,
    ) -> ArrowStoreResult<()> {
        let lock = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        apply_update_to_table(&mut table, table_name, tick, row, column, value)
    }

    fn apply_diff_delete(
        &self,
        _token: &CommitToken,
        tick: Tick,
        table_name: &str,
        row: &RowId,
    ) -> ArrowStoreResult<()> {
        let lock = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        apply_delete_to_table(&mut table, table_name, tick, row)
    }

    fn apply_diff_insert(
        &self,
        _token: &CommitToken,
        tick: Tick,
        table_name: &str,
        row: &RowId,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> ArrowStoreResult<()> {
        let type_reg = self
            .type_registry
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        let lock = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        apply_insert_to_table(&mut table, table_name, tick, row, values, &type_reg)
    }

    fn apply_diff_replace(
        &self,
        _token: &CommitToken,
        tick: Tick,
        table_name: &str,
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> ArrowStoreResult<()> {
        let type_reg = self
            .type_registry
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        let lock = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        let mut table = lock
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))?;
        apply_replace_to_table(&mut table, table_name, tick, rows, &type_reg)
    }
}

// ------------------------------------------------------------------
// Free functions — operate on &mut VersionedTable under a held lock
// ------------------------------------------------------------------

fn apply_update_to_table(
    table: &mut VersionedTable,
    table_name: &str,
    patch_tick: Tick,
    row: &RowId,
    column: &str,
    value: &serde_json::Value,
) -> ArrowStoreResult<()> {
    let (batch_idx, row_offset) = table.position_map.position_of(*row).ok_or_else(|| {
        ArrowStoreError::Generic(format!(
            "row not found in position map: table={}, row_id={}",
            table_name,
            row.as_u64()
        ))
    })?;

    let mut ticks: Vec<Tick> = table
        .versions
        .iter()
        .filter(|(_, batches)| batches.len() > batch_idx)
        .map(|(&t, _)| t)
        .collect();
    ticks.sort();
    let loc_tick = ticks.into_iter().last().ok_or_else(|| {
        ArrowStoreError::Generic(format!(
            "no tick found containing batch_idx {} for table {}",
            batch_idx, table_name
        ))
    })?;

    let batches = table
        .versions
        .get(&loc_tick)
        .ok_or_else(|| ArrowStoreError::TickNotFound {
            table: table_name.to_owned(),
            tick: loc_tick.as_u64(),
        })?;

    if batch_idx >= batches.len() {
        return Err(ArrowStoreError::Generic(format!(
            "batch index {} out of range for table {}",
            batch_idx, table_name
        )));
    }

    if row_offset >= batches[batch_idx].num_rows() {
        return Err(ArrowStoreError::Generic(format!(
            "row offset {} out of range in batch {}",
            row_offset, batch_idx
        )));
    }

    let col_idx = batches[batch_idx].schema().index_of(column).map_err(|_| {
        ArrowStoreError::Schema(format!(
            "column '{}' not found in table '{}'",
            column, table_name
        ))
    })?;

    let patch_batch = build_update_patch(&batches[batch_idx], row_offset, col_idx, value)?;

    let loc = RowLocation {
        tick: loc_tick,
        batch_index: batch_idx,
        row_index: row_offset,
        row_id: *row,
    };

    let _ = patch_tick;
    apply_patch_to_versions(table, table_name, loc_tick, &[loc], patch_batch)
}

fn apply_delete_to_table(
    table: &mut VersionedTable,
    table_name: &str,
    patch_tick: Tick,
    row: &RowId,
) -> ArrowStoreResult<()> {
    let (batch_idx, row_offset) = table.position_map.position_of(*row).ok_or_else(|| {
        ArrowStoreError::Generic(format!(
            "row not found in position map for delete: table={}, row_id={}",
            table_name,
            row.as_u64()
        ))
    })?;

    let mut ticks: Vec<Tick> = table
        .versions
        .iter()
        .filter(|(_, batches)| batches.len() > batch_idx)
        .map(|(&t, _)| t)
        .collect();
    ticks.sort();
    let loc_tick = ticks.into_iter().last().ok_or_else(|| {
        ArrowStoreError::Generic(format!(
            "no tick found for delete: table={}, batch_idx={}",
            table_name, batch_idx
        ))
    })?;

    let batches = table
        .versions
        .get(&loc_tick)
        .ok_or_else(|| ArrowStoreError::TickNotFound {
            table: table_name.to_owned(),
            tick: loc_tick.as_u64(),
        })?;

    if batch_idx >= batches.len() || row_offset >= batches[batch_idx].num_rows() {
        return Err(ArrowStoreError::Generic(format!(
            "invalid location for delete: batch={}, row={}",
            batch_idx, row_offset
        )));
    }

    let null_patch = crate::versioned_table::build_null_patch(&batches[batch_idx], row_offset)?;

    let loc = RowLocation {
        tick: loc_tick,
        batch_index: batch_idx,
        row_index: row_offset,
        row_id: *row,
    };

    let _ = patch_tick;
    apply_patch_to_versions(table, table_name, loc_tick, &[loc], null_patch)?;

    table.position_map.remove(row);

    Ok(())
}

fn apply_insert_to_table(
    table: &mut VersionedTable,
    table_name: &str,
    tick: Tick,
    row: &RowId,
    values: &serde_json::Map<String, serde_json::Value>,
    type_registry: &TypeRegistry,
) -> ArrowStoreResult<()> {
    let schema = resolve_table_schema(table, values, type_registry)?.ok_or_else(|| {
        ArrowStoreError::Schema(format!(
            "cannot determine schema for insert into table '{}'",
            table_name
        ))
    })?;

    let pk_col_name = table
        .spec
        .as_ref()
        .and_then(|s| s.primary_key_column())
        .map(|c| c.name.clone());

    if table.position_map.contains(*row) {
        return Err(ArrowStoreError::DuplicateRowId {
            table: table_name.to_owned(),
            row_id: row.as_u64(),
        });
    }

    let batch = json_map_to_record_batch(values, &schema, row)?;

    {
        let entry = table.versions.entry(tick).or_default();
        entry.push(batch);
    }

    let batch_idx = table
        .versions
        .get(&tick)
        .map(|v| v.len().saturating_sub(1))
        .unwrap_or(0);
    let row_offset = 0usize;

    if let Some(ref pk_name) = pk_col_name {
        if let Some(pk_value) = values.get(pk_name) {
            let key = match pk_value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            table
                .primary_key_index
                .insert(key, tick, batch_idx, row_offset);
        }
    }

    table.position_map.insert(*row, batch_idx, row_offset);

    Ok(())
}

fn apply_replace_to_table(
    table: &mut VersionedTable,
    table_name: &str,
    tick: Tick,
    rows: &[serde_json::Map<String, serde_json::Value>],
    type_registry: &TypeRegistry,
) -> ArrowStoreResult<()> {
    let schema = rows
        .first()
        .and_then(|row| resolve_table_schema_from_map(table, row, type_registry).ok())
        .flatten()
        .ok_or_else(|| {
            ArrowStoreError::Schema(format!(
                "cannot determine schema for ReplaceTable on '{}'",
                table_name
            ))
        })?;

    let pk_col_name = table
        .spec
        .as_ref()
        .and_then(|s| s.primary_key_column())
        .map(|c| c.name.clone());

    let mut batches: Vec<RecordBatch> = Vec::with_capacity(rows.len());
    table.position_map = RowPositionMap::new();

    for (i, row_map) in rows.iter().enumerate() {
        let row_id = if let Some(ref pk_name) = pk_col_name {
            let pk_value = row_map.get(pk_name).ok_or_else(|| {
                ArrowStoreError::Schema(format!(
                    "primary key column '{}' missing in ReplaceTable row for table '{}'",
                    pk_name, table_name
                ))
            })?;
            let id = json_value_to_u64(pk_value).ok_or_else(|| {
                ArrowStoreError::Schema(format!(
                    "primary key column '{}' value is not a valid u64 in ReplaceTable for table '{}'",
                    pk_name, table_name
                ))
            })?;
            let row_id = RowId::new(id);
            if table.position_map.contains(row_id) {
                return Err(ArrowStoreError::DuplicateRowId {
                    table: table_name.to_owned(),
                    row_id: id,
                });
            }
            row_id
        } else {
            RowId::new(i as u64)
        };

        let offset = 0usize;
        table.position_map.insert(row_id, i, offset);
        let batch = json_map_to_record_batch(row_map, &schema, &row_id)?;
        batches.push(batch);
    }

    table.versions.insert(tick, batches);
    Ok(())
}

fn apply_patch_to_versions(
    table: &mut VersionedTable,
    table_name: &str,
    tick: Tick,
    locations: &[RowLocation],
    batch: RecordBatch,
) -> ArrowStoreResult<()> {
    if locations.is_empty() {
        return Ok(());
    }
    let versions = table
        .versions
        .get_mut(&tick)
        .ok_or_else(|| ArrowStoreError::TickNotFound {
            table: table_name.to_owned(),
            tick: tick.as_u64(),
        })?;

    let mut grouped: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
    for (patch_row_idx, loc) in locations.iter().enumerate() {
        grouped
            .entry(loc.batch_index)
            .or_default()
            .push((loc.row_index, patch_row_idx));
    }

    for (batch_idx, patches) in &grouped {
        if *batch_idx >= versions.len() {
            continue;
        }
        let patched = patch_record_batch(&versions[*batch_idx], &batch, patches)?;
        versions[*batch_idx] = patched;
    }
    Ok(())
}

// ------------------------------------------------------------------
// JSON to Arrow conversion helpers
// ------------------------------------------------------------------

fn json_value_to_array(
    value: &serde_json::Value,
    data_type: &DataType,
) -> ArrowStoreResult<ArrayRef> {
    if value.is_null() {
        return match data_type {
            DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![None::<i64>]))),
            DataType::Int32 => Ok(Arc::new(Int32Array::from(vec![None::<i32>]))),
            DataType::UInt64 => Ok(Arc::new(UInt64Array::from(vec![None::<u64>]))),
            DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![None::<f64>]))),
            DataType::Boolean => Ok(Arc::new(BooleanArray::from(vec![None::<bool>]))),
            DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![None::<&str>]))),
            DataType::LargeUtf8 => Ok(Arc::new(arrow::array::LargeStringArray::from(vec![
                None::<&str>,
            ]))),
            other => Err(ArrowStoreError::Schema(format!(
                "unsupported data type for JSON conversion: {:?}",
                other
            ))),
        };
    }
    match data_type {
        DataType::Int64 => {
            let v: i64 = json_to_i64(value)?;
            Ok(Arc::new(Int64Array::from(vec![v])))
        }
        DataType::Int32 => {
            let v: i32 = json_to_i32(value)?;
            Ok(Arc::new(Int32Array::from(vec![v])))
        }
        DataType::UInt64 => {
            let v: u64 = json_to_u64(value)?;
            Ok(Arc::new(UInt64Array::from(vec![v])))
        }
        DataType::Float64 => {
            let v: f64 = json_to_f64(value)?;
            Ok(Arc::new(Float64Array::from(vec![v])))
        }
        DataType::Boolean => {
            let v: bool = json_to_bool(value)?;
            Ok(Arc::new(BooleanArray::from(vec![v])))
        }
        DataType::Utf8 => {
            let v: String = json_to_string(value)?;
            Ok(Arc::new(StringArray::from(vec![v.as_str()])))
        }
        DataType::LargeUtf8 => {
            let v: String = json_to_string(value)?;
            Ok(Arc::new(arrow::array::LargeStringArray::from(vec![
                v.as_str()
            ])))
        }
        other => Err(ArrowStoreError::Schema(format!(
            "unsupported data type for JSON conversion: {:?}",
            other
        ))),
    }
}

fn json_to_i64(value: &serde_json::Value) -> ArrowStoreResult<i64> {
    if let Some(v) = value.as_i64() {
        return Ok(v);
    }
    if let Some(v) = value.as_f64() {
        if v >= i64::MIN as f64 && v <= i64::MAX as f64 {
            return Ok(v as i64);
        }
        return Err(ArrowStoreError::Schema(format!(
            "value {} out of range for i64",
            v
        )));
    }
    Err(ArrowStoreError::Schema(format!(
        "expected i64, got {:?}",
        value
    )))
}

fn json_to_i32(value: &serde_json::Value) -> ArrowStoreResult<i32> {
    let val = value
        .as_i64()
        .or_else(|| value.as_f64().map(|f| f as i64))
        .ok_or_else(|| ArrowStoreError::Schema(format!("expected i32, got {:?}", value)))?;
    i32::try_from(val)
        .map_err(|_| ArrowStoreError::Schema(format!("value {} out of range for i32", val)))
}

fn json_to_u64(value: &serde_json::Value) -> ArrowStoreResult<u64> {
    if let Some(v) = value.as_u64() {
        return Ok(v);
    }
    if let Some(v) = value.as_i64() {
        return u64::try_from(v)
            .map_err(|_| ArrowStoreError::Schema(format!("negative value {} for u64", v)));
    }
    if let Some(v) = value.as_f64() {
        if v >= 0.0 && v < (u64::MAX as f64) {
            let as_u64 = v as u64;
            if as_u64 as f64 == v {
                return Ok(as_u64);
            }
        }
        return Err(ArrowStoreError::Schema(format!(
            "value {} out of range for u64",
            v
        )));
    }
    Err(ArrowStoreError::Schema(format!(
        "expected u64, got {:?}",
        value
    )))
}

fn json_to_f64(value: &serde_json::Value) -> ArrowStoreResult<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|v| v as f64))
        .ok_or_else(|| ArrowStoreError::Schema(format!("expected f64, got {:?}", value)))
}

fn json_to_bool(value: &serde_json::Value) -> ArrowStoreResult<bool> {
    value
        .as_bool()
        .ok_or_else(|| ArrowStoreError::Schema(format!("expected bool, got {:?}", value)))
}

fn json_to_string(value: &serde_json::Value) -> ArrowStoreResult<String> {
    match value {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Null => Ok(String::new()),
        other => Err(ArrowStoreError::Schema(format!(
            "expected utf8 string, got {}",
            serde_json::to_string(other).unwrap_or_else(|_| "<unprintable>".to_owned())
        ))),
    }
}

fn resolve_table_schema(
    table: &VersionedTable,
    values: &serde_json::Map<String, serde_json::Value>,
    reg: &TypeRegistry,
) -> ArrowStoreResult<Option<Arc<arrow_schema::Schema>>> {
    // Try from spec first
    if let Some(spec) = table.spec.as_ref() {
        let fields: Vec<Field> = spec
            .columns
            .iter()
            .map(|col| {
                let dt = reg.resolve_data_type(&col.storage_type)?;
                Ok(Field::new(&col.name, dt, col.nullable))
            })
            .collect::<ArrowStoreResult<Vec<Field>>>()?;
        return Ok(Some(Arc::new(arrow_schema::Schema::new(fields))));
    }

    // Try from existing batches
    if let Some(schema) = table
        .versions
        .values()
        .flat_map(|batches| batches.iter())
        .next()
        .map(|batch| batch.schema())
    {
        return Ok(Some(schema));
    }

    // Fallback: infer from JSON keys
    if values.is_empty() {
        return Ok(None);
    }
    let fields: Vec<Field> = values
        .keys()
        .map(|k| Field::new(k.as_str(), DataType::Utf8, true))
        .collect();
    Ok(Some(Arc::new(arrow_schema::Schema::new(fields))))
}

fn resolve_table_schema_from_map(
    table: &VersionedTable,
    row: &serde_json::Map<String, serde_json::Value>,
    reg: &TypeRegistry,
) -> ArrowStoreResult<Option<Arc<arrow_schema::Schema>>> {
    resolve_table_schema(table, row, reg)
}

/// Extract a u64 from a JSON value for use as a RowId.
fn json_value_to_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn json_map_to_record_batch(
    values: &serde_json::Map<String, serde_json::Value>,
    schema: &Arc<arrow_schema::Schema>,
    _row: &RowId,
) -> ArrowStoreResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let val = values.get(field.name()).unwrap_or(&serde_json::Value::Null);
        let array = json_value_to_array(val, field.data_type())?;
        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ArrowStoreError::Arrow(format!("json_map_to_record_batch: {}", e)))
}

fn build_update_patch(
    src_batch: &RecordBatch,
    row_idx: usize,
    col_idx: usize,
    new_value: &serde_json::Value,
) -> ArrowStoreResult<RecordBatch> {
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(src_batch.num_columns());

    for c in 0..src_batch.num_columns() {
        if c == col_idx {
            let dt = src_batch.column(c).data_type();
            let arr = json_value_to_array(new_value, dt)?;
            new_columns.push(arr);
        } else {
            // Copy the single value from the source row
            let src_arr = src_batch.column(c);
            let single = slice_array_to_single(src_arr, row_idx)?;
            new_columns.push(single);
        }
    }

    RecordBatch::try_new(src_batch.schema(), new_columns)
        .map_err(|e| ArrowStoreError::Arrow(format!("build_update_patch: {}", e)))
}

pub(crate) fn slice_array_to_single(array: &dyn Array, idx: usize) -> ArrowStoreResult<ArrayRef> {
    let sliced = array.slice(idx, 1);
    Ok(sliced)
}

pub(crate) fn make_null_array(data_type: &DataType) -> ArrowStoreResult<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(1);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(1);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(1);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(1);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(1);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(1, 0);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::with_capacity(1, 0);
            builder.append_null();
            Ok(Arc::new(builder.finish()))
        }
        other => Err(ArrowStoreError::Schema(format!(
            "unsupported null array type: {:?}",
            other
        ))),
    }
}

// ------------------------------------------------------------------
// Internal helpers
// ------------------------------------------------------------------

fn patch_record_batch(
    original: &RecordBatch,
    patch_batch: &RecordBatch,
    patches: &[(usize, usize)],
) -> ArrowStoreResult<RecordBatch> {
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(original.num_columns());

    for col_idx in 0..original.num_columns() {
        let orig_array = original.column(col_idx);
        let patch_array = patch_batch.column(col_idx);
        let patched = patch_array_values(orig_array, patch_array, patches, original.num_rows())?;
        new_columns.push(patched);
    }

    RecordBatch::try_new(original.schema(), new_columns)
        .map_err(|e| ArrowStoreError::Arrow(format!("rebuild patched batch: {}", e)))
}

fn patch_array_values(
    orig: &dyn Array,
    patch: &dyn Array,
    patches: &[(usize, usize)],
    _total_rows: usize,
) -> ArrowStoreResult<ArrayRef> {
    match orig.data_type() {
        DataType::Int64 => patch_primitive_array::<Int64Type>(orig, patch, patches),
        DataType::Int32 => patch_primitive_array::<Int32Type>(orig, patch, patches),
        DataType::UInt64 => patch_primitive_array::<UInt64Type>(orig, patch, patches),
        DataType::Float64 => patch_primitive_array::<Float64Type>(orig, patch, patches),
        DataType::Boolean => patch_boolean_array(orig, patch, patches),
        DataType::Utf8 => patch_string_array::<i32>(orig, patch, patches),
        DataType::LargeUtf8 => patch_string_array::<i64>(orig, patch, patches),
        other => Err(ArrowStoreError::Schema(format!(
            "unsupported patch type: {:?}",
            other
        ))),
    }
}

fn patch_primitive_array<T: arrow::array::ArrowPrimitiveType>(
    orig: &dyn Array,
    patch: &dyn Array,
    patches: &[(usize, usize)],
) -> ArrowStoreResult<ArrayRef>
where
    T::Native: Copy + std::fmt::Debug,
{
    let orig_arr = orig
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or_else(|| ArrowStoreError::Schema("primitive downcast failed".into()))?;
    let patch_arr = patch
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or_else(|| ArrowStoreError::Schema("primitive patch downcast failed".into()))?;

    let mut values: Vec<T::Native> = orig_arr.values().to_vec();
    let len = orig_arr.len();
    let num_bytes = (len + 7) / 8;
    let mut null_bytes: Vec<u8> = orig_arr
        .nulls()
        .map(|n| {
            let buf = n.buffer();
            let src = buf.as_slice();
            src[..num_bytes.min(src.len())].to_vec()
        })
        .unwrap_or_else(|| vec![0xFFu8; num_bytes]);

    for &(target_row, patch_row) in patches {
        if target_row >= len || patch_row >= patch_arr.len() {
            continue;
        }
        let byte_idx = target_row / 8;
        let bit_idx = (target_row % 8) as u8;
        if patch_arr.is_null(patch_row) {
            if byte_idx < null_bytes.len() {
                null_bytes[byte_idx] &= !(1u8 << bit_idx);
            }
        } else {
            values[target_row] = patch_arr.value(patch_row);
            if byte_idx < null_bytes.len() {
                null_bytes[byte_idx] |= 1u8 << bit_idx;
            }
        }
    }

    let nulls = if null_bytes.iter().any(|&b| b != 0xFFu8) {
        let bools: Vec<bool> = (0..len)
            .map(|i| {
                let byte_idx = i / 8;
                let bit_idx = (i % 8) as u8;
                byte_idx < null_bytes.len() && (null_bytes[byte_idx] & (1u8 << bit_idx)) != 0
            })
            .collect();
        Some(arrow::buffer::NullBuffer::from(bools))
    } else {
        None
    };
    Ok(Arc::new(arrow::array::PrimitiveArray::<T>::new(
        values.into(),
        nulls,
    )))
}

fn patch_boolean_array(
    orig: &dyn Array,
    patch: &dyn Array,
    patches: &[(usize, usize)],
) -> ArrowStoreResult<ArrayRef> {
    let orig_arr = orig
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArrowStoreError::Schema("boolean downcast failed".into()))?;
    let patch_arr = patch
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ArrowStoreError::Schema("boolean patch downcast failed".into()))?;

    let len = orig_arr.len();
    let mut values: Vec<Option<bool>> = (0..len)
        .map(|i| {
            if orig_arr.is_null(i) {
                None
            } else {
                Some(orig_arr.value(i))
            }
        })
        .collect();

    for &(target_row, patch_row) in patches {
        if target_row < values.len() && patch_row < patch_arr.len() {
            if patch_arr.is_null(patch_row) {
                values[target_row] = None;
            } else {
                values[target_row] = Some(patch_arr.value(patch_row));
            }
        }
    }

    Ok(Arc::new(values.into_iter().collect::<BooleanArray>()))
}

fn patch_string_array<O: arrow::array::OffsetSizeTrait>(
    orig: &dyn Array,
    patch: &dyn Array,
    patches: &[(usize, usize)],
) -> ArrowStoreResult<ArrayRef> {
    let orig_arr = orig
        .as_any()
        .downcast_ref::<arrow::array::GenericStringArray<O>>()
        .ok_or_else(|| ArrowStoreError::Schema("string downcast failed".into()))?;
    let patch_arr = patch
        .as_any()
        .downcast_ref::<arrow::array::GenericStringArray<O>>()
        .ok_or_else(|| ArrowStoreError::Schema("string patch downcast failed".into()))?;

    let mut values: Vec<Option<String>> =
        orig_arr.iter().map(|v| v.map(|s| s.to_string())).collect();

    for &(target_row, patch_row) in patches {
        if target_row < values.len() && patch_row < patch_arr.len() {
            if patch_arr.is_null(patch_row) {
                values[target_row] = None;
            } else {
                values[target_row] = Some(patch_arr.value(patch_row).to_string());
            }
        }
    }

    let arr: arrow::array::GenericStringArray<O> = values.iter().map(|v| v.as_deref()).collect();
    Ok(Arc::new(arr))
}

fn array_value_to_string(array: &dyn Array, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .map(|a| a.value(row_idx).to_string())
            .unwrap_or_default(),
        _ => format!("{:?}", array),
    }
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_guard::{CommitStore, InitStore};
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field};
    use scharnhorst_core::RowId;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic};
    use std::sync::Arc;

    fn make_int_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn make_pk_spec() -> TableSpec {
        TableSpec::new("test_table")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap()
    }

    // ---- append_batches ----

    #[test]
    fn append_batches_stores_data() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let batch = make_int_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        init_store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1));
        assert!(versions.is_some());
        assert_eq!(versions.unwrap().len(), 1);
    }

    #[test]
    fn append_batches_accumulates_at_same_tick() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let b1 = make_int_batch(vec![1], vec!["x"]);
        let b2 = make_int_batch(vec![2], vec!["y"]);
        init_store
            .append_batches("test_table", Tick(1), vec![b1])
            .unwrap();
        init_store
            .append_batches("test_table", Tick(1), vec![b2])
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1)).unwrap();
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn append_batches_missing_table_fails() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let batch = make_int_batch(vec![1], vec!["a"]);
        let result = init_store.append_batches("missing", Tick(1), vec![batch]);
        assert!(result.is_err());
    }

    // ---- rebuild_table ----

    #[test]
    fn rebuild_table_replaces_all() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::RebuildPerTick)
            .unwrap();

        let b1 = make_int_batch(vec![1, 2], vec!["a", "b"]);
        init_store
            .rebuild_table("test_table", Tick(1), vec![b1])
            .unwrap();

        let b2 = make_int_batch(vec![10, 20, 30], vec!["x", "y", "z"]);
        init_store
            .rebuild_table("test_table", Tick(1), vec![b2])
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1)).unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].num_rows(), 3);
    }

    #[test]
    fn replace_duplicate_row_id_errors() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::RebuildPerTick)
            .unwrap();
        let commit_store = init_store.into_simulation().unwrap();

        let mut row1 = serde_json::Map::new();
        row1.insert("id".to_owned(), serde_json::Value::Number(1.into()));

        let mut row2 = serde_json::Map::new();
        // Duplicate RowId — should be detected and rejected.
        row2.insert("id".to_owned(), serde_json::Value::Number(1.into()));

        let diff = Diff::ReplaceTable {
            table: "test_table".to_owned(),
            rows: vec![row1, row2],
        };
        let result = commit_store.apply_diffs(Tick(1), &[diff]);
        assert!(result.is_err(), "duplicate RowId must be rejected");
        match result.unwrap_err() {
            ArrowStoreError::DuplicateRowId { table, row_id } => {
                assert_eq!(table, "test_table");
                assert_eq!(row_id, 1);
            }
            e => panic!("expected DuplicateRowId, got {:?}", e),
        }
    }

    // ---- patch_rows ----

    #[test]
    fn patch_rows_updates_values() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::Patchable)
            .unwrap();

        let batch = make_int_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        init_store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();

        store.advance_to_simulation().unwrap();

        let locations = vec![RowLocation {
            tick: Tick(1),
            batch_index: 0,
            row_index: 1,
            row_id: RowId::new(2),
        }];
        let patch_batch = make_int_batch(vec![99], vec!["patched"]);
        commit_store
            .patch_rows("test_table", Tick(1), &locations, patch_batch)
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1)).unwrap();
        let patched_batch = &versions[0];

        let id_col = patched_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name_col = patched_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 99);
        assert_eq!(id_col.value(2), 3);
        assert_eq!(name_col.value(1), "patched");
    }

    // ---- build_primary_key_index ----

    #[test]
    fn build_pk_index_from_batches() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let batch = make_int_batch(vec![10, 20, 30], vec!["x", "y", "z"]);
        init_store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();
        init_store.build_primary_key_index("test_table").unwrap();

        let idx = store.primary_key_index("test_table").unwrap();
        assert_eq!(idx.len(), 3);
        assert!(idx.lookup("10").is_some());
        assert!(idx.lookup("20").is_some());
        assert!(idx.lookup("30").is_some());
    }

    #[test]
    fn build_pk_index_no_spec_fails() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = TableSpec::new("no_pk");
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let result = init_store.build_primary_key_index("no_pk");
        assert!(result.is_err());
    }

    // ---- truncate_before ----

    #[test]
    fn truncate_before_removes_old_data() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let batch = make_int_batch(vec![1], vec!["a"]);
        init_store
            .append_batches("test_table", Tick(10), vec![batch.clone()])
            .unwrap();
        init_store
            .append_batches("test_table", Tick(50), vec![batch.clone()])
            .unwrap();
        init_store
            .append_batches("test_table", Tick(100), vec![batch])
            .unwrap();

        store.advance_to_simulation().unwrap();

        commit_store.generate_snapshot(Tick(10)).unwrap();
        commit_store.generate_snapshot(Tick(50)).unwrap();
        commit_store.generate_snapshot(Tick(100)).unwrap();

        commit_store.truncate_before(Tick(50)).unwrap();

        assert!(store.get_snapshot(Tick(10)).is_err());
        assert!(store.get_snapshot(Tick(50)).is_ok());
        assert!(store.get_snapshot(Tick(100)).is_ok());

        let table = store.get_table("test_table").unwrap();
        assert!(table.get_version(Tick(10)).is_none());
        assert!(table.get_version(Tick(50)).is_some());
        assert!(table.get_version(Tick(100)).is_some());
    }

    // ---- write_checkpoint ----

    #[test]
    fn write_checkpoint_creates_files() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let batch = make_int_batch(vec![1, 2], vec!["a", "b"]);
        init_store
            .append_batches("test_table", Tick(5), vec![batch])
            .unwrap();

        store.advance_to_simulation().unwrap();

        commit_store.generate_snapshot(Tick(5)).unwrap();

        let result = commit_store.write_checkpoint(Tick(5));
        assert!(result.is_ok());

        let _ = fs::remove_dir_all("checkpoint_5");
    }

    #[test]
    fn write_checkpoint_respects_lifecycle() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let batch = make_int_batch(vec![1], vec!["a"]);
        init_store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();

        init_store.write_checkpoint(Tick(1)).unwrap();

        store.advance_to_simulation().unwrap();

        let result = init_store.write_checkpoint(Tick(1));
        assert!(result.is_err());

        commit_store.generate_snapshot(Tick(1)).unwrap();
        commit_store.write_checkpoint(Tick(1)).unwrap();

        let _ = fs::remove_dir_all("checkpoint_1");
    }

    #[test]
    fn write_checkpoint_commit_fails_during_init() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let batch = make_int_batch(vec![1], vec!["a"]);
        init_store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();

        let result = commit_store.write_checkpoint(Tick(1));
        assert!(result.is_err());
    }

    // ---- patch helpers ----

    #[test]
    fn patch_primitives_work() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        let orig = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let patch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![99i64])) as ArrayRef],
        )
        .unwrap();

        let patches = vec![(1usize, 0usize)];
        let result = patch_record_batch(&orig, &patch, &patches).unwrap();

        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 99);
        assert_eq!(col.value(2), 3);
    }

    #[test]
    fn generation_increments_on_snapshot() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        assert_eq!(store.generation().unwrap(), 0);

        store.advance_to_simulation().unwrap();

        commit_store.generate_snapshot(Tick(1)).unwrap();
        assert_eq!(store.generation().unwrap(), 1);
        commit_store.generate_snapshot(Tick(2)).unwrap();
        assert_eq!(store.generation().unwrap(), 2);
    }

    // ---- type registry ----

    #[test]
    fn default_registry_contains_all_builtin_types() {
        let reg = default_type_registry();
        for name in &["i64", "i32", "u64", "f64", "utf8", "large_utf8", "bool"] {
            assert!(reg.contains(name), "missing built-in type: {}", name);
        }
    }

    #[test]
    fn resolve_data_type_resolves_builtin_types() {
        let reg = default_type_registry();
        assert_eq!(reg.resolve_data_type("i64").unwrap(), DataType::Int64);
        assert_eq!(reg.resolve_data_type("i32").unwrap(), DataType::Int32);
        assert_eq!(reg.resolve_data_type("u64").unwrap(), DataType::UInt64);
        assert_eq!(reg.resolve_data_type("f64").unwrap(), DataType::Float64);
        assert_eq!(reg.resolve_data_type("utf8").unwrap(), DataType::Utf8);
        assert_eq!(
            reg.resolve_data_type("large_utf8").unwrap(),
            DataType::LargeUtf8
        );
        assert_eq!(reg.resolve_data_type("bool").unwrap(), DataType::Boolean);
    }

    #[test]
    fn resolve_data_type_rejects_unknown() {
        let reg = default_type_registry();
        let result = reg.resolve_data_type("nonexistent_type");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported storage type"),
            "error should mention unsupported type"
        );
    }

    #[test]
    fn store_register_type_makes_it_resolvable() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let commit_store = CommitStore::new(Arc::clone(&store));
        let name = "custom_int128";
        init_store
            .register_type(
                name,
                DataType::Int64,
                Arc::new(|v: &serde_json::Value| {
                    let n = v.as_i64().unwrap_or(0);
                    Ok(Arc::new(Int64Array::from(vec![n])))
                }),
                Arc::new(|| Ok(Arc::new(Int64Array::from(vec![0i64])))),
            )
            .unwrap();

        // Verify by creating a table that uses the custom type
        let spec = TableSpec::new("custom_table")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, name))
            .unwrap();
        init_store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        store.advance_to_simulation().unwrap();

        let mut values = serde_json::Map::new();
        values.insert("id".to_owned(), serde_json::Value::Number(42.into()));
        commit_store
            .apply_diffs(
                Tick(1),
                &[Diff::Insert {
                    table: "custom_table".to_owned(),
                    row: RowId::new(42),
                    values,
                }],
            )
            .unwrap();

        let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
        let batches = snap.table_batches("custom_table").unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn registry_get_returns_entry_with_factories() {
        let reg = default_type_registry();
        let entry = reg.get("i64").unwrap();
        assert_eq!(entry.data_type, DataType::Int64);

        let arr = (entry.null_array)().unwrap();
        assert_eq!(arr.len(), 1);
        let arr = (entry.json_to_array)(&serde_json::json!(42)).unwrap();
        assert_eq!(arr.len(), 1);
    }

    // ---- apply_diffs atomicity ----

    #[test]
    fn test_apply_diffs_atomic_rollback_on_partial_failure() {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec();
        init_store
            .create_table(&spec, MutationMode::Patchable)
            .unwrap();

        let commit_store = init_store.into_simulation().unwrap();

        // Insert initial row with id=1 at Tick(1)
        let mut values = serde_json::Map::new();
        values.insert("id".to_owned(), serde_json::Value::Number(1.into()));
        commit_store
            .apply_diffs(
                Tick(1),
                &[Diff::Insert {
                    table: "test_table".to_owned(),
                    row: RowId::new(1),
                    values,
                }],
            )
            .unwrap();

        // Capture pre-diff data snapshot
        let table_before = store.get_table("test_table").unwrap();
        let versions_before = table_before.get_version(Tick(1)).unwrap().clone();
        let id_val_before = versions_before[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        // Diffs: valid Update followed by invalid Update to non-existent table
        let valid_diff = Diff::Update {
            table: "test_table".to_owned(),
            row: RowId::new(1),
            column: "id".to_owned(),
            value: serde_json::Value::Number(99.into()),
        };
        let invalid_diff = Diff::Update {
            table: "non_existent".to_owned(),
            row: RowId::new(1),
            column: "id".to_owned(),
            value: serde_json::Value::Number(1.into()),
        };

        let result = commit_store.apply_diffs(Tick(2), &[valid_diff, invalid_diff]);
        assert!(result.is_err(), "partial failure must return error");

        // Verify data is UNCHANGED — the first Update was NOT applied
        let table_after = store.get_table("test_table").unwrap();
        let versions_after = table_after.get_version(Tick(1)).unwrap();
        let id_val_after = versions_after[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            id_val_after, id_val_before,
            "data must be unchanged after atomic rollback"
        );
    }
}
