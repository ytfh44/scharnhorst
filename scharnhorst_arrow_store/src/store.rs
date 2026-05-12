use std::collections::HashMap;
use std::fs;
use std::io::BufWriter;
use std::sync::{Arc, RwLock};

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder,
    Int32Array, Int32Builder, Int64Array, Int64Builder, LargeStringBuilder,
    StringArray, StringBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::DataType;
use arrow::ipc::writer::FileWriter;
use arrow_array::types::{
    Float64Type, Int32Type, Int64Type, UInt64Type,
};
use arrow_array::RecordBatch;
use arrow_schema::Field;
use scharnhorst_core::{Diff, RowId, RowPositionMap, Tick, TableId};
use scharnhorst_schema::TableSpec;

use crate::error::{ArrowStoreError, ArrowStoreResult};
use crate::index::{ForeignKeyIndex, PrimaryKeyIndex, RowLocation};
use crate::partition::{PartitionMap, PartitionSnapshot};
use crate::snapshot::WorldSnapshot;
use crate::versioned_table::{MutationMode, VersionedTable};

// ------------------------------------------------------------------
// Type Registry 鈥?extensible Arrow data-type mapping
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
            .ok_or_else(|| {
                ArrowStoreError::Schema(format!("unsupported storage type: {}", name))
            })
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
                v.as_str(),
            ])))
        }),
        Arc::new(|| Ok(Arc::new(arrow::array::LargeStringArray::from(vec![
            "",
        ])))),
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

/// Thread-safe wrapper around the raw ArrowStore state.
///
/// All mutable operations acquire a write lock; read-only operations
/// acquire a read lock. This makes `ArrowStore` safe to share across
/// threads (e.g. inside an `Arc`) without data races.
#[derive(Debug, Clone, Default)]
pub struct ArrowStore {
    inner: Arc<RwLock<ArrowStoreInner>>,
}

#[derive(Debug, Clone)]
pub struct ArrowStoreInner {
    tables: HashMap<String, VersionedTable>,
    table_id_map: HashMap<TableId, String>,
    snapshots: HashMap<Tick, Arc<WorldSnapshot>>,
    next_table_id: u64,
    generation: u64,
    type_registry: TypeRegistry,
}

impl Default for ArrowStoreInner {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            table_id_map: HashMap::new(),
            snapshots: HashMap::new(),
            next_table_id: 0,
            generation: 0,
            type_registry: default_type_registry(),
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
        name: impl Into<String>,
        data_type: DataType,
        json_to_array: JsonToArrayFn,
        null_array: NullArrayFn,
    ) -> ArrowStoreResult<()> {
        let mut inner = self.write("register_type")?;
        inner.type_registry.register(name, data_type, json_to_array, null_array);
        Ok(())
    }

    fn read(&self, _operation: &str) -> ArrowStoreResult<std::sync::RwLockReadGuard<'_, ArrowStoreInner>> {
        self.inner
            .read()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))
    }

    fn write(&self, _operation: &str) -> ArrowStoreResult<std::sync::RwLockWriteGuard<'_, ArrowStoreInner>> {
        self.inner
            .write()
            .map_err(|e| ArrowStoreError::LockPoisoned(e.to_string()))
    }

 // ------------------------------------------------------------------
 // Table management
 // ------------------------------------------------------------------

    pub fn create_table(
        &self,
        spec: &TableSpec,
        mode: MutationMode,
    ) -> ArrowStoreResult<TableId> {
        let mut inner = self.write("create_table")?;
        if inner.tables.contains_key(&spec.name) {
            return Err(ArrowStoreError::TableAlreadyExists(spec.name.clone()));
        }

        let id = TableId::new(inner.next_table_id);
        inner.next_table_id += 1;

        let mut table = VersionedTable::new(&spec.name, mode);
        table.set_spec(spec.clone());
        inner.tables.insert(spec.name.clone(), table);
        if inner.table_id_map.contains_key(&id) {
            return Err(ArrowStoreError::TableAlreadyExists(
                format!("table id {:?} already mapped", id),
            ));
        }
        inner.table_id_map.insert(id, spec.name.clone());
        Ok(id)
    }

    pub fn drop_table(&self, name: &str) -> ArrowStoreResult<()> {
        let mut inner = self.write("drop_table")?;
        let id = inner
            .table_id_map
            .iter()
            .find(|(_, n)| *n == name)
            .map(|(id, _)| *id);

        if let Some(id) = id {
            inner.table_id_map.remove(&id);
        }

        inner
            .tables
            .remove(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> ArrowStoreResult<VersionedTable> {
        let inner = self.read("get_table")?;
        inner
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))
    }

    pub fn table_names(&self) -> ArrowStoreResult<Vec<String>> {
        let inner = self.read("table_names")?;
        Ok(inner.tables.keys().cloned().collect())
    }

    pub fn table_count(&self) -> ArrowStoreResult<usize> {
        let inner = self.read("table_count")?;
        Ok(inner.tables.len())
    }

 // ------------------------------------------------------------------
 // Mutation modes
 // ------------------------------------------------------------------

    pub fn set_mutation_mode(&self, name: &str, mode: MutationMode) -> ArrowStoreResult<()> {
        let mut inner = self.write("set_mutation_mode")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        table.mutation_mode = mode;
        Ok(())
    }

    pub fn mutation_mode(&self, name: &str) -> ArrowStoreResult<MutationMode> {
        let inner = self.read("mutation_mode")?;
        let table = inner
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(table.mutation_mode())
    }

 // ------------------------------------------------------------------
 // Data ingestion
 // ------------------------------------------------------------------

    pub fn append_batches(
        &self,
        name: &str,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        let mut inner = self.write("append_batches")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let entry = table.versions.entry(tick).or_default();
        entry.extend(batches);
        Ok(())
    }

    pub fn patch_rows(
        &self,
        name: &str,
        tick: Tick,
        locations: &[RowLocation],
        batch: RecordBatch,
    ) -> ArrowStoreResult<()> {
        if locations.is_empty() {
            return Ok(());
        }
        let mut inner = self.write("patch_rows")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let versions = table
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
        name: &str,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        let mut inner = self.write("rebuild_table")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;

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

    pub fn generate_snapshot(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        let mut inner = self.write("generate_snapshot")?;
        inner.generation += 1;
        let mut snapshot = WorldSnapshot::new(tick);

        for (name, table) in &inner.tables {
            snapshot.register_table(name, Arc::new(table.clone()));

            for region_id in table.partitions().region_ids() {
                let partition = table.partitions().get(region_id)?;
                let ps = PartitionSnapshot::new(
                    region_id,
                    tick,
                    partition.batches().to_vec(),
                );
                snapshot.register_partition_snapshot(name, ps);
            }
        }

        let arc = Arc::new(snapshot);
        inner.snapshots.insert(tick, arc.clone());
        Ok(arc)
    }

    pub fn get_snapshot(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        let inner = self.read("get_snapshot")?;
        inner
            .snapshots
            .get(&tick)
            .cloned()
            .ok_or(ArrowStoreError::SnapshotNotFound(tick.as_u64()))
    }

    pub fn latest_snapshot(&self) -> ArrowStoreResult<Option<Arc<WorldSnapshot>>> {
        let inner = self.read("latest_snapshot")?;
        let latest = inner
            .snapshots
            .keys()
            .copied()
            .max()
            .and_then(|tick| inner.snapshots.get(&tick).cloned());
        Ok(latest)
    }

    pub fn snapshot_ticks(&self) -> ArrowStoreResult<Vec<Tick>> {
        let inner = self.read("snapshot_ticks")?;
        Ok(inner.snapshots.keys().copied().collect())
    }

 // ------------------------------------------------------------------
 // Indexing
 // ------------------------------------------------------------------

    pub fn build_primary_key_index(&self, name: &str) -> ArrowStoreResult<()> {
        let pk_name = {
            let inner = self.read("build_primary_key_index.read_spec")?;
            let table = inner
                .tables
                .get(name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
            let spec = table
                .spec()
                .ok_or_else(|| ArrowStoreError::Schema("no table spec for PK index".into()))?;
            let pk_col = spec
                .primary_key_column()
                .ok_or_else(|| ArrowStoreError::Schema("no primary key column defined".into()))?;
            pk_col.name.clone()
        };

        let mut inner = self.write("build_primary_key_index.build_index")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        table.primary_key_index = PrimaryKeyIndex::new();

        let versions: Vec<(Tick, Vec<RecordBatch>)> = table
            .versions
            .iter()
            .map(|(&t, b)| (t, b.clone()))
            .collect();

        for (tick, batches) in &versions {
            for (batch_idx, batch) in batches.iter().enumerate() {
                let col_idx = batch
                    .schema()
                    .index_of(&pk_name)
                    .map_err(|e| ArrowStoreError::Schema(format!("column {} not found: {}", pk_name, e)))?;
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
        name: &str,
        column: &str,
        target_table: &str,
    ) -> ArrowStoreResult<()> {
        let mut inner = self.write("build_foreign_key_index")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
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
        let inner = self.read("primary_key_index")?;
        let table = inner
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(table.primary_key_index().clone())
    }

    pub fn foreign_key_indices(&self, name: &str) -> ArrowStoreResult<Vec<ForeignKeyIndex>> {
        let inner = self.read("foreign_key_indices")?;
        let table = inner
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(table.foreign_key_indices().to_vec())
    }

 // ------------------------------------------------------------------
 // Partitioned access
 // ------------------------------------------------------------------

    pub fn partition_map(&self, name: &str) -> ArrowStoreResult<PartitionMap> {
        let inner = self.read("partition_map")?;
        let table = inner
            .tables
            .get(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(table.partitions().clone())
    }

    pub fn partition_map_mut<F, R>(&self, name: &str, f: F) -> ArrowStoreResult<R>
    where
        F: FnOnce(&mut PartitionMap) -> R,
    {
        let mut inner = self.write("partition_map_mut")?;
        let table = inner
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        Ok(f(table.partitions_mut()))
    }

    pub fn get_partition(
        &self,
        table_name: &str,
        region_id: &str,
    ) -> ArrowStoreResult<crate::partition::Partition> {
        let inner = self.read("get_partition")?;
        let table = inner
            .tables
            .get(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        table.partitions().get(region_id).cloned()
    }

 // ------------------------------------------------------------------
 // Checkpoint / diff helpers
 // ------------------------------------------------------------------

    pub fn write_checkpoint(&self, tick: Tick) -> ArrowStoreResult<()> {
        let inner = self.read("write_checkpoint")?;
        let snapshot = match inner.snapshots.get(&tick) {
            Some(s) => s,
            None => {
                let latest = inner
                    .snapshots
                    .keys()
                    .max()
                    .and_then(|k| inner.snapshots.get(k));
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
            let mut ipc_writer =
                FileWriter::try_new(writer, &schema)
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

    pub fn load_checkpoint(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
        use std::io::BufReader;
        use arrow::ipc::reader::FileReader;

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
        let entries = fs::read_dir(&dir).map_err(|e| {
            ArrowStoreError::Io(format!("read checkpoint dir {}: {}", dir, e))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                ArrowStoreError::Io(format!("read dir entry in {}: {}", dir, e))
            })?;
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

                let file = fs::File::open(&path).map_err(|e| {
                    ArrowStoreError::Io(format!("open {}: {}", path.display(), e))
                })?;
                let reader = BufReader::new(file);
                let ipc_reader = FileReader::try_new(reader, None).map_err(|e| {
                    ArrowStoreError::Arrow(format!(
                        "ipc reader for table {}: {}",
                        table_name, e
                    ))
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

 // Step 2: Acquire write lock, create tables if needed,
 // insert data, and generate snapshot all in one critical section.
        let mut inner = self.write("load_checkpoint")?;
        for (name, batches) in table_batches {
            inner
                .tables
                .entry(name.clone())
                .or_insert_with(|| VersionedTable::new(&name, MutationMode::AppendOnly));
            let table = inner
                .tables
                .get_mut(&name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(name.clone()))?;
            table.versions.insert(tick, batches);
        }

 // Inline snapshot generation -- cannot call generate_snapshot
 // while holding the write lock (RwLock is not reentrant).
        inner.generation += 1;
        let mut snapshot = WorldSnapshot::new(tick);
        for (name, table) in &inner.tables {
            snapshot.register_table(name, Arc::new(table.clone()));
            for region_id in table.partitions().region_ids() {
                let partition = table.partitions().get(region_id)?;
                let ps =
                    PartitionSnapshot::new(region_id, tick, partition.batches().to_vec());
                snapshot.register_partition_snapshot(name, ps);
            }
        }
        let arc = Arc::new(snapshot);
        inner.snapshots.insert(tick, arc.clone());
        Ok(arc)
    }

    pub fn truncate_before(&self, tick: Tick) -> ArrowStoreResult<()> {
        let mut inner = self.write("truncate_before")?;
        inner.snapshots.retain(|&t, _| t >= tick);

        for table in inner.tables.values_mut() {
            table.versions.retain(|&t, _| t >= tick);
        }
        Ok(())
    }

    pub fn generation(&self) -> ArrowStoreResult<u64> {
        let inner = self.read("generation")?;
        Ok(inner.generation)
    }

 // ------------------------------------------------------------------
 // Diff application
 // ------------------------------------------------------------------

    pub fn apply_diffs(&self, tick: Tick, diffs: &[Diff]) -> ArrowStoreResult<()> {
        let mut inner = self.write("apply_diffs")?;
        inner.apply_diffs(tick, diffs)
    }
}

// ------------------------------------------------------------------
// ArrowStoreInner diff application
// ------------------------------------------------------------------

impl ArrowStoreInner {
    pub fn apply_diffs(&mut self, tick: Tick, diffs: &[Diff]) -> ArrowStoreResult<()> {
        for diff in diffs {
            match diff {
                Diff::Update { table, row, column, value } => {
                    self.apply_update(tick, table, row, column, value)?;
                }
                Diff::Insert { table, row, values } => {
                    self.apply_insert(tick, table, row, values)?;
                }
                Diff::Delete { table, row } => {
                    self.apply_delete(tick, table, row)?;
                }
                Diff::ReplaceTable { table, rows } => {
                    self.apply_replace(tick, table, rows)?;
                }
            }
        }
        Ok(())
    }

    fn apply_update(
        &mut self,
        _tick: Tick,
        table_name: &str,
        row: &RowId,
        column: &str,
        value: &serde_json::Value,
    ) -> ArrowStoreResult<()> {
        let (batch_idx, row_offset) = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            table
                .position_map
                .position_of(*row)
                .ok_or_else(|| ArrowStoreError::Generic(format!(
                    "row not found in position map: table={}, row_id={}",
                    table_name, row.as_u64()
                )))?
        };

        let loc_tick = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            let mut ticks: Vec<Tick> = table
                .versions
                .iter()
                .filter(|(_, batches)| batches.len() > batch_idx)
                .map(|(&t, _)| t)
                .collect();
            ticks.sort();
            ticks
                .into_iter()
                .last()
                .ok_or_else(|| ArrowStoreError::Generic(format!(
                    "no tick found containing batch_idx {} for table {}",
                    batch_idx, table_name
                )))?
        };

        let (patch_batch, loc) = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
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

            let col_idx = batches[batch_idx]
                .schema()
                .index_of(column)
                .map_err(|_| ArrowStoreError::Schema(format!(
                    "column '{}' not found in table '{}'",
                    column, table_name
                )))?;

            let p_batch = build_update_patch(&batches[batch_idx], row_offset, col_idx, value)?;

            let rloc = RowLocation {
                tick: loc_tick,
                batch_index: batch_idx,
                row_index: row_offset,
                row_id: *row,
            };
            (p_batch, rloc)
        };

        self.patch_inner(table_name, loc.tick, &[loc], patch_batch)
    }

    fn apply_delete(
        &mut self,
        _tick: Tick,
        table_name: &str,
        row: &RowId,
    ) -> ArrowStoreResult<()> {
        let (batch_idx, row_offset) = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            table
                .position_map
                .position_of(*row)
                .ok_or_else(|| ArrowStoreError::Generic(format!(
                    "row not found in position map for delete: table={}, row_id={}",
                    table_name, row.as_u64()
                )))?
        };

        let loc_tick = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            let mut ticks: Vec<Tick> = table
                .versions
                .iter()
                .filter(|(_, batches)| batches.len() > batch_idx)
                .map(|(&t, _)| t)
                .collect();
            ticks.sort();
            ticks
                .into_iter()
                .last()
                .ok_or_else(|| ArrowStoreError::Generic(format!(
                    "no tick found for delete: table={}, batch_idx={}",
                    table_name, batch_idx
                )))?
        };

        let (null_patch, loc) = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
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

            let n_patch = crate::versioned_table::build_null_patch(&batches[batch_idx], row_offset)?;

            let rloc = RowLocation {
                tick: loc_tick,
                batch_index: batch_idx,
                row_index: row_offset,
                row_id: *row,
            };
            (n_patch, rloc)
        };

        self.patch_inner(table_name, loc.tick, &[loc], null_patch)?;

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        table.position_map.remove(row);

        Ok(())
    }

    fn apply_insert(
        &mut self,
        tick: Tick,
        table_name: &str,
        row: &RowId,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> ArrowStoreResult<()> {
 // Determine schema from spec or existing batches (immutable borrow only)
        let schema = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            resolve_table_schema(table, values, &self.type_registry)?
                .ok_or_else(|| ArrowStoreError::Schema(format!(
                    "cannot determine schema for insert into table '{}'",
                    table_name
                )))?
        };

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;

        let batch = json_map_to_record_batch(values, &schema, row)?;
        let entry = table.versions.entry(tick).or_default();
        entry.push(batch);

 // Also update PK index if it exists
        let pk_col_name = table
            .spec
            .as_ref()
            .and_then(|s| s.primary_key_column())
            .map(|c| c.name.clone());

        if let Some(ref pk_name) = pk_col_name {
            if let Some(pk_value) = values.get(pk_name) {
                let key = match pk_value {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let batch_idx = entry.len().saturating_sub(1);
                let row_idx_in_batch = 0usize;
                table
                    .primary_key_index
                    .insert(key, tick, batch_idx, row_idx_in_batch);
            }
        }

 // Update RowPositionMap
        let batch_idx = entry.len().saturating_sub(1);
        let row_offset = 0usize;
        table.position_map.insert(*row, batch_idx, row_offset);

        Ok(())
    }

    fn apply_replace(
        &mut self,
        tick: Tick,
        table_name: &str,
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> ArrowStoreResult<()> {
 // Determine schema from spec or first row (scoped borrow)
        let schema = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            rows.first()
                .and_then(|row| resolve_table_schema_from_map(table, row, &self.type_registry).ok())
                .flatten()
                .ok_or_else(|| ArrowStoreError::Schema(format!(
                    "cannot determine schema for ReplaceTable on '{}'",
                    table_name
                )))?
        };

        let mut batches: Vec<RecordBatch> = Vec::with_capacity(rows.len());
        for (i, row_map) in rows.iter().enumerate() {
            let temp_row_id = RowId::new(i as u64);
            let batch = json_map_to_record_batch(row_map, &schema, &temp_row_id)?;
            batches.push(batch);
        }

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;

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

    fn ensure_primary_key_index(&mut self, table_name: &str) -> ArrowStoreResult<()> {
        let pk_name = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
            let spec = table
                .spec
                .as_ref()
                .ok_or_else(|| ArrowStoreError::Schema("no table spec for PK index".into()))?;
            let pk_col = spec
                .primary_key_column()
                .ok_or_else(|| ArrowStoreError::Schema("no primary key column defined".into()))?;
            pk_col.name.clone()
        };

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(table_name.to_owned()))?;
        table.primary_key_index = PrimaryKeyIndex::new();

        let versions: Vec<(Tick, Vec<RecordBatch>)> = table
            .versions
            .iter()
            .map(|(&t, b)| (t, b.clone()))
            .collect();

        for (ver_tick, batches) in &versions {
            for (batch_idx, batch) in batches.iter().enumerate() {
                let col_idx = batch.schema().index_of(&pk_name).map_err(|e| {
                    ArrowStoreError::Schema(format!("column {} not found: {}", pk_name, e))
                })?;
                let array = batch.column(col_idx);
                for row_idx in 0..batch.num_rows() {
                    let key = array_value_to_string(array, row_idx);
                    table
                        .primary_key_index
                        .insert(key, *ver_tick, batch_idx, row_idx);
                }
            }
        }
        Ok(())
    }

    fn patch_inner(
        &mut self,
        name: &str,
        tick: Tick,
        locations: &[RowLocation],
        batch: RecordBatch,
    ) -> ArrowStoreResult<()> {
        if locations.is_empty() {
            return Ok(());
        }
        let table = self
            .tables
            .get_mut(name)
            .ok_or_else(|| ArrowStoreError::TableNotFound(name.to_owned()))?;
        let versions = table
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
}

// ------------------------------------------------------------------
// JSON to Arrow conversion helpers
// ------------------------------------------------------------------

fn json_value_to_array(value: &serde_json::Value, data_type: &DataType) -> ArrowStoreResult<ArrayRef> {
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
            Ok(Arc::new(arrow::array::LargeStringArray::from(vec![v.as_str()])))
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
            "value {} out of range for i64", v
        )));
    }
    Err(ArrowStoreError::Schema(format!("expected i64, got {:?}", value)))
}

fn json_to_i32(value: &serde_json::Value) -> ArrowStoreResult<i32> {
    let val = value
        .as_i64()
        .or_else(|| value.as_f64().map(|f| f as i64))
        .ok_or_else(|| ArrowStoreError::Schema(format!("expected i32, got {:?}", value)))?;
    i32::try_from(val).map_err(|_| ArrowStoreError::Schema(format!(
        "value {} out of range for i32", val
    )))
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
            "value {} out of range for u64", v
        )));
    }
    Err(ArrowStoreError::Schema(format!("expected u64, got {:?}", value)))
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
    <T as arrow::array::ArrowPrimitiveType>::Native: Copy,
{
    let orig_arr = orig
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or_else(|| ArrowStoreError::Schema("primitive downcast failed".into()))?;
    let patch_arr = patch
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or_else(|| ArrowStoreError::Schema("primitive patch downcast failed".into()))?;

    let mut values: Vec<<T as arrow::array::ArrowPrimitiveType>::Native> =
        orig_arr.values().to_vec();

    for &(target_row, patch_row) in patches {
        if target_row < values.len() && patch_row < patch_arr.len() {
            values[target_row] = patch_arr.value(patch_row);
        }
    }

    Ok(Arc::new(arrow::array::PrimitiveArray::<T>::new(
        values.into(),
        None,
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

    let mut values: Vec<bool> = orig_arr.iter().map(|v| v.unwrap_or(false)).collect();

    for &(target_row, patch_row) in patches {
        if target_row < values.len() && patch_row < patch_arr.len() {
            values[target_row] = patch_arr.value(patch_row);
        }
    }

    Ok(Arc::new(BooleanArray::from(values)))
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

    let mut values: Vec<Option<String>> = orig_arr
        .iter()
        .map(|v| v.map(|s| s.to_string()))
        .collect();

    for &(target_row, patch_row) in patches {
        if target_row < values.len() && patch_row < patch_arr.len() {
            if patch_arr.is_null(patch_row) {
                values[target_row] = None;
            } else {
                values[target_row] = Some(patch_arr.value(patch_row).to_string());
            }
        }
    }

    let arr: StringArray = values
        .iter()
        .map(|v| v.as_deref())
        .collect();
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
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field};
    use scharnhorst_core::RowId;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic};

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
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        let batch = make_int_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1));
        assert!(versions.is_some());
        assert_eq!(versions.unwrap().len(), 1);
    }

    #[test]
    fn append_batches_accumulates_at_same_tick() {
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        let b1 = make_int_batch(vec![1], vec!["x"]);
        let b2 = make_int_batch(vec![2], vec!["y"]);
        store
            .append_batches("test_table", Tick(1), vec![b1])
            .unwrap();
        store
            .append_batches("test_table", Tick(1), vec![b2])
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1)).unwrap();
        assert_eq!(versions.len(), 2);
    }

    #[test]
    fn append_batches_missing_table_fails() {
        let store = ArrowStore::new();
        let batch = make_int_batch(vec![1], vec!["a"]);
        let result = store.append_batches("missing", Tick(1), vec![batch]);
        assert!(result.is_err());
    }

 // ---- rebuild_table ----

    #[test]
    fn rebuild_table_replaces_all() {
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store
            .create_table(&spec, MutationMode::RebuildPerTick)
            .unwrap();

        let b1 = make_int_batch(vec![1, 2], vec!["a", "b"]);
        store
            .rebuild_table("test_table", Tick(1), vec![b1])
            .unwrap();

        let b2 = make_int_batch(vec![10, 20, 30], vec!["x", "y", "z"]);
        store
            .rebuild_table("test_table", Tick(1), vec![b2])
            .unwrap();

        let table = store.get_table("test_table").unwrap();
        let versions = table.get_version(Tick(1)).unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].num_rows(), 3);
    }

 // ---- patch_rows ----

    #[test]
    fn patch_rows_updates_values() {
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::Patchable).unwrap();

        let batch = make_int_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();

        let locations = vec![RowLocation {
            tick: Tick(1),
            batch_index: 0,
            row_index: 1,
            row_id: RowId::new(2),
        }];
        let patch_batch = make_int_batch(vec![99], vec!["patched"]);
        store
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
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        let batch = make_int_batch(vec![10, 20, 30], vec!["x", "y", "z"]);
        store
            .append_batches("test_table", Tick(1), vec![batch])
            .unwrap();
        store.build_primary_key_index("test_table").unwrap();

        let idx = store.primary_key_index("test_table").unwrap();
        assert_eq!(idx.len(), 3);
        assert!(idx.lookup("10").is_some());
        assert!(idx.lookup("20").is_some());
        assert!(idx.lookup("30").is_some());
    }

    #[test]
    fn build_pk_index_no_spec_fails() {
        let store = ArrowStore::new();
        let spec = TableSpec::new("no_pk");
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        let result = store.build_primary_key_index("no_pk");
        assert!(result.is_err());
    }

 // ---- truncate_before ----

    #[test]
    fn truncate_before_removes_old_data() {
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        let batch = make_int_batch(vec![1], vec!["a"]);
        store
            .append_batches("test_table", Tick(10), vec![batch.clone()])
            .unwrap();
        store
            .append_batches("test_table", Tick(50), vec![batch.clone()])
            .unwrap();
        store
            .append_batches("test_table", Tick(100), vec![batch])
            .unwrap();

        store.generate_snapshot(Tick(10)).unwrap();
        store.generate_snapshot(Tick(50)).unwrap();
        store.generate_snapshot(Tick(100)).unwrap();

        store.truncate_before(Tick(50)).unwrap();

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
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        let batch = make_int_batch(vec![1, 2], vec!["a", "b"]);
        store
            .append_batches("test_table", Tick(5), vec![batch])
            .unwrap();
        store.generate_snapshot(Tick(5)).unwrap();

        let result = store.write_checkpoint(Tick(5));
        assert!(result.is_ok());

        let _ = fs::remove_dir_all("checkpoint_5");
    }

 // ---- patch helpers ----

    #[test]
    fn patch_primitives_work() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("v", DataType::Int64, false),
        ]));
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
        let store = ArrowStore::new();
        let spec = make_pk_spec();
        store.create_table(&spec, MutationMode::AppendOnly).unwrap();

        assert_eq!(store.generation().unwrap(), 0);
        store.generate_snapshot(Tick(1)).unwrap();
        assert_eq!(store.generation().unwrap(), 1);
        store.generate_snapshot(Tick(2)).unwrap();
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
        assert_eq!(reg.resolve_data_type("large_utf8").unwrap(), DataType::LargeUtf8);
        assert_eq!(reg.resolve_data_type("bool").unwrap(), DataType::Boolean);
    }

    #[test]
    fn resolve_data_type_rejects_unknown() {
        let reg = default_type_registry();
        let result = reg.resolve_data_type("nonexistent_type");
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("unsupported storage type"),
            "error should mention unsupported type"
        );
    }

    #[test]
    fn store_register_type_makes_it_resolvable() {
        let store = ArrowStore::new();
        let name = "custom_int128";
        store
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
        store
            .create_table(&spec, MutationMode::AppendOnly)
            .unwrap();

        let mut values = serde_json::Map::new();
        values.insert(
            "id".to_owned(),
            serde_json::Value::Number(42.into()),
        );
        store
            .apply_diffs(
                Tick(1),
                &[Diff::Insert {
                    table: "custom_table".to_owned(),
                    row: RowId::new(42),
                    values,
                }],
            )
            .unwrap();

        let snap = store.generate_snapshot(Tick(1)).unwrap();
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
}
