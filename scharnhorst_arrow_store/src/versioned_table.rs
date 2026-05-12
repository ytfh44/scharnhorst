use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use scharnhorst_core::{RowPositionMap, Tick};
use scharnhorst_schema::TableSpec;

use crate::error::{ArrowStoreError, ArrowStoreResult};
use crate::index::{ForeignKeyIndex, PrimaryKeyIndex};
use crate::partition::PartitionMap;
use crate::store::{make_null_array, slice_array_to_single};

/// Determines how a table may be mutated across ticks.
///
/// This enum controls the expected mutation pattern for a [`VersionedTable`].
/// The mode itself does not enforce any constraints at runtime, but serves as
/// documentation and can be used by higher-level logic to validate operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutationMode {
 /// Rows are only appended; existing batches are never modified.
 ///
 /// This is the safest mode for concurrent access, as data is immutable
 /// once written.
    AppendOnly,
 /// Individual rows may be patched or updated in-place.
 ///
 /// **Warning**: This mode requires external synchronization when accessed
 /// from multiple threads. The `VersionedTable` itself does not provide
 /// internal locking.
    Patchable,
 /// The entire table is rebuilt from scratch every tick.
 ///
 /// This mode is useful for scenarios where the complete state is recomputed
 /// each tick. Like `Patchable`, it requires external synchronization for
 /// concurrent access.
    RebuildPerTick,
}

/// A versioned Arrow table that tracks historical batches per tick.
///
/// # Thread Safety
///
/// `VersionedTable` is **not** thread-safe. It does not contain any internal
/// synchronization mechanisms (such as `RwLock` or `Mutex`). All fields are
/// publicly accessible and mutable operations require exclusive access.
///
/// ## External Synchronization Required
///
/// When accessing a `VersionedTable` from multiple threads, you must provide
/// external synchronization. The typical pattern is to wrap it in a
/// thread-safe container:
///
/// ```ignore
/// use std::sync::{Arc, RwLock};
/// use scharnhorst_arrow_store::VersionedTable;
///
/// // Thread-safe wrapper
/// let table: Arc<RwLock<VersionedTable>> = Arc::new(RwLock::new(table));
///
/// // Read operations acquire read lock
/// let read_guard = table.read.unwrap;
/// let name = read_guard.name;
///
/// // Write operations acquire write lock
/// let mut write_guard = table.write.unwrap;
/// write_guard.insert_version(tick, batches)?;
/// ```
///
/// ## In This Crate
///
/// The [`ArrowStore`](crate::store::ArrowStore) type provides thread-safe access
/// to multiple `VersionedTable` instances by wrapping them in `Arc<RwLock<ArrowStoreInner>>`.
/// All public methods of `ArrowStore` handle the necessary locking internally.
///
/// # Operations and Their Safety Requirements
///
/// ## Read-Only Operations (Safe with Shared Access)
///
/// These operations only require a shared reference (`&self`) and are safe to
/// call concurrently from multiple threads **if** the `VersionedTable` is
/// properly protected by a read-write lock:
///
/// - [`name`](Self::name)
/// - [`mutation_mode`](Self::mutation_mode)
/// - [`spec`](Self::spec)
/// - [`versions`](Self::versions)
/// - [`get_version`](Self::get_version)
/// - [`latest_tick`](Self::latest_tick)
/// - [`primary_key_index`](Self::primary_key_index)
/// - [`foreign_key_indices`](Self::foreign_key_indices)
/// - [`partitions`](Self::partitions)
///
/// ## Mutable Operations (Require Exclusive Access)
///
/// These operations require a mutable reference (`&mut self`) and **must not**
/// be called concurrently. They must be protected by exclusive (write) lock:
///
/// - [`set_spec`](Self::set_spec)
/// - [`insert_version`](Self::insert_version)
/// - [`primary_key_index_mut`](Self::primary_key_index_mut)
/// - [`add_foreign_key_index`](Self::add_foreign_key_index)
/// - [`partitions_mut`](Self::partitions_mut)
///
/// # Clone Behavior
///
/// `VersionedTable` implements `Clone`, which performs a **deep clone** of all
/// data including all `RecordBatch` instances. Cloning is expensive for large
/// tables but produces an independent copy that can be safely used without
/// additional synchronization.
#[derive(Debug, Clone)]
pub struct VersionedTable {
 /// The table's identifier.
    pub(crate) name: String,
 /// The mutation mode controlling how this table may be modified.
    pub(crate) mutation_mode: MutationMode,
 /// Optional schema specification for the table.
    pub(crate) spec: Option<TableSpec>,
 /// Historical batches organized by tick.
 ///
 /// Each tick may contain multiple `RecordBatch` instances. The map allows
 /// efficient lookup of all data valid at a particular tick.
    pub(crate) versions: HashMap<Tick, Vec<RecordBatch>>,
 /// Primary key index for fast lookups by primary key value.
 ///
 /// This index must be built explicitly after inserting data.
    pub(crate) primary_key_index: PrimaryKeyIndex,
 /// Foreign key indices for referential integrity checks.
 ///
 /// Multiple foreign key indices may be defined, each referencing a
 /// different target table.
    pub(crate) foreign_key_indices: Vec<ForeignKeyIndex>,
 /// Partition map organizing the table into regions for distributed processing.
    pub(crate) partitions: PartitionMap,
 /// Maps RowId values to their physical storage positions within the table.
 ///
 /// Updated on insert, delete, replace, and rebuild operations. This is the
 /// authoritative source for translating logical RowId to physical
 /// (batch_index, row_offset) tuples.
    pub(crate) position_map: RowPositionMap,
}

impl VersionedTable {
    pub fn position_map(&self) -> &RowPositionMap {
        &self.position_map
    }
    pub fn new(name: impl Into<String>, mode: MutationMode) -> Self {
        Self {
            name: name.into(),
            mutation_mode: mode,
            spec: None,
            versions: HashMap::new(),
            primary_key_index: PrimaryKeyIndex::new(),
            foreign_key_indices: Vec::new(),
            partitions: PartitionMap::new(),
            position_map: RowPositionMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn mutation_mode(&self) -> MutationMode {
        self.mutation_mode
    }

    pub fn spec(&self) -> Option<&TableSpec> {
        self.spec.as_ref()
    }

    pub fn set_spec(&mut self, spec: TableSpec) {
        self.spec = Some(spec);
    }

    pub fn versions(&self) -> &HashMap<Tick, Vec<RecordBatch>> {
        &self.versions
    }

    pub fn insert_version(
        &mut self,
        tick: Tick,
        batches: Vec<RecordBatch>,
    ) -> ArrowStoreResult<()> {
        self.versions.insert(tick, batches);
        Ok(())
    }

    pub fn get_version(&self, tick: Tick) -> Option<&Vec<RecordBatch>> {
        self.versions.get(&tick)
    }

    pub fn latest_tick(&self) -> Option<Tick> {
        self.versions.keys().copied().max()
    }

    pub fn primary_key_index(&self) -> &PrimaryKeyIndex {
        &self.primary_key_index
    }

    pub fn primary_key_index_mut(&mut self) -> &mut PrimaryKeyIndex {
        &mut self.primary_key_index
    }

    pub fn foreign_key_indices(&self) -> &[ForeignKeyIndex] {
        &self.foreign_key_indices
    }

    pub fn add_foreign_key_index(&mut self, index: ForeignKeyIndex) {
        self.foreign_key_indices.push(index);
    }

    pub fn partitions(&self) -> &PartitionMap {
        &self.partitions
    }

    pub fn partitions_mut(&mut self) -> &mut PartitionMap {
        &mut self.partitions
    }
}

/// Build a row-sized RecordBatch where nullable columns are null and
/// non-nullable columns preserve their original value from the source row.
///
/// This is used to represent a deleted row's data within a batch. Row
/// deletion is tracked by removing the RowId from the RowPositionMap;
/// the null patch ensures batch structural consistency.
pub(crate) fn build_null_patch(
    src_batch: &RecordBatch,
    row_idx: usize,
) -> ArrowStoreResult<RecordBatch> {
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(src_batch.num_columns());
    let schema = src_batch.schema();

    for c in 0..src_batch.num_columns() {
        let field = schema.field(c);
        if field.is_nullable() {
            let null_arr = make_null_array(src_batch.column(c).data_type())?;
            new_columns.push(null_arr);
        } else {
            let single = slice_array_to_single(src_batch.column(c), row_idx)?;
            new_columns.push(single);
        }
    }

    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| ArrowStoreError::Arrow(format!("build_null_patch: {}", e)))
}
