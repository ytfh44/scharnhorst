use std::sync::Arc;

use arrow_array::Array;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use scharnhorst_arrow_store::{
    ArrowStore, ArrowStoreError, CommitStore, ForeignKeyIndex, InitStore, MutationMode, Partition,
    PartitionMap, PartitionSnapshot, PrimaryKeyIndex, RowLocation, VersionedTable, WorldSnapshot,
};
use scharnhorst_core::{RowId, Tick};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn empty_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .unwrap_or_else(|_| empty_batch_fallback())
}

fn empty_batch_fallback() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dummy",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef],
    )
    .unwrap_or_else(|_| panic!("failed to create fallback batch"))
}

fn make_spec(name: &str) -> TableSpec {
    TableSpec::new(name)
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap_or_else(|_| TableSpec::new(name))
}

// ------------------------------------------------------------------
// ArrowStore basic lifecycle
// ------------------------------------------------------------------

#[test]
fn create_and_drop_table() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("provinces");

    let id = init_store.create_table(&spec, MutationMode::AppendOnly);
    assert!(id.is_ok());

    assert_eq!(store.table_count().unwrap(), 1);
    assert!(store
        .table_names()
        .unwrap()
        .iter()
        .any(|n| n == "provinces"));

    let drop_result = init_store.drop_table("provinces");
    assert!(drop_result.is_ok());
    assert_eq!(store.table_count().unwrap(), 0);
}

#[test]
fn duplicate_table_fails() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("actors");

    let first = init_store.create_table(&spec, MutationMode::Patchable);
    assert!(first.is_ok());

    let second = init_store.create_table(&spec, MutationMode::RebuildPerTick);
    assert!(matches!(
        second.unwrap_err(),
        ArrowStoreError::TableAlreadyExists(ref s) if s == "actors"
    ));
}

#[test]
fn drop_missing_table_fails() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let result = init_store.drop_table("missing");
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::TableNotFound(ref s) if s == "missing"
    ));
}

#[test]
fn get_table_returns_correct_mode() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("events");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let mode = store.mutation_mode("events");
    assert_eq!(mode.unwrap(), MutationMode::AppendOnly);
}

#[test]
fn set_mutation_mode_changes_value() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("ledger");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    init_store
        .set_mutation_mode("ledger", MutationMode::RebuildPerTick)
        .ok();
    assert_eq!(
        store.mutation_mode("ledger").unwrap(),
        MutationMode::RebuildPerTick
    );
}

// ------------------------------------------------------------------
// VersionedTable
// ------------------------------------------------------------------

// NOTE: VersionedTable-specific tests (versions, get_version) moved to
// inline tests in versioned_table.rs since those methods are now pub(crate).

#[test]
fn versioned_table_mutation_mode_roundtrip() {
    let table = VersionedTable::new("foo", MutationMode::Patchable);
    assert_eq!(table.mutation_mode(), MutationMode::Patchable);
    assert_eq!(table.name(), "foo");
}

// ------------------------------------------------------------------
// Snapshot generation and retrieval
// ------------------------------------------------------------------

#[test]
fn generate_snapshot_copies_tables() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("world");
    init_store
        .create_table(&spec, MutationMode::RebuildPerTick)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    let snap = commit_store.generate_snapshot(Tick(100));
    assert!(snap.is_ok());

    let snap = snap.unwrap();
    assert_eq!(snap.tick(), Tick(100));
    assert!(snap.has_table("world"));
    assert_eq!(snap.table_count(), 1);
}

#[test]
fn get_snapshot_by_tick() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("map");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    commit_store.generate_snapshot(Tick(1)).ok();
    commit_store.generate_snapshot(Tick(5)).ok();

    let s1 = store.get_snapshot(Tick(1));
    assert!(s1.is_ok());
    assert_eq!(s1.unwrap().tick(), Tick(1));

    let missing = store.get_snapshot(Tick(99));
    assert!(matches!(
        missing.unwrap_err(),
        ArrowStoreError::SnapshotNotFound(99)
    ));
}

#[test]
fn latest_snapshot_returns_max_tick() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("history");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    commit_store.generate_snapshot(Tick(3)).ok();
    commit_store.generate_snapshot(Tick(7)).ok();
    commit_store.generate_snapshot(Tick(5)).ok();

    let latest = store.latest_snapshot().unwrap();
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().tick(), Tick(7));
}

#[test]
fn snapshot_ticks_iterates_all() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("ticks");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    commit_store.generate_snapshot(Tick(2)).ok();
    commit_store.generate_snapshot(Tick(4)).ok();

    let ticks: Vec<_> = store.snapshot_ticks().unwrap();
    assert_eq!(ticks.len(), 2);
}

// ------------------------------------------------------------------
// Immutable snapshot expose
// ------------------------------------------------------------------

#[test]
fn snapshot_is_immutable_from_outside() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("readonly");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    // Consumers receive Arc<WorldSnapshot>; there is no public mutable API.
    let _cloned: Arc<WorldSnapshot> = snap.clone();
    assert_eq!(_cloned.tick(), Tick(1));
}

#[test]
fn snapshot_table_batches_returns_empty_when_no_version() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("empty");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    let _snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    let batches = store.get_table_batches("empty", Tick(1));
    assert!(batches.is_ok());
    assert!(batches.unwrap().is_empty());
}

// ------------------------------------------------------------------
// Mutation modes
// ------------------------------------------------------------------

#[test]
fn mutation_modes_are_distinct() {
    assert_ne!(MutationMode::AppendOnly, MutationMode::Patchable);
    assert_ne!(MutationMode::Patchable, MutationMode::RebuildPerTick);
    assert_ne!(MutationMode::RebuildPerTick, MutationMode::AppendOnly);
}

#[test]
fn append_batches_stub_succeeds() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("events");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let result = init_store.append_batches("events", Tick(1), vec![empty_batch()]);
    assert!(result.is_ok());
}

#[test]
fn patch_rows_stub_succeeds() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("actors");
    init_store.create_table(&spec, MutationMode::Patchable).ok();

    let commit_store = init_store.into_simulation().unwrap();

    let result = commit_store.patch_rows("actors", Tick(1), &[], empty_batch());
    assert!(result.is_ok());
}

#[test]
fn rebuild_table_stub_succeeds() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("tiles");
    init_store
        .create_table(&spec, MutationMode::RebuildPerTick)
        .ok();

    let result = init_store.rebuild_table("tiles", Tick(1), vec![empty_batch()]);
    assert!(result.is_ok());
}

// ------------------------------------------------------------------
// Primary key index
// ------------------------------------------------------------------

#[test]
fn primary_key_index_insert_and_lookup() {
    let mut idx = PrimaryKeyIndex::new();
    idx.insert("key_a".to_owned(), Tick(1), 0, 2);
    idx.insert("key_a".to_owned(), Tick(2), 1, 3);
    idx.insert("key_b".to_owned(), Tick(1), 0, 0);

    assert_eq!(idx.len(), 2);
    assert!(!idx.is_empty());

    let all = idx.lookup("key_a").unwrap();
    assert_eq!(all.len(), 2);

    let latest = idx.lookup_latest("key_a").unwrap();
    assert_eq!(latest, (Tick(2), 1, 3));

    assert!(idx.lookup("missing").is_none());
}

#[test]
fn primary_key_index_remove_key() {
    let mut idx = PrimaryKeyIndex::new();
    idx.insert("x".to_owned(), Tick(1), 0, 0);
    assert!(idx.lookup("x").is_some());

    let removed = idx.remove_key("x");
    assert!(removed.is_some());
    assert!(idx.lookup("x").is_none());
}

#[test]
fn primary_key_index_keys_iter() {
    let mut idx = PrimaryKeyIndex::new();
    idx.insert("alpha".to_owned(), Tick(1), 0, 0);
    idx.insert("beta".to_owned(), Tick(1), 0, 0);

    let keys: Vec<_> = idx.keys().collect();
    assert_eq!(keys.len(), 2);
}

// ------------------------------------------------------------------
// Foreign key index
// ------------------------------------------------------------------

#[test]
fn foreign_key_index_metadata() {
    let idx = ForeignKeyIndex::new("owner_id", "actors");
    assert_eq!(idx.column_name, "owner_id");
    assert_eq!(idx.target_table, "actors");
    assert!(idx.is_empty());
}

#[test]
fn foreign_key_index_insert_and_lookup() {
    let mut idx = ForeignKeyIndex::new("province_id", "provinces");
    idx.insert("p1".to_owned(), Tick(1), 0, 0);
    idx.insert("p1".to_owned(), Tick(2), 0, 1);
    idx.insert("p2".to_owned(), Tick(1), 0, 2);

    assert_eq!(idx.len(), 2);

    let all = idx.lookup("p1").unwrap();
    assert_eq!(all.len(), 2);

    let latest = idx.lookup_latest("p2").unwrap();
    assert_eq!(latest, (Tick(1), 0, 2));
}

#[test]
fn foreign_key_index_remove_key() {
    let mut idx = ForeignKeyIndex::new("ref", "other");
    idx.insert("r".to_owned(), Tick(1), 0, 0);
    assert!(idx.remove_key("r").is_some());
    assert!(idx.lookup("r").is_none());
}

// ------------------------------------------------------------------
// PrimaryKeyIndex: with_history mode
// ------------------------------------------------------------------

#[test]
fn primary_key_index_with_history_true_lookup_returns_all() {
    let mut idx = PrimaryKeyIndex::with_history(true);
    idx.insert("k".to_owned(), Tick(1), 0, 0);
    idx.insert("k".to_owned(), Tick(3), 0, 1);
    idx.insert("k".to_owned(), Tick(2), 0, 2);

    let all = idx.lookup("k");
    // with_history(true) must return the full history
    assert!(
        all.is_some(),
        "lookup must return Some when history is enabled"
    );
    assert_eq!(all.unwrap().len(), 3);
}

#[test]
fn primary_key_index_with_history_false_lookup_returns_none() {
    let mut idx = PrimaryKeyIndex::with_history(false);
    idx.insert("k".to_owned(), Tick(1), 0, 0);
    idx.insert("k".to_owned(), Tick(3), 0, 1);

    // with_history(false): lookup must return None
    assert!(
        idx.lookup("k").is_none(),
        "lookup must return None when history is disabled"
    );
}

#[test]
fn primary_key_index_with_history_false_lookup_latest_still_works() {
    let mut idx = PrimaryKeyIndex::with_history(false);
    idx.insert("k".to_owned(), Tick(1), 0, 0);
    idx.insert("k".to_owned(), Tick(3), 0, 1);
    idx.insert("k".to_owned(), Tick(2), 0, 2);

    // lookup_latest must work regardless of history mode
    let latest = idx.lookup_latest("k");
    assert!(
        latest.is_some(),
        "lookup_latest must work even when history is disabled"
    );
    assert_eq!(latest.unwrap(), (Tick(3), 0, 1));
}

#[test]
fn primary_key_index_lookup_latest_returns_max_tick() {
    let mut idx = PrimaryKeyIndex::new();
    // Insert many versions for the same key across scattered ticks
    idx.insert("heavy".to_owned(), Tick(5), 1, 3);
    idx.insert("heavy".to_owned(), Tick(100), 0, 0);
    idx.insert("heavy".to_owned(), Tick(42), 2, 1);
    idx.insert("heavy".to_owned(), Tick(7), 1, 0);

    let latest = idx.lookup_latest("heavy").unwrap();
    assert_eq!(latest, (Tick(100), 0, 0));
}

#[test]
fn primary_key_index_remove_key_clears_both_layers() {
    let mut idx = PrimaryKeyIndex::new();
    idx.insert("k".to_owned(), Tick(1), 0, 0);
    idx.insert("k".to_owned(), Tick(2), 0, 1);

    assert!(idx.lookup_latest("k").is_some());
    assert!(idx.lookup("k").is_some());

    idx.remove_key("k");

    assert!(idx.lookup_latest("k").is_none());
    assert!(idx.lookup("k").is_none());
    assert!(!idx.keys().any(|k| k == "k"));
}

// ------------------------------------------------------------------
// RowLocation
// ------------------------------------------------------------------

#[test]
fn row_location_equality() {
    let a = RowLocation {
        tick: Tick(1),
        batch_index: 0,
        row_index: 2,
        row_id: RowId::new(42),
    };
    let b = RowLocation {
        tick: Tick(1),
        batch_index: 0,
        row_index: 2,
        row_id: RowId::new(42),
    };
    assert_eq!(a, b);
}

// ------------------------------------------------------------------
// PartitionMap
// ------------------------------------------------------------------

#[test]
fn partition_map_get_or_create() {
    let mut map = PartitionMap::new();
    let p = map.get_or_create("north");
    assert_eq!(p.region_id(), "north");
    assert!(map.contains("north"));
    assert_eq!(map.len(), 1);
}

#[test]
fn partition_map_get_missing_fails() {
    let map = PartitionMap::new();
    let result = map.get("south");
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::PartitionNotFound(ref s) if s == "south"
    ));
}

#[test]
fn partition_map_insert_and_remove() {
    let mut map = PartitionMap::new();
    let part = Partition::new("east");
    map.insert("east", part);
    assert_eq!(map.len(), 1);

    let removed = map.remove("east");
    assert!(removed.is_some());
    assert!(map.is_empty());
}

#[test]
fn partition_map_region_ids_iter() {
    let mut map = PartitionMap::new();
    map.insert("a", Partition::new("a"));
    map.insert("b", Partition::new("b"));

    let ids: Vec<_> = map.region_ids().collect();
    assert_eq!(ids.len(), 2);
}

#[test]
fn partition_add_batch() {
    let mut part = Partition::new("west");
    assert!(part.is_empty());
    part.add_batch(empty_batch());
    assert!(!part.is_empty());
}

// ------------------------------------------------------------------
// PartitionSnapshot
// ------------------------------------------------------------------

#[test]
fn partition_snapshot_fields() {
    let snap = PartitionSnapshot::new("region_12", Tick(50), vec![empty_batch()]);
    assert_eq!(snap.region_id(), "region_12");
    assert_eq!(snap.tick(), Tick(50));
    assert!(!snap.is_empty());
}

// ------------------------------------------------------------------
// Partitioned table access by region_id
// ------------------------------------------------------------------

#[test]
fn store_partition_access() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("pop_groups");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    // Populate partition map directly via mutable access.
    init_store
        .partition_map_mut("pop_groups", |pm| {
            let part = pm.get_or_create("region_north");
            part.add_batch(empty_batch());
        })
        .unwrap();

    let pm = store.partition_map("pop_groups").unwrap();
    assert!(pm.contains("region_north"));
}

#[test]
fn snapshot_registers_partition_views() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("units");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    init_store
        .partition_map_mut("units", |pm| {
            let part = pm.get_or_create("region_south");
            part.add_batch(empty_batch());
        })
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();

    let snap = commit_store.generate_snapshot(Tick(10)).unwrap();
    assert!(snap.has_partition("units", "region_south"));
    assert_eq!(snap.tick(), Tick(10));
    assert!(snap.has_table("units"));
}

#[test]
fn snapshot_partition_missing_fails() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("buildings");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let commit_store = init_store.into_simulation().unwrap();

    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    assert!(!snap.has_partition("buildings", "nowhere"));
}

// ------------------------------------------------------------------
// Index integration via ArrowStore
// ------------------------------------------------------------------

#[test]
fn store_build_primary_key_index_stub() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("items");
    init_store.create_table(&spec, MutationMode::Patchable).ok();

    let result = init_store.build_primary_key_index("items");
    assert!(result.is_ok());

    let idx = store.primary_key_index("items").unwrap();
    assert!(idx.is_empty());
}

#[test]
fn store_build_foreign_key_index() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("orders");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .ok();

    let result = init_store.build_foreign_key_index("orders", "customer_id", "customers");
    assert!(result.is_ok());

    let indices = store.foreign_key_indices("orders").unwrap();
    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0].column_name, "customer_id");
    assert_eq!(indices[0].target_table, "customers");
}

// ------------------------------------------------------------------
// Checkpoint stubs
// ------------------------------------------------------------------

#[test]
fn write_checkpoint_stub_ok() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    assert!(init_store.write_checkpoint(Tick(100)).is_ok());
}

#[test]
fn load_checkpoint_missing_dir_fails() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let result = init_store.load_checkpoint(Tick(0));
    assert!(result.is_err());
    // Now returns Io error instead of Unimplemented
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::Io(ref s) if s.contains("checkpoint directory not found")
    ));
}

#[test]
fn write_and_load_checkpoint_roundtrip() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("roundtrip_table");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();

    let batch = empty_batch();
    init_store
        .append_batches("roundtrip_table", Tick(5), vec![batch.clone()])
        .unwrap();
    let commit_store = init_store.into_simulation().unwrap();
    commit_store.generate_snapshot(Tick(5)).unwrap();

    // Write checkpoint to disk
    commit_store.write_checkpoint(Tick(5)).unwrap();

    // Create a fresh store and load the checkpoint
    let store2 = Arc::new(ArrowStore::new());
    let init_store2 = InitStore::new(Arc::clone(&store2));
    let _commit_store2 = CommitStore::new(Arc::clone(&store2));
    let snapshot = init_store2.load_checkpoint(Tick(5)).unwrap();

    assert_eq!(snapshot.tick(), Tick(5));
    assert!(snapshot.has_table("roundtrip_table"));
    let loaded_batches = store2
        .get_table_batches("roundtrip_table", Tick(5))
        .unwrap();
    assert_eq!(loaded_batches.len(), 1);
    assert_eq!(loaded_batches[0].num_rows(), 3);

    // Cleanup
    let _ = std::fs::remove_dir_all("checkpoint_5");
}

#[test]
fn load_checkpoint_wrong_tick_fails() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("wrong_tick_table");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();

    let batch = empty_batch();
    init_store
        .append_batches("wrong_tick_table", Tick(10), vec![batch])
        .unwrap();
    let commit_store = init_store.into_simulation().unwrap();
    commit_store.generate_snapshot(Tick(10)).unwrap();
    commit_store.write_checkpoint(Tick(10)).unwrap();

    // Try loading a tick that was never checkpointed
    let store2 = Arc::new(ArrowStore::new());
    let init_store2 = InitStore::new(Arc::clone(&store2));
    let _commit_store2 = CommitStore::new(Arc::clone(&store2));
    let result = init_store2.load_checkpoint(Tick(999));
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::Io(ref s) if s.contains("checkpoint directory not found")
    ));

    // Cleanup
    let _ = std::fs::remove_dir_all("checkpoint_10");
}

#[test]
fn load_checkpoint_restores_after_truncate() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_spec("restore_table");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();

    let batch = empty_batch();
    init_store
        .append_batches("restore_table", Tick(20), vec![batch])
        .unwrap();
    let commit_store = init_store.into_simulation().unwrap();
    commit_store.generate_snapshot(Tick(20)).unwrap();
    commit_store.write_checkpoint(Tick(20)).unwrap();

    // Truncate data
    commit_store.truncate_before(Tick(100)).unwrap();

    // Verify data is gone
    assert!(store.get_table("restore_table").is_ok());
    let restored_batches = store.get_table_batches("restore_table", Tick(20)).unwrap();
    assert!(restored_batches.is_empty());

    // Load checkpoint into fresh store and verify data is restored
    let store2 = Arc::new(ArrowStore::new());
    let init_store2 = InitStore::new(Arc::clone(&store2));
    let _commit_store2 = CommitStore::new(Arc::clone(&store2));
    let snapshot = init_store2.load_checkpoint(Tick(20)).unwrap();
    assert!(snapshot.has_table("restore_table"));
    let batches = store2.get_table_batches("restore_table", Tick(20)).unwrap();
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 3);

    // Cleanup
    let _ = std::fs::remove_dir_all("checkpoint_20");
}

// ------------------------------------------------------------------
// TableId mapping
// ------------------------------------------------------------------

#[test]
fn table_id_increments() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec_a = make_spec("a");
    let spec_b = make_spec("b");

    let id_a = init_store
        .create_table(&spec_a, MutationMode::AppendOnly)
        .unwrap();
    let id_b = init_store
        .create_table(&spec_b, MutationMode::AppendOnly)
        .unwrap();

    assert_ne!(id_a, id_b);
}

// ------------------------------------------------------------------
// Error variants cover all expected cases
// ------------------------------------------------------------------

#[test]
fn error_display_roundtrip() {
    let e1 = ArrowStoreError::TableNotFound("t".to_owned());
    assert_eq!(e1.to_string(), "table not found: t");

    let e2 = ArrowStoreError::SnapshotNotFound(7);
    assert_eq!(e2.to_string(), "snapshot not found for tick 7");

    let e3 = ArrowStoreError::PrimaryKeyViolation {
        table: "x".to_owned(),
        key: "k".to_owned(),
    };
    assert!(e3.to_string().contains("k") && e3.to_string().contains("x"));
}

// ------------------------------------------------------------------
// Diff application tests
// ------------------------------------------------------------------

use scharnhorst_core::Diff;

fn make_pk_spec() -> TableSpec {
    TableSpec::new("test_diff")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap()
        .with_column(ColumnSpec::new("value", FieldSemantic::Quantity, "i64"))
        .unwrap()
}

fn setup_store_with_data() -> (Arc<ArrowStore>, CommitStore) {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let spec = make_pk_spec();
    init_store
        .create_table(&spec, MutationMode::Patchable)
        .unwrap();

    let mut row1 = serde_json::Map::new();
    row1.insert("id".to_owned(), serde_json::Value::Number(1.into()));
    row1.insert("name".to_owned(), serde_json::Value::String("a".to_owned()));
    row1.insert("value".to_owned(), serde_json::Value::Number(10.into()));

    let mut row2 = serde_json::Map::new();
    row2.insert("id".to_owned(), serde_json::Value::Number(2.into()));
    row2.insert("name".to_owned(), serde_json::Value::String("b".to_owned()));
    row2.insert("value".to_owned(), serde_json::Value::Number(20.into()));

    let mut row3 = serde_json::Map::new();
    row3.insert("id".to_owned(), serde_json::Value::Number(3.into()));
    row3.insert("name".to_owned(), serde_json::Value::String("c".to_owned()));
    row3.insert("value".to_owned(), serde_json::Value::Number(30.into()));

    let diffs = vec![
        Diff::Insert {
            table: "test_diff".to_owned(),
            row: RowId::new(1),
            values: row1,
        },
        Diff::Insert {
            table: "test_diff".to_owned(),
            row: RowId::new(2),
            values: row2,
        },
        Diff::Insert {
            table: "test_diff".to_owned(),
            row: RowId::new(3),
            values: row3,
        },
    ];
    let commit_store = init_store.into_simulation().unwrap();
    commit_store.apply_diffs(Tick(1), &diffs).unwrap();
    (store, commit_store)
}

#[test]
fn apply_update_diff_changes_value() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(1),
        column: "name".to_owned(),
        value: serde_json::Value::String("patched".to_owned()),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();
    assert_eq!(batches.len(), 3);

    let name_col0 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col0.value(0), "patched");

    let name_col1 = batches[1]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col1.value(0), "b");

    let name_col2 = batches[2]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col2.value(0), "c");
}

#[test]
fn apply_update_diff_changes_numeric_value() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
        column: "value".to_owned(),
        value: serde_json::Value::Number(999.into()),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();
    let value_col0 = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col0.value(0), 10);

    let value_col1 = batches[1]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col1.value(0), 999);

    let value_col2 = batches[2]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col2.value(0), 30);
}

#[test]
fn apply_insert_diff_adds_row() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    let mut values = serde_json::Map::new();
    values.insert("id".to_owned(), serde_json::Value::Number(4.into()));
    values.insert("name".to_owned(), serde_json::Value::String("d".to_owned()));
    values.insert("value".to_owned(), serde_json::Value::Number(40.into()));

    let diff = Diff::Insert {
        table: "test_diff".to_owned(),
        row: RowId::new(4),
        values,
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();
    assert_eq!(batches.len(), 4);

    let new_batch = &batches[3];
    assert_eq!(new_batch.num_rows(), 1);
    let id_col = new_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 4);
}

#[test]
fn apply_delete_diff_marks_deleted() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Delete {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();

    let id_col = batches[1]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 2);

    let id_col0 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col0.value(0), 1);

    let id_col2 = batches[2]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col2.value(0), 3);
}

#[test]
fn apply_replace_diff_rebuilds_table() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(2);

    let mut row1 = serde_json::Map::new();
    row1.insert("id".to_owned(), serde_json::Value::Number(100.into()));
    row1.insert(
        "name".to_owned(),
        serde_json::Value::String("new_a".to_owned()),
    );
    row1.insert("value".to_owned(), serde_json::Value::Number(1000.into()));

    let mut row2 = serde_json::Map::new();
    row2.insert("id".to_owned(), serde_json::Value::Number(200.into()));
    row2.insert(
        "name".to_owned(),
        serde_json::Value::String("new_b".to_owned()),
    );
    row2.insert("value".to_owned(), serde_json::Value::Number(2000.into()));

    let diff = Diff::ReplaceTable {
        table: "test_diff".to_owned(),
        rows: vec![row1, row2],
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();
    assert_eq!(batches.len(), 2);

    // Each row becomes its own batch in ReplaceTable
    let id_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 100);

    let id_col2 = batches[1]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col2.value(0), 200);
}

#[test]
fn apply_multiple_diffs_in_sequence() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    let mut insert_values = serde_json::Map::new();
    insert_values.insert("id".to_owned(), serde_json::Value::Number(4.into()));
    insert_values.insert("name".to_owned(), serde_json::Value::String("d".to_owned()));
    insert_values.insert("value".to_owned(), serde_json::Value::Number(40.into()));

    let diffs = vec![
        Diff::Update {
            table: "test_diff".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("updated_a".to_owned()),
        },
        Diff::Insert {
            table: "test_diff".to_owned(),
            row: RowId::new(4),
            values: insert_values,
        },
    ];
    commit_store.apply_diffs(tick, &diffs).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();
    assert_eq!(batches.len(), 4);

    let name_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "updated_a");

    assert_eq!(batches[3].num_rows(), 1);
}

#[test]
fn apply_diffs_then_snapshot_reflects_changes() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    // Apply diffs
    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(3),
        column: "value".to_owned(),
        value: serde_json::Value::Number(9999.into()),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    // Generate snapshot AFTER applying diffs
    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();

    let value_col = batches[2]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col.value(0), 9999);
}

#[test]
fn apply_diffs_missing_table_fails() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));
    let commit_store = init_store.into_simulation().unwrap();
    let diff = Diff::Update {
        table: "nonexistent".to_owned(),
        row: RowId::new(1),
        column: "col".to_owned(),
        value: serde_json::Value::Number(1.into()),
    };
    let result = commit_store.apply_diffs(Tick(1), &[diff]);
    assert!(result.is_err());
}

// ------------------------------------------------------------------
// Lifecycle guard tests
// ------------------------------------------------------------------

#[test]
fn create_table_rejected_after_simulation() {
    let store = Arc::new(ArrowStore::new());
    let spec = make_spec("homelands");

    let init1 = InitStore::new(Arc::clone(&store));
    let _table_id = init1.create_table(&spec, MutationMode::AppendOnly).unwrap();

    let _commit = init1.into_simulation().unwrap();

    let init2 = InitStore::new(Arc::clone(&store));
    let result = init2.create_table(&spec, MutationMode::AppendOnly);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("operation not allowed during simulation")
            || err.contains("Lifecycle")
            || err.contains("Simulation")
    );
}

#[test]
fn apply_diffs_rejected_during_initialization() {
    let store = Arc::new(ArrowStore::new());
    let spec = make_spec("homelands");

    let init = InitStore::new(Arc::clone(&store));
    let _table_id = init.create_table(&spec, MutationMode::AppendOnly).unwrap();

    let commit = CommitStore::new(Arc::clone(&store));

    let mut values = serde_json::Map::new();
    values.insert("id".to_owned(), serde_json::Value::Number(42.into()));

    let diff = Diff::Insert {
        table: "homelands".to_owned(),
        row: RowId::new(42),
        values,
    };
    let result = commit.apply_diffs(Tick(0), &[diff]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("operation not allowed during initialization")
            || err.contains("Lifecycle")
            || err.contains("Initialization")
    );
}

#[test]
fn apply_diffs_missing_row_fails() {
    let (_store, commit_store) = setup_store_with_data();
    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(999),
        column: "name".to_owned(),
        value: serde_json::Value::String("nope".to_owned()),
    };
    let result = commit_store.apply_diffs(Tick(1), &[diff]);
    assert!(result.is_err());
}

#[test]
fn apply_diffs_wrong_column_fails() {
    let (_store, commit_store) = setup_store_with_data();
    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(1),
        column: "nonexistent_column".to_owned(),
        value: serde_json::Value::String("nope".to_owned()),
    };
    let result = commit_store.apply_diffs(Tick(1), &[diff]);
    assert!(result.is_err());
}

// ------------------------------------------------------------------
// Delete semantics: nullable column null patches and position map
// ------------------------------------------------------------------

fn setup_store_with_nullable_table() -> (Arc<ArrowStore>, CommitStore) {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _commit_store = CommitStore::new(Arc::clone(&store));

    let spec = TableSpec::new("nullable_test")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap()
        .with_column(
            ColumnSpec::new("optional_note", FieldSemantic::Raw, "utf8").with_nullable(true),
        )
        .unwrap()
        .with_column(ColumnSpec::new("score", FieldSemantic::Quantity, "i64"))
        .unwrap();

    init_store
        .create_table(&spec, MutationMode::Patchable)
        .unwrap();

    let mut row1 = serde_json::Map::new();
    row1.insert("id".to_owned(), serde_json::Value::Number(1.into()));
    row1.insert(
        "name".to_owned(),
        serde_json::Value::String("alpha".to_owned()),
    );
    row1.insert(
        "optional_note".to_owned(),
        serde_json::Value::String("first_note".to_owned()),
    );
    row1.insert("score".to_owned(), serde_json::Value::Number(100.into()));

    let mut row2 = serde_json::Map::new();
    row2.insert("id".to_owned(), serde_json::Value::Number(2.into()));
    row2.insert(
        "name".to_owned(),
        serde_json::Value::String("beta".to_owned()),
    );
    row2.insert(
        "optional_note".to_owned(),
        serde_json::Value::String("second_note".to_owned()),
    );
    row2.insert("score".to_owned(), serde_json::Value::Number(200.into()));

    let diffs = vec![
        Diff::Insert {
            table: "nullable_test".to_owned(),
            row: RowId::new(1),
            values: row1,
        },
        Diff::Insert {
            table: "nullable_test".to_owned(),
            row: RowId::new(2),
            values: row2,
        },
    ];
    let commit_store = init_store.into_simulation().unwrap();
    commit_store.apply_diffs(Tick(1), &diffs).unwrap();
    (store, commit_store)
}

#[test]
fn delete_nullable_column_becomes_null_in_patch() {
    let (store, commit_store) = setup_store_with_nullable_table();
    let tick = Tick(1);

    // Verify position_map has the row before delete
    {
        let table = store.get_table("nullable_test").unwrap();
        let pos_map = table.position_map();
        assert!(
            pos_map.contains(RowId::new(1)),
            "row 1 should exist before delete"
        );
    }

    let diff = Diff::Delete {
        table: "nullable_test".to_owned(),
        row: RowId::new(1),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("nullable_test", tick).unwrap();
    assert!(
        batches.len() >= 2,
        "expected at least 2 batches after delete, got {}",
        batches.len()
    );

    let note_col_b0 = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        note_col_b0.is_null(0),
        "optional_note should be null for deleted row in batch 0"
    );

    let note_col_b1 = batches[1]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        !note_col_b1.is_null(0),
        "optional_note should NOT be null for non-deleted row in batch 1"
    );
    assert_eq!(
        note_col_b1.value(0),
        "second_note",
        "non-deleted row's optional_note preserves original value"
    );
}

#[test]
fn delete_non_nullable_column_preserves_value_in_patch() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Delete {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let _snap = commit_store.generate_snapshot(tick).unwrap();
    let batches = store.get_table_batches("test_diff", tick).unwrap();
    assert!(batches.len() >= 2);

    let name_col = batches[1]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(
        !name_col.is_null(0),
        "non-nullable column should NOT be null in null patch"
    );
    assert_eq!(
        name_col.value(0),
        "b",
        "non-nullable column preserves original value in null patch"
    );
}

#[test]
fn delete_removes_row_from_position_map() {
    let (store, commit_store) = setup_store_with_data();
    let tick = Tick(2);

    let diff = Diff::Delete {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
    };
    commit_store.apply_diffs(tick, &[diff]).unwrap();

    let table = store.get_table("test_diff").unwrap();
    let pos_map = table.position_map();
    assert!(
        !pos_map.contains(RowId::new(2)),
        "deleted RowId should be removed from position_map"
    );
    assert!(
        pos_map.contains(RowId::new(1)),
        "non-deleted RowId should remain in position_map"
    );
    assert!(
        pos_map.contains(RowId::new(3)),
        "non-deleted RowId should remain in position_map"
    );
}

#[test]
fn delete_nonexistent_row_fails() {
    let (_store, commit_store) = setup_store_with_data();
    let diff = Diff::Delete {
        table: "test_diff".to_owned(),
        row: RowId::new(999),
    };
    let result = commit_store.apply_diffs(Tick(2), &[diff]);
    assert!(result.is_err(), "deleting non-existent row should fail");
}

#[test]
fn double_delete_fails() {
    let (_store, commit_store) = setup_store_with_data();
    let tick = Tick(2);

    let diff = Diff::Delete {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
    };
    commit_store.apply_diffs(tick, std::slice::from_ref(&diff)).unwrap();

    let result = commit_store.apply_diffs(tick, &[diff]);
    assert!(
        result.is_err(),
        "deleting an already-deleted row should fail"
    );
}

#[test]
fn delete_on_wrong_table_fails() {
    let (_store, commit_store) = setup_store_with_data();
    let diff = Diff::Delete {
        table: "nonexistent_table".to_owned(),
        row: RowId::new(1),
    };
    let result = commit_store.apply_diffs(Tick(2), &[diff]);
    assert!(
        result.is_err(),
        "deleting from non-existent table should fail"
    );
}

// ------------------------------------------------------------------
// String patch: null preservation and LargeUtf8
// ------------------------------------------------------------------

/// Would have failed: string patch must preserve nulls in unaffected rows.
/// If the null bitmap is not correctly maintained, patching one row's
/// string value could overwrite null entries in other rows.
#[test]
fn patch_string_preserves_nulls_in_unaffected_rows() {
    use arrow_array::GenericStringArray;
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));

    let spec = TableSpec::new("string_null_test")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("label", FieldSemantic::Name, "utf8").with_nullable(true))
        .unwrap();
    init_store
        .create_table(&spec, MutationMode::Patchable)
        .unwrap();

    let mut row1 = serde_json::Map::new();
    row1.insert("id".to_owned(), serde_json::Value::Number(1.into()));
    row1.insert(
        "label".to_owned(),
        serde_json::Value::String("alpha".to_owned()),
    );
    let mut row2 = serde_json::Map::new();
    row2.insert("id".to_owned(), serde_json::Value::Number(2.into()));
    row2.insert("label".to_owned(), serde_json::Value::Null);
    let commit_store = init_store.into_simulation().unwrap();
    commit_store
        .apply_diffs(
            Tick(1),
            &[
                Diff::Insert {
                    table: "string_null_test".to_owned(),
                    row: RowId::new(1),
                    values: row1,
                },
                Diff::Insert {
                    table: "string_null_test".to_owned(),
                    row: RowId::new(2),
                    values: row2,
                },
            ],
        )
        .unwrap();

    let diff = Diff::Update {
        table: "string_null_test".to_owned(),
        row: RowId::new(1),
        column: "label".to_owned(),
        value: serde_json::Value::String("beta".to_owned()),
    };
    commit_store.apply_diffs(Tick(1), &[diff]).unwrap();

    let batches = store
        .get_table_batches("string_null_test", Tick(1))
        .unwrap();

    let label_col = batches[0].column_by_name("label").unwrap();
    let label_arr: &GenericStringArray<i32> = label_col
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .unwrap();

    assert_eq!(label_arr.len(), 1, "batch0 should contain 1 row (RowId=1)");
    assert_eq!(
        label_arr.value(0),
        "beta",
        "patched RowId=1 should have new value"
    );

    let label_col2 = batches[1].column_by_name("label").unwrap();
    let label_arr2: &GenericStringArray<i32> = label_col2
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .unwrap();

    assert_eq!(label_arr2.len(), 1, "batch1 should contain 1 row (RowId=2)");
    assert!(
        label_arr2.is_null(0),
        "untouched RowId=2 with null should remain null in its batch"
    );
}

/// Would have failed: patch_string_array must handle LargeUtf8
/// by returning GenericStringArray<i64>. If the implementation hardcodes
/// i32, LargeUtf8 patches would fail type downcast at runtime.
#[test]
fn patch_string_array_handles_large_utf8() {
    use arrow_array::GenericStringArray;

    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));

    let spec = TableSpec::new("large_utf8_test")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "large_utf8"))
        .unwrap();
    init_store
        .create_table(&spec, MutationMode::Patchable)
        .unwrap();

    let mut row = serde_json::Map::new();
    row.insert("id".to_owned(), serde_json::Value::Number(1.into()));
    row.insert(
        "name".to_owned(),
        serde_json::Value::String("alpha".to_owned()),
    );
    let commit_store = init_store.into_simulation().unwrap();
    commit_store
        .apply_diffs(
            Tick(1),
            &[Diff::Insert {
                table: "large_utf8_test".to_owned(),
                row: RowId::new(1),
                values: row,
            }],
        )
        .unwrap();

    let diff = Diff::Update {
        table: "large_utf8_test".to_owned(),
        row: RowId::new(1),
        column: "name".to_owned(),
        value: serde_json::Value::String("beta".to_owned()),
    };
    commit_store.apply_diffs(Tick(1), &[diff]).unwrap();

    let batches = store.get_table_batches("large_utf8_test", Tick(1)).unwrap();
    let name_col = batches[0].column_by_name("name").unwrap();

    let name_arr: &GenericStringArray<i64> = name_col
        .as_any()
        .downcast_ref::<GenericStringArray<i64>>()
        .unwrap();
    assert_eq!(
        name_arr.value(0),
        "beta",
        "LargeUtf8 patch should produce correct value"
    );

    let wrong_downcast = name_col.as_any().downcast_ref::<GenericStringArray<i32>>();
    assert!(
        wrong_downcast.is_none(),
        "LargeUtf8 patch must return i64 offsets, not i32"
    );
}

// ------------------------------------------------------------------
// Phase 0 invariant fortification: may-fail edge case tests
// ------------------------------------------------------------------

/// Probes: InitStore.drop_table after simulation must return error.
/// Guards against tables being dropped during active simulation.
#[test]
fn drop_table_rejected_after_simulation() {
    let store = Arc::new(ArrowStore::new());
    let spec = make_spec("doomed_table");

    let init = InitStore::new(Arc::clone(&store));
    init.create_table(&spec, MutationMode::AppendOnly).unwrap();
    let _commit = init.into_simulation().unwrap();

    // Attempt drop via a new InitStore (which acquires a stale InitToken).
    // The underlying ArrowStore lifecycle is Simulation; drop must reject.
    let init2 = InitStore::new(Arc::clone(&store));
    let result = init2.drop_table("doomed_table");
    assert!(
        result.is_err(),
        "drop_table must be rejected after simulation starts"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("operation not allowed during simulation")
            || err.contains("Lifecycle")
            || err.contains("Simulation"),
        "error must mention lifecycle violation, got: {err}"
    );
}

/// Probes: InitStore.register_type after simulation must return error.
/// Guards against type registrations that would affect Arrow decoding mid-simulation.
#[test]
fn register_type_rejected_after_simulation() {
    let store = Arc::new(ArrowStore::new());

    let init = InitStore::new(Arc::clone(&store));
    let _commit = init.into_simulation().unwrap();

    let init2 = InitStore::new(Arc::clone(&store));
    let result = init2.register_type(
        "mid_sim_type",
        DataType::Int64,
        Arc::new(|_v| Ok(Arc::new(Int64Array::from(vec![0i64])) as ArrayRef)),
        Arc::new(|| Ok(Arc::new(Int64Array::from(vec![0i64])) as ArrayRef)),
    );
    assert!(
        result.is_err(),
        "register_type must be rejected after simulation starts"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("operation not allowed during simulation")
            || err.contains("Lifecycle")
            || err.contains("Simulation"),
        "error must mention lifecycle violation, got: {err}"
    );
}

/// Probes: InitStore.set_mutation_mode after simulation must return error.
/// Guards against mutation-mode changes that would alter tick-over-tick merge semantics.
#[test]
fn set_mutation_mode_rejected_after_simulation() {
    let store = Arc::new(ArrowStore::new());
    let spec = make_spec("mutable_table");

    let init = InitStore::new(Arc::clone(&store));
    init.create_table(&spec, MutationMode::AppendOnly).unwrap();
    let _commit = init.into_simulation().unwrap();

    let init2 = InitStore::new(Arc::clone(&store));
    let result = init2.set_mutation_mode("mutable_table", MutationMode::Patchable);
    assert!(
        result.is_err(),
        "set_mutation_mode must be rejected after simulation"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("operation not allowed during simulation")
            || err.contains("Lifecycle")
            || err.contains("Simulation"),
        "error must mention lifecycle violation, got: {err}"
    );
}

/// Probes: inserting a diff with a duplicate RowId returns ArrowStoreError::DuplicateRowId.
/// Guards against silent overwrite of position-map entries, which orphans Arrow batch data.
#[test]
fn duplicate_rowid_on_insert_rejected() {
    let store = Arc::new(ArrowStore::new());
    let spec = TableSpec::new("test_dup")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64").with_nullable(false))
        .unwrap_or_else(|_| make_spec("test_dup"));

    let init = InitStore::new(Arc::clone(&store));
    init.create_table(&spec, MutationMode::AppendOnly).unwrap();
    let commit = init.into_simulation().unwrap();

    let row_id = RowId::new(99);
    let mut values1 = serde_json::Map::new();
    values1.insert("id".to_owned(), serde_json::Value::Number(99.into()));
    let diff1 = scharnhorst_core::Diff::Insert {
        table: "test_dup".to_owned(),
        row: row_id,
        values: values1,
    };
    commit.apply_diffs(Tick(0), &[diff1]).unwrap();

    let mut values2 = serde_json::Map::new();
    values2.insert("id".to_owned(), serde_json::Value::Number(99.into()));
    let diff2 = scharnhorst_core::Diff::Insert {
        table: "test_dup".to_owned(),
        row: row_id,
        values: values2,
    };
    let result = commit.apply_diffs(Tick(1), &[diff2]);
    assert!(result.is_err(), "duplicate RowId insert must be rejected");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Duplicate") || err.contains("duplicate") || err.contains("already exists"),
        "error must mention duplicate, got: {err}"
    );
}
