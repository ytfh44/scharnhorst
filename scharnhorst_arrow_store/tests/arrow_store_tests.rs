use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use scharnhorst_arrow_store::{
    ArrowStore, ArrowStoreError, ForeignKeyIndex, MutationMode, Partition, PartitionMap,
    PartitionSnapshot, PrimaryKeyIndex, RowLocation, VersionedTable, WorldSnapshot,
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
    let schema = Arc::new(Schema::new(vec![Field::new("dummy", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef])
        .unwrap_or_else(|_| panic!("failed to create fallback batch"))
}

fn make_spec(name: &str) -> TableSpec {
    TableSpec::new(name)
        .with_column(ColumnSpec::new(
            "id",
            FieldSemantic::Id,
            "i64",
        ))
        .unwrap_or_else(|_| TableSpec::new(name))
}

// ------------------------------------------------------------------
// ArrowStore basic lifecycle
// ------------------------------------------------------------------

#[test]
fn create_and_drop_table() {
    let store = ArrowStore::new();
    let spec = make_spec("provinces");

    let id = store.create_table(&spec, MutationMode::AppendOnly);
    assert!(id.is_ok());

    assert_eq!(store.table_count().unwrap(), 1);
    assert!(store.table_names().unwrap().iter().any(|n| n == "provinces"));

    let drop_result = store.drop_table("provinces");
    assert!(drop_result.is_ok());
    assert_eq!(store.table_count().unwrap(), 0);
}

#[test]
fn duplicate_table_fails() {
    let store = ArrowStore::new();
    let spec = make_spec("actors");

    let first = store.create_table(&spec, MutationMode::Patchable);
    assert!(first.is_ok());

    let second = store.create_table(&spec, MutationMode::RebuildPerTick);
    assert!(matches!(
        second.unwrap_err(),
        ArrowStoreError::TableAlreadyExists(ref s) if s == "actors"
    ));
}

#[test]
fn drop_missing_table_fails() {
    let store = ArrowStore::new();
    let result = store.drop_table("missing");
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::TableNotFound(ref s) if s == "missing"
    ));
}

#[test]
fn get_table_returns_correct_mode() {
    let store = ArrowStore::new();
    let spec = make_spec("events");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    let mode = store.mutation_mode("events");
    assert_eq!(mode.unwrap(), MutationMode::AppendOnly);
}

#[test]
fn set_mutation_mode_changes_value() {
    let store = ArrowStore::new();
    let spec = make_spec("ledger");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    store.set_mutation_mode("ledger", MutationMode::RebuildPerTick).ok();
    assert_eq!(
        store.mutation_mode("ledger").unwrap(),
        MutationMode::RebuildPerTick
    );
}

// ------------------------------------------------------------------
// VersionedTable
// ------------------------------------------------------------------

#[test]
fn versioned_table_stores_versions() {
    let mut table = VersionedTable::new("test", MutationMode::AppendOnly);
    let batch = empty_batch();

    table.insert_version(Tick(10), vec![batch.clone()]).ok();
    table.insert_version(Tick(20), vec![batch.clone()]).ok();

    assert_eq!(table.versions().len(), 2);
    assert!(table.get_version(Tick(10)).is_some());
    assert!(table.get_version(Tick(99)).is_none());
    assert_eq!(table.latest_tick(), Some(Tick(20)));
}

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
    let store = ArrowStore::new();
    let spec = make_spec("world");
    store.create_table(&spec, MutationMode::RebuildPerTick).ok();

    let snap = store.generate_snapshot(Tick(100));
    assert!(snap.is_ok());

    let snap = snap.unwrap();
    assert_eq!(snap.tick(), Tick(100));
    assert!(snap.has_table("world"));
    assert_eq!(snap.table_count(), 1);
}

#[test]
fn get_snapshot_by_tick() {
    let store = ArrowStore::new();
    let spec = make_spec("map");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    store.generate_snapshot(Tick(1)).ok();
    store.generate_snapshot(Tick(5)).ok();

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
    let store = ArrowStore::new();
    let spec = make_spec("history");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    store.generate_snapshot(Tick(3)).ok();
    store.generate_snapshot(Tick(7)).ok();
    store.generate_snapshot(Tick(5)).ok();

    let latest = store.latest_snapshot().unwrap();
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().tick(), Tick(7));
}

#[test]
fn snapshot_ticks_iterates_all() {
    let store = ArrowStore::new();
    let spec = make_spec("ticks");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    store.generate_snapshot(Tick(2)).ok();
    store.generate_snapshot(Tick(4)).ok();

    let ticks: Vec<_> = store.snapshot_ticks().unwrap();
    assert_eq!(ticks.len(), 2);
}

// ------------------------------------------------------------------
// Immutable snapshot expose
// ------------------------------------------------------------------

#[test]
fn snapshot_is_immutable_from_outside() {
    let store = ArrowStore::new();
    let spec = make_spec("readonly");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    let snap = store.generate_snapshot(Tick(1)).unwrap();
 // Consumers receive Arc<WorldSnapshot>; there is no public mutable API.
    let _cloned: Arc<WorldSnapshot> = snap.clone();
    assert_eq!(_cloned.tick(), Tick(1));
}

#[test]
fn snapshot_table_batches_returns_empty_when_no_version() {
    let store = ArrowStore::new();
    let spec = make_spec("empty");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    let snap = store.generate_snapshot(Tick(1)).unwrap();
    let batches = snap.table_batches("empty");
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
    let store = ArrowStore::new();
    let spec = make_spec("events");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    let result = store.append_batches("events", Tick(1), vec![empty_batch()]);
    assert!(result.is_ok());
}

#[test]
fn patch_rows_stub_succeeds() {
    let store = ArrowStore::new();
    let spec = make_spec("actors");
    store.create_table(&spec, MutationMode::Patchable).ok();

    let result = store.patch_rows("actors", Tick(1), &[], empty_batch());
    assert!(result.is_ok());
}

#[test]
fn rebuild_table_stub_succeeds() {
    let store = ArrowStore::new();
    let spec = make_spec("tiles");
    store.create_table(&spec, MutationMode::RebuildPerTick).ok();

    let result = store.rebuild_table("tiles", Tick(1), vec![empty_batch()]);
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
    assert_eq!(part.batches().len(), 1);
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
    let store = ArrowStore::new();
    let spec = make_spec("pop_groups");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

 // Populate partition map directly via mutable access.
    store.partition_map_mut("pop_groups", |pm| {
        let part = pm.get_or_create("region_north");
        part.add_batch(empty_batch());
    }).unwrap();

    let pm = store.partition_map("pop_groups").unwrap();
    assert!(pm.contains("region_north"));
    assert_eq!(pm.get("region_north").unwrap().batches().len(), 1);
}

#[test]
fn snapshot_registers_partition_views() {
    let store = ArrowStore::new();
    let spec = make_spec("units");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    store.partition_map_mut("units", |pm| {
        let part = pm.get_or_create("region_south");
        part.add_batch(empty_batch());
    }).unwrap();

    let snap = store.generate_snapshot(Tick(10)).unwrap();
    assert!(snap.has_partition("units", "region_south"));

    let ps = snap.partition_snapshot("units", "region_south");
    assert!(ps.is_ok());
    assert_eq!(ps.unwrap().tick(), Tick(10));
}

#[test]
fn snapshot_partition_missing_fails() {
    let store = ArrowStore::new();
    let spec = make_spec("buildings");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    let snap = store.generate_snapshot(Tick(1)).unwrap();
    let result = snap.partition_snapshot("buildings", "nowhere");
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::PartitionNotFound(ref s) if s == "buildings:nowhere"
    ));
}

// ------------------------------------------------------------------
// Index integration via ArrowStore
// ------------------------------------------------------------------

#[test]
fn store_build_primary_key_index_stub() {
    let store = ArrowStore::new();
    let spec = make_spec("items");
    store.create_table(&spec, MutationMode::Patchable).ok();

    let result = store.build_primary_key_index("items");
    assert!(result.is_ok());

    let idx = store.primary_key_index("items").unwrap();
    assert!(idx.is_empty());
}

#[test]
fn store_build_foreign_key_index() {
    let store = ArrowStore::new();
    let spec = make_spec("orders");
    store.create_table(&spec, MutationMode::AppendOnly).ok();

    let result = store.build_foreign_key_index("orders", "customer_id", "customers");
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
    let store = ArrowStore::new();
    assert!(store.write_checkpoint(Tick(100)).is_ok());
}

#[test]
fn load_checkpoint_missing_dir_fails() {
    let store = ArrowStore::new();
    let result = store.load_checkpoint(Tick(0));
    assert!(result.is_err());
 // Now returns Io error instead of Unimplemented
    assert!(matches!(
        result.unwrap_err(),
        ArrowStoreError::Io(ref s) if s.contains("checkpoint directory not found")
    ));
}

#[test]
fn write_and_load_checkpoint_roundtrip() {
    let store = ArrowStore::new();
    let spec = make_spec("roundtrip_table");
    store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();

    let batch = empty_batch();
    store
        .append_batches("roundtrip_table", Tick(5), vec![batch.clone()])
        .unwrap();
    store.generate_snapshot(Tick(5)).unwrap();

 // Write checkpoint to disk
    store.write_checkpoint(Tick(5)).unwrap();

 // Create a fresh store and load the checkpoint
    let store2 = ArrowStore::new();
    let snapshot = store2.load_checkpoint(Tick(5)).unwrap();

    assert_eq!(snapshot.tick(), Tick(5));
    assert!(snapshot.has_table("roundtrip_table"));
    let loaded_batches = snapshot.table_batches("roundtrip_table").unwrap();
    assert_eq!(loaded_batches.len(), 1);
    assert_eq!(loaded_batches[0].num_rows(), 3);

 // Cleanup
    let _ = std::fs::remove_dir_all("checkpoint_5");
}

#[test]
fn load_checkpoint_wrong_tick_fails() {
    let store = ArrowStore::new();
    let spec = make_spec("wrong_tick_table");
    store.create_table(&spec, MutationMode::AppendOnly).unwrap();

    let batch = empty_batch();
    store
        .append_batches("wrong_tick_table", Tick(10), vec![batch])
        .unwrap();
    store.generate_snapshot(Tick(10)).unwrap();
    store.write_checkpoint(Tick(10)).unwrap();

 // Try loading a tick that was never checkpointed
    let store2 = ArrowStore::new();
    let result = store2.load_checkpoint(Tick(999));
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
    let store = ArrowStore::new();
    let spec = make_spec("restore_table");
    store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();

    let batch = empty_batch();
    store
        .append_batches("restore_table", Tick(20), vec![batch])
        .unwrap();
    store.generate_snapshot(Tick(20)).unwrap();
    store.write_checkpoint(Tick(20)).unwrap();

 // Truncate data
    store.truncate_before(Tick(100)).unwrap();

 // Verify data is gone
    assert!(store.get_table("restore_table").is_ok());
    let table = store.get_table("restore_table").unwrap();
    assert!(table.get_version(Tick(20)).is_none());

 // Load checkpoint into fresh store and verify data is restored
    let store2 = ArrowStore::new();
    let snapshot = store2.load_checkpoint(Tick(20)).unwrap();
    assert!(snapshot.has_table("restore_table"));
    let batches = snapshot.table_batches("restore_table").unwrap();
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
    let store = ArrowStore::new();
    let spec_a = make_spec("a");
    let spec_b = make_spec("b");

    let id_a = store.create_table(&spec_a, MutationMode::AppendOnly).unwrap();
    let id_b = store.create_table(&spec_b, MutationMode::AppendOnly).unwrap();

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

fn make_int_batch_cols(ids: Vec<i64>, names: Vec<&str>, values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn setup_store_with_data() -> ArrowStore {
    let store = ArrowStore::new();
    let spec = make_pk_spec();
    store
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
    store.apply_diffs(Tick(1), &diffs).unwrap();
    store
}

#[test]
fn apply_update_diff_changes_value() {
    let store = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(1),
        column: "name".to_owned(),
        value: serde_json::Value::String("patched".to_owned()),
    };
    store.apply_diffs(tick, &[diff]).unwrap();

    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();
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
    let store = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
        column: "value".to_owned(),
        value: serde_json::Value::Number(999.into()),
    };
    store.apply_diffs(tick, &[diff]).unwrap();

    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();
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
    let store = setup_store_with_data();
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
    store.apply_diffs(tick, &[diff]).unwrap();

    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();
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
    let store = setup_store_with_data();
    let tick = Tick(1);

    let diff = Diff::Delete {
        table: "test_diff".to_owned(),
        row: RowId::new(2),
    };
    store.apply_diffs(tick, &[diff]).unwrap();

    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();

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
    let store = setup_store_with_data();
    let tick = Tick(2);

    let mut row1 = serde_json::Map::new();
    row1.insert("id".to_owned(), serde_json::Value::Number(100.into()));
    row1.insert("name".to_owned(), serde_json::Value::String("new_a".to_owned()));
    row1.insert("value".to_owned(), serde_json::Value::Number(1000.into()));

    let mut row2 = serde_json::Map::new();
    row2.insert("id".to_owned(), serde_json::Value::Number(200.into()));
    row2.insert("name".to_owned(), serde_json::Value::String("new_b".to_owned()));
    row2.insert("value".to_owned(), serde_json::Value::Number(2000.into()));

    let diff = Diff::ReplaceTable {
        table: "test_diff".to_owned(),
        rows: vec![row1, row2],
    };
    store.apply_diffs(tick, &[diff]).unwrap();

    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();
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
    let store = setup_store_with_data();
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
    store.apply_diffs(tick, &diffs).unwrap();

    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();
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
    let store = setup_store_with_data();
    let tick = Tick(1);

 // Apply diffs
    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(3),
        column: "value".to_owned(),
        value: serde_json::Value::Number(9999.into()),
    };
    store.apply_diffs(tick, &[diff]).unwrap();

 // Generate snapshot AFTER applying diffs
    let snap = store.generate_snapshot(tick).unwrap();
    let batches = snap.table_batches("test_diff").unwrap();

    let value_col = batches[2]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col.value(0), 9999);
}

#[test]
fn apply_diffs_missing_table_fails() {
    let store = ArrowStore::new();
    let diff = Diff::Update {
        table: "nonexistent".to_owned(),
        row: RowId::new(1),
        column: "col".to_owned(),
        value: serde_json::Value::Number(1.into()),
    };
    let result = store.apply_diffs(Tick(1), &[diff]);
    assert!(result.is_err());
}

#[test]
fn apply_diffs_missing_row_fails() {
    let store = setup_store_with_data();
    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(999),
        column: "name".to_owned(),
        value: serde_json::Value::String("nope".to_owned()),
    };
    let result = store.apply_diffs(Tick(1), &[diff]);
    assert!(result.is_err());
}

#[test]
fn apply_diffs_wrong_column_fails() {
    let store = setup_store_with_data();
    let diff = Diff::Update {
        table: "test_diff".to_owned(),
        row: RowId::new(1),
        column: "nonexistent_column".to_owned(),
        value: serde_json::Value::String("nope".to_owned()),
    };
    let result = store.apply_diffs(Tick(1), &[diff]);
    assert!(result.is_err());
}
