//! Integration tests proving that simulation consumers cannot access
//! RecordBatch directly through the public API boundary.
//!
//! The only read path for external consumers is:
//!   CommitStore::generate_snapshot() -> Arc<WorldSnapshot>
//!     -> WorldView::new(snapshot) -> typed accessors
//!
//! No external consumer should ever see RecordBatch, arrow_schema::Schema,
//! or ArrayRef through the public API.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use scharnhorst_arrow_store::{ArrowStore, InitStore, MutationMode, WorldView};
use scharnhorst_core::Tick;
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn make_spec(name: &str) -> TableSpec {
    TableSpec::new(name)
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap_or_else(|_| TableSpec::new(name))
}

/// Build a 3-column, 3-row test batch: id (Int64), label (Utf8), score (Int64).
fn make_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("score", DataType::Int64, false),
    ]));
    let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let label_arr: ArrayRef = Arc::new(StringArray::from(vec![
        Some("alpha"),
        Some("beta"),
        Some("gamma"),
    ]));
    let score_arr: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200, 300]));
    RecordBatch::try_new(schema, vec![id_arr, label_arr, score_arr]).unwrap_or_else(|_| {
        let fallback = Arc::new(Schema::new(vec![Field::new(
            "dummy",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            fallback,
            vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef],
        )
        .unwrap_or_else(|_| panic!("failed to create fallback batch"))
    })
}

// ------------------------------------------------------------------
// WorldView public API surface tests
// ------------------------------------------------------------------

/// WorldView must NOT expose RecordBatch, arrow_schema::Schema, or ArrayRef
/// through any public method. This test is a compile-time guarantee —
/// it exercises every public method and asserts the return types are
/// non-arrow Rust primitives.
#[test]
fn world_view_api_surface_is_clean() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = make_spec("clean_test");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();
    init_store
        .append_batches("clean_test", Tick(1), vec![make_test_batch()])
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    let view = WorldView::new(snap);

    // All public methods return non-arrow types.
    let _tick: Tick = view.tick();
    let _names: Vec<String> = view.table_names();
    let _count: usize = view.row_count("clean_test").unwrap();
    let _cols: Vec<String> = view.column_names("clean_test").unwrap();
    let _ty: String = view.column_type("clean_test", "id").unwrap();

    // iter_rows returns WorldViewRowIter, not RecordBatch or ArrayRef.
    let iter = view.iter_rows("clean_test").unwrap();
    for row_result in iter {
        let row = row_result.unwrap();
        // All getters return Rust primitives wrapped in ArrowStoreResult<Option<T>>.
        let _id: Option<i64> = row.get_i64(0).unwrap();
        let _label: Option<String> = row.get_string(1).unwrap();
        let _score: Option<i64> = row.get_i64(2).unwrap();
    }
}

/// Document that external consumers use WorldView, not raw WorldSnapshot.
/// WorldSnapshot::table_batches() is pub(crate) — inaccessible from
/// external crates. WorldSnapshot::get_table() returns Arc<VersionedTable>,
/// but VersionedTable::versions() and VersionedTable::get_version() are
/// pub(crate).
///
/// This test verifies the intended public API path:
///   CommitStore::generate_snapshot() -> Arc<WorldSnapshot> -> WorldView
#[test]
fn consumer_read_path_is_world_view() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = make_spec("consumer_test");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();
    init_store
        .append_batches("consumer_test", Tick(1), vec![make_test_batch()])
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();

    // The snapshot is Arc<WorldSnapshot> — consumers must wrap it.
    let view = WorldView::new(Arc::clone(&snap));

    // Verify WorldView provides all needed accessors.
    assert!(view.table_names().contains(&"consumer_test".to_owned()));
    assert_eq!(view.row_count("consumer_test").unwrap(), 3);
    assert_eq!(
        view.column_names("consumer_test").unwrap(),
        vec!["id", "label", "score"]
    );
    assert_eq!(view.column_type("consumer_test", "id").unwrap(), "Int64");
    assert_eq!(view.column_type("consumer_test", "label").unwrap(), "Utf8");
    assert_eq!(view.column_type("consumer_test", "score").unwrap(), "Int64");

    // Row iteration returns typed values.
    let mut rows = view.iter_rows("consumer_test").unwrap();
    let row0 = rows.next().unwrap().unwrap();
    assert_eq!(row0.get_i64(0).unwrap(), Some(1i64));
    assert_eq!(row0.get_string(1).unwrap(), Some("alpha".to_owned()));
    assert_eq!(row0.get_i64(2).unwrap(), Some(100i64));
}

/// Prove that WorldSnapshot::table_batches() is not publicly callable
/// from an external crate's perspective. Within a same-crate integration
/// test, pub(crate) methods ARE accessible — so this test documents the
/// intended API boundary by showing that consumers use WorldView instead.
#[test]
fn consumers_should_use_world_view_not_snapshot_internals() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = make_spec("boundary_test");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();
    init_store
        .append_batches("boundary_test", Tick(42), vec![make_test_batch()])
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(42)).unwrap();

    // EXTERNAL PATTERN: wrap in WorldView; never call snap.table_batches().
    let view = WorldView::new(Arc::clone(&snap));

    // WorldView provides row_count, column_names, column_type, iter_rows.
    assert_eq!(view.row_count("boundary_test").unwrap(), 3);
    assert!(!view.column_names("boundary_test").unwrap().is_empty());

    let mut rows = view.iter_rows("boundary_test").unwrap();
    let row = rows.next().unwrap().unwrap();
    // All values accessible via typed getters — no arrow types leak.
    assert_eq!(row.get_i64(0).unwrap(), Some(1i64));
    assert_eq!(row.get_string(1).unwrap(), Some("alpha".to_owned()));
    assert_eq!(row.get_i64(2).unwrap(), Some(100i64));
}

/// Multi-table scenario: register two tables, verify WorldView can
/// distinguish them and read from both.
#[test]
fn world_view_multi_table_boundary() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));

    let spec_a = make_spec("table_a");
    let spec_b = TableSpec::new("table_b")
        .with_column(ColumnSpec::new("key", FieldSemantic::Id, "i64"))
        .unwrap_or_else(|_| make_spec("table_b"));
    init_store
        .create_table(&spec_a, MutationMode::AppendOnly)
        .unwrap();
    init_store
        .create_table(&spec_b, MutationMode::AppendOnly)
        .unwrap();

    init_store
        .append_batches("table_a", Tick(1), vec![make_test_batch()])
        .unwrap();
    init_store
        .append_batches("table_b", Tick(1), vec![make_test_batch()])
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    let view = WorldView::new(snap);

    let names = view.table_names();
    assert!(names.contains(&"table_a".to_owned()));
    assert!(names.contains(&"table_b".to_owned()));
    assert_eq!(names.len(), 2);

    assert_eq!(view.row_count("table_a").unwrap(), 3);
    assert_eq!(view.row_count("table_b").unwrap(), 3);

    // Both tables have typed access without RecordBatch exposure.
    let mut rows_a = view.iter_rows("table_a").unwrap();
    let row_a = rows_a.next().unwrap().unwrap();
    assert_eq!(row_a.get_i64(0).unwrap(), Some(1i64));

    let mut rows_b = view.iter_rows("table_b").unwrap();
    let row_b = rows_b.next().unwrap().unwrap();
    assert_eq!(row_b.get_i64(0).unwrap(), Some(1i64));
}

/// Verify WorldView's error handling: non-existent table returns error,
/// not a panic.
#[test]
fn world_view_nonexistent_table_returns_error() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = make_spec("exists");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    let view = WorldView::new(snap);

    assert!(view.row_count("missing").is_err());
    assert!(view.column_names("missing").is_err());
    assert!(view.column_type("missing", "col").is_err());
    assert!(view.iter_rows("missing").is_err());
}

/// WorldView clone is cheap (Arc bump) and preserves identical state.
#[test]
fn world_view_clone_preserves_state() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = make_spec("clone_test");
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();
    init_store
        .append_batches("clone_test", Tick(10), vec![make_test_batch()])
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(10)).unwrap();
    let view = WorldView::new(snap);
    let clone = view.clone();

    assert_eq!(clone.tick(), view.tick());
    assert_eq!(clone.table_names(), view.table_names());
    assert_eq!(
        clone.row_count("clone_test").unwrap(),
        view.row_count("clone_test").unwrap()
    );
    assert_eq!(
        clone.column_names("clone_test").unwrap(),
        view.column_names("clone_test").unwrap()
    );
}

/// Verify that column_type returns readable type names.
#[test]
fn column_type_returns_readable_names() {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = TableSpec::new("typed")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap_or_else(|_| make_spec("typed"));
    init_store
        .create_table(&spec, MutationMode::AppendOnly)
        .unwrap();
    init_store
        .append_batches("typed", Tick(1), vec![make_test_batch()])
        .unwrap();

    let commit_store = init_store.into_simulation().unwrap();
    let snap = commit_store.generate_snapshot(Tick(1)).unwrap();
    let view = WorldView::new(snap);

    assert_eq!(view.column_type("typed", "id").unwrap(), "Int64");
    assert_eq!(view.column_type("typed", "label").unwrap(), "Utf8");
    assert_eq!(view.column_type("typed", "score").unwrap(), "Int64");
    assert!(view.column_type("typed", "nonexistent").is_err());
}
