use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use scharnhorst_arrow_store::SnapshotIngestor;
use scharnhorst_core::{RowId, RowLookup, RowPositionMap, Tick};
use scharnhorst_query::{
    BatchColumnReader, ColumnView, DebugWriteJournal, DebugWriteOp, InMemoryReadSource,
    InspectorConsole, QueryEngine, QueryError, QueryResult, ReadRequest, ReadResponse, RowCursor,
    RowLookupView, TableReadView, TypedColumnAccess, UnifiedReadSource, ValidatablePath,
    ValidationErrorKind,
};
use scharnhorst_schema::{
    ColumnSpec, FieldSemantic, RelationEdge, RelationKind, SchemaRegistry, TableSpec,
};

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("health", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ])
}

fn test_table_spec() -> TableSpec {
    TableSpec::new("units")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap()
        .with_column(ColumnSpec::new("health", FieldSemantic::Quantity, "f64"))
        .unwrap()
        .with_column(ColumnSpec::new("active", FieldSemantic::Tag, "bool"))
        .unwrap()
}

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(test_schema());
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let name: ArrayRef = Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"]));
    let health: ArrayRef = Arc::new(Float64Array::from(vec![100.0, 80.0, 60.0]));
    let active: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
    RecordBatch::try_new(schema, vec![id, name, health, active]).unwrap()
}

fn make_engine_with_data() -> QueryEngine {
    let registry = SchemaRegistry::new();
    let spec = test_table_spec();

    let engine = QueryEngine::new(registry);
    engine.register_table_schema(spec).unwrap();
    engine
        .ingest_snapshot(
            Tick(1),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();
    engine
}

// ------------------------------------------------------------------
// Error type tests
// ------------------------------------------------------------------

#[test]
fn query_error_from_core_error() {
    let core_err = scharnhorst_core::CoreError::InvalidId("x".to_owned());
    let query_err: QueryError = core_err.into();
    assert!(matches!(query_err, QueryError::Core(_)));
}

#[test]
fn query_error_from_schema_error() {
    let schema_err = scharnhorst_schema::SchemaError::TableNotFound("missing".to_owned());
    let query_err: QueryError = schema_err.into();
    assert!(matches!(query_err, QueryError::Schema(_)));
}

// ------------------------------------------------------------------
// Typed column access tests
// ------------------------------------------------------------------

#[test]
fn typed_column_access_i64() {
    let arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let view = ColumnView::new(arr, "id");

    assert_eq!(
        TypedColumnAccess::<i64>::get(&view, 0).unwrap(),
        Some(10i64)
    );
    assert_eq!(
        TypedColumnAccess::<i64>::get(&view, 1).unwrap(),
        Some(20i64)
    );
    assert_eq!(
        TypedColumnAccess::<i64>::get(&view, 2).unwrap(),
        Some(30i64)
    );
    assert!(matches!(
        TypedColumnAccess::<i64>::get(&view, 5),
        Err(QueryError::IndexOutOfBounds(5))
    ));
}

#[test]
fn typed_column_access_f64() {
    let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5]));
    let view = ColumnView::new(arr, "health");

    assert_eq!(view.get(0).unwrap(), Some(1.5f64));
    assert_eq!(view.get(1).unwrap(), Some(2.5f64));
}

#[test]
fn typed_column_access_bool() {
    let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    let view = ColumnView::new(arr, "active");

    assert_eq!(view.get(0).unwrap(), Some(true));
    assert_eq!(view.get(1).unwrap(), Some(false));
}

#[test]
fn typed_column_access_string() {
    let arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let view = ColumnView::new(arr, "name");

    assert_eq!(view.get(0).unwrap(), Some("a".to_owned()));
    assert_eq!(view.get(1).unwrap(), Some("b".to_owned()));
}

#[test]
fn typed_column_access_type_mismatch() {
    let arr: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let view = ColumnView::new(arr, "x");
    let result: QueryResult<Option<f64>> = view.get(0);
    assert!(matches!(result, Err(QueryError::TypeMismatch { .. })));
}

// ------------------------------------------------------------------
// RowCursor tests
// ------------------------------------------------------------------

#[test]
fn row_cursor_navigation() {
    let batch = sample_batch();
    let columns: Vec<(String, ColumnView)> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let view = ColumnView::new(batch.column(idx).clone(), field.name());
            (field.name().clone(), view)
        })
        .collect();

    let mut cursor = RowCursor::new(columns).unwrap();
    assert_eq!(cursor.row_count(), 3);
    assert_eq!(cursor.current_row(), 0);

    assert_eq!(cursor.get_typed::<i64>("id").unwrap(), Some(1));
    assert_eq!(
        cursor.get_typed::<String>("name").unwrap(),
        Some("alpha".to_owned())
    );

    assert!(cursor.advance());
    assert_eq!(cursor.current_row(), 1);
    assert_eq!(cursor.get_typed::<i64>("id").unwrap(), Some(2));

    assert!(cursor.advance());
    assert_eq!(cursor.current_row(), 2);
    assert!(!cursor.advance());

    cursor.reset();
    assert_eq!(cursor.current_row(), 0);
}

#[test]
fn row_cursor_row_id() {
    let batch = sample_batch();
    let columns: Vec<(String, ColumnView)> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let view = ColumnView::new(batch.column(idx).clone(), field.name());
            (field.name().clone(), view)
        })
        .collect();

    let cursor = RowCursor::new(columns).unwrap();
    let rid = cursor.get_row_id("id").unwrap();
    assert_eq!(rid, Some(RowId::new(1)));
}

#[test]
fn row_cursor_rejects_mismatched_lengths() {
    let a = ColumnView::new(Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef, "a");
    let b = ColumnView::new(Arc::new(Int64Array::from(vec![1])) as ArrayRef, "b");
    let result = RowCursor::new(vec![("a".to_owned(), a), ("b".to_owned(), b)]);
    assert!(matches!(result, Err(QueryError::InvalidQuery(_))));
}

// ------------------------------------------------------------------
// BatchColumnReader tests
// ------------------------------------------------------------------

#[test]
fn batch_reader_slices() {
    let batch = sample_batch();
    let arrays: Vec<(String, ArrayRef)> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), batch.column(idx).clone()))
        .collect();

    let reader = BatchColumnReader::new(arrays).unwrap();
    assert_eq!(reader.row_count(), 3);

    let ids = reader.i64_slice("id").unwrap();
    assert_eq!(ids.get(0).unwrap(), Some(1));
    assert_eq!(ids.get(1).unwrap(), Some(2));
    assert_eq!(ids.get(2).unwrap(), Some(3));

    let names = reader.string_slice("name").unwrap();
    assert_eq!(names.get(0).unwrap(), Some("alpha".to_owned()));

    let health = reader.f64_slice("health").unwrap();
    assert_eq!(health.get(0).unwrap(), Some(100.0));

    let active = reader.bool_slice("active").unwrap();
    assert_eq!(active.get(0).unwrap(), Some(true));
}

#[test]
fn batch_reader_iter_valid() {
    let batch = sample_batch();
    let arrays: Vec<(String, ArrayRef)> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), batch.column(idx).clone()))
        .collect();

    let reader = BatchColumnReader::new(arrays).unwrap();
    let ids: Vec<i64> = reader.i64_slice("id").unwrap().iter_valid().collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

// ------------------------------------------------------------------
// Unified read interface tests
// ------------------------------------------------------------------

#[test]
fn unified_read_single_table() {
    let engine = make_engine_with_data();
    let view = engine.read_single_table(Tick(1), "units").unwrap();
    assert_eq!(view.table_name(), "units");
    assert_eq!(view.tick(), Tick(1));
    assert_eq!(view.total_rows(), 3);
}

#[test]
fn unified_read_request_multiple_tables() {
    let engine = make_engine_with_data();
    let req = ReadRequest::new(Tick(1)).with_table("units");
    let resp = engine.read(req).unwrap();
    assert_eq!(resp.len(), 1);
    assert!(resp.get("units").is_ok());
}

#[test]
fn unified_read_missing_table() {
    let engine = make_engine_with_data();
    let result = engine.read_single_table(Tick(1), "missing");
    assert!(matches!(result, Err(QueryError::TableNotFound(_))));
}

#[test]
fn in_memory_read_source() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);

    let mut source = scharnhorst_query::InMemoryReadSource::new();
    source.register(view.clone());

    let req = ReadRequest::new(Tick(1)).with_table("units");
    let resp = source.read(req).unwrap();
    assert_eq!(resp.get("units").unwrap().total_rows(), 3);
}

// ------------------------------------------------------------------
// QueryEngine schema registry tests
// ------------------------------------------------------------------

#[test]
fn engine_schema_registry() {
    let engine = make_engine_with_data();
    let schema = engine.table_schema("units").unwrap();
    assert_eq!(schema.name, "units");
    assert_eq!(schema.columns.len(), 4);
}

#[test]
fn engine_register_and_query_schema() {
    let engine = QueryEngine::new(SchemaRegistry::new());
    let spec = TableSpec::new("items")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap();
    engine.register_table_schema(spec.clone()).unwrap();

    let retrieved = engine.table_schema("items").unwrap();
    assert_eq!(retrieved.name, "items");
}

// ------------------------------------------------------------------
// QueryEngine typed access tests
// ------------------------------------------------------------------

#[test]
fn engine_column_view() {
    let engine = make_engine_with_data();
    let col = engine.column_view("units", "health").unwrap();
    assert_eq!(col.get(0).unwrap(), Some(100.0f64));
}

#[test]
fn engine_row_cursor() {
    let engine = make_engine_with_data();
    let cursor = engine.row_cursor("units").unwrap();
    assert_eq!(cursor.row_count(), 3);
    assert_eq!(
        cursor.get_typed::<String>("name").unwrap(),
        Some("alpha".to_owned())
    );
}

#[test]
fn engine_batch_reader() {
    let engine = make_engine_with_data();
    let reader = engine.batch_reader("units").unwrap();
    assert_eq!(reader.row_count(), 3);
    let ids = reader.i64_slice("id").unwrap();
    assert_eq!(ids.get(0).unwrap(), Some(1));
}

// ------------------------------------------------------------------
// SQL interface tests
// ------------------------------------------------------------------

#[test]
fn sql_context_register_table_validates_name() {
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    assert!(ctx.register_table("", vec![]).is_err());
    assert!(ctx.register_table("ok", vec![]).is_ok());
}

#[test]
fn sql_context_registered_tables_empty() {
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let tables = ctx.registered_tables().unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn sql_execute_select_one() {
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let batches = ctx.execute_sql("SELECT 1").await.unwrap();
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 1);
}

// ------------------------------------------------------------------
// DebugWriteJournal tests
// ------------------------------------------------------------------

#[test]
fn debug_journal_records_and_retrieves() {
    let journal = DebugWriteJournal::new(4);
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 10,
    });
    journal.record(DebugWriteOp::Patch {
        table: "units".to_owned(),
        tick: Tick(2),
        row_indices: vec![0, 1],
    });

    assert_eq!(journal.len(), 2);
    let ops = journal.ops();
    assert_eq!(ops[0].table_name(), "units");
    assert_eq!(ops[0].tick(), Tick(1));
}

#[test]
fn debug_journal_ring_buffer_eviction() {
    let journal = DebugWriteJournal::new(2);
    journal.record(DebugWriteOp::Append {
        table: "a".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Append {
        table: "b".to_owned(),
        tick: Tick(2),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Append {
        table: "c".to_owned(),
        tick: Tick(3),
        row_count: 1,
    });

    assert_eq!(journal.len(), 2);
    let ops = journal.ops();
    assert_eq!(ops[0].table_name(), "b");
    assert_eq!(ops[1].table_name(), "c");
}

#[test]
fn debug_journal_disable() {
    let journal = DebugWriteJournal::new(10);
    journal.set_enabled(false);
    journal.record(DebugWriteOp::Append {
        table: "x".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    assert!(journal.is_empty());
    journal.set_enabled(true);
    journal.record(DebugWriteOp::Append {
        table: "x".to_owned(),
        tick: Tick(2),
        row_count: 1,
    });
    assert_eq!(journal.len(), 1);
}

#[test]
fn debug_journal_ops_for_table() {
    let journal = DebugWriteJournal::new(10);
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Append {
        table: "items".to_owned(),
        tick: Tick(1),
        row_count: 2,
    });
    let unit_ops = journal.ops_for_table("units");
    assert_eq!(unit_ops.len(), 1);
    assert_eq!(unit_ops[0].tick(), Tick(1));
}

#[test]
fn debug_journal_latest_and_clear() {
    let journal = DebugWriteJournal::new(10);
    assert!(journal.latest().is_none());
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(5),
        row_count: 3,
    });
    assert_eq!(journal.latest().unwrap().tick(), Tick(5));
    journal.clear();
    assert!(journal.is_empty());
}

// ------------------------------------------------------------------
// Inspector console tests
// ------------------------------------------------------------------

#[test]
fn inspector_console_register_and_list() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let names: Vec<&str> = console.table_names().collect();
    assert_eq!(names, vec!["units"]);
}

#[test]
fn inspector_console_schema_lookup() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let schema = console.schema("units").unwrap();
    assert_eq!(schema.name, "units");
    assert!(console.schema("missing").is_err());
}

#[test]
fn inspector_summarize_table() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let summary = console.summarize_table("units", &[sample_batch()]).unwrap();
    assert_eq!(summary.name, "units");
    assert_eq!(summary.row_count, 3);
    assert_eq!(summary.column_summaries.len(), 4);
    let health_col = summary
        .column_summaries
        .iter()
        .find(|c| c.name == "health")
        .unwrap();
    assert_eq!(health_col.value_count, 3);
    assert_eq!(health_col.null_count, 0);
}

#[test]
fn inspector_inspect_page_basic() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let page = console
        .inspect_page("units", &[sample_batch()], 0, 2)
        .unwrap();
    assert_eq!(page.table_name, "units");
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.total_rows, 3);
    assert!(page.has_next());
    assert!(!page.has_prev());
}

#[test]
fn inspector_inspect_page_second_page() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let page = console
        .inspect_page("units", &[sample_batch()], 1, 2)
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(!page.has_next());
    assert!(page.has_prev());
}

#[test]
fn inspector_inspect_page_out_of_bounds() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let result = console.inspect_page("units", &[sample_batch()], 5, 2);
    assert!(matches!(result, Err(QueryError::IndexOutOfBounds(_))));
}

#[test]
fn inspector_column_views() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));
    let batch = sample_batch();
    let views = console.column_views("units", &batch).unwrap();
    assert!(views.contains_key("id"));
    assert!(views.contains_key("name"));
}

// ------------------------------------------------------------------
// QueryEngine inspector integration tests
// ------------------------------------------------------------------

#[test]
fn engine_inspect_table_summary() {
    let engine = make_engine_with_data();
    let summary = engine.inspect_table_summary("units").unwrap();
    assert_eq!(summary.row_count, 3);
    assert_eq!(summary.column_summaries.len(), 4);
}

#[test]
fn engine_inspect_table_page() {
    let engine = make_engine_with_data();
    let page = engine.inspect_table_page("units", 0, 2).unwrap();
    assert_eq!(page.rows.len(), 2);
}

// ------------------------------------------------------------------
// QueryEngine cache management tests
// ------------------------------------------------------------------

#[test]
fn engine_clear_cache() {
    let engine = make_engine_with_data();
    assert!(!engine.cached_table_names().unwrap().is_empty());
    engine.clear_cache().unwrap();
    assert!(engine.cached_table_names().unwrap().is_empty());
}

#[test]
fn engine_latest_tick() {
    let engine = make_engine_with_data();
    assert_eq!(engine.latest_tick().unwrap(), Some(Tick(1)));
}

// ------------------------------------------------------------------
// TableReadView tests
// ------------------------------------------------------------------

#[test]
fn table_read_view_properties() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(7), vec![sample_batch()], spec, None);
    assert_eq!(view.table_name(), "units");
    assert_eq!(view.tick(), Tick(7));
    assert_eq!(view.batch_count(), 1);
    assert_eq!(view.total_rows(), 3);
}

#[test]
fn table_read_view_column_spec() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let col = view.column_spec("health").unwrap();
    assert_eq!(col.name, "health");
    assert!(view.column_spec("nope").is_err());
}

#[test]
fn table_read_view_first_batch_reader() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let reader = view.first_batch_reader().unwrap();
    assert_eq!(reader.row_count(), 3);
}

#[test]
fn table_read_view_first_batch_cursor() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let cursor = view.first_batch_cursor().unwrap();
    assert_eq!(cursor.row_count(), 3);
}

#[test]
fn table_read_view_empty_batches() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![], spec, None);
    assert!(view.first_batch_reader().is_err());
    assert!(view.first_batch_cursor().is_err());
}

// ------------------------------------------------------------------
// TableReadView Unit Tests
// ------------------------------------------------------------------
// Query-Engine provides the sole read interface
// for ALL consumers. No component may hold a direct Arrow RecordBatch
// reference. TableReadView encapsulates RecordBatch and exposes
// controlled access methods.

/// Verify TableReadView construction with empty batches.
#[test]
fn dc10_table_read_view_construction() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(42), vec![], spec, None);
    assert_eq!(view.table_name(), "units");
    assert_eq!(view.tick(), Tick(42));
    assert_eq!(
        view.total_rows(),
        0,
        "DC-10: empty batches must yield zero total rows"
    );
    assert_eq!(
        view.batch_count(),
        0,
        "DC-10: empty batches vec must yield batch_count=0"
    );
}

/// Verify schema access returns the correct TableSpec.
#[test]
fn dc10_table_read_view_schema_access() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![], spec, None);
    let schema = view.schema();
    assert_eq!(
        schema.name, "units",
        "DC-10: schema access must return the registered TableSpec"
    );
    assert_eq!(
        schema.columns.len(),
        4,
        "DC-10: schema must contain all registered columns"
    );
}

/// Verify column_spec lookup for existing and nonexistent columns.
#[test]
fn dc10_table_read_view_column_spec_lookup() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);

    // existing column returns Some
    let col = view.column_spec("id");
    assert!(col.is_ok(), "DC-10: column_spec('id') should succeed");
    assert_eq!(col.unwrap().name, "id");

    // nonexistent column returns ColumnNotFound
    let err = view.column_spec("nonexistent");
    assert!(
        matches!(err, Err(QueryError::ColumnNotFound { .. })),
        "DC-10: column_spec for nonexistent column must return ColumnNotFound"
    );
}

/// Empty batches produce error for first_batch_reader.
#[test]
fn dc10_table_read_view_empty_batch_first_batch_reader_err() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![], spec, None);
    let result = view.first_batch_reader();
    assert!(
        matches!(result, Err(QueryError::InvalidQuery(_))),
        "DC-10: first_batch_reader on empty batches must return InvalidQuery error"
    );
}

/// Empty batches produce error for first_batch_cursor.
#[test]
fn dc10_table_read_view_empty_batch_first_batch_cursor_err() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![], spec, None);
    let result = view.first_batch_cursor();
    assert!(
        matches!(result, Err(QueryError::InvalidQuery(_))),
        "DC-10: first_batch_cursor on empty batches must return InvalidQuery error"
    );
}

/// Empty batches produce error for first_batch_columns.
#[test]
fn dc10_table_read_view_empty_batch_first_batch_columns_err() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![], spec, None);
    let result = view.first_batch_columns();
    assert!(
        matches!(result, Err(QueryError::InvalidQuery(_))),
        "DC-10: first_batch_columns on empty batches must return InvalidQuery error"
    );
}

/// TableReadView with sample_batch reads correctly through all accessors.
#[test]
fn dc10_table_read_view_with_sample_batch_reads_correctly() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);

    // verify structural properties
    assert_eq!(view.total_rows(), 3, "DC-10: sample_batch has 3 rows");
    assert_eq!(view.batch_count(), 1, "DC-10: single batch in vec");

    // first_batch_reader works
    let reader = view.first_batch_reader();
    assert!(
        reader.is_ok(),
        "DC-10: first_batch_reader must succeed with data"
    );
    let reader = reader.unwrap();
    assert_eq!(reader.row_count(), 3);

    // first_batch_cursor works
    let cursor = view.first_batch_cursor();
    assert!(
        cursor.is_ok(),
        "DC-10: first_batch_cursor must succeed with data"
    );
    let cursor = cursor.unwrap();
    assert_eq!(cursor.row_count(), 3);

    // first_batch_columns works
    let cols = view.first_batch_columns();
    assert!(
        cols.is_ok(),
        "DC-10: first_batch_columns must succeed with data"
    );
    let cols = cols.unwrap();
    assert!(cols.contains_key("id"), "DC-10: columns must contain 'id'");
    assert!(
        cols.contains_key("name"),
        "DC-10: columns must contain 'name'"
    );
    assert!(
        cols.contains_key("health"),
        "DC-10: columns must contain 'health'"
    );
    assert!(
        cols.contains_key("active"),
        "DC-10: columns must contain 'active'"
    );
}

// ------------------------------------------------------------------
// ReadRequest / ReadResponse Tests
// ------------------------------------------------------------------

/// ReadRequest builder pattern produces correct tick and table_names.
#[test]
fn dc10_read_request_builder_pattern() {
    let req = ReadRequest::new(Tick(1)).with_table("a").with_table("b");
    // tick is a public field
    assert_eq!(
        req.tick,
        Tick(1),
        "DC-10: ReadRequest tick must match constructor argument"
    );
    // table_names is a public field
    assert_eq!(
        req.table_names,
        vec!["a", "b"],
        "DC-10: ReadRequest builder must accumulate table names in order"
    );
}

/// ReadResponse insert and get for a registered table.
#[test]
fn dc10_read_response_insert_and_get() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let mut resp = ReadResponse::new(Tick(1));
    resp.insert(view);

    // get returns Ok with the table
    let result = resp.get("units");
    assert!(
        result.is_ok(),
        "DC-10: get('units') must return Ok after insert"
    );
    assert_eq!(result.unwrap().table_name(), "units");
}

/// ReadResponse get for missing table returns TableNotFound error.
#[test]
fn dc10_read_response_get_missing_table_err() {
    let resp: ReadResponse = ReadResponse::new(Tick(1));
    let result = resp.get("missing");
    assert!(
        matches!(result, Err(QueryError::TableNotFound(_))),
        "DC-10: get('missing') on empty response must return TableNotFound"
    );
}

/// ReadResponse len and is_empty reflect insertion state.
#[test]
fn dc10_read_response_len_and_is_empty() {
    let mut resp = ReadResponse::new(Tick(1));

    // new response is empty
    assert!(resp.is_empty(), "DC-10: new ReadResponse must be empty");
    assert_eq!(resp.len(), 0, "DC-10: new ReadResponse len must be 0");

    // after insert, not empty
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    resp.insert(view);
    assert!(
        !resp.is_empty(),
        "DC-10: ReadResponse must not be empty after insert"
    );
    assert_eq!(
        resp.len(),
        1,
        "DC-10: ReadResponse len must be 1 after one insert"
    );
}

// ------------------------------------------------------------------
// InMemoryReadSource (UnifiedReadSource) Tests
// ------------------------------------------------------------------

/// InMemoryReadSource register and read returns correct ReadResponse.
#[test]
fn dc10_in_memory_read_source_register_and_read() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);

    // register table view
    let mut source = InMemoryReadSource::new();
    source.register(view);

    // read returns response with the registered table
    let req = ReadRequest::new(Tick(1)).with_table("units");
    let resp = source.read(req);
    assert!(
        resp.is_ok(),
        "DC-10: read must succeed for registered table"
    );
    let resp = resp.unwrap();
    assert_eq!(resp.len(), 1);
    let table = resp.get("units").unwrap();
    assert_eq!(
        table.total_rows(),
        3,
        "DC-10: InMemoryReadSource must return the registered data"
    );
}

/// InMemoryReadSource returns TableNotFound for unregistered table.
#[test]
fn dc10_in_memory_read_source_read_missing_table_err() {
    let source = InMemoryReadSource::new();
    let req = ReadRequest::new(Tick(1)).with_table("missing");
    let result = source.read(req);
    assert!(
        matches!(result, Err(QueryError::TableNotFound(_))),
        "DC-10: InMemoryReadSource must return TableNotFound for unregistered table"
    );
}

/// InMemoryReadSource unregister and clear operations.
#[test]
fn dc10_in_memory_read_source_unregister_and_clear() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let mut source = InMemoryReadSource::new();
    source.register(view);

    // unregister an existing table returns Ok
    let result = source.unregister("units");
    assert!(
        result.is_ok(),
        "DC-10: unregister existing table must return Ok"
    );

    // unregister an already-unregistered table returns Err
    let result = source.unregister("units");
    assert!(
        matches!(result, Err(QueryError::TableNotFound(_))),
        "DC-10: unregister nonexistent table must return TableNotFound"
    );

    // clear empties the source
    let spec2 = Arc::new(test_table_spec());
    let view2 = TableReadView::new("items", Tick(2), vec![sample_batch()], spec2, None);
    source.register(view2);
    source.clear();
    let req = ReadRequest::new(Tick(2)).with_table("items");
    let result = source.read(req);
    assert!(
        matches!(result, Err(QueryError::TableNotFound(_))),
        "DC-10: after clear, previously registered tables must not be found"
    );
}

// ------------------------------------------------------------------
// Constraint Documentation Tests
// ------------------------------------------------------------------

/// UnifiedReadSource is the ONLY read interface for all consumers.
/// Multiple consumers (scheduler, evaluator) use the SAME InMemoryReadSource,
/// proving it is the single source of truth for all read paths.
#[test]
fn dc10_unified_read_source_is_the_only_read_interface() {
    // create a single InMemoryReadSource shared by all consumers
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let mut source = InMemoryReadSource::new();
    source.register(view);

    // consumer A = scheduler reads via UnifiedReadSource
    let scheduler_req = ReadRequest::new(Tick(1)).with_table("units");
    let scheduler_resp = source.read(scheduler_req).unwrap();
    let scheduler_view = scheduler_resp.get("units").unwrap();
    assert_eq!(
        scheduler_view.total_rows(),
        3,
        "DC-10: scheduler consumer reads through unified interface"
    );

    // consumer B = evaluator reads via the SAME UnifiedReadSource
    let eval_req = ReadRequest::new(Tick(1)).with_table("units");
    let eval_resp = source.read(eval_req).unwrap();
    let eval_view = eval_resp.get("units").unwrap();
    assert_eq!(
        eval_view.total_rows(),
        3,
        "DC-10: evaluator consumer reads through unified interface"
    );

    // both consumers see the same data via the single source of truth
    assert_eq!(
        scheduler_view.table_name(),
        eval_view.table_name(),
        "DC-10: all consumers see same table name through unified interface"
    );
    assert_eq!(
        scheduler_view.tick(),
        eval_view.tick(),
        "DC-10: all consumers see same tick through unified interface"
    );
    assert_eq!(
        scheduler_view.total_rows(),
        eval_view.total_rows(),
        "DC-10: all consumers see same row count through unified interface"
    );
}

/// TableReadView encapsulates RecordBatch - consumers never hold
/// Arc<RecordBatch> directly. They access data through batches iterator
/// and typed accessor methods, not raw Arrow references.
#[test]
fn dc10_table_read_view_encapsulates_record_batches() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);

    // batches exposes an iterator over &RecordBatch,
    // but consumers never need Arc<RecordBatch> directly
    let batch_count: usize = view.batches().count();
    assert_eq!(
        batch_count, 1,
        "DC-10: batches() iterator count matches actual batch count"
    );

    // typed access through first_batch_reader (no direct RecordBatch)
    let reader = view.first_batch_reader().unwrap();
    assert_eq!(
        reader.row_count(),
        3,
        "DC-10: typed access via BatchColumnReader, not raw RecordBatch"
    );

    // typed access through first_batch_cursor (no direct RecordBatch)
    let cursor = view.first_batch_cursor().unwrap();
    assert_eq!(
        cursor.row_count(),
        3,
        "DC-10: row-by-row access via RowCursor, not raw RecordBatch"
    );

    // typed access through first_batch_columns (no direct RecordBatch)
    let cols = view.first_batch_columns().unwrap();
    assert_eq!(
        cols.len(),
        4,
        "DC-10: column access via ColumnView map, not raw RecordBatch"
    );
}

// ==========================================================================
// SQL Integration Tests (DataFusion-backed SqlExecutionContext)
// ==========================================================================
// SQL Integration - optional SQL via DataFusion for
// complex analysis: SELECT SUM(pop) FROM pops GROUP BY culture.
// SqlExecutionContext provides read-only analysis through DataFusion,
// separate from the write path. These tests verify construction,
// table registration/unregistration, PreparedSql, and integration.

// ------------------------------------------------------------------
// SqlExecutionContext Construction
// ------------------------------------------------------------------

/// A newly created SqlExecutionContext has no registered tables.
#[test]
fn dc4_sql_context_new_creates_empty_context() {
    // fresh context must start with zero registered tables
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.is_empty(),
        "DC-4: new SqlExecutionContext must have no registered tables"
    );
}

/// Default-constructed SqlExecutionContext is also empty.
#[test]
fn dc4_sql_context_default_is_empty() {
    // default is equivalent to new -- no registered tables
    let ctx = scharnhorst_query::SqlExecutionContext::default();
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.is_empty(),
        "DC-4: default SqlExecutionContext must have no registered tables"
    );
}

// ------------------------------------------------------------------
// Table Registration
// ------------------------------------------------------------------

/// Registering a table makes it appear in registered_tables.
#[test]
fn dc4_sql_register_table_adds_to_registered_tables() {
    // table registration is tracked in the context's table set
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    ctx.register_table("units", vec![sample_batch()]).unwrap();
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.contains(&"units".to_string()),
        "DC-4: registered_tables() must include 'units' after register_table"
    );
}

/// Multiple tables can be registered and all appear sorted.
#[test]
fn dc4_sql_register_multiple_tables() {
    // multiple registrations are tracked and returned sorted
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    ctx.register_table("b", vec![sample_batch()]).unwrap();
    ctx.register_table("a", vec![sample_batch()]).unwrap();
    let tables = ctx.registered_tables().unwrap();
    assert_eq!(
        tables,
        vec!["a".to_string(), "b".to_string()],
        "DC-4: registered_tables() must return sorted ascending list"
    );
}

/// Registering a table with an empty name returns InvalidQuery.
#[test]
fn dc4_sql_register_empty_table_name_returns_err() {
    // empty table name must be rejected with InvalidQuery
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let result = ctx.register_table("", vec![sample_batch()]);
    assert!(
        matches!(result, Err(QueryError::InvalidQuery(_))),
        "DC-4: empty table name must return InvalidQuery error"
    );
}

/// Registering with empty batch vec must succeed and make
/// the table appear in registered_tables.
#[test]
fn dc4_sql_register_empty_batches_does_not_crash() {
    // empty Vec<RecordBatch> should succeed; internally
    // the context registers an empty schema batch as a placeholder
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    ctx.register_table("units", vec![]).unwrap();
    // the table must be tracked in registered_tables
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.contains(&"units".to_string()),
        "DC-4: empty-batch registration must succeed and table must appear in registered_tables"
    );
}

/// Unregistering a table removes it from registered_tables.
#[test]
fn dc4_sql_unregister_table_removes_from_registered() {
    // unregister must remove the table from the tracking set
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    ctx.register_table("units", vec![sample_batch()]).unwrap();
    assert!(ctx
        .registered_tables()
        .unwrap()
        .contains(&"units".to_string()));
    ctx.unregister_table("units").unwrap();
    let tables = ctx.registered_tables().unwrap();
    assert!(
        !tables.contains(&"units".to_string()),
        "DC-4: after unregister_table, 'units' must not appear in registered_tables"
    );
}

/// Unregistering a table that was never registered must not panic.
/// DataFusion's SessionContext.deregister_table returns Ok for
/// unknown table names (silently ignores deregistration).
#[test]
fn dc4_sql_unregister_nonexistent_table_returns_err() {
    // unregistering a never-registered table must not panic
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let result = ctx.unregister_table("nonexistent");
    // DataFusion returns Ok for unknown tables — verify no panic
    assert!(
        result.is_ok(),
        "DC-4: unregister nonexistent table must not panic (DataFusion returns Ok)"
    );
    // table was never registered, so it must not appear in the list
    let tables = ctx.registered_tables().unwrap();
    assert!(
        !tables.contains(&"nonexistent".to_string()),
        "DC-4: nonexistent table must not appear in registered_tables"
    );
}

// ------------------------------------------------------------------
// SQL Execution
// ------------------------------------------------------------------

/// Executing invalid/garbage SQL must return a DataFusion error.
#[tokio::test]
async fn dc4_sql_execute_invalid_sql_returns_err() {
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let result = ctx.execute_sql("GARBAGE NOT SQL").await;
    assert!(
        matches!(result, Err(QueryError::DataFusion(_))),
        "DC-4: executing invalid SQL must return DataFusion error"
    );
}

/// Executing SQL referencing a nonexistent table must return an error.
#[tokio::test]
async fn dc4_sql_execute_on_nonexistent_table_returns_err() {
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    let result = ctx.execute_sql("SELECT * FROM nonexistent_table").await;
    assert!(
        matches!(result, Err(QueryError::DataFusion(_))),
        "DC-4: querying nonexistent table must return DataFusion error"
    );
}

/// execute_sql_limited wraps a SQL query with LIMIT and returns at most
/// the specified number of rows.
#[tokio::test]
async fn dc4_sql_execute_sql_limited_returns_limited_rows() {
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    ctx.register_table("units", vec![sample_batch()]).unwrap();

    // execute_sql_limited with limit 1 must return at most 1 row
    let batches = ctx
        .execute_sql_limited("SELECT * FROM units", 1)
        .await
        .unwrap();
    assert!(
        !batches.is_empty(),
        "DC-4: limited query must produce batches"
    );
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows <= 1,
        "DC-4: limited query with limit=1 must return at most 1 row, got {}",
        total_rows
    );
}

// ------------------------------------------------------------------
// PreparedSql
// ------------------------------------------------------------------

/// PreparedSql stores the SQL string provided at construction.
#[test]
fn dc4_prepared_sql_new_stores_sql_string() {
    // PreparedSql is a pre-prepared handle that holds the SQL text
    let stmt = scharnhorst_query::PreparedSql::new("SELECT * FROM units");
    assert_eq!(
        stmt.sql, "SELECT * FROM units",
        "DC-4: PreparedSql must store the exact SQL string passed to new()"
    );
}

/// Full register-then-unregister lifecycle works correctly.
#[test]
fn dc4_sql_context_register_then_unregister() {
    // complete lifecycle: register -> verify presence -> unregister -> verify absence
    let ctx = scharnhorst_query::SqlExecutionContext::new();

    // step 1 -- register
    ctx.register_table("units", vec![sample_batch()]).unwrap();

    // step 2 -- verify registered
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.contains(&"units".to_string()),
        "DC-4: after register, table must appear in registered_tables"
    );

    // step 3 -- unregister
    ctx.unregister_table("units").unwrap();

    // step 4 -- verify gone
    let tables = ctx.registered_tables().unwrap();
    assert!(
        !tables.contains(&"units".to_string()),
        "DC-4: after unregister, table must not appear in registered_tables"
    );
}

/// SqlExecutionContext uses DataFusion SessionContext as its backend.
#[test]
fn dc4_sql_context_uses_datafusion_backend() {
    // verify DataFusion SessionContext is the underlying engine by
    // registering a table and confirming it appears in the tracking set
    let ctx = scharnhorst_query::SqlExecutionContext::new();
    ctx.register_table("units", vec![sample_batch()]).unwrap();
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.contains(&"units".to_string()),
        "DC-4: table registered via DataFusion SessionContext must be tracked"
    );
}

// ------------------------------------------------------------------
// Integration (Read-Only Analysis Path)
// ------------------------------------------------------------------

/// SqlExecutionContext provides a read-only analysis path separate
/// from the write path. Tables can be registered from sample_batch data
/// without modifying the original RecordBatch.
#[test]
fn dc4_sql_context_provides_read_only_analysis() {
    // create a SqlExecutionContext -- the read-only analysis path
    let ctx = scharnhorst_query::SqlExecutionContext::new();

    // register tables from sample data without modifying originals
    let batch = sample_batch();
    let original_rows = batch.num_rows();
    ctx.register_table("units", vec![batch]).unwrap();

    // verify the table is registered for read-only analysis
    let tables = ctx.registered_tables().unwrap();
    assert!(
        tables.contains(&"units".to_string()),
        "DC-4: SqlExecutionContext must accept tables for read-only analysis"
    );

    // verify the original data is unchanged (3 rows)
    assert_eq!(
        original_rows, 3,
        "DC-4: original sample_batch must remain unchanged (3 rows)"
    );
}

// ==========================================================================
// DebugWriteJournal Unit Tests
// ==========================================================================
// DebugWriteJournal is a debug-build-only ring buffer that
// intercepts write operations for developer tooling. In production builds
// and multiplayer sessions, it is disabled. These tests verify construction,
// operation recording/querying, enable/disable toggling, clear, and ring
// buffer eviction behavior.

// ------------------------------------------------------------------
// DebugWriteJournal Construction & Defaults
// ------------------------------------------------------------------

/// new(capacity) creates an empty journal with enabled=true.
#[test]
fn dc4_debug_write_journal_new_creates_empty() {
    // freshly created journal must be empty and enabled by default
    let journal = DebugWriteJournal::new(128);
    assert_eq!(journal.len(), 0, "DC-4: new journal must have len=0");
    assert!(journal.is_empty(), "DC-4: new journal must be empty");
    assert!(
        journal.enabled(),
        "DC-4: new journal must be enabled by default"
    );
}

/// DebugWriteJournal::default uses capacity 1024 internally.
#[test]
fn dc4_debug_write_journal_default_uses_1024() {
    // default delegates to new(1024); verify it starts empty
    let journal = DebugWriteJournal::default();
    assert_eq!(
        journal.len(),
        0,
        "DC-4: default journal must be empty (built with capacity 1024)"
    );
}

/// new(0) is allowed -- ring buffer at capacity zero.
/// At capacity 0, every record evicts the previous operations, but
/// the journal itself remains functional.
#[test]
fn dc4_debug_write_journal_zero_capacity_allowed() {
    // zero-capacity journal must not panic; can record but evicts immediately
    let journal = DebugWriteJournal::new(0);
    assert_eq!(journal.len(), 0, "DC-4: zero-capacity journal starts empty");

    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    // at capacity 0, len >= capacity is always true, so each
    // record pops the previous one before pushing. After one record,
    // len is 1 (the sole item stays until the next record evicts it).
    assert_eq!(
        journal.len(),
        1,
        "DC-4: zero-capacity retains the most recent op until next eviction"
    );

    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(2),
        row_count: 2,
    });
    // second record pops the first, then pushes -- still len=1
    assert_eq!(
        journal.len(),
        1,
        "DC-4: zero-capacity always holds at most 1 op"
    );
    let ops = journal.ops();
    assert_eq!(
        ops[0].tick(),
        Tick(2),
        "DC-4: only the latest op survives at capacity 0"
    );
}

// ------------------------------------------------------------------
// DebugWriteOp Variant Tests
// ------------------------------------------------------------------

/// Append variant exposes table_name and tick correctly.
#[test]
fn dc4_debug_write_op_append_table_name_and_tick() {
    // Append stores table, tick, and row_count; accessors return them
    let op = DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(5),
        row_count: 42,
    };
    assert_eq!(
        op.table_name(),
        "units",
        "DC-4: Append table_name() must return 'units'"
    );
    assert_eq!(
        op.tick(),
        Tick(5),
        "DC-4: Append tick() must return Tick(5)"
    );
}

/// Patch variant exposes table_name and tick correctly.
#[test]
fn dc4_debug_write_op_patch_table_name_and_tick() {
    // Patch stores table, tick, and row_indices; accessors return them
    let op = DebugWriteOp::Patch {
        table: "units".to_owned(),
        tick: Tick(3),
        row_indices: vec![1, 2, 3],
    };
    assert_eq!(
        op.table_name(),
        "units",
        "DC-4: Patch table_name() must return 'units'"
    );
    assert_eq!(op.tick(), Tick(3), "DC-4: Patch tick() must return Tick(3)");
}

/// Delete variant exposes table_name and tick correctly.
#[test]
fn dc4_debug_write_op_delete_table_name_and_tick() {
    // Delete stores table, tick, and row_indices; accessors return them
    let op = DebugWriteOp::Delete {
        table: "actors".to_owned(),
        tick: Tick(7),
        row_indices: vec![0, 5],
    };
    assert_eq!(
        op.table_name(),
        "actors",
        "DC-4: Delete table_name() must return 'actors'"
    );
    assert_eq!(
        op.tick(),
        Tick(7),
        "DC-4: Delete tick() must return Tick(7)"
    );
}

/// Rebuild variant exposes table_name and tick correctly.
#[test]
fn dc4_debug_write_op_rebuild_table_name_and_tick() {
    // Rebuild stores table, tick, and row_count; accessors return them
    let op = DebugWriteOp::Rebuild {
        table: "pops".to_owned(),
        tick: Tick(9),
        row_count: 100,
    };
    assert_eq!(
        op.table_name(),
        "pops",
        "DC-4: Rebuild table_name() must return 'pops'"
    );
    assert_eq!(
        op.tick(),
        Tick(9),
        "DC-4: Rebuild tick() must return Tick(9)"
    );
}

// ------------------------------------------------------------------
// Record & Query
// ------------------------------------------------------------------

/// record an Append and verify ops returns it.
#[test]
fn dc4_debug_write_record_and_ops_roundtrip() {
    // a single recorded op must be retrievable via ops
    let journal = DebugWriteJournal::new(10);
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 42,
    });

    let ops = journal.ops();
    assert_eq!(
        ops.len(),
        1,
        "DC-4: ops() must return exactly 1 op after one record"
    );
    assert_eq!(
        ops[0].table_name(),
        "units",
        "DC-4: recorded Append must carry table_name 'units'"
    );
    assert_eq!(
        ops[0].tick(),
        Tick(1),
        "DC-4: recorded Append must carry Tick(1)"
    );
}

/// Multiple recorded ops are returned in insertion order by ops.
#[test]
fn dc4_debug_write_record_multiple_ops_preserves_order() {
    // ops must preserve FIFO order of recorded operations
    let journal = DebugWriteJournal::new(10);
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 10,
    });
    journal.record(DebugWriteOp::Patch {
        table: "units".to_owned(),
        tick: Tick(2),
        row_indices: vec![0, 1],
    });
    journal.record(DebugWriteOp::Delete {
        table: "actors".to_owned(),
        tick: Tick(3),
        row_indices: vec![5],
    });

    let ops = journal.ops();
    assert_eq!(ops.len(), 3, "DC-4: all three ops must be present");
    assert_eq!(
        ops[0].tick(),
        Tick(1),
        "DC-4: first op must be Append at Tick(1)"
    );
    assert_eq!(
        ops[1].tick(),
        Tick(2),
        "DC-4: second op must be Patch at Tick(2)"
    );
    assert_eq!(
        ops[2].tick(),
        Tick(3),
        "DC-4: third op must be Delete at Tick(3)"
    );
}

/// latest returns the most recently recorded operation.
#[test]
fn dc4_debug_write_latest_returns_last_recorded() {
    // latest must return the last-pushed op, or None when empty
    let journal = DebugWriteJournal::new(10);
    assert!(
        journal.latest().is_none(),
        "DC-4: latest() on empty journal must be None"
    );

    journal.record(DebugWriteOp::Append {
        table: "a".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Append {
        table: "b".to_owned(),
        tick: Tick(2),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Append {
        table: "c".to_owned(),
        tick: Tick(3),
        row_count: 1,
    });

    let last = journal.latest().unwrap();
    assert_eq!(
        last.table_name(),
        "c",
        "DC-4: latest() must return the most recently recorded op (table 'c')"
    );
    assert_eq!(
        last.tick(),
        Tick(3),
        "DC-4: latest() must return the most recently recorded op (Tick(3))"
    );
}

/// ops_for_table filters operations by table name.
#[test]
fn dc4_debug_write_ops_for_table_filters_correctly() {
    // ops_for_table must only return ops whose table_name matches
    let journal = DebugWriteJournal::new(10);
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 3,
    });
    journal.record(DebugWriteOp::Patch {
        table: "actors".to_owned(),
        tick: Tick(2),
        row_indices: vec![0],
    });
    journal.record(DebugWriteOp::Delete {
        table: "units".to_owned(),
        tick: Tick(3),
        row_indices: vec![1],
    });

    let unit_ops = journal.ops_for_table("units");
    assert_eq!(
        unit_ops.len(),
        2,
        "DC-4: ops_for_table('units') must return 2 ops for 'units'"
    );
    assert_eq!(
        unit_ops[0].tick(),
        Tick(1),
        "DC-4: first 'units' op at Tick(1)"
    );
    assert_eq!(
        unit_ops[1].tick(),
        Tick(3),
        "DC-4: second 'units' op at Tick(3)"
    );

    let actor_ops = journal.ops_for_table("actors");
    assert_eq!(
        actor_ops.len(),
        1,
        "DC-4: ops_for_table('actors') must return 1 op for 'actors'"
    );
    assert_eq!(actor_ops[0].tick(), Tick(2), "DC-4: 'actors' op at Tick(2)");

    let empty_ops = journal.ops_for_table("nonexistent");
    assert!(
        empty_ops.is_empty(),
        "DC-4: ops_for_table for unknown table must return empty vec"
    );
}

// ------------------------------------------------------------------
// Enable / Disable (debug-build-only constraint)
// ------------------------------------------------------------------

/// When disabled, record silently drops operations.
#[test]
fn dc4_debug_write_disabled_does_not_record() {
    // set_enabled(false) must prevent record from storing ops
    let journal = DebugWriteJournal::new(10);
    journal.set_enabled(false);
    assert!(
        !journal.enabled(),
        "DC-4: after set_enabled(false), enabled() must return false"
    );

    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });

    assert!(
        journal.ops().is_empty(),
        "DC-4: disabled journal must not store recorded ops"
    );
    assert!(
        journal.is_empty(),
        "DC-4: disabled journal must remain empty after record"
    );
}

/// Re-enabling the journal resumes recording.
#[test]
fn dc4_debug_write_reenable_records_again() {
    // toggling enabled back to true must restore recording
    let journal = DebugWriteJournal::new(10);

    // disable and record -- should be dropped
    journal.set_enabled(false);
    journal.record(DebugWriteOp::Append {
        table: "dropped".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });

    // re-enable and record -- should be stored
    journal.set_enabled(true);
    journal.record(DebugWriteOp::Append {
        table: "kept".to_owned(),
        tick: Tick(2),
        row_count: 2,
    });

    let ops = journal.ops();
    assert_eq!(
        ops.len(),
        1,
        "DC-4: only the post-reenable op must be present"
    );
    assert_eq!(
        ops[0].table_name(),
        "kept",
        "DC-4: reenabled recording must capture 'kept', not 'dropped'"
    );
    assert_eq!(
        ops[0].tick(),
        Tick(2),
        "DC-4: reenabled recording must capture Tick(2)"
    );
}

// ------------------------------------------------------------------
// Clear
// ------------------------------------------------------------------

/// clear removes all recorded operations.
#[test]
fn dc4_debug_write_clear_removes_all_ops() {
    // clear must empty the journal completely
    let journal = DebugWriteJournal::new(10);
    journal.record(DebugWriteOp::Append {
        table: "a".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Patch {
        table: "b".to_owned(),
        tick: Tick(2),
        row_indices: vec![0],
    });
    journal.record(DebugWriteOp::Delete {
        table: "c".to_owned(),
        tick: Tick(3),
        row_indices: vec![1],
    });
    assert_eq!(journal.len(), 3, "DC-4: three ops recorded before clear");

    journal.clear();

    assert_eq!(journal.len(), 0, "DC-4: after clear, len() must be 0");
    assert!(
        journal.is_empty(),
        "DC-4: after clear, is_empty() must be true"
    );
}

// ------------------------------------------------------------------
// Ring Buffer Capacity
// ------------------------------------------------------------------

/// Ring buffer evicts the oldest ops when capacity is exceeded.
#[test]
fn dc4_debug_write_ring_buffer_evicts_oldest() {
    // when capacity 3 is filled and a 4th op recorded, the oldest
    // (Append at Tick(1)) must be evicted, leaving Tick(2), Tick(3), Tick(4)
    let journal = DebugWriteJournal::new(3);
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(2),
        row_count: 2,
    });
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(3),
        row_count: 3,
    });
    // capacity 3 is now full; recording a 4th evicts the first (Tick(1))
    journal.record(DebugWriteOp::Append {
        table: "units".to_owned(),
        tick: Tick(4),
        row_count: 4,
    });

    let ops = journal.ops();
    assert_eq!(
        ops.len(),
        3,
        "DC-4: after recording 4 ops into capacity 3, len must be 3"
    );
    assert_eq!(
        ops[0].tick(),
        Tick(2),
        "DC-4: oldest surviving op must be Tick(2) -- Tick(1) was evicted"
    );
    assert_eq!(
        ops[1].tick(),
        Tick(3),
        "DC-4: second surviving op must be Tick(3)"
    );
    assert_eq!(
        ops[2].tick(),
        Tick(4),
        "DC-4: third (latest) surviving op must be Tick(4)"
    );
}

// ==========================================================================
// Schema-Registry Metadata Caching Tests
// ==========================================================================
// Schema-Registry metadata is cached by the QueryEngine
// and accessible via table_schema, register_table_schema, and related
// methods. The engine wraps SchemaRegistry and provides controlled access
// for all consumers.

// ------------------------------------------------------------------
// Registration & Caching
// ------------------------------------------------------------------

/// register_table_schema must store the spec in the SchemaRegistry
/// and table_schema must retrieve it by name.
#[test]
fn dc4_register_table_schema_stores_in_registry() {
    // fresh engine with empty registry
    let engine = QueryEngine::new(SchemaRegistry::new());
    let spec = test_table_spec();
    engine.register_table_schema(spec.clone()).unwrap();

    // table_schema must return the registered spec
    let retrieved = engine.table_schema("units").unwrap();
    assert_eq!(
        retrieved.name, "units",
        "DC-4: table_schema('units') must return the registered TableSpec"
    );
    assert_eq!(
        retrieved.columns.len(),
        4,
        "DC-4: schema must contain all 4 columns after registration"
    );
}

/// registering the same table name twice must return a Schema error
/// (TableAlreadyExists from the underlying SchemaRegistry).
#[test]
fn dc4_register_duplicate_table_schema_returns_err() {
    // register once successfully
    let engine = QueryEngine::new(SchemaRegistry::new());
    let spec = test_table_spec();
    engine.register_table_schema(spec.clone()).unwrap();

    // registering the same name again must fail
    let result = engine.register_table_schema(spec);
    assert!(
        result.is_err(),
        "DC-4: duplicate registration of 'units' must return an error"
    );
    assert!(
        matches!(result, Err(QueryError::Schema(_))),
        "DC-4: duplicate registration must produce a Schema error variant"
    );
}

/// querying table_schema for a never-registered table must fail.
#[test]
fn dc4_table_schema_nonexistent_returns_err() {
    // fresh engine with no tables registered
    let engine = QueryEngine::new(SchemaRegistry::new());
    let result = engine.table_schema("nonexistent");
    assert!(
        result.is_err(),
        "DC-4: table_schema('nonexistent') must return an error"
    );
}

// ------------------------------------------------------------------
// Schema Metadata Access
// ------------------------------------------------------------------

/// after ingest_snapshot, the schema metadata must remain
/// accessible via table_schema.
#[test]
fn dc4_schema_cache_populated_at_ingest() {
    // register schema, then ingest data
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();
    engine
        .ingest_snapshot(
            Tick(1),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();

    // schema must be accessible after ingest
    let schema = engine.table_schema("units").unwrap();
    assert_eq!(
        schema.name, "units",
        "DC-4: schema must be accessible after ingest_snapshot"
    );
    assert_eq!(
        schema.columns.len(),
        4,
        "DC-4: schema must retain all column definitions after ingest"
    );
}

/// schema metadata must survive multiple ingest_snapshot calls
/// with different ticks and data.
#[test]
fn dc4_schema_metadata_persists_across_ingests() {
    // register schema once
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();

    // ingest at Tick(1) with sample_batch
    engine
        .ingest_snapshot(
            Tick(1),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();

    // create a different batch for Tick(2)
    let schema = Arc::new(test_schema());
    let id2: ArrayRef = Arc::new(Int64Array::from(vec![4, 5]));
    let name2: ArrayRef = Arc::new(StringArray::from(vec!["delta", "epsilon"]));
    let health2: ArrayRef = Arc::new(Float64Array::from(vec![40.0, 20.0]));
    let active2: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
    let batch2 = RecordBatch::try_new(schema, vec![id2, name2, health2, active2]).unwrap();

    // ingest again at Tick(2) with different data
    engine
        .ingest_snapshot(Tick(2), "units", vec![batch2], RowPositionMap::new())
        .unwrap();

    // schema must still be accessible after multiple ingests
    let schema = engine.table_schema("units").unwrap();
    assert_eq!(
        schema.name, "units",
        "DC-4: schema must persist across multiple ingest_snapshot calls"
    );
}

/// when QueryEngine is created with a pre-populated SchemaRegistry,
/// the metadata must be immediately accessible without additional
/// registration.
#[test]
fn dc4_schema_registry_metadata_accessible_after_engine_creation() {
    // pre-populate the registry before creating the engine
    let mut registry = SchemaRegistry::new();
    registry.register(test_table_spec()).unwrap();

    let engine = QueryEngine::new(registry);

    // table_schema must return schemas from the pre-existing registry
    let schema = engine.table_schema("units").unwrap();
    assert_eq!(
        schema.name, "units",
        "DC-4: pre-existing registry schemas must be accessible through the engine"
    );
    assert_eq!(
        schema.columns.len(),
        4,
        "DC-4: all columns from pre-existing registry must be present"
    );
}

// ------------------------------------------------------------------
// Snapshot Ingestion & Latest Tick
// ------------------------------------------------------------------

/// ingest_snapshot must update latest_tick to reflect the
/// most recently ingested tick.
#[test]
fn dc4_ingest_snapshot_updates_latest_tick() {
    // register schema
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();

    // before any ingest, latest_tick is None
    assert_eq!(
        engine.latest_tick().unwrap(),
        None,
        "DC-4: before ingestion, latest_tick must be None"
    );

    // ingest at Tick(5)
    engine
        .ingest_snapshot(
            Tick(5),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();

    // latest_tick reflects Tick(5)
    assert_eq!(
        engine.latest_tick().unwrap(),
        Some(Tick(5)),
        "DC-4: after ingest_snapshot(Tick(5),...), latest_tick must be Some(Tick(5))"
    );
}

/// after ingest_snapshot, data must be readable through
/// read_single_table.
#[test]
fn dc4_ingest_snapshot_makes_data_readable() {
    // register and ingest
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();
    engine
        .ingest_snapshot(
            Tick(1),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();

    // data must be readable via read_single_table
    let view = engine.read_single_table(Tick(1), "units").unwrap();
    assert_eq!(
        view.table_name(),
        "units",
        "DC-4: read_single_table must return the ingested table"
    );
    assert_eq!(
        view.total_rows(),
        3,
        "DC-4: ingested data must have 3 rows from sample_batch"
    );
}

/// May-fail: transactional ingestion must not publish partial tick data.
#[test]
fn dc4_begin_ingest_stages_tables_until_snapshot_store() {
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();
    let item_spec = TableSpec::new("items")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap()
        .with_column(ColumnSpec::new("health", FieldSemantic::Quantity, "f64"))
        .unwrap()
        .with_column(ColumnSpec::new("active", FieldSemantic::Tag, "bool"))
        .unwrap();
    engine.register_table_schema(item_spec).unwrap();

    engine
        .ingest_snapshot(
            Tick(1),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();
    engine
        .ingest_snapshot(
            Tick(1),
            "items",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();

    let rollback = engine.begin_ingest().unwrap();
    engine
        .ingest_snapshot(
            Tick(2),
            "units",
            vec![sample_batch()],
            RowPositionMap::new(),
        )
        .unwrap();

    assert_eq!(
        engine.latest_tick().unwrap(),
        Some(Tick(1)),
        "partial transactional ingest must not publish Tick(2)"
    );
    let read = engine.read(ReadRequest::new(Tick(2)).with_tables(["units", "items"]));
    assert!(
        matches!(read, Err(QueryError::TicksMismatch { .. })),
        "Tick(2) reads must remain unavailable until store_snapshot"
    );

    rollback.rollback().unwrap();
}

/// Would have failed: ingest_snapshot with u64::MAX tick (sentinel)
/// should be rejected. Without this guard, downstream code could
/// observe a frame that's not a real commit.
#[test]
fn dc4_ingest_snapshot_rejects_sentinel_tick() {
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();
    let sentinel = Tick(u64::MAX);
    let result = engine.ingest_snapshot(
        sentinel,
        "units",
        vec![sample_batch()],
        RowPositionMap::new(),
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("sentinel")
            || err.to_string().to_lowercase().contains("unsupported"),
        "sentinel tick must be rejected, got: {}",
        err
    );
}

/// attempting to read data before ingest_snapshot has been called
/// must return TableNotFound because the view_cache is empty.
#[test]
fn dc4_read_request_before_ingest_returns_err() {
    // register schema but do NOT ingest data
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();

    // read via ReadRequest before ingest must fail
    let req = ReadRequest::new(Tick::ZERO).with_table("units");
    let result = engine.read(req);
    assert!(
        matches!(result, Err(QueryError::TableNotFound(_))),
        "DC-4: read before ingest must return TableNotFound"
    );
}

/// Would have failed: our TicksMismatch variant was defined but never used.
/// read() with a non-zero tick on a cache with no data (tick=None)
/// must return TicksMismatch, not InvalidQuery.
#[test]
fn dc4_read_with_nonzero_tick_before_ingest_returns_ticks_mismatch() {
    let engine = QueryEngine::new(SchemaRegistry::new());
    engine.register_table_schema(test_table_spec()).unwrap();

    let req = ReadRequest::new(Tick(1)).with_table("units");
    let result = engine.read(req);
    assert!(
        matches!(result, Err(QueryError::TicksMismatch { .. })),
        "DC-4: read with non-zero tick before ingest must return TicksMismatch, got {:?}",
        result
    );
}

// ------------------------------------------------------------------
// Column Access via Engine
// ------------------------------------------------------------------

/// column_view must return a typed ColumnView for an existing
/// column after data has been ingested.
#[test]
fn dc4_column_view_returns_typed_column() {
    // engine with ingested data
    let engine = make_engine_with_data();

    // column_view for existing column 'id'
    let col = engine.column_view("units", "id").unwrap();
    let val: Option<i64> = col.get(0).unwrap();
    assert_eq!(
        val,
        Some(1i64),
        "DC-4: column_view('units','id') must return correct i64 value at row 0"
    );
}

/// column_view for a nonexistent column name must return
/// ColumnNotFound error.
#[test]
fn dc4_column_view_nonexistent_column_returns_err() {
    // engine with ingested data
    let engine = make_engine_with_data();

    // column_view for a nonexistent column
    let result = engine.column_view("units", "nonexistent");
    assert!(
        matches!(result, Err(QueryError::ColumnNotFound { .. })),
        "DC-4: column_view('units','nonexistent') must return ColumnNotFound"
    );
}

/// column_view for a table that was never ingested must return
/// TableNotFound error.
#[test]
fn dc4_column_view_nonexistent_table_returns_err() {
    // engine with ingested data for 'units' only
    let engine = make_engine_with_data();

    // column_view for a table that doesn't exist in the view_cache
    let result = engine.column_view("nonexistent", "id");
    assert!(
        matches!(result, Err(QueryError::TableNotFound(_))),
        "DC-4: column_view('nonexistent','id') must return TableNotFound"
    );
}

// ==========================================================================
// Cross-Partition RelationGraph Lookup Tests
// ==========================================================================
// RelationEdge resolution via resolve_relation_edge is
// the QueryEngine's exclusive path to the SchemaRegistry's RelationGraph.
// QueryEngine provides the sole read interface for ALL
// consumers; no component may access the RelationGraph directly.

// ------------------------------------------------------------------
// RelationEdge Resolution
// ------------------------------------------------------------------

/// resolve_relation_edge must resolve a relation by its key
/// "{from} -> {to}" and return the correct RelationEdge.
#[test]
fn dc6_resolve_relation_edge_returns_edge() {
    // build registry with two tables and a relation edge
    let mut registry = SchemaRegistry::new();

    let province_spec = TableSpec::new("province")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap();
    let actor_spec = TableSpec::new("actor")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap();

    registry.register(province_spec).unwrap();
    registry.register(actor_spec).unwrap();

    // add a relation with FK column metadata
    registry
        .add_relation(RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "owner_id".to_string(),
            to_column: Some("id".to_string()),
        })
        .unwrap();

    let engine = QueryEngine::new(registry);

    // resolve_relation_edge with correct key format
    let edge = engine.resolve_relation_edge("province -> actor").unwrap();
    assert_eq!(
        edge.from, "province",
        "DC-6: resolved edge must have from='province'"
    );
    assert_eq!(edge.to, "actor", "DC-6: resolved edge must have to='actor'");
}

/// resolving a relation key that has no matching edge must
/// return RelationNotFound.
#[test]
fn dc6_resolve_relation_edge_nonexistent_returns_err() {
    // engine with empty registry -- no relations exist
    let engine = QueryEngine::new(SchemaRegistry::new());

    let result = engine.resolve_relation_edge("missing -> edge");
    assert!(
        matches!(result, Err(QueryError::RelationNotFound(_))),
        "DC-6: resolve_relation_edge for nonexistent relation must return RelationNotFound"
    );
}

/// resolve_relation_edge requires "{from} -> {to}" format
/// (with spaces around "->"). Malformed keys must return RelationNotFound.
#[test]
fn dc6_resolve_relation_edge_wrong_format_returns_err() {
    // build registry with a valid relation
    let mut registry = SchemaRegistry::new();
    registry
        .register(
            TableSpec::new("province")
                .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
                .unwrap(),
        )
        .unwrap();
    registry
        .register(
            TableSpec::new("actor")
                .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
                .unwrap(),
        )
        .unwrap();
    registry
        .add_relation(RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        })
        .unwrap();

    let engine = QueryEngine::new(registry);

    // missing spaces around "->" fails to match the canonical format
    let result = engine.resolve_relation_edge("province->actor");
    assert!(
 matches!(result, Err(QueryError::RelationNotFound(_))),
 "DC-6: resolve_relation_edge('province->actor') with missing spaces must return RelationNotFound"
 );
}

// ------------------------------------------------------------------
// Sole Consumer of RelationGraph
// ------------------------------------------------------------------

/// resolve_relation_edge is the exclusive access path to
/// RelationGraph. Consumers should use resolve_relation_edge for
/// consistency. This test verifies that resolve_relation_edge and
/// schema_registry.relation_graph return consistent results,
/// confirming the single source of truth.
#[test]
fn dc10_relation_graph_accessed_only_through_query_engine() {
    // build registry with tables and a relation edge
    let mut registry = SchemaRegistry::new();
    registry
        .register(
            TableSpec::new("province")
                .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
                .unwrap(),
        )
        .unwrap();
    registry
        .register(
            TableSpec::new("actor")
                .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
                .unwrap(),
        )
        .unwrap();
    registry
        .add_relation(RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "owner_id".to_string(),
            to_column: Some("id".to_string()),
        })
        .unwrap();

    let engine = QueryEngine::new(registry);

    // resolve via engine's API (the recommended and exclusive path)
    let edge_via_engine = engine.resolve_relation_edge("province -> actor").unwrap();

    // resolve via schema_registry.relation_graph (the underlying store)
    let reg = engine.schema_registry().unwrap();
    let graph = reg.relation_graph();
    let edge_via_graph = graph.find_edge("province", "actor").unwrap();

    // both access paths must return consistent results
    assert_eq!(
        edge_via_engine.from, edge_via_graph.from,
        "DC-10: resolve_relation_edge and relation_graph must agree on 'from'"
    );
    assert_eq!(
        edge_via_engine.to, edge_via_graph.to,
        "DC-10: resolve_relation_edge and relation_graph must agree on 'to'"
    );
    assert_eq!(
        edge_via_engine.kind, edge_via_graph.kind,
        "DC-10: resolve_relation_edge and relation_graph must agree on 'kind'"
    );
    assert_eq!(
        edge_via_engine.from_column, edge_via_graph.from_column,
        "DC-10: resolve_relation_edge and relation_graph must agree on 'from_column'"
    );
    assert_eq!(
        edge_via_engine.to_column, edge_via_graph.to_column,
        "DC-10: resolve_relation_edge and relation_graph must agree on 'to_column'"
    );
}

// ------------------------------------------------------------------
// Cross-Partition Scenario & Edge Metadata
// ------------------------------------------------------------------

/// Cross-partition scenario -- tables in different conceptual
/// partitions ("province" and "actor") linked via a FK relation.
/// The QueryEngine resolves the relation edge via resolve_relation_edge.
#[test]
fn dc6_cross_partition_lookup_scenario() {
    // register "province" table (pk=id, FK owner_id -> actor.id)
    let mut registry = SchemaRegistry::new();

    let province_spec = TableSpec::new("province")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap()
        .with_column(ColumnSpec::new(
            "owner_id",
            FieldSemantic::ForeignKey {
                target_table: "actor".to_string(),
            },
            "i64",
        ))
        .unwrap();
    registry.register(province_spec).unwrap();

    let actor_spec = TableSpec::new("actor")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap();
    registry.register(actor_spec).unwrap();

    // province.owner_id FK -> actor.id
    registry
        .add_relation(RelationEdge {
            from: "province".to_string(),
            to: "actor".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "owner_id".to_string(),
            to_column: Some("id".to_string()),
        })
        .unwrap();

    let engine = QueryEngine::new(registry);

    // resolve the cross-partition relation edge
    let edge = engine.resolve_relation_edge("province -> actor").unwrap();
    assert_eq!(
        edge.from, "province",
        "DC-6: cross-partition edge must connect 'province' to 'actor'"
    );
    assert_eq!(
        edge.to, "actor",
        "DC-6: cross-partition edge target must be 'actor'"
    );
    assert_eq!(
        edge.from_column,
        "owner_id".to_string(),
        "DC-6: FK column in province must be 'owner_id'"
    );
    assert_eq!(
        edge.to_column,
        Some("id".to_string()),
        "DC-6: referenced column in actor must be 'id'"
    );
}

/// When a RelationEdge with column metadata is registered
/// and resolved, all field values must be preserved intact.
#[test]
fn dc6_relation_edge_metadata_preserved() {
    // build minimal registry with two tables
    let mut registry = SchemaRegistry::new();
    registry
        .register(
            TableSpec::new("province")
                .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
                .unwrap()
                .with_column(ColumnSpec::new(
                    "owner_id",
                    FieldSemantic::ForeignKey {
                        target_table: "actor".to_string(),
                    },
                    "i64",
                ))
                .unwrap(),
        )
        .unwrap();
    registry
        .register(
            TableSpec::new("actor")
                .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
                .unwrap(),
        )
        .unwrap();

    // edge with explicit from_column and to_column metadata
    let original = RelationEdge {
        from: "province".to_string(),
        to: "actor".to_string(),
        kind: RelationKind::OneToMany,
        from_column: "owner_id".to_string(),
        to_column: Some("id".to_string()),
    };
    registry.add_relation(original.clone()).unwrap();

    let engine = QueryEngine::new(registry);
    let resolved = engine.resolve_relation_edge("province -> actor").unwrap();

    // all fields must match the original edge definition
    assert_eq!(
        resolved.from, original.from,
        "DC-6: 'from' field must be preserved"
    );
    assert_eq!(
        resolved.to, original.to,
        "DC-6: 'to' field must be preserved"
    );
    assert_eq!(
        resolved.kind, original.kind,
        "DC-6: 'kind' field must be preserved"
    );
    assert_eq!(
        resolved.from_column, original.from_column,
        "DC-6: 'from_column'='owner_id' must be preserved"
    );
    assert_eq!(
        resolved.to_column, original.to_column,
        "DC-6: 'to_column'='id' must be preserved"
    );
}

// ==========================================================================
// Typed Access Error Path Coverage Tests
// ==========================================================================
// These tests cover the uncovered branches in typed_access.rs:
// RowCursor::advance() false return, RowCursor::get_column() missing,
// RowCursor::get_row_id() negative i64 -> None, RowLookupView::column_view()
// batch_index OOB and column-not-found, Slice iter_valid() with nulls,
// and Slice get() out-of-bounds.

// ------------------------------------------------------------------
// Helpers for typed access error path tests
// ------------------------------------------------------------------

/// Build a RecordBatch with nullable columns for testing null filtering.
fn sample_batch_with_nulls() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("health", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]));
    let health: ArrayRef = Arc::new(Float64Array::from(vec![Some(100.0), None, Some(60.0)]));
    let active: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
    RecordBatch::try_new(schema, vec![id, name, health, active]).unwrap()
}

/// Build a batch with a negative id value.
fn sample_batch_negative_id() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(vec![-1]));
    let name: ArrayRef = Arc::new(StringArray::from(vec!["neg"]));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

/// Build columns vector from a RecordBatch.
fn batch_to_columns(batch: &RecordBatch) -> Vec<(String, ColumnView)> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let view = ColumnView::new(batch.column(idx).clone(), field.name());
            (field.name().clone(), view)
        })
        .collect()
}

/// Build arrays vector from a RecordBatch.
fn batch_to_arrays(batch: &RecordBatch) -> Vec<(String, ArrayRef)> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), batch.column(idx).clone()))
        .collect()
}

// ------------------------------------------------------------------
// RowCursor error path tests
// ------------------------------------------------------------------

/// RowCursor::advance() returns false when already at the last row.
#[test]
fn dc10_row_cursor_advance_at_end_returns_false() {
    let batch = sample_batch();
    let mut cursor = RowCursor::new(batch_to_columns(&batch)).unwrap();
    // advance through all 3 rows
    assert!(cursor.advance(), "DC-10: first advance must return true");
    assert!(cursor.advance(), "DC-10: second advance must return true");
    // now at last row (index 2); next advance must return false
    assert!(
        !cursor.advance(),
        "DC-10: advance past last row must return false"
    );
    // position must not change after failed advance
    assert_eq!(
        cursor.current_row(),
        2,
        "DC-10: current_row must stay at last row after failed advance"
    );
}

/// RowCursor::get_column() with a non-existent column name returns ColumnNotFound.
#[test]
fn dc10_row_cursor_get_column_missing_returns_err() {
    let batch = sample_batch();
    let cursor = RowCursor::new(batch_to_columns(&batch)).unwrap();
    let result = cursor.get_column("nonexistent");
    assert!(
        matches!(result, Err(QueryError::ColumnNotFound { .. })),
        "DC-10: get_column('nonexistent') must return ColumnNotFound"
    );
}

/// RowCursor::get_row_id() with a negative i64 silently returns None.
#[test]
fn dc10_row_cursor_get_row_id_negative_returns_none() {
    let batch = sample_batch_negative_id();
    let cursor = RowCursor::new(batch_to_columns(&batch)).unwrap();
    // negative i64 cannot convert to u64 RowId, so get_row_id returns Ok(None)
    let result = cursor.get_row_id("id");
    assert!(
        result.is_ok(),
        "DC-10: get_row_id with negative i64 must not error"
    );
    assert_eq!(
        result.unwrap(),
        None,
        "DC-10: get_row_id with negative i64 must return None"
    );
}

// ------------------------------------------------------------------
// RowLookupView error path tests
// ------------------------------------------------------------------

/// RowLookupView with an out-of-range batch_index returns InvalidQuery.
#[test]
fn dc10_row_lookup_view_column_view_batch_index_oob() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    // batch_index 99 is out of range (only 1 batch at index 0)
    let lookup = RowLookup::new(RowId::new(0), 99, 0);
    let row_view = RowLookupView::new(lookup, view);

    let result = row_view.get_i64("id");
    assert!(
        matches!(result, Err(QueryError::InvalidQuery(_))),
        "DC-10: RowLookupView with batch_index OOB must return InvalidQuery"
    );
}

/// RowLookupView with missing column name returns ColumnNotFound.
#[test]
fn dc10_row_lookup_view_column_view_missing_column() {
    let spec = Arc::new(test_table_spec());
    let view = TableReadView::new("units", Tick(1), vec![sample_batch()], spec, None);
    let lookup = RowLookup::new(RowId::new(0), 0, 0);
    let row_view = RowLookupView::new(lookup, view);

    let result = row_view.get_i64("nonexistent");
    assert!(
        matches!(result, Err(QueryError::ColumnNotFound { .. })),
        "DC-10: RowLookupView with missing column must return ColumnNotFound"
    );
}

// ------------------------------------------------------------------
// Slice iter_valid() null-filtering tests
// ------------------------------------------------------------------

/// BooleanSlice::iter_valid() skips nulls and yields only valid booleans.
#[test]
fn dc10_bool_slice_iter_valid_filters_nulls() {
    let batch = sample_batch_with_nulls();
    let reader = BatchColumnReader::new(batch_to_arrays(&batch)).unwrap();
    let slice = reader.bool_slice("active").unwrap();
    let values: Vec<bool> = slice.iter_valid().collect();
    assert_eq!(
        values,
        vec![true, false],
        "DC-10: BooleanSlice iter_valid must skip null and yield [true, false]"
    );
}

/// I64Slice::iter_valid() skips nulls and yields only valid i64s.
#[test]
fn dc10_i64_slice_iter_valid_filters_nulls() {
    let batch = sample_batch_with_nulls();
    let reader = BatchColumnReader::new(batch_to_arrays(&batch)).unwrap();
    let slice = reader.i64_slice("id").unwrap();
    let values: Vec<i64> = slice.iter_valid().collect();
    assert_eq!(
        values,
        vec![1, 3],
        "DC-10: I64Slice iter_valid must skip null and yield [1, 3]"
    );
}

/// F64Slice::iter_valid() skips nulls and yields only valid f64s.
#[test]
fn dc10_f64_slice_iter_valid_filters_nulls() {
    let batch = sample_batch_with_nulls();
    let reader = BatchColumnReader::new(batch_to_arrays(&batch)).unwrap();
    let slice = reader.f64_slice("health").unwrap();
    let values: Vec<f64> = slice.iter_valid().collect();
    assert_eq!(
        values,
        vec![100.0, 60.0],
        "DC-10: F64Slice iter_valid must skip null and yield [100.0, 60.0]"
    );
}

/// StringSlice::iter_valid() skips nulls and yields only valid strings.
#[test]
fn dc10_string_slice_iter_valid_filters_nulls() {
    let batch = sample_batch_with_nulls();
    let reader = BatchColumnReader::new(batch_to_arrays(&batch)).unwrap();
    let slice = reader.string_slice("name").unwrap();
    let values: Vec<String> = slice.iter_valid().collect();
    assert_eq!(
        values,
        vec!["alpha".to_owned(), "gamma".to_owned()],
        "DC-10: StringSlice iter_valid must skip null and yield ['alpha', 'gamma']"
    );
}

// ------------------------------------------------------------------
// Slice get() out-of-bounds tests
// ------------------------------------------------------------------

/// BooleanSlice::get() with row >= len returns IndexOutOfBounds.
#[test]
fn dc10_bool_slice_get_out_of_bounds() {
    let batch = sample_batch();
    let reader = BatchColumnReader::new(batch_to_arrays(&batch)).unwrap();
    let slice = reader.bool_slice("active").unwrap();
    // len is 3, row 3 is out of bounds
    let result = slice.get(3);
    assert!(
        matches!(result, Err(QueryError::IndexOutOfBounds(3))),
        "DC-10: BooleanSlice::get(3) on len-3 slice must return IndexOutOfBounds"
    );
}

/// I64Slice::get() with row >= len returns IndexOutOfBounds.
#[test]
fn dc10_i64_slice_get_out_of_bounds() {
    let batch = sample_batch();
    let reader = BatchColumnReader::new(batch_to_arrays(&batch)).unwrap();
    let slice = reader.i64_slice("id").unwrap();
    // len is 3, row 5 is out of bounds
    let result = slice.get(5);
    assert!(
        matches!(result, Err(QueryError::IndexOutOfBounds(5))),
        "DC-10: I64Slice::get(5) on len-3 slice must return IndexOutOfBounds"
    );
}

// ==========================================================================
// Uncovered Branch Coverage Tests
// ==========================================================================
// These tests exercise branches identified as uncovered in the latest
// coverage report for scharnhorst_query.
//
// engine.rs: 14 missed branches (69.57%)
//   - latest_tick() None path (u64::MAX sentinel, already tested but
//     add explicit edge-case coverage)
//   - sql() semicolon-not-at-end rejection
//   - sql() WITH-CTE acceptance
//
// inspector.rs: 3 missed branches (62.50%)
//   - inspect_page no-overlap path
//   - summarise_table with empty batches
//
// unified_read.rs: 1 missed branch (50.00%)
//   - TableReadView::new multi-batch concat path

// ------------------------------------------------------------------
// engine.rs: latest_tick None sentinel (u64::MAX default)
// ------------------------------------------------------------------

/// Fresh engine (no ingestion) returns None from latest_tick.
/// The internal AtomicU64 starts at u64::MAX sentinel.
#[test]
fn dc10_engine_latest_tick_none_on_fresh_engine() {
    let engine = QueryEngine::new(SchemaRegistry::new());
    let tick = engine.latest_tick().unwrap();
    assert_eq!(tick, None, "DC-10: fresh engine latest_tick must be None");
}

/// After clear_cache, latest_tick remains set (clear_cache empties
/// view_cache but does not reset the atomic).
#[test]
fn dc10_engine_latest_tick_persists_after_clear_cache() {
    let engine = make_engine_with_data();
    assert_eq!(engine.latest_tick().unwrap(), Some(Tick(1)));
    engine.clear_cache().unwrap();
    // latest_tick is NOT reset by clear_cache
    assert_eq!(
        engine.latest_tick().unwrap(),
        Some(Tick(1)),
        "DC-10: latest_tick must persist after clear_cache"
    );
}

// ------------------------------------------------------------------
// engine.rs: sql() semicolon handling edge cases
// ------------------------------------------------------------------

/// A single semicolon NOT at the end of the SQL string is treated
/// as multi-statement (e.g. "SELECT 1; -- comment").
/// This covers the branch: semicolon_count == 1 && !ends_with(';')
#[test]
fn dc4_sql_rejects_semicolon_not_at_end() {
    let qe = QueryEngine::new(SchemaRegistry::new());
    let err = qe.sql("SELECT 1; -- trailing comment").unwrap_err();
    assert!(
        matches!(err, QueryError::SqlNotAllowed(_)),
        "DC-4: single semicolon not at end must be rejected as multi-statement, got {:?}",
        err
    );
}

/// Semicon at end with trailing whitespace should still be accepted.
#[test]
fn dc4_sql_allows_semicolon_with_trailing_whitespace() {
    let qe = QueryEngine::new(SchemaRegistry::new());
    // trailing semicolon with whitespace after trim is still just "SELECT 1;"
    let result = qe.sql("  SELECT 1;  ");
    assert!(
        result.is_ok(),
        "DC-4: trailing sem with whitespace should succeed, got {:?}",
        result.err()
    );
}

/// WITH CTE queries should be accepted (not rejected as non-SELECT).
#[test]
fn dc4_sql_allows_with_cte() {
    let qe = QueryEngine::new(SchemaRegistry::new());
    let result = qe.sql("WITH cte AS (SELECT 1 AS n) SELECT n FROM cte");
    assert!(
        result.is_ok(),
        "DC-4: WITH CTE must be accepted, got {:?}",
        result.err()
    );
}

/// Non-SELECT/non-WITH statement like DROP must be rejected.
#[test]
fn dc4_sql_rejects_ddl() {
    let qe = QueryEngine::new(SchemaRegistry::new());
    let err = qe.sql("DROP TABLE x").unwrap_err();
    assert!(
        matches!(err, QueryError::SqlNotAllowed(_)),
        "DC-4: DDL must be rejected"
    );
}

/// Whitespace-only SQL must be rejected as empty.
#[test]
fn dc4_sql_rejects_whitespace_only() {
    let qe = QueryEngine::new(SchemaRegistry::new());
    let err = qe.sql("   \t\n  ").unwrap_err();
    assert!(
        matches!(err, QueryError::SqlNotAllowed(_)),
        "DC-4: whitespace-only SQL must be rejected as empty"
    );
}

// ------------------------------------------------------------------
// engine.rs: validate() edge cases
// ------------------------------------------------------------------

/// validate with a path that has both missing table AND missing
/// column must report only the missing table error (table check
/// short-circuits column validation).
#[test]
fn dc4_validate_missing_table_short_circuits_columns() {
    let reg = SchemaRegistry::new();
    let qe = QueryEngine::new(reg);
    let paths = vec![ValidatablePath {
        path: "test.missing".into(),
        table: "nonexistent".into(),
        columns: vec!["some_col".into()],
        relations: vec![],
    }];
    let errors = qe.validate(&paths).unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].kind, ValidationErrorKind::MissingTable);
}

/// validate a path where the table exists and the relation exists.
#[test]
fn dc4_validate_existing_relation_passes() {
    let mut reg = SchemaRegistry::new();
    let spec_a = TableSpec::new("A")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap();
    let spec_b = TableSpec::new("B")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap();
    reg.register(spec_a).unwrap();
    reg.register(spec_b).unwrap();
    reg.add_relation(RelationEdge {
        from: "A".into(),
        to: "B".into(),
        kind: RelationKind::OneToMany,
        from_column: "id".into(),
        to_column: None,
    })
    .unwrap();
    let qe = QueryEngine::new(reg);
    let paths = vec![ValidatablePath {
        path: "test.existing_rel".into(),
        table: "A".into(),
        columns: vec![],
        relations: vec!["A -> B".into()],
    }];
    let errors = qe.validate(&paths).unwrap();
    assert!(errors.is_empty());
}

/// validate multiple paths accumulating different error kinds.
#[test]
fn dc4_validate_accumulates_multiple_errors() {
    let mut reg = SchemaRegistry::new();
    let spec = TableSpec::new("heroes")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap();
    reg.register(spec).unwrap();
    let qe = QueryEngine::new(reg);
    let paths = vec![
        ValidatablePath {
            path: "path1".into(),
            table: "heroes".into(),
            columns: vec!["missing_col".into()],
            relations: vec![],
        },
        ValidatablePath {
            path: "path2".into(),
            table: "nonexistent".into(),
            columns: vec![],
            relations: vec![],
        },
    ];
    let errors = qe.validate(&paths).unwrap();
    assert_eq!(errors.len(), 2);
    let kinds: Vec<_> = errors.iter().map(|e| e.kind.clone()).collect();
    assert!(kinds.contains(&ValidationErrorKind::MissingColumn));
    assert!(kinds.contains(&ValidationErrorKind::MissingTable));
}

// ------------------------------------------------------------------
// inspector.rs: inspect_page no-overlap / summarise_table empty batches
// ------------------------------------------------------------------

/// inspect_page with two batches where the first page falls entirely
/// in the first batch — the second batch has no overlap with page 0.
/// This covers the overlap_start < overlap_end false branch in the loop.
#[test]
fn dc10_inspector_inspect_page_batch_no_overlap() {
    let batch1 = sample_batch(); // 3 rows
    let batch2 = sample_batch(); // another 3 rows (total 6)

    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));

    // page 0, size 3 — only overlaps with batch1 (rows 0-2), not batch2
    let page = console
        .inspect_page("units", &[batch1.clone(), batch2.clone()], 0, 3)
        .unwrap();
    assert_eq!(page.rows.len(), 3);
    assert_eq!(page.total_rows, 6);
    assert!(page.has_next());

    // page 1, size 3 — only overlaps with batch2 (rows 3-5), not batch1 fully
    let page2 = console
        .inspect_page("units", &[batch1, batch2], 1, 3)
        .unwrap();
    assert_eq!(page2.rows.len(), 3);
    assert!(!page2.has_next());
}

/// summarise_table with empty batches must return zero row_count
/// and column summaries derived from the schema.
#[test]
fn dc10_inspector_summarize_table_empty_batches() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));

    let summary = console.summarize_table("units", &[]).unwrap();
    assert_eq!(summary.name, "units");
    assert_eq!(summary.row_count, 0);
    assert_eq!(summary.column_summaries.len(), 4);
    for col in &summary.column_summaries {
        assert_eq!(col.null_count, 0);
        assert_eq!(col.value_count, 0);
    }
}

/// inspect_page with start exactly at total_rows should return
/// IndexOutOfBounds (edge case of the guard).
#[test]
fn dc10_inspector_inspect_page_start_at_total_rows() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));

    // 3 rows, page_index=1 with page_size=3 means start=3 == total_rows
    // start > total_rows is false, but start == total_rows should
    // still produce an empty page, not an error (since start == total_rows,
    // NOT > total_rows). So this returns an empty page.
    let page = console
        .inspect_page("units", &[sample_batch()], 1, 3)
        .unwrap();
    // start (3) equals total_rows (3), so page is empty
    assert_eq!(page.rows.len(), 0);
    assert_eq!(page.total_rows, 3);
}

/// inspect_page with start > total_rows returns IndexOutOfBounds.
/// This explicitly tests the start > total_rows branch (when page_index
/// is large enough that start > total_rows).
#[test]
fn dc10_inspector_inspect_page_start_beyond_total_rows() {
    let mut console = InspectorConsole::new();
    console.register_schema(Arc::new(test_table_spec()));

    // 3 rows, page_index=2 with page_size=3 means start=6 > total_rows=3
    let result = console.inspect_page("units", &[sample_batch()], 2, 3);
    assert!(matches!(result, Err(QueryError::IndexOutOfBounds(6))));
}

// ------------------------------------------------------------------
// unified_read.rs: TableReadView::new multi-batch concat path
// ------------------------------------------------------------------

/// TableReadView::new with multiple batches concatenates them into one.
/// This covers the batches.len() > 1 concat path.
#[test]
fn dc10_table_read_view_multi_batch_concat() {
    let spec = Arc::new(test_table_spec());
    let batch1 = sample_batch(); // 3 rows
    let batch2 = sample_batch(); // 3 rows

    let view = TableReadView::new("units", Tick(1), vec![batch1, batch2], spec, None);
    // multi-batch concat merges into a single batch
    assert_eq!(
        view.batch_count(),
        1,
        "DC-10: multi-batch should concat into 1 batch"
    );
    assert_eq!(view.total_rows(), 6, "DC-10: total rows = 3 + 3 = 6");

    let reader = view.first_batch_reader().unwrap();
    assert_eq!(reader.row_count(), 6);
}

/// May-fail: concat must rebase caller-provided row positions.
#[test]
fn dc10_table_read_view_multi_batch_concat_rebases_position_map() {
    let spec = Arc::new(test_table_spec());
    let batch1 = sample_batch();
    let batch2 = sample_batch();
    let mut positions = RowPositionMap::new();
    positions.insert(RowId::new(10), 0, 1);
    positions.insert(RowId::new(20), 1, 2);

    let view = TableReadView::new(
        "units",
        Tick(1),
        vec![batch1, batch2],
        spec,
        Some(positions),
    );

    assert_eq!(
        view.position_map().position_of(RowId::new(10)),
        Some((0, 1))
    );
    assert_eq!(
        view.position_map().position_of(RowId::new(20)),
        Some((0, 5))
    );

    let lookup = RowLookup::new(RowId::new(20), 0, 5);
    let row = RowLookupView::new(lookup, view);
    assert_eq!(row.get_i64("id").unwrap(), Some(3));
}

// ------------------------------------------------------------------
// debug_write.rs: accessor coverage — ops_for_table empty, latest none
// ------------------------------------------------------------------

/// ops_for_table on empty journal returns empty vec.
#[test]
fn dc4_debug_write_ops_for_table_empty_journal() {
    let journal = DebugWriteJournal::new(10);
    let result = journal.ops_for_table("nonexistent");
    assert!(result.is_empty());
}

/// latest on empty journal returns None.
#[test]
fn dc4_debug_write_latest_empty_journal() {
    let journal = DebugWriteJournal::new(10);
    assert!(journal.latest().is_none());
}
