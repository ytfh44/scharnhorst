//! 10.4 Verify Bevy view updates correctly when simulation state changes.
//!
//! Tests the refresh-handler protocol and ViewModel generation bump.

use std::sync::Arc;

use scharnhorst_arrow_store::{ArrowStore, MutationMode};
use scharnhorst_bevy::{SnapshotRefreshHandler, ViewModel};
use scharnhorst_core::Tick;
use scharnhorst_integration_tests::harness::TestWorld;
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};

#[test]
fn view_model_generation_bumps_on_refresh() {
    let vm = ViewModel::new();
    let before = vm.generation().expect("read generation");

    let snapshot = scharnhorst_arrow_store::WorldSnapshot::new(Tick(1));
    vm.refresh(Arc::new(snapshot), 1).expect("refresh");

    let after = vm.generation().expect("read generation after");
    assert_eq!(after, before.saturating_add(1));
}

#[test]
fn view_model_tick_matches_refreshed_snapshot() {
    let vm = ViewModel::new();
    let snapshot = scharnhorst_arrow_store::WorldSnapshot::new(Tick(5));
    vm.refresh(Arc::new(snapshot), 2).expect("refresh");

    let tick = vm.latest_tick().expect("read tick").expect("some tick");
    assert_eq!(tick, Tick(5));
}

#[test]
fn refresh_handler_updates_view_model() {
    let schema_registry = SchemaRegistry::new();
    let query_engine = Arc::new(QueryEngine::new(schema_registry));

    let spec = TableSpec::new("actors")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap_or_else(|_| TableSpec::new("actors"));
    query_engine.register_table_schema(spec.clone()).ok();

 // Set up the ArrowStore with a snapshot at tick 5 so the callback
 // can retrieve a real snapshot.
    let arrow_store = Arc::new(ArrowStore::default());
    arrow_store
        .create_table(&spec, MutationMode::AppendOnly)
        .expect("create table in arrow store");
    arrow_store
        .generate_snapshot(Tick(5))
        .expect("generate snapshot at tick 5");

 // Ingest a snapshot at tick >= 5 into the QueryEngine for read-back.
    {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("color", DataType::Utf8, false),
        ]));
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let name: ArrayRef = Arc::new(StringArray::from(vec!["alpha"]));
        let color: ArrayRef = Arc::new(StringArray::from(vec!["red"]));
        let batch =
            RecordBatch::try_new(schema, vec![id, name, color]).expect("build batch");
        query_engine
            .ingest_snapshot(scharnhorst_core::Tick(10), "actors", vec![batch], scharnhorst_core::RowPositionMap::new())
            .expect("ingest snapshot");
 // Store the current WorldSnapshot in query-engine so snapshot() works.
        let ws = arrow_store.get_snapshot(Tick(5)).expect("get snapshot");
        query_engine.store_world_snapshot((*ws).clone()).expect("store snapshot");
    }

    let vm = Arc::new(ViewModel::new());
    let handler = SnapshotRefreshHandler::new(
        Arc::clone(&vm),
        Arc::clone(&query_engine),
        Arc::clone(&arrow_store),
    );

 // Simulate a scheduler refresh broadcast.
    let result = handler.callback()(5, 3);
    assert!(result.is_ok(), "refresh handler failed: {:?}", result);

    let gen = vm.generation().expect("read generation");
    assert!(gen > 0, "generation should have bumped after callback refresh");
}

#[test]
fn input_command_buffer_drains_to_scheduler() {
    let world = TestWorld::build_mvp().expect("build world");

    world
        .input_buffer
        .set_tick(scharnhorst_core::Tick::ZERO)
        .expect("set tick");
    world
        .input_buffer
        .submit_player_command("player_1", scharnhorst_journal::Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::Value::Null,
        })
        .expect("submit");

    let drained = world.input_buffer.drain().expect("drain");
    assert_eq!(drained.len(), 1);
    assert!(world.input_buffer.is_empty().expect("is_empty"));
}
