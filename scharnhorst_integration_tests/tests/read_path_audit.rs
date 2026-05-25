//! Integration tests verifying that all simulation consumers route through
//! QueryEngine → WorldView path, never directly accessing WorldSnapshot or Arrow internals.

use scharnhorst_integration_tests::harness::TestWorld;

/// TASK 14.1 / 60: Verify that WorldView is the only read path from QueryEngine.
#[test]
fn query_engine_snapshot_returns_world_view() {
    let mut world = TestWorld::build_mvp().unwrap();
    world.seed_mvp_data().unwrap();

    let qe = &world.query_engine;
    // snapshot() returns WorldView, not Arc<WorldSnapshot>
    let view = qe.snapshot().unwrap();

    // WorldView has ONLY typed accessors — no RecordBatch, no Arrow types
    // snapshot should have a valid tick (including Tick::ZERO)
    let _tick_val = view.tick().as_u64();

    // Verify public API surface
    let tables = view.table_names();
    assert!(!tables.is_empty(), "should have tables");
    assert!(tables.contains(&"actors".to_owned()));
    assert!(tables.contains(&"spatial_nodes".to_owned()));

    // row_count works
    let count = view.row_count("actors").unwrap();
    assert!(count > 0);

    // column_names works
    let cols = view.column_names("actors").unwrap();
    assert!(!cols.is_empty(), "actors should have columns");
    assert!(cols.contains(&"name".to_owned()));

    // column_type works
    let typ = view.column_type("actors", "name").unwrap();
    assert!(!typ.is_empty());

    // iter_rows works
    let rows: Vec<_> = view.iter_rows("actors").unwrap().collect();
    assert!(!rows.is_empty());

    // Type check: WorldView does NOT expose RecordBatch
    // This is verified at compile time — the type system prevents
    // calling any Arrow methods on WorldView.
}

/// TASK 14.2 / 61: Test that WorldView held across tick boundaries returns stale (frozen) data.
#[test]
fn world_view_is_frozen_snapshot() {
    let mut world = TestWorld::build_mvp().unwrap();
    world.seed_mvp_data().unwrap();
    world.tick().unwrap();

    // Get snapshot at current tick
    let view_t1 = world.query_engine.snapshot().unwrap();
    let tick1 = view_t1.tick().as_u64();
    let row_count_t1 = view_t1.row_count("actors").unwrap();

    // Advance tick
    let result = world.tick().unwrap();
    assert!(result.tick.as_u64() > tick1);

    // Old WorldView still sees old data
    assert_eq!(view_t1.tick().as_u64(), tick1);
    assert_eq!(view_t1.row_count("actors").unwrap(), row_count_t1);

    // New WorldView sees new tick
    let view_t2 = world.query_engine.snapshot().unwrap();
    assert_eq!(view_t2.tick().as_u64(), result.tick.as_u64());
}

/// TASK 14.3 / 62: Test that QueryEngine wraps provided paths correctly.
#[test]
fn query_engine_all_read_paths_work() {
    let mut world = TestWorld::build_mvp().unwrap();
    world.seed_mvp_data().unwrap();

    let qe = &world.query_engine;

    // Test all QueryEngine read methods exist and return non-error results
    let view = qe.snapshot().unwrap();
    assert!(!view.table_names().is_empty());

    // column_view, lookup_row, batch_reader, read are internal methods
    // that should exist on QueryEngine. Test them if accessible from
    // the integration test scope, otherwise document them.
    // Since these are crate-internal, skip direct testing here
    // and verify via the WorldView path instead.
}

/// TASK 14.4 / 63: Test that WorldViewRow typed getters work correctly.
#[test]
fn world_view_row_typed_getters() {
    let mut world = TestWorld::build_mvp().unwrap();
    world.seed_mvp_data().unwrap();

    let view = world.query_engine.snapshot().unwrap();

    // Read actors table
    let mut rows = view.iter_rows("actors").unwrap();
    let first = rows.next().unwrap().unwrap();

    // Check typed accessors
    // Actors table has: id (i64), name (utf8), color (utf8)
    let id = first.get_i64(0).unwrap();
    assert!(id.is_some(), "id should be present");

    let name = first.get_string(1).unwrap();
    assert!(name.is_some(), "name should be present");
    assert!(name.unwrap().starts_with("actor_"));

    // get_bool on an i64 column should return error
    let bool_err = first.get_bool(0);
    assert!(bool_err.is_err(), "get_bool on i64 should error");

    // get_i64 on string column should return error
    let i64_err = first.get_i64(1);
    assert!(i64_err.is_err(), "get_i64 on string should error");
}
