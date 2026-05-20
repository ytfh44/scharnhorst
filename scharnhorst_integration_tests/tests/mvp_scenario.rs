//! 10.1 MVP scenario: 2 Actors, 10 Spatial Nodes, simple ownership transfer.
//!
//! Verifies that the full stack (arrow store, schema, journal, scheduler)
//! can ingest seed data and process an ownership-transfer command.

use scharnhorst_core::RowId;
use scharnhorst_integration_tests::harness::TestWorld;

#[test]
fn mvp_world_builds_and_seeds() {
    let mut world = TestWorld::build_mvp().expect("build mvp world");
    world.seed_mvp_data().expect("seed mvp data");

    assert_eq!(world.arrow_store.table_count().expect("get table count"), 2);
    let tick_val = world.scheduler.current_tick().map(|t| t.0);
    assert!(tick_val.is_ok());
}

#[test]
fn mvp_ownership_transfer_command_enqueued() {
    let mut world = TestWorld::build_mvp().expect("build mvp world");
    world.seed_mvp_data().expect("seed mvp data");

    world
        .transfer_node_owner(RowId::new(5), RowId::new(0), RowId::new(1))
        .expect("enqueue transfer");

    let pending = world
        .scheduler
        .pending_command_count()
        .expect("query pending commands");
    assert_eq!(pending, 1);
}

#[test]
fn mvp_tick_advances_after_transfer() {
    let mut world = TestWorld::build_mvp().expect("build mvp world");
    world.seed_mvp_data().expect("seed mvp data");

    world
        .transfer_node_owner(RowId::new(5), RowId::new(0), RowId::new(1))
        .expect("enqueue transfer");

    let before = world.scheduler.current_tick().expect("current tick");
    let result = world.tick().expect("tick");
    let after = world.scheduler.current_tick().expect("current tick");

    assert_eq!(result.tick, before);
    assert_eq!(after.0, before.0.saturating_add(1));
}

#[test]
fn mvp_history_records_commits() {
    let mut world = TestWorld::build_mvp().expect("build mvp world");
    world.seed_mvp_data().expect("seed mvp data");

    world.tick().expect("tick");
    world.tick().expect("tick");

    let history_len = world
        .scheduler
        .journal_mut()
        .expect("lock journal")
        .history()
        .len();
    assert!(history_len >= 1, "expected at least 1 commit in history");
}
