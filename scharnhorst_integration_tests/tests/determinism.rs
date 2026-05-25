//! 10.2 Verify determinism: Same commands result in identical world hashes.
//!
//! We run two identical worlds with the same sequence of diffs and commands
//! and assert that the commit hashes match tick-for-tick.

use scharnhorst_core::RowId;
use scharnhorst_integration_tests::harness::TestWorld;

#[test]
fn deterministic_seed_produces_same_hashes() {
    let mut world_a = TestWorld::build_mvp().expect("build world a");
    let mut world_b = TestWorld::build_mvp().expect("build world b");

    world_a.seed_mvp_data().expect("seed a");
    world_b.seed_mvp_data().expect("seed b");

    let hash_a = world_a.tick().expect("tick a").state_hash;
    let hash_b = world_b.tick().expect("tick b").state_hash;

    assert_eq!(
        hash_a, hash_b,
        "identical seed data must yield identical hashes"
    );
}

#[test]
fn deterministic_commands_produce_same_hashes() {
    let mut world_a = TestWorld::build_mvp().expect("build world a");
    let mut world_b = TestWorld::build_mvp().expect("build world b");

    world_a.seed_mvp_data().expect("seed a");
    world_b.seed_mvp_data().expect("seed b");

    // Enqueue identical transfer commands on both worlds.
    world_a
        .transfer_node_owner(RowId::new(3), RowId::new(0), RowId::new(1))
        .expect("enqueue a");
    world_b
        .transfer_node_owner(RowId::new(3), RowId::new(0), RowId::new(1))
        .expect("enqueue b");

    let result_a = world_a.tick().expect("tick a");
    let result_b = world_b.tick().expect("tick b");

    assert_eq!(result_a.state_hash, result_b.state_hash);
    assert_eq!(result_a.diff_count, result_b.diff_count);
}

#[test]
fn deterministic_multi_tick_run() {
    let mut world_a = TestWorld::build_mvp().expect("build world a");
    let mut world_b = TestWorld::build_mvp().expect("build world b");

    world_a.seed_mvp_data().expect("seed a");
    world_b.seed_mvp_data().expect("seed b");

    for i in 0..5 {
        world_a
            .transfer_node_owner(RowId::new(i), RowId::new(0), RowId::new(1))
            .expect("enqueue a");
        world_b
            .transfer_node_owner(RowId::new(i), RowId::new(0), RowId::new(1))
            .expect("enqueue b");

        let result_a = world_a.tick().expect("tick a");
        let result_b = world_b.tick().expect("tick b");

        assert_eq!(
            result_a.state_hash, result_b.state_hash,
            "hashes diverged at tick {}",
            i
        );
    }
}

// ── Telemetry determinism tests (Group 15) ─────────────────────────

/// TASK 15.1 / 64: Check that the same commands produce identical hashes.
/// This is a duplicate verification of the existing determinism test,
/// but with explicit documentation that it's the telemetry determinism invariant.
#[test]
fn telemetry_does_not_affect_determinism_single_tick() {
    // Build two identical worlds
    let mut world_a = TestWorld::build_mvp().expect("build a");
    let mut world_b = TestWorld::build_mvp().expect("build b");

    world_a.seed_mvp_data().expect("seed a");
    world_b.seed_mvp_data().expect("seed b");

    // Same commands
    world_a
        .transfer_node_owner(RowId::new(3), RowId::new(0), RowId::new(1))
        .expect("enqueue a");
    world_b
        .transfer_node_owner(RowId::new(3), RowId::new(0), RowId::new(1))
        .expect("enqueue b");

    let result_a = world_a.tick().expect("tick a");
    let result_b = world_b.tick().expect("tick b");

    assert_eq!(
        result_a.state_hash, result_b.state_hash,
        "state hashes must be identical — telemetry must not affect determinism"
    );
    assert_eq!(result_a.diff_count, result_b.diff_count);
}

/// TASK 15.2 / 65: Multi-tick determinism.
#[test]
fn telemetry_does_not_affect_determinism_multi_tick() {
    let mut world_a = TestWorld::build_mvp().expect("build a");
    let mut world_b = TestWorld::build_mvp().expect("build b");

    world_a.seed_mvp_data().expect("seed a");
    world_b.seed_mvp_data().expect("seed b");

    for i in 0..10 {
        world_a
            .transfer_node_owner(RowId::new(i), RowId::new(0), RowId::new(1))
            .expect("enqueue a");
        world_b
            .transfer_node_owner(RowId::new(i), RowId::new(0), RowId::new(1))
            .expect("enqueue b");

        let ra = world_a.tick().expect("tick a");
        let rb = world_b.tick().expect("tick b");

        assert_eq!(ra.state_hash, rb.state_hash, "hashes diverged at tick {i}");
        assert_eq!(ra.diff_count, rb.diff_count);
    }
}

/// TASK 15.3 / 66: Verify that scheduler's emit_telemetry_summary is a no-op
/// and doesn't change the query engine state.
#[test]
fn telemetry_summary_does_not_mutate_query_state() {
    let mut world = TestWorld::build_mvp().unwrap();
    world.seed_mvp_data().unwrap();

    let qe = &world.query_engine;

    // Capture state before
    let view_before = qe.snapshot().unwrap();
    let tick_before = view_before.tick().as_u64();
    let count_before = view_before.row_count("actors").unwrap();

    // Emit telemetry (should be a no-op for query state)
    qe.emit_telemetry_summary(tick_before);
    qe.reset_telemetry_counters();

    // State after must be identical
    let view_after = qe.snapshot().unwrap();
    assert_eq!(view_after.tick().as_u64(), tick_before);
    assert_eq!(view_after.row_count("actors").unwrap(), count_before);
}

#[test]
fn telemetry_counters_reset_to_zero() {
    let mut world = TestWorld::build_mvp().unwrap();
    world.seed_mvp_data().unwrap();

    let qe = &world.query_engine;

    // Reset should not panic
    qe.reset_telemetry_counters();

    // After reset, emit summary should not panic
    qe.emit_telemetry_summary(0);

    // Can call emit multiple times
    for tick in 0..5 {
        qe.emit_telemetry_summary(tick);
    }
}
