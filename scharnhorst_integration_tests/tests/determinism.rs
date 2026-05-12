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

    assert_eq!(hash_a, hash_b, "identical seed data must yield identical hashes");
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
