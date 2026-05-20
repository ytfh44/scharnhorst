//! REFRESH_SIGNAL integration tests: verify that the rule-ir Evaluator
//! registers a refresh callback with the scheduler and that the callback clears
//! the prefetch cache at tick boundaries.

use std::sync::{Arc, RwLock};

use scharnhorst_arrow_store::ArrowStore;
use scharnhorst_core::{RowId, Tick};
use scharnhorst_journal::Journal;
use scharnhorst_query::QueryEngine;
use scharnhorst_rules::Evaluator;
use scharnhorst_scheduler::{RefreshCallback, Scheduler, SchedulerError};
use scharnhorst_schema::SchemaRegistry;

fn make_test_scheduler() -> Scheduler {
    Scheduler::new(
        Journal::new(Arc::new(ArrowStore::new())),
        QueryEngine::new(SchemaRegistry::new()),
    )
}

fn make_test_evaluator() -> Evaluator {
    let query_engine = Arc::new(RwLock::new(QueryEngine::new(SchemaRegistry::new())));
    let journal = Arc::new(RwLock::new(Journal::new(Arc::new(ArrowStore::new()))));
    Evaluator::new(query_engine, journal, "test", RowId::new(0))
}

/// Build a RefreshCallback that, when invoked, acquires a write lock on the
/// evaluator and calls advance_tick_u64 to clear the prefetch cache.
fn evaluator_refresh_callback(ev: Arc<RwLock<Evaluator>>) -> RefreshCallback {
    Arc::new(move |tick: u64, _gen: u64| {
        let mut guard = ev
            .write()
            .map_err(|e| SchedulerError::Generic(format!("evaluator lock poisoned: {e}")))?;
        guard.advance_tick_u64(tick);
        Ok(())
    })
}

// ------------------------------------------------------------------
// Test: evaluator callback can be registered with the scheduler
// ------------------------------------------------------------------

#[test]
fn evaluator_registers_refresh_callback() {
    let scheduler = make_test_scheduler();
    let ev = Arc::new(RwLock::new(make_test_evaluator()));
    let cb = evaluator_refresh_callback(Arc::clone(&ev));

    // Register the evaluator as a refresh-signal consumer.
    let handle = scheduler
        .register_consumer("rule-ir", cb)
        .expect("register_consumer should succeed");

    // The handle should be non-empty.
    assert!(!handle.0.is_empty());

    // Verify "rule-ir" appears in the consumer list.
    let names = scheduler
        .consumer_names()
        .expect("consumer_names should succeed");
    assert!(
        names.contains(&"rule-ir".to_owned()),
        "expected 'rule-ir' in consumer names, got: {:?}",
        names
    );
}

// ------------------------------------------------------------------
// Test: broadcast_refresh triggers the evaluator callback, which
// advances the tick and clears the cache.
// ------------------------------------------------------------------

#[test]
fn refresh_signal_clears_evaluator_cache() {
    let scheduler = make_test_scheduler();
    let ev = Arc::new(RwLock::new(make_test_evaluator()));
    let cb = evaluator_refresh_callback(Arc::clone(&ev));

    scheduler
        .register_consumer("rule-ir", cb)
        .expect("register_consumer should succeed");

    // Simulate a scheduler broadcast: tick=5, generation=1.
    scheduler
        .broadcast_refresh(Tick(5), 1)
        .expect("broadcast_refresh should succeed");

    // Read-back: the evaluator should have advanced to tick 5 with an empty cache.
    let guard = ev.read().expect("read lock should succeed");
    assert_eq!(
        guard.current_tick(),
        Tick(5),
        "evaluator tick should match the broadcast tick"
    );
    assert!(
        guard.cache().is_empty(),
        "evaluator cache should be empty after REFRESH_SIGNAL"
    );
}

// ------------------------------------------------------------------
// Test: multiple consumers receive REFRESH_SIGNAL independently
// ------------------------------------------------------------------

#[test]
fn multiple_evaluators_receive_refresh_signal() {
    let scheduler = make_test_scheduler();

    let ev1 = Arc::new(RwLock::new(make_test_evaluator()));
    let ev2 = Arc::new(RwLock::new(make_test_evaluator()));

    scheduler
        .register_consumer("rule-ir-a", evaluator_refresh_callback(Arc::clone(&ev1)))
        .expect("register consumer a");
    scheduler
        .register_consumer("rule-ir-b", evaluator_refresh_callback(Arc::clone(&ev2)))
        .expect("register consumer b");

    scheduler
        .broadcast_refresh(Tick(42), 3)
        .expect("broadcast_refresh should succeed");

    let g1 = ev1.read().expect("read lock ev1");
    let g2 = ev2.read().expect("read lock ev2");

    assert_eq!(g1.current_tick(), Tick(42));
    assert!(g1.cache().is_empty());
    assert_eq!(g2.current_tick(), Tick(42));
    assert!(g2.cache().is_empty());
}

// ------------------------------------------------------------------
// Test: RefreshCallback error propagation
// ------------------------------------------------------------------

#[test]
fn evaluator_callback_error_propagates() {
    let scheduler = make_test_scheduler();

    // A callback that always fails.
    let bad_cb: RefreshCallback =
        Arc::new(|_tick, _gen| Err(SchedulerError::Generic("simulated failure".to_owned())));

    scheduler
        .register_consumer("bad-consumer", bad_cb)
        .expect("register_consumer");

    let result = scheduler.broadcast_refresh(Tick(0), 0);
    assert!(
        result.is_err(),
        "broadcast should fail when a consumer returns an error"
    );
    assert!(
        matches!(
            &result,
            Err(SchedulerError::RefreshSignalFailed {
                consumer: name,
                ..
            }) if name == "bad-consumer"
        ),
        "expected RefreshSignalFailed, got: {:?}",
        result
    );
}
