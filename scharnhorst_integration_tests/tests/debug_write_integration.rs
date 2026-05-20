//! Integration tests for the DebugWriteJournal SQL write path.
//!
//! These tests verify the full pipeline:
//! SQL -> SqlParser -> Diff -> DebugWriteJournal record -> Journal::submit_diff
//!
//! The `execute_sql_write` method on `QueryEngine` is only available in debug
//! builds (`#[cfg(debug_assertions)]`), so the tests that exercise it are
//! gated accordingly.

use scharnhorst_core::Tick;
use scharnhorst_journal::Journal;
use scharnhorst_query::{DebugWriteJournal, QueryEngine};
use scharnhorst_schema::SchemaRegistry;

fn make_engine() -> QueryEngine {
    QueryEngine::new(SchemaRegistry::new())
}

// ---------------------------------------------------------------------------
// Tests that exercise execute_sql_write (debug-assertions only)
// ---------------------------------------------------------------------------

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_update_submits_diff() {
    let engine = make_engine();
    let mut journal = Journal::default();

    let sql = "UPDATE actor_state SET treasury = 1000 WHERE actor_id = 1";
    engine
        .execute_sql_write(sql, &mut journal, Tick(1))
        .unwrap();

    assert_eq!(journal.pending_diff_count(), 1);
}

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_insert_submits_diff() {
    let engine = make_engine();
    let mut journal = Journal::default();

    let sql = "INSERT INTO actor_state (actor_id, treasury) VALUES (5, 10000)";
    engine
        .execute_sql_write(sql, &mut journal, Tick(1))
        .unwrap();

    assert_eq!(journal.pending_diff_count(), 1);
}

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_delete_submits_diff() {
    let engine = make_engine();
    let mut journal = Journal::default();

    let sql = "DELETE FROM actor_state WHERE actor_id = 3";
    engine
        .execute_sql_write(sql, &mut journal, Tick(1))
        .unwrap();

    assert_eq!(journal.pending_diff_count(), 1);
}

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_is_recorded_in_debug_journal() {
    let engine = make_engine();
    let mut journal = Journal::default();

    let sql = "UPDATE actor_state SET treasury = 500 WHERE actor_id = 2";
    engine
        .execute_sql_write(sql, &mut journal, Tick(1))
        .unwrap();

    // The debug journal ring buffer must have recorded the operation
    assert_eq!(engine.debug_journal().len(), 1);

    let ops = engine.debug_journal().ops();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].table_name(), "actor_state");
    assert_eq!(ops[0].tick(), Tick(1));
}

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_disabled_journal_still_submits_diff() {
    let engine = make_engine();
    let mut journal = Journal::default();

    // Simulate multiplayer session: disable the debug journal ring buffer
    engine.debug_journal().set_enabled(false);

    let sql = "DELETE FROM actor_state WHERE actor_id = 3";
    engine
        .execute_sql_write(sql, &mut journal, Tick(1))
        .unwrap();

    // The diff must still be submitted to the journal system
    assert_eq!(journal.pending_diff_count(), 1);
    // But the debug journal ring buffer must be empty (no-op when disabled)
    assert!(engine.debug_journal().is_empty());
}

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_invalid_sql_returns_error() {
    let engine = make_engine();
    let mut journal = Journal::default();

    // SELECT is not a write statement
    let result = engine.execute_sql_write("SELECT * FROM actor_state", &mut journal, Tick(1));
    assert!(result.is_err());
}

#[test]
#[cfg(debug_assertions)]
fn execute_sql_write_multiple_ops_accumulate() {
    let engine = make_engine();
    let mut journal = Journal::default();

    engine
        .execute_sql_write(
            "UPDATE actor_state SET treasury = 100 WHERE actor_id = 1",
            &mut journal,
            Tick(1),
        )
        .unwrap();
    engine
        .execute_sql_write(
            "INSERT INTO actor_state (actor_id, name) VALUES (99, 'Test')",
            &mut journal,
            Tick(1),
        )
        .unwrap();
    engine
        .execute_sql_write(
            "DELETE FROM actor_state WHERE actor_id = 5",
            &mut journal,
            Tick(1),
        )
        .unwrap();

    assert_eq!(journal.pending_diff_count(), 3);
    assert_eq!(engine.debug_journal().len(), 3);
}

// ---------------------------------------------------------------------------
// Tests that do not require execute_sql_write (always available)
// ---------------------------------------------------------------------------

#[test]
fn debug_write_journal_set_enabled_toggle() {
    // Verify the enabled/disabled toggle works independently of SQL execution.
    // This is the mechanism used to disable the debug journal in multiplayer.
    let journal = DebugWriteJournal::default();

    assert!(journal.enabled());

    journal.set_enabled(false);
    assert!(!journal.enabled());

    journal.set_enabled(true);
    assert!(journal.enabled());
}

#[test]
fn debug_write_journal_disabled_ignores_records() {
    let journal = DebugWriteJournal::new(10);

    journal.set_enabled(false);
    journal.record(scharnhorst_query::DebugWriteOp::Append {
        table: "test".to_owned(),
        tick: Tick(1),
        row_count: 1,
    });

    // When disabled, records are silently dropped
    assert!(journal.is_empty());
    assert_eq!(journal.len(), 0);
}
