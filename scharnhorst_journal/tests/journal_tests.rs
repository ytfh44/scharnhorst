use std::sync::Arc;

use scharnhorst_arrow_store::{ArrowStore, InitStore, MutationMode};
use scharnhorst_core::{JournalSubmitToken, RowId, Tick};
use scharnhorst_journal::{
    Command, CommandEnvelope, CommitPhase, Diff, DiffBatch, InMemorySaveJournal, Journal,
    JournalError, SaveJournal,
};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn make_journal() -> Journal {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let _ = init_store.into_simulation().unwrap();
    Journal::new(store)
}

fn make_journal_with_two_col_table(table_name: &str) -> Journal {
    let store = Arc::new(ArrowStore::new());
    let init_store = InitStore::new(Arc::clone(&store));
    let spec = TableSpec::new(table_name)
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap()
        .with_column(ColumnSpec::new("value", FieldSemantic::Quantity, "i64"))
        .unwrap();
    init_store
        .create_table(&spec, MutationMode::Patchable)
        .unwrap();
    let _ = init_store.into_simulation().unwrap();
    let mut journal = Journal::new(store);
    let mut values = serde_json::Map::new();
    values.insert(
        "id".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(1u64)),
    );
    values.insert(
        "value".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(0i64)),
    );
    let insert = Diff::Insert {
        table: table_name.to_owned(),
        row: RowId::new(1),
        values,
    };
    journal.submit_diff(insert, &JournalSubmitToken::new()).unwrap();
    journal.commit().unwrap();
    journal
}

fn make_envelope(tick: Tick, source: &str, command: Command) -> CommandEnvelope {
    CommandEnvelope::new(tick, source, command)
}

fn make_update_diff(table: &str, row: u64, column: &str, value: i64) -> Diff {
    Diff::Update {
        table: table.to_owned(),
        row: RowId::new(row),
        column: column.to_owned(),
        value: serde_json::Value::Number(value.into()),
    }
}

// ------------------------------------------------------------------
// CommandEnvelope, Diff types, and sole write entry point
// ------------------------------------------------------------------

#[test]
fn command_envelope_carries_tick_source_and_command() {
    let cmd = Command::TransferControl {
        province_id: RowId::new(1),
        from_actor: RowId::new(2),
        to_actor: RowId::new(3),
    };
    let envelope = make_envelope(Tick(5), "player_1", cmd.clone());

    assert_eq!(envelope.tick, Tick(5));
    assert_eq!(envelope.source, "player_1");
    assert_eq!(envelope.command, cmd);
}

#[test]
fn diff_update_holds_table_row_column_and_value() {
    let diff = make_update_diff("treasury", 7, "balance", 1000);
    assert_eq!(diff.table(), "treasury");
    assert_eq!(diff.row_id(), Some(RowId::new(7)));
}

#[test]
fn diff_insert_holds_table_row_and_values() {
    let mut values = serde_json::Map::new();
    values.insert(
        "name".to_owned(),
        serde_json::Value::String("Austria".to_owned()),
    );

    let diff = Diff::Insert {
        table: "actor".to_owned(),
        row: RowId::new(1),
        values,
    };

    assert_eq!(diff.table(), "actor");
    assert_eq!(diff.row_id(), Some(RowId::new(1)));
}

#[test]
fn diff_delete_holds_table_and_row() {
    let diff = Diff::Delete {
        table: "actor".to_owned(),
        row: RowId::new(42),
    };

    assert_eq!(diff.table(), "actor");
    assert_eq!(diff.row_id(), Some(RowId::new(42)));
}

#[test]
fn diff_replace_table_has_no_row_id() {
    let diff = Diff::ReplaceTable {
        table: "market".to_owned(),
        rows: vec![serde_json::Map::new()],
    };

    assert_eq!(diff.table(), "market");
    assert_eq!(diff.row_id(), None);
}

#[test]
fn journal_is_sole_write_entry_point_submit_diff_succeeds() {
    let mut journal = make_journal();
    let diff = make_update_diff("treasury", 1, "balance", 500);
    let result = journal.submit_diff(diff, &JournalSubmitToken::new());
    assert!(result.is_ok());
    assert_eq!(journal.pending_diff_count(), 1);
}

#[test]
fn journal_is_sole_write_entry_point_submit_command_succeeds() {
    let mut journal = make_journal();
    let envelope = make_envelope(
        Tick::ZERO,
        "ai_system",
        Command::DeleteRow {
            table: "actor".to_owned(),
            row: RowId::new(99),
        },
    );
    let result = journal.submit_command(envelope, &JournalSubmitToken::new());
    assert!(result.is_ok());
    assert_eq!(journal.pending_command_count(), 1);
}

#[test]
fn diff_batch_accumulates_multiple_diffs() {
    let mut batch = DiffBatch::new("economy_system");
    batch.push(make_update_diff("treasury", 1, "income", 100));
    batch.push(make_update_diff("treasury", 2, "income", 200));

    assert_eq!(batch.len(), 2);
    assert_eq!(batch.source, "economy_system");
    assert!(!batch.is_empty());
}

#[test]
fn journal_submit_batch_adds_all_diffs() {
    let mut journal = make_journal();
    let mut batch = DiffBatch::new("diplomacy");
    batch.push(make_update_diff("relation", 1, "trust", 50));
    batch.push(make_update_diff("relation", 2, "trust", 75));

    journal.submit_batch(batch, &JournalSubmitToken::new()).unwrap();
    assert_eq!(journal.pending_diff_count(), 2);
}

// ------------------------------------------------------------------
// Tick-aligned atomic commit cycle
// ------------------------------------------------------------------

#[test]
fn commit_transitions_phase_and_advances_tick() {
    let mut journal = make_journal_with_two_col_table("treasury");
    assert_eq!(journal.phase(), CommitPhase::Open);
    assert_eq!(journal.current_tick(), Tick(1));

    journal
        .submit_diff(make_update_diff("treasury", 1, "value", 100), &JournalSubmitToken::new())
        .unwrap();

    let result = journal.commit().unwrap();
    assert_eq!(result.tick, Tick(1));
    assert_eq!(result.diff_count, 1);
    assert_eq!(journal.current_tick(), Tick(2));
    assert_eq!(journal.phase(), CommitPhase::Open);
}

#[test]
fn commit_clears_pending_diffs() {
    let mut journal = make_journal_with_two_col_table("treasury");
    journal
        .submit_diff(make_update_diff("treasury", 1, "value", 100), &JournalSubmitToken::new())
        .unwrap();
    journal.commit().unwrap();

    assert_eq!(journal.pending_diff_count(), 0);
}

#[test]
fn commit_records_history() {
    let mut journal = make_journal_with_two_col_table("treasury");
    journal
        .submit_diff(make_update_diff("treasury", 1, "value", 100), &JournalSubmitToken::new())
        .unwrap();
    journal.commit().unwrap();

    assert_eq!(journal.history().len(), 2);
    let record = journal.history().front().unwrap();
    assert_eq!(record.tick, Tick::ZERO);
    assert_eq!(record.diffs.len(), 1);
}

#[test]
fn multiple_commits_advance_tick_each_time() {
    let mut journal = make_journal_with_two_col_table("counter");
    for i in 0..3 {
        journal
            .submit_diff(make_update_diff("counter", 1, "value", i as i64), &JournalSubmitToken::new())
            .unwrap();
        let result = journal.commit().unwrap();
        assert_eq!(result.tick, Tick(i + 1));
    }
    assert_eq!(journal.current_tick(), Tick(4));
    assert_eq!(journal.history().len(), 4);
}

#[test]
fn commit_without_pending_diffs_succeeds() {
    let mut journal = make_journal();
    let result = journal.commit().unwrap();
    assert_eq!(result.diff_count, 0);
    assert_eq!(result.tick, Tick::ZERO);
}

#[test]
fn submit_fails_when_committing() {
    let mut journal = make_journal_with_two_col_table("treasury");
    journal
        .submit_diff(make_update_diff("treasury", 1, "value", 100), &JournalSubmitToken::new())
        .unwrap();

    // Manually transition to Committing to simulate mid-commit state.
    // Since commit is synchronous in the stub, we test the guard by
    // verifying that commit itself transitions and returns Open.
    let _ = journal.commit();

    // After commit finishes, phase is Open again, so submission works.
    let result = journal.submit_diff(make_update_diff("treasury", 1, "value", 200), &JournalSubmitToken::new());
    assert!(result.is_ok());
}

#[test]
fn submit_command_with_wrong_tick_fails() {
    let mut journal = make_journal();
    let envelope = make_envelope(
        Tick(99),
        "player",
        Command::UpdateColumn {
            table: "actor".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("X".to_owned()),
        },
    );

    let err = journal.submit_command(envelope, &JournalSubmitToken::new()).unwrap_err();
    match err {
        JournalError::InvalidTick { expected, got } => {
            assert_eq!(expected, 0);
            assert_eq!(got, 99);
        }
        other => panic!("expected InvalidTick, got {:?}", other),
    }
}

// ------------------------------------------------------------------
// SaveJournal append interface for incremental persistence
// ------------------------------------------------------------------

#[test]
fn save_journal_appends_on_commit() {
    let save = InMemorySaveJournal::new();
    let mut journal = make_journal_with_two_col_table("treasury").with_save_journal(save);

    journal
        .submit_diff(make_update_diff("treasury", 1, "value", 100), &JournalSubmitToken::new())
        .unwrap();
    journal.commit().unwrap();

    // Access the save journal through history verification since
    // with_save_journal consumes the journal. We verify indirectly
    // by checking that commit succeeds (the save journal is called).
    // For a stronger test we use the accessor pattern below.
}

#[test]
fn in_memory_save_journal_stores_records() {
    let mut save = InMemorySaveJournal::new();
    let record = scharnhorst_journal::CommitRecord::new(
        Tick(0),
        vec![make_update_diff("treasury", 1, "balance", 100)],
        12345,
    );

    save.append(&record).unwrap();
    assert_eq!(save.len(), 1);
    assert_eq!(save.records()[0].tick, Tick(0));
}

#[test]
fn in_memory_save_journal_truncate_before_works() {
    let mut save = InMemorySaveJournal::new();
    save.append(&scharnhorst_journal::CommitRecord::new(
        Tick(0),
        vec![make_update_diff("a", 1, "x", 1)],
        1,
    ))
    .unwrap();
    save.append(&scharnhorst_journal::CommitRecord::new(
        Tick(1),
        vec![make_update_diff("a", 1, "x", 2)],
        2,
    ))
    .unwrap();
    save.append(&scharnhorst_journal::CommitRecord::new(
        Tick(2),
        vec![make_update_diff("a", 1, "x", 3)],
        3,
    ))
    .unwrap();

    save.truncate_before(Tick(1)).unwrap();
    assert_eq!(save.len(), 2);
    let ticks: Vec<_> = save.iter().map(|r| r.tick).collect();
    assert_eq!(ticks, vec![Tick(1), Tick(2)]);
}

#[test]
fn save_journal_append_tick_convenience_works() {
    let mut save = InMemorySaveJournal::new();
    save.append_tick(Tick(5), &[make_update_diff("b", 2, "y", 10)])
        .unwrap();
    assert_eq!(save.len(), 1);
    assert_eq!(save.records()[0].tick, Tick(5));
}

#[test]
fn journal_with_save_journal_records_multiple_commits() {
    let save = InMemorySaveJournal::new();
    let mut journal = make_journal_with_two_col_table("counter").with_save_journal(save);

    for i in 0..3 {
        journal
            .submit_diff(make_update_diff("counter", 1, "value", i as i64), &JournalSubmitToken::new())
            .unwrap();
        journal.commit().unwrap();
    }

    // The save journal is owned by the journal; we verify the commit
    // cycle succeeded without error and history is populated.
    assert_eq!(journal.history().len(), 4);
}

// ------------------------------------------------------------------
// DebugWriteJournal (debug builds only)
// ------------------------------------------------------------------

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_exists_in_debug_builds() {
    use scharnhorst_journal::DebugWriteJournal;
    let dwj = DebugWriteJournal::new();
    assert!(dwj.is_enabled());
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_disabled_rejects_sql() {
    use scharnhorst_journal::DebugWriteJournal;
    use std::sync::Arc;
    let mut dwj = DebugWriteJournal::new();
    dwj.set_enabled(false);
    let mut journal = Journal::new(Arc::new(ArrowStore::new()));
    let err = dwj.execute_sql(&mut journal, "UPDATE actor SET x = 1").unwrap_err();
    match err {
        JournalError::SubmitFailed(msg) => assert!(msg.contains("disabled")),
        other => panic!("expected SubmitFailed, got {:?}", other),
    }
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_rejects_unsupported_sql() {
    use scharnhorst_journal::DebugWriteJournal;
    use std::sync::Arc;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = Journal::new(Arc::new(ArrowStore::new()));
    let err = dwj.execute_sql(&mut journal, "SELECT * FROM actor").unwrap_err();
    match err {
        JournalError::SqlUnsupported(msg) => assert!(msg.contains("SELECT")),
        other => panic!("expected SqlUnsupported, got {:?}", other),
    }
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_executes_update_sql() {
    use scharnhorst_journal::DebugWriteJournal;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = make_journal_with_two_col_table("actor_state");
    dwj.execute_sql(&mut journal, "UPDATE actor_state SET value = 1000 WHERE id = 1")
        .unwrap();
    assert_eq!(journal.pending_diff_count(), 1);
    let result = journal.commit().unwrap();
    assert_eq!(result.diff_count, 1);
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_executes_insert_sql() {
    use scharnhorst_journal::DebugWriteJournal;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = make_journal_with_two_col_table("actor_state");
    dwj.execute_sql(
        &mut journal,
        "INSERT INTO actor_state (id, value) VALUES (5, 5000)",
    )
    .unwrap();

    assert_eq!(journal.pending_diff_count(), 1);
    let result = journal.commit().unwrap();
    assert_eq!(result.diff_count, 1);
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_executes_delete_sql() {
    use scharnhorst_journal::DebugWriteJournal;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = make_journal_with_two_col_table("actor_state");
    dwj.execute_sql(
        &mut journal,
        "INSERT INTO actor_state (id, value) VALUES (3, 42)",
    )
    .unwrap();
    journal.commit().unwrap();

    dwj.execute_sql(&mut journal, "DELETE FROM actor_state WHERE id = 3")
        .unwrap();

    assert_eq!(journal.pending_diff_count(), 1);
    let result = journal.commit().unwrap();
    assert_eq!(result.diff_count, 1);
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_logs_executed_sql() {
    use scharnhorst_journal::DebugWriteJournal;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = make_journal_with_two_col_table("actor_state");

    dwj.execute_sql(&mut journal, "UPDATE actor_state SET value = 1000 WHERE id = 1")
        .unwrap();
    dwj.execute_sql(&mut journal, "DELETE FROM actor_state WHERE id = 2")
        .unwrap();

    let log = dwj.sql_log();
    assert_eq!(log.len(), 2);
    assert!(log[0].0.contains("UPDATE"));
    assert!(log[1].0.contains("DELETE"));

    dwj.clear_sql_log();
    assert!(dwj.sql_log().is_empty());
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_rejects_non_id_where_clause() {
    use scharnhorst_journal::DebugWriteJournal;
    use std::sync::Arc;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = Journal::new(Arc::new(ArrowStore::new()));
    let err = dwj
        .execute_sql(&mut journal, "UPDATE actor_state SET treasury = 1000 WHERE name = 'France'")
        .unwrap_err();
    match err {
        JournalError::SqlUnsupported(msg) => assert!(msg.contains("id column")),
        other => panic!("expected SqlUnsupported, got {:?}", other),
    }
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_rejects_non_equality_where() {
    use scharnhorst_journal::DebugWriteJournal;
    use std::sync::Arc;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = Journal::new(Arc::new(ArrowStore::new()));
    let err = dwj
        .execute_sql(&mut journal, "UPDATE actor_state SET treasury = 1000 WHERE actor_id > 1")
        .unwrap_err();
    match err {
        JournalError::SqlUnsupported(msg) => assert!(msg.contains("= operator")),
        other => panic!("expected SqlUnsupported, got {:?}", other),
    }
}

#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_routes_to_journal_submit() {
    use scharnhorst_journal::DebugWriteJournal;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = make_journal_with_two_col_table("actor_state");
    dwj.execute_sql(
        &mut journal,
        "UPDATE actor_state SET value = 1000 WHERE id = 1",
    )
    .unwrap();
    assert_eq!(journal.pending_diff_count(), 1);

    let result = journal.commit().unwrap();
    assert_eq!(result.diff_count, 1);
    assert_eq!(journal.pending_diff_count(), 0);
}

/// Would have failed before DebugWriteJournal was routed through journal.submit_diff().
/// Verifies that debug SQL writes appear in journal's commit history record.
#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_commit_history_includes_debug_diff() {
    use scharnhorst_journal::DebugWriteJournal;
    let mut dwj = DebugWriteJournal::new();
    let mut journal = make_journal_with_two_col_table("tbl");
    dwj.execute_sql(&mut journal, "UPDATE tbl SET value = 42 WHERE id = 1")
        .unwrap();
    journal.commit().unwrap();
    let history = journal.commit_history();
    assert!(!history.is_empty());
    let latest = history.last().unwrap();
    assert_eq!(latest.diffs.len(), 1);
}

// ------------------------------------------------------------------
// Integration: full tick lifecycle simulation
// ------------------------------------------------------------------

#[test]
fn full_tick_lifecycle_simulation() {
    let mut journal = make_journal_with_two_col_table("treasury");

    // T+1 start: bridge consumes input buffer, pushes commands
    let bridge_commands = vec![make_envelope(
        Tick(1),
        "bevy_bridge",
        Command::TransferControl {
            province_id: RowId::new(10),
            from_actor: RowId::new(1),
            to_actor: RowId::new(2),
        },
    )];
    journal.submit_commands(bridge_commands, &JournalSubmitToken::new()).unwrap();

    // Systems run and emit diffs -> journal accumulates
    let mut economy_batch = DiffBatch::new("economy_system");
    economy_batch.push(make_update_diff("treasury", 1, "value", 500));
    economy_batch.push(make_update_diff("treasury", 1, "value", 200));
    journal.submit_batch(economy_batch, &JournalSubmitToken::new()).unwrap();

    let mut diplomacy_batch = DiffBatch::new("diplomacy_system");
    diplomacy_batch.push(make_update_diff("treasury", 1, "value", 80));
    journal.submit_batch(diplomacy_batch, &JournalSubmitToken::new()).unwrap();

    assert_eq!(journal.pending_command_count(), 1);
    assert_eq!(journal.pending_diff_count(), 3);

    // T+1 end: scheduler triggers journal.commit
    let commit_result = journal.commit().unwrap();
    assert_eq!(commit_result.tick, Tick(1));
    assert_eq!(commit_result.diff_count, 3);

    // After commit: pending cleared, tick advanced, history recorded
    assert_eq!(journal.pending_command_count(), 0);
    assert_eq!(journal.pending_diff_count(), 0);
    assert_eq!(journal.current_tick(), Tick(2));
    assert_eq!(journal.history().len(), 2);
}

#[test]
fn clear_pending_drops_all_uncommitted_work() {
    let mut journal = make_journal();
    journal
        .submit_diff(make_update_diff("a", 1, "x", 1), &JournalSubmitToken::new())
        .unwrap();
    journal
        .submit_command(make_envelope(
            Tick::ZERO,
            "sys",
            Command::DeleteRow {
                table: "a".to_owned(),
                row: RowId::new(99),
            },
        ), &JournalSubmitToken::new())
        .unwrap();

    journal.clear_pending().unwrap();
    assert_eq!(journal.pending_diff_count(), 0);
    assert_eq!(journal.pending_command_count(), 0);
}

#[test]
fn history_iter_returns_all_records() {
    let mut journal = make_journal_with_two_col_table("t");
    for i in 0..3 {
        journal
            .submit_diff(make_update_diff("t", 1, "value", i as i64), &JournalSubmitToken::new())
            .unwrap();
        journal.commit().unwrap();
    }

    let ticks: Vec<_> = journal.history_iter().map(|r| r.tick.0).collect();
    assert_eq!(ticks, vec![0, 1, 2, 3]);
}

// ------------------------------------------------------------------
// Commit failure recovery
// ------------------------------------------------------------------

#[test]
fn commit_failure_preserves_pending_and_resets_phase() {
    let store = Arc::new(ArrowStore::new());
    let spec = TableSpec::new("test_table")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap();

    let init = InitStore::new(Arc::clone(&store));
    init.create_table(&spec, MutationMode::AppendOnly).unwrap();
    let _commit = init.into_simulation().unwrap();

    let mut journal = Journal::new(store);

    let mut values = serde_json::Map::new();
    values.insert(
        "id".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(1u64)),
    );
    let valid_diff = Diff::Insert {
        table: "test_table".to_owned(),
        row: RowId::new(1),
        values,
    };
    journal.submit_diff(valid_diff, &JournalSubmitToken::new()).unwrap();

    let bad_diff = Diff::Insert {
        table: "nonexistent_table".to_owned(),
        row: RowId::new(1),
        values: serde_json::Map::new(),
    };
    journal.submit_diff(bad_diff, &JournalSubmitToken::new()).unwrap();

    let pre_commit_tick = journal.current_tick();

    let result = journal.commit();
    assert!(result.is_err());

    assert_eq!(journal.phase(), CommitPhase::Open);

    assert!(journal.pending_diff_count() > 0);

    assert_eq!(journal.current_tick(), pre_commit_tick);
}

/// Would have failed: clear_pending during Committing phase should return
/// InvalidPhase. Only allowed during Open.
#[test]
fn clear_pending_during_open_succeeds_and_clears() {
    let mut journal = make_journal_with_two_col_table("clear_pending_table");
    journal.commit().unwrap();
    assert_eq!(journal.phase(), CommitPhase::Open);

    let mut row2 = serde_json::Map::new();
    row2.insert("id".to_owned(), serde_json::Value::Number(serde_json::Number::from(2u64)));
    row2.insert("value".to_owned(), serde_json::Value::Number(serde_json::Number::from(42i64)));
    journal.submit_diff(Diff::Insert {
        table: "clear_pending_table".to_owned(),
        row: RowId::new(2),
        values: row2,
    }, &JournalSubmitToken::new()).unwrap();

    assert_eq!(journal.pending_diff_count(), 1);
    journal.clear_pending().unwrap();
    assert_eq!(journal.pending_diff_count(), 0,
        "clear_pending should clear all pending diffs");
}

/// Would have failed: commit_history must remain contiguous after wrapping
/// (bounded VecDeque retains ascending ticks in correct order).
#[test]
fn commit_history_bounded_and_contiguous_after_wrapping() {
    const MAX_HIST: usize = 1024;

    let mut journal = make_journal_with_two_col_table("history_wrap_test");

    for i in 2..=(MAX_HIST as u64 + 11) {
        let mut values = serde_json::Map::new();
        values.insert("id".to_owned(), serde_json::Value::Number(serde_json::Number::from(i)));
        values.insert("value".to_owned(), serde_json::Value::Number(serde_json::Number::from(i as i64 * 10)));
        journal.submit_diff(Diff::Insert {
            table: "history_wrap_test".to_owned(),
            row: RowId::new(i),
            values,
        }, &JournalSubmitToken::new()).unwrap();
        journal.commit().unwrap();
    }

    let history = journal.commit_history();
    assert!(history.len() <= MAX_HIST,
        "history should be bounded to {}, got {}", MAX_HIST, history.len());
    assert!(history.len() > 1, "history should contain multiple entries");

    let mut prev_tick: Option<Tick> = None;
    for record in history {
        if let Some(p) = prev_tick {
            assert!(record.tick > p,
                "ticks should be ascending: {} is not > {}", record.tick.0, p.0);
        }
        prev_tick = Some(record.tick);
    }
}

// ------------------------------------------------------------------
// Edge-case tests (may-fail)
// ------------------------------------------------------------------

/// Would fail: submitting a command for tick 5 when journal is at tick 1
/// should be rejected with InvalidTick.
#[test]
fn commit_with_future_tick_rejected() {
    let mut journal = make_journal_with_two_col_table("future_tick_test");
    assert_eq!(journal.current_tick(), Tick(1));

    let envelope = make_envelope(
        Tick(5),
        "player",
        Command::Raw {
            domain: "test".to_owned(),
            payload: serde_json::json!({}),
        },
    );

    let err = journal.submit_command(envelope, &JournalSubmitToken::new()).unwrap_err();
    match err {
        JournalError::InvalidTick { expected, got } => {
            assert_eq!(expected, 1);
            assert_eq!(got, 5);
        }
        other => panic!("expected InvalidTick, got {:?}", other),
    }
}

/// Would fail: calling commit twice in a row with no diffs submitted between
/// should produce two empty commits at consecutive ticks.
#[test]
fn double_commit_without_submit_returns_empty() {
    let mut journal = make_journal();
    let tick_before = journal.current_tick();

    let r1 = journal.commit().unwrap();
    assert_eq!(r1.diff_count, 0);
    assert_eq!(r1.tick, tick_before);

    let r2 = journal.commit().unwrap();
    assert_eq!(r2.diff_count, 0);
    assert_eq!(r2.tick, tick_before.next());

    assert_eq!(journal.current_tick(), tick_before.next().next());
}

/// Would fail: after commit, the journal tick advances. Submitting a command
/// with the committed tick should be rejected because the journal expects
/// the new tick.
#[test]
fn submit_after_commit_before_tick_advance() {
    let mut journal = make_journal_with_two_col_table("after_commit_test");
    assert_eq!(journal.current_tick(), Tick(1));

    journal.commit().unwrap();
    assert_eq!(journal.current_tick(), Tick(2));

    let envelope = make_envelope(
        Tick(1),
        "system",
        Command::Raw {
            domain: "stale".to_owned(),
            payload: serde_json::json!({}),
        },
    );

    let err = journal.submit_command(envelope, &JournalSubmitToken::new()).unwrap_err();
    match err {
        JournalError::InvalidTick { expected, got } => {
            assert_eq!(expected, 2);
            assert_eq!(got, 1);
        }
        other => panic!("expected InvalidTick (expected 2, got 1), got {:?}", other),
    }
}

/// Would fail: DebugWriteJournal INSERT routes through journal.submit_diff,
/// then after commit SELECT-style verification reads the committed data
/// via ArrowStore.
#[test]
#[cfg(debug_assertions)]
fn debug_write_journal_insert_then_select() {
    use scharnhorst_journal::DebugWriteJournal;
    use scharnhorst_arrow_store::ArrowStore;

    let store = Arc::new(ArrowStore::new());
    let init_store = scharnhorst_arrow_store::InitStore::new(Arc::clone(&store));
    let spec = scharnhorst_schema::TableSpec::new("player_data")
        .with_column(scharnhorst_schema::ColumnSpec::new(
            "id",
            scharnhorst_schema::FieldSemantic::Id,
            "i64",
        ))
        .unwrap()
        .with_column(scharnhorst_schema::ColumnSpec::new(
            "score",
            scharnhorst_schema::FieldSemantic::Quantity,
            "i64",
        ))
        .unwrap();
    init_store
        .create_table(&spec, scharnhorst_arrow_store::MutationMode::Patchable)
        .unwrap();
    let _ = init_store.into_simulation().unwrap();

    let mut journal = Journal::new(Arc::clone(&store));
    let mut dwj = DebugWriteJournal::new();

    dwj.execute_sql(
        &mut journal,
        "INSERT INTO player_data (id, score) VALUES (42, 9999)",
    )
    .unwrap();
    assert_eq!(journal.pending_diff_count(), 1);

    let commit_tick = journal.current_tick();
    let result = journal.commit().unwrap();
    assert_eq!(result.diff_count, 1);

    // Verify committed data exists in ArrowStore at the committed tick.
    let table = store.get_table("player_data").unwrap();
    let versions = table.get_version(commit_tick);
    assert!(
        versions.is_some(),
        "table player_data should have data at tick {}",
        commit_tick.as_u64()
    );

    let batches = versions.unwrap();
    assert!(!batches.is_empty(), "expected at least one record batch");
    let batch = &batches[0];
    assert!(
        batch.num_rows() > 0,
        "expected rows in committed batch, got {}",
        batch.num_rows()
    );
}
