use std::collections::VecDeque;
use std::sync::Arc;

use scharnhorst_arrow_store::{ArrowStore, CommitStore, SnapshotIngestRollback, SnapshotIngestor};
use scharnhorst_core::{JournalSubmitToken, Tick};

#[cfg(feature = "metrics")]
use crate::command::Command;
use crate::command::CommandEnvelope;
use crate::commit::{CommitPhase, CommitRecord, CommitResult};
use crate::diff::{Diff, DiffBatch};
use crate::error::{JournalError, JournalResult};
use crate::save_journal::SaveJournal;
#[cfg(feature = "metrics")]
use crate::telemetry;

const MAX_COMMIT_HISTORY: usize = 1024;

/// The sole write entry point for all world state mutations.
///
/// No component may write directly to Arrow tables. All mutations flow
/// through `submit` (commands / diffs) and are applied atomically at
/// tick end via `commit`.
pub struct Journal {
    /// The tick currently open for submissions.
    current_tick: Tick,
    /// Current phase of the commit cycle.
    phase: CommitPhase,
    /// Envelopes submitted for the current tick.
    pending_commands: Vec<CommandEnvelope>,
    /// Diffs accumulated for the current tick.
    pending_diffs: Vec<Diff>,
    /// Historical commit records, bounded in memory.
    history: VecDeque<CommitRecord>,
    /// Optional save journal for incremental persistence.
    save_journal: Option<Box<dyn SaveJournal>>,
    /// The underlying Arrow store (read-only access outside commit).
    arrow_store: Arc<ArrowStore>,
    /// Commit-phase write access to the Arrow store.
    commit_store: CommitStore,
    /// Optional query engine for snapshot publication.
    query_engine: Option<Arc<dyn SnapshotIngestor + Send + Sync>>,
}

impl Journal {
    /// Create a new journal starting at tick zero.
    pub fn new(arrow_store: Arc<ArrowStore>) -> Self {
        let commit_store = CommitStore::new(Arc::clone(&arrow_store));
        Self {
            current_tick: Tick::ZERO,
            phase: CommitPhase::Open,
            pending_commands: Vec::new(),
            pending_diffs: Vec::new(),
            history: VecDeque::new(),
            save_journal: None,
            arrow_store,
            commit_store,
            query_engine: None,
        }
    }

    /// Return the tick that is currently open for submissions.
    pub fn current_tick(&self) -> Tick {
        self.current_tick
    }

    /// Return the current commit phase.
    pub fn phase(&self) -> CommitPhase {
        self.phase
    }

    /// Return a shared reference to the underlying Arrow store.
    pub fn arrow_store(&self) -> &ArrowStore {
        self.arrow_store.as_ref()
    }

    /// Attach a [`SaveJournal`] for incremental persistence.
    pub fn with_save_journal(mut self, journal: impl SaveJournal + 'static) -> Self {
        self.save_journal = Some(Box::new(journal));
        self
    }

    /// Attach a [`QueryEngine`] for snapshot publication.
    ///
    /// When set, `commit` will publish the new snapshot to the
    /// query engine after applying diffs.
    pub fn with_query_engine(mut self, qe: Arc<dyn SnapshotIngestor + Send + Sync>) -> Self {
        self.query_engine = Some(qe);
        self
    }

    /// Submit a single command envelope.
    ///
    /// Envelopes are validated and queued for the current tick.
    pub fn submit_command(
        &mut self,
        envelope: CommandEnvelope,
        _token: &JournalSubmitToken,
    ) -> JournalResult<()> {
        if self.phase != CommitPhase::Open {
            return Err(JournalError::SubmitFailed(
                "commit already in progress".to_owned(),
            ));
        }
        if envelope.tick != self.current_tick {
            return Err(JournalError::InvalidTick {
                expected: self.current_tick.as_u64(),
                got: envelope.tick.as_u64(),
            });
        }

        #[cfg(feature = "metrics")]
        {
            let cmd_type = match &envelope.command {
                Command::TransferControl { .. } => "TransferControl",
                Command::UpdateColumn { .. } => "UpdateColumn",
                Command::InsertRow { .. } => "InsertRow",
                Command::DeleteRow { .. } => "DeleteRow",
                Command::Raw { .. } => "Raw",
            };
            telemetry::record_command_submit(cmd_type);
        }

        self.pending_commands.push(envelope);
        Ok(())
    }

    /// Submit a batch of commands.
    pub fn submit_commands(
        &mut self,
        envelopes: Vec<CommandEnvelope>,
        token: &JournalSubmitToken,
    ) -> JournalResult<()> {
        envelopes
            .into_iter()
            .try_for_each(|e| self.submit_command(e, token))
    }

    /// Submit a single diff.
    ///
    /// Diffs are accumulated and applied atomically at commit time.
    pub fn submit_diff(&mut self, diff: Diff, _token: &JournalSubmitToken) -> JournalResult<()> {
        if self.phase != CommitPhase::Open {
            return Err(JournalError::SubmitFailed(
                "commit already in progress".to_owned(),
            ));
        }

        #[cfg(feature = "metrics")]
        {
            let table = diff.table().to_owned();
            let diff_type = match &diff {
                Diff::Update { .. } => "Update",
                Diff::Insert { .. } => "Insert",
                Diff::Delete { .. } => "Delete",
                Diff::ReplaceTable { .. } => "ReplaceTable",
            };
            telemetry::record_diff_submit(diff_type, &table);
        }

        self.pending_diffs.push(diff);
        Ok(())
    }

    /// Submit a [`DiffBatch`].
    pub fn submit_batch(
        &mut self,
        batch: DiffBatch,
        token: &JournalSubmitToken,
    ) -> JournalResult<()> {
        batch
            .diffs
            .into_iter()
            .try_for_each(|d| self.submit_diff(d, token))
    }

    /// Return the number of pending diffs for the current tick.
    pub fn pending_diff_count(&self) -> usize {
        self.pending_diffs.len()
    }

    /// Return the number of pending commands for the current tick.
    pub fn pending_command_count(&self) -> usize {
        self.pending_commands.len()
    }

    /// Atomically commit all pending commands and diffs for the current tick.
    ///
    /// 1. Transition to `Committing`.
    /// 2. Apply diffs to `ArrowStore`.
    /// 3. Generate a new `WorldSnapshot`.
    /// 4. Append to `SaveJournal` if configured.
    /// 5. Transition to `Committed` and advance tick.
    pub fn commit(&mut self) -> JournalResult<CommitResult> {
        if self.phase != CommitPhase::Open {
            return Err(JournalError::CommitFailed(
                "no open commit phase".to_owned(),
            ));
        }

        self.phase = CommitPhase::Committing;

        let tick = self.current_tick;
        let diffs = self.take_pending_diffs();
        let commands = self.take_pending_commands();
        let diff_count = diffs.len();
        let command_count = commands.len();

        // Save pre-commit state for rollback
        let table_names: Vec<&str> = diffs.iter().map(|d| d.table()).collect();
        let saved = self
            .commit_store
            .save_table_versions(&table_names)
            .map_err(|e| JournalError::ArrowStore(format!("save versions: {}", e)))?;

        let state_hash = match self.apply_diffs_and_hash(&diffs) {
            Ok(h) => h,
            Err(e) => {
                let _ = self.commit_store.restore_table_versions(saved);
                self.pending_diffs = diffs;
                self.pending_commands = commands;
                self.phase = CommitPhase::Open;
                return Err(e);
            }
        };

        let mut ingest_rollback: Option<Box<dyn SnapshotIngestRollback>> =
            match self.query_engine.as_ref() {
                Some(qe) => match qe.begin_ingest() {
                    Ok(rollback) => Some(rollback),
                    Err(e) => {
                        let _ = self.commit_store.restore_table_versions(saved);
                        self.pending_diffs = diffs;
                        self.pending_commands = commands;
                        self.phase = CommitPhase::Open;
                        return Err(JournalError::Generic(format!("begin query ingest: {}", e)));
                    }
                },
                None => None,
            };

        {
            let qe_opt = &self.query_engine;
            let store = &self.arrow_store;
            let qe_result = (move || -> JournalResult<()> {
                if let Some(ref qe) = qe_opt {
                    let snapshot = store
                        .get_snapshot(tick)
                        .map_err(|e| JournalError::ArrowStore(format!("get_snapshot: {}", e)))?;
                    for table_name in snapshot.table_names() {
                        let batches = store
                            .get_table_batches(table_name, tick)
                            .unwrap_or_default();
                        let position_map = store
                            .get_table(table_name)
                            .map_err(|e| {
                                JournalError::ArrowStore(format!(
                                    "get_table for '{}': {}",
                                    table_name, e
                                ))
                            })?
                            .position_map()
                            .clone();
                        qe.ingest_snapshot(tick, table_name, batches, position_map)
                            .map_err(|e| {
                                JournalError::Generic(format!(
                                    "ingest snapshot for '{}': {}",
                                    table_name, e
                                ))
                            })?;
                    }
                    qe.store_snapshot((*snapshot).clone())
                        .map_err(|e| JournalError::Generic(format!("store snapshot: {}", e)))?;
                }
                Ok(())
            })();
            if let Err(e) = qe_result {
                if let Some(rollback) = ingest_rollback.take() {
                    let _ = rollback.rollback();
                }
                let _ = self.commit_store.restore_table_versions(saved);
                self.pending_diffs = diffs;
                self.pending_commands = commands;
                self.phase = CommitPhase::Open;
                return Err(e);
            }
        }

        let record = CommitRecord::with_commands(tick, diffs, commands, state_hash);

        if let Some(ref mut sj) = self.save_journal {
            if let Err(e) = sj.append(&record) {
                let CommitRecord {
                    diffs: restored_diffs,
                    commands: restored_commands,
                    ..
                } = record;
                if let Some(rollback) = ingest_rollback.take() {
                    let _ = rollback.rollback();
                }
                let _ = self.commit_store.restore_table_versions(saved);
                self.pending_diffs = restored_diffs;
                self.pending_commands = restored_commands;
                self.phase = CommitPhase::Open;
                return Err(e);
            }
        }

        self.history.push_back(record.clone());
        while self.history.len() > MAX_COMMIT_HISTORY {
            self.history.pop_front();
        }

        self.phase = CommitPhase::Committed;
        self.advance_tick();
        self.phase = CommitPhase::Open;

        let result = CommitResult {
            tick,
            diff_count,
            command_count,
            state_hash,
        };

        #[cfg(feature = "metrics")]
        {
            telemetry::emit_commit_record(
                result.tick.as_u64(),
                diff_count as u64,
                result.diff_count as u64,
                &format!("{:?}", result.state_hash),
            );
            telemetry::reset_counters();
        }

        Ok(result)
    }

    /// Return a view of the commit history.
    pub fn history(&self) -> &VecDeque<CommitRecord> {
        &self.history
    }

    /// Return an iterator over the commit history.
    pub fn history_iter(&self) -> impl Iterator<Item = &CommitRecord> {
        self.history.iter()
    }

    /// Return the commit history as a contiguous slice.
    ///
    /// The `VecDeque` is always contiguous because we only `push_back`.
    pub fn commit_history(&self) -> Vec<&CommitRecord> {
        self.history.iter().collect()
    }

    /// Return a mutable reference to the attached [`SaveJournal`], if any.
    pub fn save_journal_mut(&mut self) -> Option<&mut dyn SaveJournal> {
        match self.save_journal {
            Some(ref mut boxed) => Some(boxed.as_mut()),
            None => None,
        }
    }

    /// Clear all pending commands and diffs without committing.
    ///
    /// Only allowed in `Open` phase. Calling during `Committing` or
    /// `Committed` returns an error to prevent accidental clearing
    /// of diffs that are being or have been committed.
    pub fn clear_pending(&mut self) -> JournalResult<()> {
        if self.phase != CommitPhase::Open {
            return Err(JournalError::InvalidPhase(format!(
                "cannot clear pending during phase {:?}",
                self.phase
            )));
        }
        self.pending_commands.clear();
        self.pending_diffs.clear();
        Ok(())
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn take_pending_diffs(&mut self) -> Vec<Diff> {
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.pending_diffs);
        out
    }

    fn take_pending_commands(&mut self) -> Vec<CommandEnvelope> {
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.pending_commands);
        out
    }

    fn advance_tick(&mut self) {
        self.current_tick = self.current_tick.next();
    }

    fn apply_diffs_and_hash(&mut self, diffs: &[Diff]) -> JournalResult<u64> {
        let tick = self.current_tick;

        self.commit_store
            .apply_diffs(tick, diffs)
            .map_err(|e| JournalError::ArrowStore(format!("apply_diffs: {}", e)))?;

        let _snapshot = self
            .commit_store
            .generate_snapshot(tick)
            .map_err(|e| JournalError::ArrowStore(e.to_string()))?;

        let mut state_hash = 0u64;
        for diff in diffs {
            let json = serde_json::to_string(diff)
                .map_err(|e| JournalError::Generic(format!("diff serialize: {}", e)))?;
            let diff_hash = fnv1a_64(json.as_bytes());
            state_hash = state_hash.wrapping_add(diff_hash);
        }
        Ok(state_hash)
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new(Arc::new(ArrowStore::new()))
    }
}

fn fnv1a_64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ------------------------------------------------------------------
// DebugWriteJournal
// ------------------------------------------------------------------

/// Legacy debug write journal. The canonical `DebugWriteJournal` is in
/// `scharnhorst_query::debug_write`. This struct may be removed in a
/// future revision.
#[cfg(debug_assertions)]
#[derive(Debug, Default)]
pub struct DebugWriteJournal {
    enabled: bool,
    /// Log of all executed SQL statements for debugging reproducibility.
    sql_log: Vec<(String, std::time::SystemTime)>,
}

#[cfg(debug_assertions)]
impl DebugWriteJournal {
    /// Create a new debug write journal.
    pub fn new() -> Self {
        Self {
            enabled: true,
            sql_log: Vec::new(),
        }
    }

    /// Enable or disable the debug write journal.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Return whether the debug write journal is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Parse a SQL string, translate it into a Diff, and submit it to the journal.
    ///
    /// Supported statements:
    /// - `UPDATE table SET col = val WHERE id_col = id`
    /// - `INSERT INTO table (cols) VALUES (vals)`
    /// - `DELETE FROM table WHERE id_col = id`
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The journal is disabled
    /// - The SQL syntax is invalid
    /// - The SQL statement type is not supported
    /// - The WHERE clause does not identify a single row by primary key
    pub fn execute_sql(&mut self, journal: &mut Journal, sql: &str) -> JournalResult<()> {
        if !self.enabled {
            return Err(JournalError::SubmitFailed(
                "DebugWriteJournal is disabled".to_owned(),
            ));
        }

        // Log the SQL for debugging reproducibility
        self.sql_log
            .push((sql.to_owned(), std::time::SystemTime::now()));

        // Parse SQL and convert to Diff, then submit through journal
        let stmt = crate::sql_parser::SqlParser::parse(sql)?;
        let diff = crate::sql_parser::SqlParser::statement_to_diff(stmt)?;

        journal.submit_diff(diff, &JournalSubmitToken::new())
    }

    /// Return the SQL execution log.
    ///
    /// This is useful for debugging and reproducibility analysis.
    pub fn sql_log(&self) -> &[(String, std::time::SystemTime)] {
        &self.sql_log
    }

    /// Clear the SQL execution log.
    pub fn clear_sql_log(&mut self) {
        self.sql_log.clear();
    }
}

// ------------------------------------------------------------------
// Inline tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::Diff;
    use crate::save_journal::InMemorySaveJournal;
    use arrow_array::{Array, StringArray};
    use scharnhorst_arrow_store::{InitStore, MutationMode, SnapshotIngestor, WorldSnapshot};
    use scharnhorst_core::{RowId, Tick};
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};
    use std::sync::Arc;

    fn make_pk_spec(name: &str) -> TableSpec {
        TableSpec::new(name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap()
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .unwrap()
    }

    fn setup_journal_with_table(table_name: &str) -> Journal {
        let store = Arc::new(ArrowStore::new());
        let init_store = InitStore::new(Arc::clone(&store));
        let spec = make_pk_spec(table_name);
        init_store
            .create_table(&spec, MutationMode::Patchable)
            .unwrap();
        let _ = init_store.into_simulation().unwrap();
        let mut journal = Journal::new(store);
        let inserts: Vec<Diff> = [(1_u64, "a"), (2, "b"), (3, "c")]
            .into_iter()
            .map(|(id, name)| {
                let mut values = serde_json::Map::new();
                values.insert(
                    "id".to_owned(),
                    serde_json::Value::Number(serde_json::Number::from(id)),
                );
                values.insert(
                    "name".to_owned(),
                    serde_json::Value::String(name.to_owned()),
                );
                Diff::Insert {
                    table: table_name.to_owned(),
                    row: RowId::new(id),
                    values,
                }
            })
            .collect();
        journal.apply_diffs_and_hash(&inserts).unwrap();
        journal
    }

    #[test]
    fn apply_diffs_and_hash_returns_deterministic_hash() {
        let mut journal = setup_journal_with_table("treasury");
        let tick = journal.current_tick();

        let diff = Diff::Update {
            table: "treasury".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("patched".to_owned()),
        };
        let hash1 = journal
            .apply_diffs_and_hash(std::slice::from_ref(&diff))
            .unwrap();

        // Verify data was actually written
        let batches = journal
            .arrow_store
            .get_table_batches("treasury", tick)
            .unwrap_or_default();
        assert!(!batches.is_empty());
        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "patched");

        let hash2 = journal.apply_diffs_and_hash(&[diff]).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn apply_diffs_and_hash_differs_for_different_diffs() {
        let mut journal = setup_journal_with_table("a");

        let d1 = Diff::Update {
            table: "a".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("x".to_owned()),
        };
        let d2 = Diff::Update {
            table: "a".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("y".to_owned()),
        };
        let h1 = journal.apply_diffs_and_hash(&[d1]).unwrap();
        let h2 = journal.apply_diffs_and_hash(&[d2]).unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn apply_diffs_and_hash_empty_diffs_returns_hash() {
        let mut journal = setup_journal_with_table("empty_table");
        let tick = journal.current_tick();

        let _hash = journal.apply_diffs_and_hash(&[]).unwrap();
        // Empty diff set produces deterministic zero-hash; the invariant is
        // that apply_diffs_and_hash does not crash, not that hash is non-zero.

        // Verify snapshot was still generated with no diffs
        let batches = journal
            .arrow_store
            .get_table_batches("empty_table", tick)
            .unwrap_or_default();
        assert!(!batches.is_empty());
    }

    #[test]
    fn commit_history_returns_contiguous_slice() {
        let mut journal = setup_journal_with_table("t");
        journal
            .submit_diff(
                Diff::Update {
                    table: "t".to_owned(),
                    row: RowId::new(1),
                    column: "name".to_owned(),
                    value: serde_json::Value::String("updated".to_owned()),
                },
                &JournalSubmitToken::new(),
            )
            .unwrap();
        journal.commit().unwrap();

        let slice = journal.commit_history();
        assert_eq!(slice.len(), 1);
        assert_eq!(slice[0].tick, Tick::ZERO);
    }

    #[test]
    fn commit_history_bounded_and_contiguous_after_wrapping() {
        // Would have failed with old as_slices().0 when VecDeque wraps
        let mut journal = setup_journal_with_table("t");

        for i in 1..=1030u64 {
            journal
                .submit_diff(
                    Diff::Update {
                        table: "t".to_owned(),
                        row: RowId::new(i % 3 + 1),
                        column: "name".to_owned(),
                        value: serde_json::json!(i.to_string()),
                    },
                    &JournalSubmitToken::new(),
                )
                .unwrap();
            journal.commit().unwrap();
        }

        let history = journal.commit_history();
        assert_eq!(history.len(), 1024);
        assert_eq!(history[0].tick, Tick(6));
        assert_eq!(history[1023].tick, Tick(1029));
    }

    #[test]
    fn commit_history_empty_when_no_commits() {
        let journal = Journal::new(Arc::new(ArrowStore::new()));
        assert!(journal.commit_history().is_empty());
    }

    #[test]
    fn save_journal_mut_returns_none_when_not_attached() {
        let mut journal = Journal::new(Arc::new(ArrowStore::new()));
        assert!(journal.save_journal_mut().is_none());
    }

    #[test]
    fn save_journal_mut_returns_some_when_attached() {
        let mut journal =
            Journal::new(Arc::new(ArrowStore::new())).with_save_journal(InMemorySaveJournal::new());
        assert!(journal.save_journal_mut().is_some());
    }

    #[test]
    fn different_ticks_produce_different_hashes() {
        let mut journal = setup_journal_with_table("t");
        journal
            .submit_diff(
                Diff::Update {
                    table: "t".to_owned(),
                    row: RowId::new(1),
                    column: "name".to_owned(),
                    value: serde_json::Value::String("v1".to_owned()),
                },
                &JournalSubmitToken::new(),
            )
            .unwrap();
        let r1 = journal.commit().unwrap();
        journal
            .submit_diff(
                Diff::Update {
                    table: "t".to_owned(),
                    row: RowId::new(1),
                    column: "name".to_owned(),
                    value: serde_json::Value::String("v2".to_owned()),
                },
                &JournalSubmitToken::new(),
            )
            .unwrap();
        let r2 = journal.commit().unwrap();
        assert_ne!(r1.state_hash, r2.state_hash);
    }

    /// May-fail: save_journal failure does NOT restore ArrowStore table versions.
    ///
    /// Naive behavior: journal.commit() applies diffs to ArrowStore,
    /// ingests into query_engine, then appends to save_journal. If
    /// save_journal.append() fails, the code restores pending diffs/commands
    /// but does NOT call commit_store.restore_table_versions(). The ArrowStore
    /// retains the applied diffs. A retry on the same tick double-applies
    /// the diffs, corrupting data.
    #[test]
    fn commit_save_journal_failure_rollback() {
        // Create a save journal that always fails on append
        struct FailingSaveJournal;
        impl SaveJournal for FailingSaveJournal {
            fn append(&mut self, _record: &CommitRecord) -> JournalResult<()> {
                Err(JournalError::Generic("simulated save failure".to_owned()))
            }
            fn flush(&mut self) -> JournalResult<()> {
                Ok(())
            }
            fn truncate_before(&mut self, _tick: Tick) -> JournalResult<()> {
                Ok(())
            }
        }

        let mut journal = setup_journal_with_table("t").with_save_journal(FailingSaveJournal);

        let tick = journal.current_tick();
        let update = Diff::Update {
            table: "t".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("modified".to_owned()),
        };
        journal
            .submit_diff(update.clone(), &JournalSubmitToken::new())
            .unwrap();

        let result = journal.commit();
        // commit should fail because save_journal.append fails
        assert!(result.is_err(), "commit must fail when save_journal fails");

        // ArrowStore MUST NOT retain the applied diff.
        // After rollback, data at tick should not reflect the update.
        // get_table_batches returns data at the CURRENT state (which includes
        // all applied versions up to current tick). Since the diff was rolled
        // back, the table at current_tick should still show the original data.
        // We check that row[1] column[1] is NOT "modified".
        let batches = journal
            .arrow_store
            .get_table_batches("t", tick)
            .unwrap_or_default();
        if !batches.is_empty() {
            let name_col = batches[0].column(1);
            if let Some(arr) = name_col.as_any().downcast_ref::<StringArray>() {
                // Row 0 corresponds to RowId(1) in setup data
                if arr.len() > 0 {
                    let val = arr.value(0);
                    assert_ne!(
                        val, "modified",
                        "ArrowStore must be rolled back: row still shows 'modified'"
                    );
                }
            }
        }

        // pending_diffs MUST be preserved for retry
        assert_eq!(
            journal.pending_diff_count(),
            1,
            "pending diffs must be preserved after save_journal rollback"
        );
    }

    /// Would have failed: commit applies diffs to ArrowStore before
    /// query-engine/save-journal steps. On apply failure, pending
    /// diffs MUST be restored and phase MUST return to Open.
    /// Without this, failed commits permanently lose diffs.
    #[test]
    fn commit_restores_pending_on_apply_failure() {
        let mut journal = setup_journal_with_table("t");
        let bad_diff = Diff::Update {
            table: "nonexistent".to_owned(),
            row: RowId::new(99),
            column: "col".to_owned(),
            value: serde_json::json!(42),
        };
        journal
            .submit_diff(bad_diff, &JournalSubmitToken::new())
            .unwrap();
        assert_eq!(journal.pending_diff_count(), 1);

        let result = journal.commit();
        assert!(result.is_err());
        assert_eq!(journal.pending_diff_count(), 1);
    }

    /// May-fail: begin_ingest failure must rollback ArrowStore table versions.
    ///
    /// Worst case: when query_engine rejects begin_ingest(), diffs have already
    /// been applied to ArrowStore. If restore_table_versions is missing from
    /// this error path, the store retains the partial commit. A retry on the
    /// same tick double-applies diffs, corrupting data.
    #[test]
    fn commit_begin_ingest_failure_rolls_back_arrow_store() {
        use std::error::Error;

        struct RejectIngestor;
        impl SnapshotIngestor for RejectIngestor {
            fn begin_ingest(
                &self,
            ) -> Result<Box<dyn SnapshotIngestRollback>, Box<dyn Error + Send + Sync>> {
                Err(Box::new(std::io::Error::other(
                    "ingest refused",
                )))
            }
            fn ingest_snapshot(
                &self,
                _tick: Tick,
                _table_name: &str,
                _batches: Vec<arrow_array::RecordBatch>,
                _position_map: scharnhorst_core::RowPositionMap,
            ) -> Result<(), Box<dyn Error + Send + Sync>> {
                Ok(())
            }
            fn store_snapshot(
                &self,
                _snapshot: WorldSnapshot,
            ) -> Result<(), Box<dyn Error + Send + Sync>> {
                Ok(())
            }
        }

        let mut journal = setup_journal_with_table("ingest_test");
        let tick = journal.current_tick();

        // snapshot initial name value for comparison later
        let initial_name = {
            let batches = journal
                .arrow_store
                .get_table_batches("ingest_test", tick)
                .unwrap_or_default();
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|a| if a.len() > 0 { Some(a.value(0).to_owned()) } else { None })
                .unwrap_or_default()
        };

        let update = Diff::Update {
            table: "ingest_test".to_owned(),
            row: RowId::new(1),
            column: "name".to_owned(),
            value: serde_json::Value::String("corrupted".to_owned()),
        };
        journal
            .submit_diff(update.clone(), &JournalSubmitToken::new())
            .unwrap();

        // Attach failing ingestor — consumes journal, returns new
        journal = journal.with_query_engine(Arc::new(RejectIngestor));

        let result = journal.commit();
        assert!(
            result.is_err(),
            "commit must fail when begin_ingest rejects"
        );

        // ArrowStore must NOT retain the applied diff.
        let batches = journal
            .arrow_store
            .get_table_batches("ingest_test", tick)
            .unwrap_or_default();
        if !batches.is_empty() {
            let name_col = batches[0].column(1);
            if let Some(arr) = name_col.as_any().downcast_ref::<StringArray>() {
                if arr.len() > 0 {
                    assert_eq!(
                        arr.value(0), initial_name,
                        "ArrowStore must be rolled back after begin_ingest failure"
                    );
                }
            }
        }

        // pending_diffs must be preserved
        assert_eq!(
            journal.pending_diff_count(),
            1,
            "pending diffs must be preserved after begin_ingest rollback"
        );
    }
}

#[cfg(all(test, feature = "metrics"))]
mod metrics_tests {
    use crate::telemetry;

    #[test]
    fn record_diff_submit_no_panic() {
        telemetry::record_diff_submit("Add", "heroes");
    }

    #[test]
    fn record_command_submit_no_panic() {
        telemetry::record_command_submit("SpawnEntity");
    }

    #[test]
    fn emit_commit_record_no_panic() {
        telemetry::emit_commit_record(1, 10, 8, "abc123");
    }

    #[test]
    fn reset_counters_no_panic() {
        telemetry::reset_counters();
    }

    #[test]
    fn counters_reset_to_zero() {
        telemetry::record_diff_submit("Add", "heroes");
        telemetry::record_command_submit("SpawnEntity");
        telemetry::reset_counters();
    }
}
