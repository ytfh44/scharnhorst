use std::collections::VecDeque;
use std::sync::Arc;

use scharnhorst_core::Tick;
use scharnhorst_arrow_store::{ArrowStore, SnapshotIngestor};

use crate::command::CommandEnvelope;
use crate::commit::{CommitPhase, CommitRecord, CommitResult};
use crate::diff::{Diff, DiffBatch};
use crate::error::{JournalError, JournalResult};
use crate::save_journal::SaveJournal;

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
 arrow_store: ArrowStore,
 /// Optional query engine for snapshot publication.
 query_engine: Option<Arc<dyn SnapshotIngestor + Send + Sync>>,
}

impl Journal {
 /// Create a new journal starting at tick zero.
 pub fn new(arrow_store: ArrowStore) -> Self {
 Self {
 current_tick: Tick::ZERO,
 phase: CommitPhase::Open,
 pending_commands: Vec::new(),
 pending_diffs: Vec::new(),
 history: VecDeque::new(),
 save_journal: None,
 arrow_store,
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
 &self.arrow_store
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
 pub fn submit_command(&mut self, envelope: CommandEnvelope) -> JournalResult<()> {
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
 self.pending_commands.push(envelope);
 Ok(())
 }

 /// Submit a batch of commands.
 pub fn submit_commands(&mut self, envelopes: Vec<CommandEnvelope>) -> JournalResult<()> {
 envelopes.into_iter().try_for_each(|e| self.submit_command(e))
 }

 /// Submit a single diff.
 ///
 /// Diffs are accumulated and applied atomically at commit time.
 pub fn submit_diff(&mut self, diff: Diff) -> JournalResult<()> {
 if self.phase != CommitPhase::Open {
 return Err(JournalError::SubmitFailed(
 "commit already in progress".to_owned(),
 ));
 }
 self.pending_diffs.push(diff);
 Ok(())
 }

 /// Submit a [`DiffBatch`].
 pub fn submit_batch(&mut self, batch: DiffBatch) -> JournalResult<()> {
 batch.diffs.into_iter().try_for_each(|d| self.submit_diff(d))
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
 let diff_count = diffs.len();

 let state_hash = self.apply_diffs_and_hash(&diffs)?;

 let record = CommitRecord::new(tick, diffs, state_hash);

 // Publish snapshot to query-engine after diff application.
 if let Some(ref qe) = self.query_engine {
 let snapshot = self
.arrow_store
.get_snapshot(tick)
.map_err(|e| JournalError::ArrowStore(format!("get_snapshot: {}", e)))?;
 for table_name in snapshot.table_names() {
 let batches = snapshot.table_batches(table_name)
.map_err(|e| JournalError::ArrowStore(format!(
 "table_batches for '{}': {}", table_name, e
 )))?;
 let vt = snapshot.get_table(table_name)
.map_err(|e| JournalError::ArrowStore(format!(
 "get_table for '{}': {}", table_name, e
 )))?;
 let position_map = vt.position_map().clone();
 qe.ingest_snapshot(tick, table_name, batches, position_map)
.map_err(|e| JournalError::Generic(format!(
 "ingest snapshot for '{}': {}", table_name, e
 )))?;
 }
 qe.store_snapshot((*snapshot).clone())
 .map_err(|e| JournalError::Generic(format!(
 "store snapshot: {}", e
 )))?;
 }

 if let Some(ref mut sj) = self.save_journal {
 sj.append(&record)?;
 }

 self.history.push_back(record.clone());

 self.phase = CommitPhase::Committed;
 self.pending_commands.clear();
 self.advance_tick();
 self.phase = CommitPhase::Open;

 Ok(CommitResult {
 tick,
 diff_count,
 state_hash,
 })
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
 pub fn commit_history(&self) -> &[CommitRecord] {
 self.history.as_slices().0
 }

 /// Return a mutable reference to the attached [`SaveJournal`], if any.
 pub fn save_journal_mut(&mut self) -> Option<&mut dyn SaveJournal> {
 match self.save_journal {
 Some(ref mut boxed) => Some(boxed.as_mut()),
 None => None,
 }
 }

 /// Clear all pending commands and diffs without committing.
 pub fn clear_pending(&mut self) {
 self.pending_commands.clear();
 self.pending_diffs.clear();
 }

 // ------------------------------------------------------------------
 // Internal helpers
 // ------------------------------------------------------------------

 fn take_pending_diffs(&mut self) -> Vec<Diff> {
 let mut out = Vec::new();
 std::mem::swap(&mut out, &mut self.pending_diffs);
 out
 }

 fn advance_tick(&mut self) {
 self.current_tick = self.current_tick.next();
 }

 fn apply_diffs_and_hash(&mut self, diffs: &[Diff]) -> JournalResult<u64> {
 use std::hash::{Hash, Hasher};

 let tick = self.current_tick;

 // Apply diffs to the Arrow store before snapshotting
 self.arrow_store
.apply_diffs(tick, diffs)
.map_err(|e| JournalError::ArrowStore(format!("apply_diffs: {}", e)))?;

 let _snapshot = self
.arrow_store
.generate_snapshot(tick)
.map_err(|e| JournalError::ArrowStore(e.to_string()))?;

 let mut hasher = std::collections::hash_map::DefaultHasher::new();
 tick.hash(&mut hasher);
 diffs.len().hash(&mut hasher);
 for diff in diffs {
 let json = serde_json::to_string(diff)
.map_err(|e| JournalError::Generic(format!("diff serialize: {}", e)))?;
 json.hash(&mut hasher);
 }
 Ok(hasher.finish())
 }
}

impl Default for Journal {
 fn default() -> Self {
 Self::new(ArrowStore::new())
 }
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
 pending_diffs: Vec<Diff>,
 /// Log of all executed SQL statements for debugging reproducibility.
 sql_log: Vec<(String, std::time::SystemTime)>,
}

#[cfg(debug_assertions)]
impl DebugWriteJournal {
 /// Create a new debug write journal.
 pub fn new() -> Self {
 Self {
 enabled: true,
 pending_diffs: Vec::new(),
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

 /// Parse a SQL string and translate it into pending diffs.
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
 pub fn execute_sql(&mut self, sql: &str) -> JournalResult<()> {
 if !self.enabled {
 return Err(JournalError::SubmitFailed(
 "DebugWriteJournal is disabled".to_owned(),
 ));
 }

 // Log the SQL for debugging reproducibility
 self.sql_log.push((sql.to_owned(), std::time::SystemTime::now()));

 // Parse SQL and convert to Diff
 let stmt = crate::sql_parser::SqlParser::parse(sql)?;
 let diff = crate::sql_parser::SqlParser::statement_to_diff(stmt)?;

 self.pending_diffs.push(diff);
 Ok(())
 }

 /// Drain all pending diffs and return them.
 ///
 /// This is called by the journal system at tick boundaries to apply
 /// the pending changes atomically.
 pub fn take_pending(&mut self) -> Vec<Diff> {
 let mut out = Vec::new();
 std::mem::swap(&mut out, &mut self.pending_diffs);
 out
 }

 /// Return the number of pending diffs.
 pub fn pending_count(&self) -> usize {
 self.pending_diffs.len()
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
 use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
 use arrow_schema::{DataType, Field};
 use scharnhorst_arrow_store::MutationMode;
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

 fn make_int_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
 let schema = Arc::new(arrow_schema::Schema::new(vec![
 Field::new("id", DataType::Int64, false),
 Field::new("name", DataType::Utf8, false),
 ]));
 RecordBatch::try_new(
 schema,
 vec![
 Arc::new(Int64Array::from(ids)) as ArrayRef,
 Arc::new(StringArray::from(names)) as ArrayRef,
 ],
 )
.unwrap()
 }

 fn setup_journal_with_table(table_name: &str) -> Journal {
 let store = ArrowStore::new();
 let spec = make_pk_spec(table_name);
 store.create_table(&spec, MutationMode::Patchable).unwrap();
 let mut journal = Journal::new(store);
 let inserts: Vec<Diff> = [(1_u64, "a"), (2, "b"), (3, "c")]
.into_iter()
.map(|(id, name)| {
 let mut values = serde_json::Map::new();
 values.insert(
 "id".to_owned(),
 serde_json::Value::Number(serde_json::Number::from(id)),
 );
 values.insert("name".to_owned(), serde_json::Value::String(name.to_owned()));
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
 let hash1 = journal.apply_diffs_and_hash(std::slice::from_ref(&diff)).unwrap();

 // Verify data was actually written
 let snap = journal.arrow_store.get_snapshot(tick).unwrap();
 let batches = snap.table_batches("treasury").unwrap();
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

 let hash = journal.apply_diffs_and_hash(&[]).unwrap();
 assert_ne!(hash, 0);

 // Verify snapshot was still generated with no diffs
 let snap = journal.arrow_store.get_snapshot(tick).unwrap();
 let batches = snap.table_batches("empty_table").unwrap();
 assert!(!batches.is_empty());
 }

 #[test]
 fn commit_history_returns_contiguous_slice() {
 let mut journal = setup_journal_with_table("t");
 journal
.submit_diff(Diff::Update {
 table: "t".to_owned(),
 row: RowId::new(1),
 column: "name".to_owned(),
 value: serde_json::Value::String("updated".to_owned()),
 })
.unwrap();
 journal.commit().unwrap();

 let slice = journal.commit_history();
 assert_eq!(slice.len(), 1);
 assert_eq!(slice[0].tick, Tick::ZERO);
 }

 #[test]
 fn commit_history_empty_when_no_commits() {
 let journal = Journal::new(ArrowStore::new());
 assert!(journal.commit_history().is_empty());
 }

 #[test]
 fn save_journal_mut_returns_none_when_not_attached() {
 let mut journal = Journal::new(ArrowStore::new());
 assert!(journal.save_journal_mut().is_none());
 }

 #[test]
 fn save_journal_mut_returns_some_when_attached() {
 let mut journal =
 Journal::new(ArrowStore::new()).with_save_journal(InMemorySaveJournal::new());
 assert!(journal.save_journal_mut().is_some());
 }

 #[test]
 fn different_ticks_produce_different_hashes() {
 let mut journal = setup_journal_with_table("t");
 journal
.submit_diff(Diff::Update {
 table: "t".to_owned(),
 row: RowId::new(1),
 column: "name".to_owned(),
 value: serde_json::Value::String("v1".to_owned()),
 })
.unwrap();
 let r1 = journal.commit().unwrap();
 journal
.submit_diff(Diff::Update {
 table: "t".to_owned(),
 row: RowId::new(1),
 column: "name".to_owned(),
 value: serde_json::Value::String("v2".to_owned()),
 })
.unwrap();
 let r2 = journal.commit().unwrap();
 assert_ne!(r1.state_hash, r2.state_hash);
 }
}
