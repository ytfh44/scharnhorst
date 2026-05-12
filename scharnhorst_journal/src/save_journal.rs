use scharnhorst_core::Tick;

use crate::commit::CommitRecord;
use crate::diff::Diff;
use crate::error::JournalResult;

/// Append-only sink for incremental persistence.
///
/// Each tick's committed diffs are serialized as a flat sequence of
/// `(tick_number, Vec<Diff>)` entries. Implementations may write to
/// files, IPC channels, or in-memory buffers.
pub trait SaveJournal: Send + Sync {
 /// Append a single commit record to the journal.
    fn append(&mut self, record: &CommitRecord) -> JournalResult<()>;

 /// Append a raw tick/diff pair (convenience overload).
    fn append_tick(&mut self, tick: Tick, diffs: &[Diff]) -> JournalResult<()> {
        let record = CommitRecord::new(tick, diffs.to_vec(), 0);
        self.append(&record)
    }

 /// Flush any buffered data to the underlying storage.
    fn flush(&mut self) -> JournalResult<()>;

 /// Truncate all entries before the given tick (e.g. after a full save).
    fn truncate_before(&mut self, tick: Tick) -> JournalResult<()>;
}

/// An in-memory [`SaveJournal`] useful for tests and debugging.
#[derive(Debug, Clone, Default)]
pub struct InMemorySaveJournal {
    entries: Vec<CommitRecord>,
}

impl InMemorySaveJournal {
 /// Create a new empty in-memory save journal.
    pub fn new() -> Self {
        Self::default()
    }

 /// Return all stored commit records.
    pub fn records(&self) -> &[CommitRecord] {
        &self.entries
    }

 /// Return an iterator over all stored commit records.
    pub fn iter(&self) -> impl Iterator<Item = &CommitRecord> {
        self.entries.iter()
    }

 /// Return the number of stored commit records.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

 /// Return true if no records are stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl SaveJournal for InMemorySaveJournal {
    fn append(&mut self, record: &CommitRecord) -> JournalResult<()> {
        self.entries.push(record.clone());
        Ok(())
    }

    fn flush(&mut self) -> JournalResult<()> {
        Ok(())
    }

    fn truncate_before(&mut self, tick: Tick) -> JournalResult<()> {
        self.entries.retain(|r| r.tick >= tick);
        Ok(())
    }
}
