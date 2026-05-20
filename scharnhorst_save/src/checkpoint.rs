use std::path::{Path, PathBuf};

use scharnhorst_core::{Tick, TierRegistry};
use scharnhorst_journal::{CommitRecord, JournalError, SaveJournal};

use crate::error::{SaveError, SaveResult};
use crate::snapshot_persistence::{PersistedSnapshot, SnapshotPersistence};

/// Retention policy for snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub max_snapshots: usize,
    pub auto_checkpoint_threshold: usize,
}

impl RetentionPolicy {
    pub const DEFAULT_MAX_SNAPSHOTS: usize = 3;
    pub const DEFAULT_AUTO_CHECKPOINT_THRESHOLD: usize = 1000;

    pub fn default_policy() -> Self {
        Self {
            max_snapshots: Self::DEFAULT_MAX_SNAPSHOTS,
            auto_checkpoint_threshold: Self::DEFAULT_AUTO_CHECKPOINT_THRESHOLD,
        }
    }

    pub fn with_max_snapshots(mut self, n: usize) -> Self {
        self.max_snapshots = n;
        self
    }

    pub fn with_auto_checkpoint_threshold(mut self, n: usize) -> Self {
        self.auto_checkpoint_threshold = n;
        self
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

/// Tracks the save journal and enforces retention / auto-checkpoint rules.
pub struct CheckpointManager {
    persistence: SnapshotPersistence,
    policy: RetentionPolicy,
    journal_path: PathBuf,
    journal_entries_since_snapshot: usize,
    tier_registry: Option<TierRegistry>,
}

impl CheckpointManager {
    pub fn new(
        base_dir: impl AsRef<Path>,
        policy: RetentionPolicy,
        journal_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            persistence: SnapshotPersistence::new(base_dir),
            policy,
            journal_path: journal_path.as_ref().to_path_buf(),
            journal_entries_since_snapshot: 0,
            tier_registry: None,
        }
    }

    /// Attach a TierRegistry for authority-table filtering during saves.
    pub fn with_tier_registry(mut self, tier_registry: TierRegistry) -> Self {
        self.persistence = self.persistence.with_tier_registry(tier_registry.clone());
        self.tier_registry = Some(tier_registry);
        self
    }

    pub fn tier_registry(&self) -> Option<&TierRegistry> {
        self.tier_registry.as_ref()
    }

    pub fn persistence(&self) -> &SnapshotPersistence {
        &self.persistence
    }

    pub fn policy(&self) -> RetentionPolicy {
        self.policy
    }

    pub fn journal_entries_since_snapshot(&self) -> usize {
        self.journal_entries_since_snapshot
    }

    /// Record that a journal entry was appended.
    pub fn record_journal_entry(&mut self) {
        self.journal_entries_since_snapshot += 1;
    }

    /// Reset the journal counter after a full snapshot.
    pub fn reset_journal_counter(&mut self) {
        self.journal_entries_since_snapshot = 0;
    }

    /// Determine whether an auto-checkpoint should be triggered.
    pub fn should_auto_checkpoint(&self) -> bool {
        self.journal_entries_since_snapshot >= self.policy.auto_checkpoint_threshold
    }

    /// Trigger auto-checkpoint if the threshold has been reached.
    ///
    /// Enforces retention policy (deletes oldest snapshots beyond
    /// `max_snapshots`). Does NOT truncate the on-disk journal because
    /// truncation without a covering snapshot would cause data loss
    /// on crash recovery. Full snapshot writing is deferred to a layer
    /// that holds world state (`ArrowStore` / `Journal`).
    ///
    /// Callers that DO have access to a current `PersistedSnapshot`
    /// should call [`write_checkpoint_snapshot`] instead, which
    /// performs the complete auto-checkpoint (snapshot write + retention
    /// + truncation).
    ///
    /// Returns `true` if the threshold was reached.
    pub fn try_auto_checkpoint(&mut self) -> SaveResult<bool> {
        if self.should_auto_checkpoint() {
            self.enforce_retention()?;
            self.reset_journal_counter();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Enforce the 3-snapshot retention window by deleting oldest snapshots.
    pub fn enforce_retention(&self) -> SaveResult<()> {
        let snapshots = self.persistence.list_snapshots()?;
        if snapshots.len() <= self.policy.max_snapshots {
            return Ok(());
        }
        let to_remove = snapshots.len() - self.policy.max_snapshots;
        snapshots
            .into_iter()
            .take(to_remove)
            .map(|(tick, _)| self.persistence.delete_snapshot(tick))
            .collect::<SaveResult<Vec<_>>>()
            .map(|_| ())
    }

    /// Write a full snapshot, enforce retention, and truncate the journal.
    ///
    /// Authority-table filtering is handled internally by
    /// [`SnapshotPersistence`] if a [`TierRegistry`] was attached.
    pub fn write_checkpoint_snapshot(&mut self, snapshot: &PersistedSnapshot) -> SaveResult<()> {
        self.persistence.write_snapshot(snapshot)?;
        self.enforce_retention()?;
        self.truncate_journal()?;
        self.reset_journal_counter();
        Ok(())
    }

    /// Truncate the on-disk save journal file.
    pub fn truncate_journal(&mut self) -> SaveResult<()> {
        if self.journal_path.exists() {
            std::fs::remove_file(&self.journal_path)
                .map_err(|e| SaveError::JournalTruncation(e.to_string()))?;
        }
        self.reset_journal_counter();
        Ok(())
    }

    /// Return the path to the save journal file.
    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }
}

/// In-memory save journal that also notifies a checkpoint manager on append.
pub struct CheckpointingSaveJournal {
    inner: Vec<CommitRecord>,
    manager: Option<CheckpointManager>,
}

impl CheckpointingSaveJournal {
    pub fn new() -> Self {
        Self {
            inner: Vec::new(),
            manager: None,
        }
    }

    pub fn with_manager(mut self, manager: CheckpointManager) -> Self {
        self.manager = Some(manager);
        self
    }

    pub fn records(&self) -> &[CommitRecord] {
        &self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &CommitRecord> {
        self.inner.iter()
    }

    pub fn records_after(&self, tick: Tick) -> impl Iterator<Item = &CommitRecord> {
        self.inner.iter().filter(move |r| r.tick > tick)
    }

    pub fn manager(&self) -> Option<&CheckpointManager> {
        self.manager.as_ref()
    }

    pub fn manager_mut(&mut self) -> Option<&mut CheckpointManager> {
        self.manager.as_mut()
    }
}

impl SaveJournal for CheckpointingSaveJournal {
    fn append(&mut self, record: &CommitRecord) -> scharnhorst_journal::JournalResult<()> {
        self.inner.push(record.clone());
        if let Some(manager) = self.manager.as_mut() {
            manager.record_journal_entry();
            manager
                .try_auto_checkpoint()
                .map_err(|e| JournalError::SaveJournal(e.to_string()))?;
        }
        Ok(())
    }

    fn flush(&mut self) -> scharnhorst_journal::JournalResult<()> {
        Ok(())
    }

    fn truncate_before(&mut self, tick: Tick) -> scharnhorst_journal::JournalResult<()> {
        self.inner.retain(|r| r.tick >= tick);
        Ok(())
    }
}

impl Default for CheckpointingSaveJournal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("sch_chk_test_{}_{}", std::process::id(), id))
    }

    fn cleanup(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn retention_policy_defaults() {
        let policy = RetentionPolicy::default();
        assert_eq!(policy.max_snapshots, 3);
        assert_eq!(policy.auto_checkpoint_threshold, 1000);
    }

    #[test]
    fn checkpoint_manager_tracks_entries() {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let journal = dir.join("journal.bin");
        let mut mgr = CheckpointManager::new(&dir, RetentionPolicy::default(), &journal);
        mgr.record_journal_entry();
        mgr.record_journal_entry();
        assert_eq!(mgr.journal_entries_since_snapshot(), 2);
        mgr.reset_journal_counter();
        assert!(!mgr.should_auto_checkpoint());
        cleanup(&dir);
    }

    #[test]
    fn auto_checkpoint_trigger() {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let journal = dir.join("journal.bin");
        let mut mgr = CheckpointManager::new(
            &dir,
            RetentionPolicy::default().with_auto_checkpoint_threshold(5),
            &journal,
        );
        for _ in 0..5 {
            mgr.record_journal_entry();
        }
        assert!(mgr.should_auto_checkpoint());
        cleanup(&dir);
    }

    #[test]
    fn checkpointing_journal_appends() {
        let mut journal = CheckpointingSaveJournal::new();
        let record = CommitRecord::new(Tick(1), Vec::new(), 0);
        journal.append(&record).unwrap();
        assert_eq!(journal.len(), 1);
    }
}
