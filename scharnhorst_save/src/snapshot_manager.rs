//! SnapshotManager: manages snapshot lifecycle and retention policy.
//!
//! Implements the 3-snapshot retention strategy as specified in :
//! - Keeps the most recent 3 full snapshots on disk
//! - Enables rollback to previous states
//! - SaveJournal is always relative to the most recent retained snapshot

use std::path::{Path, PathBuf};

use scharnhorst_core::Tick;
use scharnhorst_content::SchemaManifest;

use crate::error::{SaveError, SaveResult};
use crate::snapshot_persistence::{PersistedSnapshot, SnapshotHeader, SnapshotPersistence};

/// Configuration for snapshot retention and journal management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotConfig {
 /// Maximum number of snapshots to retain (default: 3)
    pub max_snapshots: usize,
 /// Maximum number of diffs in journal before auto-checkpoint (default: 1000)
    pub max_journal_diffs: usize,
}

impl SnapshotConfig {
 /// Default maximum number of snapshots to retain.
    pub const DEFAULT_MAX_SNAPSHOTS: usize = 3;
 /// Default maximum journal size before auto-checkpoint.
    pub const DEFAULT_MAX_JOURNAL_DIFFS: usize = 1000;

 /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

 /// Set the maximum number of snapshots to retain.
    pub fn with_max_snapshots(mut self, n: usize) -> Self {
        self.max_snapshots = n;
        self
    }

 /// Set the maximum journal size before auto-checkpoint.
    pub fn with_max_journal_diffs(mut self, n: usize) -> Self {
        self.max_journal_diffs = n;
        self
    }
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            max_snapshots: Self::DEFAULT_MAX_SNAPSHOTS,
            max_journal_diffs: Self::DEFAULT_MAX_JOURNAL_DIFFS,
        }
    }
}

/// Information about a snapshot file on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
 /// Generation number (tick) of the snapshot.
    pub generation: Tick,
 /// Path to the snapshot file.
    pub path: PathBuf,
}

impl SnapshotInfo {
 /// Create a new snapshot info.
    pub fn new(generation: Tick, path: PathBuf) -> Self {
        Self { generation, path }
    }
}

/// Manages snapshot lifecycle, retention policy, and rollback capabilities.
///
/// Implements the requirement: retains the most recent 3 full snapshots
/// on disk, enabling rollback to previous states.
pub struct SnapshotManager {
    persistence: SnapshotPersistence,
    config: SnapshotConfig,
    journal_path: PathBuf,
}

impl SnapshotManager {
 /// Create a new snapshot manager.
 ///
 /// # Arguments
 ///
 /// * `base_dir` - Directory where snapshots are stored
 /// * `config` - Configuration for retention policy
 /// * `journal_path` - Path to the save journal file
    pub fn new(
        base_dir: impl AsRef<Path>,
        config: SnapshotConfig,
        journal_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            persistence: SnapshotPersistence::new(base_dir),
            config,
            journal_path: journal_path.as_ref().to_path_buf(),
        }
    }

 /// Get the configuration.
    pub fn config(&self) -> &SnapshotConfig {
        &self.config
    }

 /// Get the base directory for snapshots.
    pub fn base_dir(&self) -> &Path {
        self.persistence.base_dir()
    }

 /// Get the journal path.
    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

 /// List all snapshots in the base directory, sorted by generation ascending.
 ///
 /// Returns a vector of (generation, path) tuples, sorted from oldest to newest.
    pub fn list_snapshots(&self) -> SaveResult<Vec<SnapshotInfo>> {
        let snapshots = self.persistence.list_snapshots()?;
        Ok(snapshots
            .into_iter()
            .map(|(generation, path)| SnapshotInfo::new(generation, path))
            .collect())
    }

 /// Get the latest (most recent) snapshot, if any.
    pub fn get_latest_snapshot(&self) -> SaveResult<Option<SnapshotInfo>> {
        let snapshots = self.list_snapshots()?;
        Ok(snapshots.into_iter().last())
    }

 /// Get a specific snapshot by generation.
    pub fn get_snapshot(&self, generation: Tick) -> SaveResult<Option<SnapshotInfo>> {
        let path = self.persistence.snapshot_path(generation);
        if path.exists() {
            Ok(Some(SnapshotInfo::new(generation, path)))
        } else {
            Ok(None)
        }
    }

 /// Get the snapshot for rollback to a specific generation.
 ///
 /// This returns the snapshot at the exact generation specified.
 /// The caller is responsible for replaying diffs from that generation
 /// to the desired tick.
 ///
 /// # Errors
 ///
 /// Returns `SaveError::SnapshotNotFound` if no snapshot exists at the given generation.
    pub fn get_snapshot_for_rollback(&self, generation: Tick) -> SaveResult<SnapshotInfo> {
        let path = self.persistence.snapshot_path(generation);
        if path.exists() {
            Ok(SnapshotInfo::new(generation, path))
        } else {
            Err(SaveError::SnapshotNotFound(format!(
                "generation {}",
                generation
            )))
        }
    }

 /// Read a snapshot's full data from disk.
    pub fn read_snapshot(&self, generation: Tick) -> SaveResult<PersistedSnapshot> {
        self.persistence.read_snapshot(generation)
    }

 /// Save a new snapshot and enforce retention policy.
 ///
 /// This method:
 /// 1. Writes the snapshot to a temporary file
 /// 2. Renames it to the final location (atomic operation)
 /// 3. Cleans up old snapshots if exceeding retention limit
 ///
 /// # Arguments
 ///
 /// * `snapshot` - The snapshot to save
 ///
 /// # Returns
 ///
 /// The path to the saved snapshot file.
    pub fn save_snapshot(&self, snapshot: &PersistedSnapshot) -> SaveResult<PathBuf> {
        let temp_path = self.temp_snapshot_path(snapshot.header.generation);
        let final_path = self.persistence.snapshot_path(snapshot.header.generation);

        if final_path.exists() {
            return Err(SaveError::SnapshotAlreadyExists(format!(
                "generation {}",
                snapshot.header.generation
            )));
        }

        self.write_snapshot_to_temp(snapshot, &temp_path)?;

        std::fs::rename(&temp_path, &final_path)
            .map_err(|e| SaveError::Io(format!("rename snapshot: {}", e)))?;

        self.cleanup_old_snapshots()?;

        Ok(final_path)
    }

 /// Write snapshot data to a temporary file.
    fn write_snapshot_to_temp(
        &self,
        snapshot: &PersistedSnapshot,
        temp_path: &Path,
    ) -> SaveResult<()> {
        if let Some(parent) = temp_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| SaveError::Io(format!("create temp dir: {}", e)))?;
            }
        }

        let header_bytes = serde_json::to_vec(&snapshot.header)
            .map_err(|e| SaveError::IpcSerialization(format!("header json: {}", e)))?;

        let mut file = std::fs::File::create(temp_path)
            .map_err(|e| SaveError::Io(format!("create temp snapshot: {}", e)))?;

        use std::io::Write;
        let header_len = header_bytes.len() as u64;
        file.write_all(&header_len.to_le_bytes())
            .map_err(|e| SaveError::Io(format!("write header len: {}", e)))?;
        file.write_all(&header_bytes)
            .map_err(|e| SaveError::Io(format!("write header: {}", e)))?;

        let table_count = snapshot.table_data.len() as u64;
        file.write_all(&table_count.to_le_bytes())
            .map_err(|e| SaveError::Io(format!("write table count: {}", e)))?;

        for (name, data) in &snapshot.table_data {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u64;
            file.write_all(&name_len.to_le_bytes())
                .map_err(|e| SaveError::Io(format!("write name len: {}", e)))?;
            file.write_all(name_bytes)
                .map_err(|e| SaveError::Io(format!("write name: {}", e)))?;
            let data_len = data.len() as u64;
            file.write_all(&data_len.to_le_bytes())
                .map_err(|e| SaveError::Io(format!("write data len: {}", e)))?;
            file.write_all(data)
                .map_err(|e| SaveError::Io(format!("write data: {}", e)))?;
        }

        file.flush()
            .map_err(|e| SaveError::Io(format!("flush temp snapshot: {}", e)))?;

        Ok(())
    }

 /// Get a temporary path for atomic snapshot writing.
    fn temp_snapshot_path(&self, generation: Tick) -> PathBuf {
        self.persistence
            .base_dir()
            .join(format!(".tmp_snapshot_gen_{}.arrow", generation))
    }

 /// Clean up old snapshots to enforce the retention policy.
 ///
 /// Deletes the oldest snapshots if the total count exceeds `max_snapshots`.
 /// This is called automatically after saving a new snapshot.
    pub fn cleanup_old_snapshots(&self) -> SaveResult<()> {
        let snapshots = self.list_snapshots()?;

        if snapshots.len() <= self.config.max_snapshots {
            return Ok(());
        }

        let to_remove = snapshots.len() - self.config.max_snapshots;
        let snapshots_to_delete: Vec<_> = snapshots.into_iter().take(to_remove).collect();

        for snapshot_info in snapshots_to_delete {
            self.persistence
                .delete_snapshot(snapshot_info.generation)?;
        }

        Ok(())
    }

 /// Delete a specific snapshot by generation.
    pub fn delete_snapshot(&self, generation: Tick) -> SaveResult<()> {
        self.persistence.delete_snapshot(generation)
    }

 /// Check if a snapshot exists for the given generation.
    pub fn has_snapshot(&self, generation: Tick) -> bool {
        self.persistence.snapshot_path(generation).exists()
    }

 /// Get the count of retained snapshots.
    pub fn snapshot_count(&self) -> SaveResult<usize> {
        self.list_snapshots().map(|v| v.len())
    }

 /// Determine if an auto-checkpoint should be triggered based on journal size.
 ///
 /// # Arguments
 ///
 /// * `journal_entries` - Number of entries in the current journal
    pub fn should_auto_checkpoint(&self, journal_entries: usize) -> bool {
        journal_entries >= self.config.max_journal_diffs
    }

 /// Create a new persisted snapshot with the given parameters.
    pub fn create_snapshot(
        &self,
        schema_manifest: SchemaManifest,
        generation: Tick,
        state_hash: u64,
    ) -> PersistedSnapshot {
        PersistedSnapshot::new(SnapshotHeader::new(schema_manifest, generation, state_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_content::SchemaManifest;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_dir() -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "sch_snap_mgr_test_{}_{}",
            std::process::id(),
            id
        ))
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    fn create_test_snapshot(manager: &SnapshotManager, generation: u64) -> SaveResult<PathBuf> {
        let manifest = SchemaManifest::new("1.0.0");
        let persisted = PersistedSnapshot::new(SnapshotHeader::new(
            manifest,
            Tick(generation),
            generation,
        ))
        .with_table_data("test_table", vec![generation as u8]);
        manager.save_snapshot(&persisted)
    }

    #[test]
    fn config_defaults() {
        let config = SnapshotConfig::default();
        assert_eq!(config.max_snapshots, 3);
        assert_eq!(config.max_journal_diffs, 1000);
    }

    #[test]
    fn config_custom_values() {
        let config = SnapshotConfig::new()
            .with_max_snapshots(5)
            .with_max_journal_diffs(500);
        assert_eq!(config.max_snapshots, 5);
        assert_eq!(config.max_journal_diffs, 500);
    }

    #[test]
    fn list_snapshots_sorted() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
 // Use a higher max_snapshots to avoid retention policy deleting snapshots during test
        let config = SnapshotConfig::new().with_max_snapshots(10);
        let manager = SnapshotManager::new(&dir, config, &journal);

        for gen in [100u64, 50, 200, 25] {
            create_test_snapshot(&manager, gen)?;
        }

        let snapshots = manager.list_snapshots()?;
        assert_eq!(snapshots.len(), 4);
        assert_eq!(snapshots[0].generation, Tick(25));
        assert_eq!(snapshots[1].generation, Tick(50));
        assert_eq!(snapshots[2].generation, Tick(100));
        assert_eq!(snapshots[3].generation, Tick(200));

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn get_latest_snapshot_returns_most_recent() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        assert!(manager.get_latest_snapshot()?.is_none());

        create_test_snapshot(&manager, 10)?;
        create_test_snapshot(&manager, 20)?;

        let latest = manager.get_latest_snapshot()?;
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().generation, Tick(20));

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn get_snapshot_for_rollback_existing() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        create_test_snapshot(&manager, 42)?;

        let snapshot = manager.get_snapshot_for_rollback(Tick(42))?;
        assert_eq!(snapshot.generation, Tick(42));

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn get_snapshot_for_rollback_missing() {
        let dir = temp_dir();
        cleanup(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        let result = manager.get_snapshot_for_rollback(Tick(999));
        assert!(matches!(result, Err(SaveError::SnapshotNotFound(_))));

        cleanup(&dir);
    }

    #[test]
    fn retention_policy_deletes_oldest() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let config = SnapshotConfig::new().with_max_snapshots(3);
        let manager = SnapshotManager::new(&dir, config, &journal);

        for gen in [1u64, 2, 3, 4, 5] {
            create_test_snapshot(&manager, gen)?;
        }

        let snapshots = manager.list_snapshots()?;
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].generation, Tick(3));
        assert_eq!(snapshots[1].generation, Tick(4));
        assert_eq!(snapshots[2].generation, Tick(5));

        assert!(!manager.has_snapshot(Tick(1)));
        assert!(!manager.has_snapshot(Tick(2)));
        assert!(manager.has_snapshot(Tick(3)));
        assert!(manager.has_snapshot(Tick(4)));
        assert!(manager.has_snapshot(Tick(5)));

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn retention_policy_keeps_exactly_max() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let config = SnapshotConfig::new().with_max_snapshots(3);
        let manager = SnapshotManager::new(&dir, config, &journal);

        for gen in [1u64, 2, 3] {
            create_test_snapshot(&manager, gen)?;
        }

        let snapshots = manager.list_snapshots()?;
        assert_eq!(snapshots.len(), 3);

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn cleanup_old_snapshots_respects_config() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let config = SnapshotConfig::new().with_max_snapshots(2);
        let manager = SnapshotManager::new(&dir, config, &journal);

        for gen in [10u64, 20, 30, 40] {
            create_test_snapshot(&manager, gen)?;
        }

        manager.cleanup_old_snapshots()?;

        let snapshots = manager.list_snapshots()?;
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].generation, Tick(30));
        assert_eq!(snapshots[1].generation, Tick(40));

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn auto_checkpoint_threshold() {
        let dir = temp_dir();
        cleanup(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let journal = dir.join("journal.bin");
        let config = SnapshotConfig::new().with_max_journal_diffs(100);
        let manager = SnapshotManager::new(&dir, config, &journal);

        assert!(!manager.should_auto_checkpoint(99));
        assert!(manager.should_auto_checkpoint(100));
        assert!(manager.should_auto_checkpoint(101));

        cleanup(&dir);
    }

    #[test]
    fn snapshot_count_returns_correct_value() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        assert_eq!(manager.snapshot_count()?, 0);

        create_test_snapshot(&manager, 1)?;
        assert_eq!(manager.snapshot_count()?, 1);

        create_test_snapshot(&manager, 2)?;
        assert_eq!(manager.snapshot_count()?, 2);

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn delete_snapshot_removes_file() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        create_test_snapshot(&manager, 42)?;
        assert!(manager.has_snapshot(Tick(42)));

        manager.delete_snapshot(Tick(42))?;
        assert!(!manager.has_snapshot(Tick(42)));

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn save_snapshot_is_atomic() -> SaveResult<()> {
        let dir = temp_dir();
        cleanup(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        let manifest = SchemaManifest::new("1.0.0");
        let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 0x1234))
            .with_table_data("actors", vec![1, 2, 3])
            .with_table_data("provinces", vec![4, 5, 6]);

        let path = manager.save_snapshot(&snapshot)?;
        assert!(path.exists());

        let temp_path = dir.join(".tmp_snapshot_gen_100.arrow");
        assert!(!temp_path.exists());

        let loaded = manager.read_snapshot(Tick(100))?;
        assert_eq!(loaded.header.generation, Tick(100));
        assert_eq!(loaded.header.state_hash, 0x1234);
        assert_eq!(loaded.table_data.len(), 2);

        cleanup(&dir);
        Ok(())
    }

    #[test]
    fn save_snapshot_prevents_overwrite() {
        let dir = temp_dir();
        cleanup(&dir);
        let _ = std::fs::create_dir_all(&dir);

        let journal = dir.join("journal.bin");
        let manager = SnapshotManager::new(&dir, SnapshotConfig::default(), &journal);

        let manifest = SchemaManifest::new("1.0.0");
        let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(100), 0x1234));

        manager.save_snapshot(&snapshot).unwrap();

        let snapshot2 = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 0x5678));
        let result = manager.save_snapshot(&snapshot2);

        assert!(matches!(result, Err(SaveError::SnapshotAlreadyExists(_))));

        cleanup(&dir);
    }
}
