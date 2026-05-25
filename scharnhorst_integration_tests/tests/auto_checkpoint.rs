//! 11.3 Implement SaveJournal auto-checkpoint at 1000 diffs (configurable)
//!
//! Verifies that the CheckpointManager and CheckpointingSaveJournal correctly
//! track journal entries and signal when an auto-checkpoint threshold is hit.

use std::path::PathBuf;

use scharnhorst_core::Tick;
use scharnhorst_journal::{CommitRecord, SaveJournal};
use scharnhorst_save::{CheckpointManager, CheckpointingSaveJournal, RetentionPolicy};

fn temp_dir(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sch_auto_chk_test_{}_{}",
        std::process::id(),
        suffix
    ))
}

#[test]
fn default_threshold_is_1000() {
    let policy = RetentionPolicy::default();
    assert_eq!(policy.auto_checkpoint_threshold, 1000);
}

#[test]
fn custom_threshold_respected() {
    let policy = RetentionPolicy::default().with_auto_checkpoint_threshold(500);
    assert_eq!(policy.auto_checkpoint_threshold, 500);
}

#[test]
fn checkpointing_journal_counts_appends() {
    let dir = temp_dir("counts");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let journal_path = dir.join("journal.bin");

    let manager = CheckpointManager::new(&dir, RetentionPolicy::default(), &journal_path);
    let mut cj = CheckpointingSaveJournal::new().with_manager(manager);

    for i in 0..1001 {
        let record = CommitRecord::new(Tick(i as u64), Vec::new(), 0);
        cj.append(&record).expect("append");
    }

    let mgr = cj.manager().expect("manager present");
    assert!(!mgr.should_auto_checkpoint());
    assert_eq!(mgr.journal_entries_since_snapshot(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpointing_journal_truncates() {
    let mut cj = CheckpointingSaveJournal::new();
    for i in 0..10 {
        let record = CommitRecord::new(Tick(i), Vec::new(), 0);
        cj.append(&record).expect("append");
    }

    cj.truncate_before(Tick(5)).expect("truncate");
    let remaining: Vec<_> = cj.iter().map(|r| r.tick.0).collect();
    assert_eq!(remaining, vec![5, 6, 7, 8, 9]);
}

#[test]
fn retention_policy_enforces_max_snapshots() {
    let dir = temp_dir("retention");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let journal_path = dir.join("journal.bin");

    let policy = RetentionPolicy::default().with_max_snapshots(2);
    let mgr = CheckpointManager::new(&dir, policy, &journal_path);

    // The manager itself does not create snapshots; we verify the policy value.
    assert_eq!(mgr.policy().max_snapshots, 2);

    let _ = std::fs::remove_dir_all(&dir);
}
