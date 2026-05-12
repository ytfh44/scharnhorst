use serde::{Deserialize, Serialize};
use scharnhorst_core::Tick;

use crate::diff::Diff;

/// The outcome of an atomic commit cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResult {
 /// The tick that was committed.
    pub tick: Tick,
 /// Number of diffs applied.
    pub diff_count: usize,
 /// A deterministic hash representing the post-commit world state.
    pub state_hash: u64,
}

/// The current phase of the commit cycle for a given tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CommitPhase {
 /// Accepting commands and diffs for the current tick.
    Open,
 /// Commit is in progress; no further submissions accepted.
    Committing,
 /// Commit finished; snapshot published and consumers notified.
    Committed,
}

/// A record of a completed commit, suitable for persistence or replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
 /// The tick that was committed.
    pub tick: Tick,
 /// The diffs that were applied.
    pub diffs: Vec<Diff>,
 /// The resulting state hash.
    pub state_hash: u64,
}

impl CommitRecord {
 /// Create a new commit record.
    pub fn new(tick: Tick, diffs: Vec<Diff>, state_hash: u64) -> Self {
        Self {
            tick,
            diffs,
            state_hash,
        }
    }
}
