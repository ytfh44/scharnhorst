use thiserror::Error;

/// Unified error type for the save system.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SaveError {
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("snapshot already exists: {0}")]
    SnapshotAlreadyExists(String),

    #[error("corrupted snapshot: {0}")]
    CorruptedSnapshot(String),

    #[error("state hash mismatch: expected {expected}, got {got}")]
    StateHashMismatch { expected: u64, got: u64 },

    #[error("journal truncation failed: {0}")]
    JournalTruncation(String),

    #[error("journal replay failed at tick {tick}: {reason}")]
    JournalReplay { tick: u64, reason: String },

    #[error("migration failed from version {from} to {to}: {reason}")]
    MigrationFailed {
        from: String,
        to: String,
        reason: String,
    },

    #[error("no migration path from version {0}")]
    NoMigrationPath(String),

    #[error("mod fingerprint mismatch: mod={mod_id} expected={expected} got={got}")]
    ModFingerprintMismatch {
        mod_id: String,
        expected: String,
        got: String,
    },

    #[error("critical mod missing: {0}")]
    CriticalModMissing(String),

    #[error("mod load tolerance violation: {0}")]
    ModToleranceViolation(String),

    #[error("invalid load phase transition: {from} -> {to}")]
    InvalidPhaseTransition { from: String, to: String },

    #[error("load phase {0} already completed")]
    PhaseAlreadyCompleted(String),

    #[error("ipc serialization error: {0}")]
    IpcSerialization(String),

    #[error("ipc deserialization error: {0}")]
    IpcDeserialization(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("generic error: {0}")]
    Generic(String),

    #[error(transparent)]
    Core(#[from] scharnhorst_core::CoreError),

    #[error(transparent)]
    Schema(#[from] scharnhorst_schema::SchemaError),

    #[error(transparent)]
    ArrowStore(#[from] scharnhorst_arrow_store::ArrowStoreError),

    #[error(transparent)]
    Journal(#[from] scharnhorst_journal::JournalError),

    #[error(transparent)]
    Query(#[from] scharnhorst_query::QueryError),

    #[error(transparent)]
    Content(#[from] scharnhorst_content::ContentError),
}

/// Shorthand result type used within the save crate.
pub type SaveResult<T> = Result<T, SaveError>;
