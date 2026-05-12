use thiserror::Error;

use scharnhorst_journal::error::JournalError;

/// The unified error type for the scheduler subsystem.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    #[error("system already registered: {0}")]
    SystemAlreadyRegistered(String),

    #[error("system not found: {0}")]
    SystemNotFound(String),

    #[error("dependency cycle detected: {0}")]
    DependencyCycle(String),

    #[error("write conflict in phase {phase}: systems {a} and {b} both write table {table}")]
    WriteConflict {
        phase: String,
        a: String,
        b: String,
        table: String,
    },

    #[error("commit failed at tick {tick}: {reason}")]
    CommitFailed { tick: u64, reason: String },

    #[error("invalid tick: expected {expected}, got {got}")]
    InvalidTick { expected: u64, got: u64 },

    #[error("journal error: {0}")]
    Journal(String),

    #[error("arrow store error: {0}")]
    ArrowStore(String),

    #[error("query engine error: {0}")]
    QueryEngine(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("refresh signal failed for consumer '{consumer}': {source}")]
    RefreshSignalFailed {
        consumer: String,
        source: Box<SchedulerError>,
    },

    #[error("rng stream error: {0}")]
    Rng(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("generic error: {0}")]
    Generic(String),
}

impl From<JournalError> for SchedulerError {
    fn from(err: JournalError) -> Self {
        SchedulerError::Journal(err.to_string())
    }
}

/// Shorthand result type used within the scheduler crate.
pub type SchedulerResult<T> = Result<T, SchedulerError>;
