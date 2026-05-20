use scharnhorst_core::Tick;
use thiserror::Error;

/// The unified error type for the Bevy bridge subsystem.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BevyBridgeError {
    #[error("query engine error: {0}")]
    QueryEngine(String),

    #[error("scheduler error: {0}")]
    Scheduler(String),

    #[error("journal error: {0}")]
    Journal(String),

    #[error("arrow store error: {0}")]
    ArrowStore(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("snapshot not available for tick {0}")]
    SnapshotNotAvailable(u64),

    #[error("entity materialization failed for row {0}")]
    MaterializationFailed(u64),

    #[error("entity not found for row {0}")]
    EntityNotFound(u64),

    #[error("sync failed: {0}")]
    SyncFailed(String),

    #[error("input buffer error: {0}")]
    InputBuffer(String),

    #[error("refresh handler error: {0}")]
    RefreshHandler(String),

    #[error("command rejected: player commands only; AI and internal must bypass bridge")]
    NonPlayerCommandRejected,

    #[error("tick alignment error: expected tick {expected}, got {actual} - {reason}")]
    TickAlignmentError {
        expected: Tick,
        actual: Tick,
        reason: String,
    },

    #[error("lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("generic error: {0}")]
    Generic(String),
}

/// Shorthand result type used within the Bevy bridge crate.
pub type BevyBridgeResult<T> = Result<T, BevyBridgeError>;
