use thiserror::Error;

/// The unified error type for the journal subsystem.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JournalError {
    #[error("commit failed: {0}")]
    CommitFailed(String),

    #[error("submit failed: {0}")]
    SubmitFailed(String),

    #[error("invalid journal phase for operation: {0}")]
    InvalidPhase(String),

    #[error("invalid tick: expected {expected}, got {got}")]
    InvalidTick { expected: u64, got: u64 },

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("row not found: {0}")]
    RowNotFound(String),

    #[error("diff validation failed: {0}")]
    DiffValidation(String),

    #[error("command validation failed: {0}")]
    CommandValidation(String),

    #[error("save journal error: {0}")]
    SaveJournal(String),

    #[error("arrow store error: {0}")]
    ArrowStore(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("generic error: {0}")]
    Generic(String),

    /// SQL parsing errors (debug builds only).
    #[error("SQL parse error: {0}")]
    SqlParse(String),

    /// SQL execution errors (debug builds only).
    #[error("SQL execution error: {0}")]
    SqlExecution(String),

    /// Unsupported SQL feature.
    #[error("unsupported SQL feature: {0}")]
    SqlUnsupported(String),

    /// SQL type conversion error.
    #[error("SQL type error: expected {expected}, got {actual}")]
    SqlTypeError { expected: String, actual: String },
}

/// Shorthand result type used within the journal crate.
pub type JournalResult<T> = Result<T, JournalError>;
