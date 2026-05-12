use thiserror::Error;

/// Errors raised by the Arrow store subsystem.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ArrowStoreError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("partition not found: {0}")]
    PartitionNotFound(String),

    #[error("snapshot not found for tick {0}")]
    SnapshotNotFound(u64),

    #[error("tick not found: tick {tick} does not exist in table {table}")]
    TickNotFound { table: String, tick: u64 },

    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("primary key violation: duplicate key {key} in table {table}")]
    PrimaryKeyViolation { table: String, key: String },

    #[error("foreign key violation: key {key} in table {table} references missing row")]
    ForeignKeyViolation { table: String, key: String },

    #[error("invalid mutation mode for operation on table {0}")]
    InvalidMutationMode(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("arrow error: {0}")]
    Arrow(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("unimplemented: {0}")]
    Unimplemented(String),

    #[error("generic error: {0}")]
    Generic(String),
}

/// Shorthand result type used within the Arrow store crate.
pub type ArrowStoreResult<T> = Result<T, ArrowStoreError>;
