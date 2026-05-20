use thiserror::Error;

/// Errors raised by the query engine and related interfaces.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueryError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("column not found: {column} in table {table}")]
    ColumnNotFound { column: String, table: String },

    #[error("type mismatch for column {column}: expected {expected}, got {got}")]
    TypeMismatch {
        column: String,
        expected: String,
        got: String,
    },

    #[error("snapshot not available for tick {0}")]
    SnapshotNotAvailable(u64),

    #[error("schema registry error: {0}")]
    SchemaRegistry(String),

    #[error("arrow error: {0}")]
    Arrow(String),

    #[error("datafusion error: {0}")]
    DataFusion(String),

    #[error("SQL error: {0}")]
    Sql(String),

    #[error("unified read error: {0}")]
    UnifiedRead(String),

    #[error("inspector error: {0}")]
    Inspector(String),

    #[error("relation not found: {0}")]
    RelationNotFound(String),

    #[error("tick mismatch: requested {requested:?}, actual {actual:?}")]
    TicksMismatch {
        requested: scharnhorst_core::Tick,
        actual: Option<scharnhorst_core::Tick>,
    },

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("index out of bounds: {0}")]
    IndexOutOfBounds(usize),

    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("SQL write error: {0}")]
    SqlWrite(String),

    #[error(transparent)]
    Core(#[from] scharnhorst_core::CoreError),

    #[error(transparent)]
    Schema(#[from] scharnhorst_schema::SchemaError),

    #[error(transparent)]
    ArrowStore(#[from] scharnhorst_arrow_store::ArrowStoreError),
}

/// Shorthand result type used within the query crate.
pub type QueryResult<T> = Result<T, QueryError>;
