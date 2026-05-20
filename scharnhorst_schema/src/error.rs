use thiserror::Error;

/// Errors raised by the schema subsystem.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("invalid table name: '{0}' — must be non-empty")]
    InvalidTableName(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("duplicate column name: {0}")]
    DuplicateColumn(String),

    #[error("relation already exists between {from} and {to}")]
    RelationAlreadyExists { from: String, to: String },

    #[error("relation not found: {from} -> {to}")]
    RelationNotFound { from: String, to: String },

    #[error("circular relation detected: {0}")]
    CircularRelation(String),

    #[error("invalid semantic: {0}")]
    InvalidSemantic(String),

    #[error("schema registry is frozen")]
    RegistryFrozen,

    /// Migration version mismatch: expected {expected}, found {actual}
    #[error("migration version mismatch: expected {expected}, found {actual}")]
    MigrationVersionMismatch { expected: String, actual: String },

    /// No migration path found from {from} to {to}
    #[error("no migration path found from {from} to {to}")]
    NoMigrationPath { from: String, to: String },

    /// Manifest serialization error
    #[error("manifest serialization failed: {0}")]
    ManifestSerialization(String),

    /// Manifest deserialization error
    #[error("manifest deserialization failed: {0}")]
    ManifestDeserialization(String),

    /// Phase error: operation not allowed in current phase
    #[error("phase error: {0}")]
    PhaseError(String),

    #[error("{0}")]
    Generic(String),

    #[error(transparent)]
    Core(#[from] scharnhorst_core::CoreError),
}

pub type SchemaResult<T> = Result<T, SchemaError>;
