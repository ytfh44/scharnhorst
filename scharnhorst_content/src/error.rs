use thiserror::Error;

/// Errors raised by the content loader and compilation pipeline.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ContentError {
    #[error("overlay not found: {0}")]
    OverlayNotFound(String),

    #[error("overlay conflict: {0}")]
    OverlayConflict(String),

    #[error("name resolution failed: {0}")]
    NameResolutionFailed(String),

    #[error("duplicate name: {0}")]
    DuplicateName(String),

    #[error("compilation failed for table {table}: {reason}")]
    CompilationFailed { table: String, reason: String },

    #[error("schema mismatch: expected {expected}, got {got}")]
    SchemaMismatch { expected: String, got: String },

    #[error("mod fingerprint mismatch: mod={mod_id} expected={expected} got={got}")]
    ModFingerprintMismatch {
        mod_id: String,
        expected: String,
        got: String,
    },

    #[error("manifest serialization error: {0}")]
    ManifestSerialization(String),

    #[error("invalid lifecycle phase transition: {from} -> {to}")]
    InvalidPhaseTransition { from: String, to: String },

    #[error("lifecycle phase {0} already completed")]
    PhaseAlreadyCompleted(String),

    #[error("schema registry is frozen")]
    RegistryFrozen,

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
}

/// Shorthand result type used within the content crate.
pub type ContentResult<T> = Result<T, ContentError>;
