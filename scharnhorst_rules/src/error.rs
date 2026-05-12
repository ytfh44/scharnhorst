use thiserror::Error;

/// The unified error type for the rules / IR evaluator subsystem.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RuleError {
    #[error("evaluation error: {0}")]
    Evaluation(String),

    #[error("scope error: {0}")]
    Scope(String),

    #[error("unknown relation: {0}")]
    UnknownRelation(String),

    #[error("unknown variable: {0}")]
    UnknownVariable(String),

    #[error("type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("cache error: {0}")]
    Cache(String),

    #[error("query engine error: {0}")]
    QueryEngine(String),

    #[error("journal wrapper error: {0}")]
    JournalWrapper(String),

    #[error("modifier error: {0}")]
    Modifier(String),

    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("generic error: {0}")]
    Generic(String),

    #[error(transparent)]
    Core(#[from] scharnhorst_core::CoreError),

    #[error(transparent)]
    Schema(#[from] scharnhorst_schema::SchemaError),

    #[error(transparent)]
    Query(#[from] scharnhorst_query::QueryError),

    #[error("journal error: {0}")]
    Journal(#[from] scharnhorst_journal::JournalError),
}

/// Shorthand result type used within the rules crate.
pub type RuleResult<T> = Result<T, RuleError>;

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_core::CoreError;

    #[test]
    fn evaluation_error_display() {
        let err = RuleError::Evaluation("bad expr".to_owned());
        assert_eq!(err.to_string(), "evaluation error: bad expr");
    }

    #[test]
    fn scope_error_display() {
        let err = RuleError::Scope("stack empty".to_owned());
        assert_eq!(err.to_string(), "scope error: stack empty");
    }

    #[test]
    fn unknown_relation_display() {
        let err = RuleError::UnknownRelation("province -> actor".to_owned());
        assert_eq!(err.to_string(), "unknown relation: province -> actor");
    }

    #[test]
    fn type_mismatch_display() {
        let err = RuleError::TypeMismatch {
            expected: "FixedPoint".to_owned(),
            got: "String".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "type mismatch: expected FixedPoint, got String"
        );
    }

    #[test]
    fn from_core_error_conversion() {
        let core = CoreError::InvalidFixedPointScale {
            expected: 2,
            got: 1,
        };
        let rule: RuleError = core.into();
        assert!(matches!(rule, RuleError::Core(_)));
    }
}