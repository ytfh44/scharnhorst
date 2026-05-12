use thiserror::Error;

/// The unified error type for the scharnhorst engine.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("invalid identifier: {0}")]
    InvalidId(String),

    #[error("arithmetic overflow")]
    ArithmeticOverflow,

    #[error("division by zero")]
    DivisionByZero,

    #[error("invalid fixed-point scale: expected {expected}, got {got}")]
    InvalidFixedPointScale { expected: u32, got: u32 },

    #[error("schema error: {0}")]
    Schema(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("generic: {0}")]
    Generic(String),
}

/// Shorthand result type used across crates.
pub type CoreResult<T> = Result<T, CoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_id_display() {
        let err = CoreError::InvalidId("bad_id".to_string());
        let msg = err.to_string();
        assert!(msg.contains("invalid identifier"));
        assert!(msg.contains("bad_id"));
    }

    #[test]
    fn arithmetic_overflow_display() {
        let err = CoreError::ArithmeticOverflow;
        let msg = err.to_string();
        assert!(msg.contains("overflow"));
    }

    #[test]
    fn division_by_zero_display() {
        let err = CoreError::DivisionByZero;
        let msg = err.to_string();
        assert!(msg.contains("division by zero"));
    }

    #[test]
    fn invalid_fixed_point_scale_display() {
        let err = CoreError::InvalidFixedPointScale {
            expected: 4,
            got: 2,
        };
        let msg = err.to_string();
        assert!(msg.contains("invalid fixed-point scale"));
        assert!(msg.contains("4"));
        assert!(msg.contains("2"));
    }

    #[test]
    fn schema_display() {
        let err = CoreError::Schema("test message".to_string());
        let msg = err.to_string();
        assert!(msg.contains("schema error"));
        assert!(msg.contains("test message"));
    }

    #[test]
    fn io_display() {
        let err = CoreError::Io("disk full".to_string());
        let msg = err.to_string();
        assert!(msg.contains("io error"));
        assert!(msg.contains("disk full"));
    }

    #[test]
    fn generic_display() {
        let err = CoreError::Generic("something happened".to_string());
        let msg = err.to_string();
        assert!(msg.contains("generic"));
        assert!(msg.contains("something happened"));
    }

    #[test]
    fn core_result_ok() {
        let result: CoreResult<i32> = Ok(42);
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn core_result_err() {
        let result: CoreResult<i32> = Err(CoreError::DivisionByZero);
        assert!(result.is_err());
    }

    #[test]
    fn debug_format() {
        let err = CoreError::InvalidId("x".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("InvalidId"));
    }
}
