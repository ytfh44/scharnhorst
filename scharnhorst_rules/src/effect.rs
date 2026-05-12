use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use scharnhorst_core::RowId;

use crate::expr::Expr;

/// An effect produced when a rule trigger evaluates to true.
///
/// Effects are evaluated by the Evaluator and produce state changes
/// submitted as Diff objects through the journal-system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
 /// Update a column value on the current scope row.
    UpdateColumn {
        table: String,
        column: String,
 /// Expression evaluated to produce the new value.
        value: Expr,
    },
 /// Insert a new row into a table.
    InsertRow {
        table: String,
        row: RowId,
 /// Column-name -> expression mappings, evaluated to produce row values.
        values: HashMap<String, Expr>,
    },
 /// Set a variable binding in the evaluator's scope.
    SetVariable {
        name: String,
        value: Expr,
    },
 /// Execute a sequence of effects in order.
    Sequence(Vec<Effect>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Expr;

    #[test]
    fn effect_update_column_construction() {
        let effect = Effect::UpdateColumn {
            table: "actor_state".to_owned(),
            column: "treasury".to_owned(),
            value: Expr::constant(scharnhorst_core::FixedPoint::from_i64(100, 2).unwrap()),
        };
        assert!(matches!(effect, Effect::UpdateColumn { .. }));
    }

    #[test]
    fn effect_insert_row_construction() {
        let mut values = HashMap::new();
        values.insert("treasury".to_owned(), Expr::constant(scharnhorst_core::FixedPoint::from_i64(500, 2).unwrap()));
        let effect = Effect::InsertRow {
            table: "actor_state".to_owned(),
            row: RowId::new(99),
            values,
        };
        assert!(matches!(effect, Effect::InsertRow { .. }));
    }

    #[test]
    fn effect_set_variable_construction() {
        let effect = Effect::SetVariable {
            name: "x".to_owned(),
            value: Expr::constant(scharnhorst_core::FixedPoint::from_i64(42, 2).unwrap()),
        };
        assert!(matches!(effect, Effect::SetVariable { .. }));
    }

    #[test]
    fn effect_sequence_construction() {
        let effects = vec![
            Effect::SetVariable {
                name: "x".to_owned(),
                value: Expr::constant(scharnhorst_core::FixedPoint::from_i64(1, 0).unwrap()),
            },
            Effect::SetVariable {
                name: "y".to_owned(),
                value: Expr::constant(scharnhorst_core::FixedPoint::from_i64(2, 0).unwrap()),
            },
        ];
        let seq = Effect::Sequence(effects);
        assert!(matches!(seq, Effect::Sequence(_)));
    }

    #[test]
    fn effect_serialization_roundtrip() {
        let effect = Effect::UpdateColumn {
            table: "actor_state".to_owned(),
            column: "treasury".to_owned(),
            value: Expr::constant(scharnhorst_core::FixedPoint::from_i64(100, 2).unwrap()),
        };
        let json = serde_json::to_string(&effect).unwrap();
        let back: Effect = serde_json::from_str(&json).unwrap();
        assert_eq!(effect, back);
    }
}
