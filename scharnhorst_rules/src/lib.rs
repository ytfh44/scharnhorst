//! scharnhorst_rules: rule IR, evaluator, scope traversal, modifier aggregation,
//! and prefetch cache.
//!
//! The evaluator is a pure function over the current world snapshot. All reads
//! route through the query-engine ; all writes route through the journal
//! as [`Diff`] objects.

pub mod cache;
pub mod effect;
pub mod error;
pub mod evaluator;
pub mod expr;
pub mod modifier;
pub mod scope;

pub use cache::{CachedRow, PrefetchCache};
pub use effect::Effect;
pub use error::{RuleError, RuleResult};
pub use evaluator::{EvalValue, Evaluator};
pub use expr::{ArithmeticOp, ColumnPath, CompareOp, Expr, VarRef};
pub use modifier::{Modifier, ModifierOp, ModifierRegistry};
pub use scope::{JumpDirection, Scope, ScopeJump};
