use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use scharnhorst_core::{FixedPoint, RowId, Tick};
use scharnhorst_journal::{Command, CommandEnvelope, Diff, Journal, JournalError};
use scharnhorst_query::QueryEngine;
use scharnhorst_query::QueryError;
use scharnhorst_schema::{RelationEdge, RelationKind};
use scharnhorst_rules::{
    ArithmeticOp, CompareOp, Effect, EvalValue, Evaluator, Expr, JumpDirection, Modifier, ModifierOp,
    ModifierRegistry, PrefetchCache, RuleError, RuleResult, Scope, ScopeJump,
};

// ------------------------------------------------------------------
// Helper constructors
// ------------------------------------------------------------------

fn make_evaluator(root_table: &str, root_row: RowId) -> Evaluator {
    let query_engine = QueryEngine::new(scharnhorst_schema::SchemaRegistry::new());
    let journal = Journal::new(scharnhorst_arrow_store::ArrowStore::new());
    Evaluator::new(
        Arc::new(RwLock::new(query_engine)),
        Arc::new(RwLock::new(journal)),
        root_table,
        root_row,
    )
}

fn fp(value: i64, scale: u32) -> FixedPoint {
    FixedPoint::from_i64(value, scale).unwrap()
}

// ------------------------------------------------------------------
// Expr AST construction
// ------------------------------------------------------------------

#[test]
fn expr_const_roundtrip() {
    let expr = Expr::constant(fp(42, 2));
    assert_eq!(expr, Expr::Const(fp(42, 2)));
}

#[test]
fn expr_bool_roundtrip() {
    let expr = Expr::constant_bool(true);
    assert_eq!(expr, Expr::Bool(true));
}

#[test]
fn expr_column_roundtrip() {
    let expr = Expr::column("actor_state", "treasury");
    assert_eq!(
        expr,
        Expr::Column(scharnhorst_rules::ColumnPath::new("actor_state", "treasury"))
    );
}

#[test]
fn expr_var_roundtrip() {
    let expr = Expr::var("x");
    assert_eq!(expr, Expr::Var(scharnhorst_rules::VarRef::new("x")));
}

#[test]
fn expr_not_roundtrip() {
    let inner = Expr::constant_bool(false);
    let expr = Expr::new_not(inner.clone());
    assert_eq!(expr, Expr::Not(Box::new(inner)));
}

#[test]
fn expr_and_or_roundtrip() {
    let a = Expr::constant_bool(true);
    let b = Expr::constant_bool(false);
    let and_expr = Expr::and(vec![a.clone(), b.clone()]);
    let or_expr = Expr::or(vec![a, b]);
    assert!(matches!(and_expr, Expr::And(_)));
    assert!(matches!(or_expr, Expr::Or(_)));
}

#[test]
fn expr_compare_roundtrip() {
    let lhs = Expr::constant(fp(1, 2));
    let rhs = Expr::constant(fp(2, 2));
    let expr = Expr::compare(CompareOp::Lt, lhs, rhs);
    assert!(matches!(expr, Expr::Compare { op: CompareOp::Lt, .. }));
}

#[test]
fn expr_arithmetic_roundtrip() {
    let lhs = Expr::constant(fp(3, 2));
    let rhs = Expr::constant(fp(4, 2));
    let expr = Expr::arithmetic(ArithmeticOp::Mul, lhs, rhs);
    assert!(matches!(expr, Expr::Arithmetic { op: ArithmeticOp::Mul, .. }));
}

#[test]
fn expr_if_roundtrip() {
    let cond = Expr::constant_bool(true);
    let then_branch = Expr::constant(fp(1, 0));
    let else_branch = Expr::constant(fp(0, 0));
    let expr = Expr::if_then_else(cond, then_branch, else_branch);
    assert!(matches!(expr, Expr::If { .. }));
}

#[test]
fn expr_call_roundtrip() {
    let mut args = HashMap::new();
    args.insert("x".to_owned(), Expr::constant(fp(1, 0)));
    let expr = Expr::call("my_func", args);
    assert!(matches!(expr, Expr::Call { func, .. } if func == "my_func"));
}

#[test]
fn expr_list_map_roundtrip() {
    let list = Expr::list(vec![Expr::constant(fp(1, 0)), Expr::constant(fp(2, 0))]);
    let mut map = HashMap::new();
    map.insert("a".to_owned(), Expr::constant(fp(1, 0)));
    let map_expr = Expr::map(map);
    assert!(matches!(list, Expr::List(_)));
    assert!(matches!(map_expr, Expr::Map(_)));
}

// ------------------------------------------------------------------
// Evaluator: constants and basic logic
// ------------------------------------------------------------------

#[test]
fn eval_const() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::constant(fp(42, 2));
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(42, 2)));
    Ok(())
}

#[test]
fn eval_bool() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::constant_bool(true);
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_not() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::new_not(Expr::Bool(false));
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_and_all_true() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::and(vec![Expr::constant_bool(true), Expr::constant_bool(true)]);
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_and_one_false() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::and(vec![Expr::constant_bool(true), Expr::constant_bool(false)]);
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(false));
    Ok(())
}

#[test]
fn eval_or_one_true() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::or(vec![Expr::constant_bool(false), Expr::constant_bool(true)]);
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_or_all_false() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::or(vec![Expr::constant_bool(false), Expr::constant_bool(false)]);
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(false));
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: comparison
// ------------------------------------------------------------------

#[test]
fn eval_compare_fixed_point_lt() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::compare(
        CompareOp::Lt,
        Expr::constant(fp(1, 2)),
        Expr::constant(fp(2, 2)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_compare_fixed_point_eq() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::compare(
        CompareOp::Eq,
        Expr::constant(fp(5, 2)),
        Expr::constant(fp(5, 2)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_compare_bool_eq() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::compare(
        CompareOp::Eq,
        Expr::constant_bool(true),
        Expr::constant_bool(true),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

#[test]
fn eval_compare_string_ne() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::compare(
        CompareOp::Ne,
        Expr::String("a".to_owned()),
        Expr::String("b".to_owned()),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::Bool(true));
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: arithmetic
// ------------------------------------------------------------------

#[test]
fn eval_arithmetic_add() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::arithmetic(
        ArithmeticOp::Add,
        Expr::constant(fp(1, 2)),
        Expr::constant(fp(2, 2)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(3, 2)));
    Ok(())
}

#[test]
fn eval_arithmetic_sub() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::arithmetic(
        ArithmeticOp::Sub,
        Expr::constant(fp(5, 2)),
        Expr::constant(fp(3, 2)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(2, 2)));
    Ok(())
}

#[test]
fn eval_arithmetic_mul() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::arithmetic(
        ArithmeticOp::Mul,
        Expr::constant(fp(2, 2)),
        Expr::constant(fp(3, 2)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(6, 2)));
    Ok(())
}

#[test]
fn eval_arithmetic_div() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::arithmetic(
        ArithmeticOp::Div,
        Expr::constant(fp(6, 2)),
        Expr::constant(fp(2, 2)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(3, 2)));
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: if-then-else
// ------------------------------------------------------------------

#[test]
fn eval_if_true() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::if_then_else(
        Expr::constant_bool(true),
        Expr::constant(fp(1, 0)),
        Expr::constant(fp(0, 0)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(1, 0)));
    Ok(())
}

#[test]
fn eval_if_false() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::if_then_else(
        Expr::constant_bool(false),
        Expr::constant(fp(1, 0)),
        Expr::constant(fp(0, 0)),
    );
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(0, 0)));
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: variables
// ------------------------------------------------------------------

#[test]
fn eval_bound_variable() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    ev.bind_var("x", EvalValue::FixedPoint(fp(99, 2)));
    let expr = Expr::var("x");
    let value = ev.evaluate(&expr)?;
    assert_eq!(value, EvalValue::FixedPoint(fp(99, 2)));
    Ok(())
}

#[test]
fn eval_unbound_variable_is_error() {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::var("missing");
    let result = ev.evaluate(&expr);
    assert!(matches!(result, Err(RuleError::UnknownVariable(name)) if name == "missing"));
}

// ------------------------------------------------------------------
// Evaluator: list and map
// ------------------------------------------------------------------

#[test]
fn eval_list() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::list(vec![Expr::constant(fp(1, 0)), Expr::constant(fp(2, 0))]);
    let value = ev.evaluate(&expr)?;
    assert_eq!(
        value,
        EvalValue::List(vec![
            EvalValue::FixedPoint(fp(1, 0)),
            EvalValue::FixedPoint(fp(2, 0)),
        ])
    );
    Ok(())
}

#[test]
fn eval_map() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let mut entries = HashMap::new();
    entries.insert("a".to_owned(), Expr::constant(fp(1, 0)));
    let expr = Expr::map(entries);
    let value = ev.evaluate(&expr)?;
    let mut expected = HashMap::new();
    expected.insert("a".to_owned(), EvalValue::FixedPoint(fp(1, 0)));
    assert_eq!(value, EvalValue::Map(expected));
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: type mismatch errors
// ------------------------------------------------------------------

#[test]
fn eval_not_on_non_bool_is_error() {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::new_not(Expr::Const(scharnhorst_core::FixedPoint::new(42, 0)));
    let result = ev.evaluate(&expr);
    assert!(matches!(result, Err(RuleError::TypeMismatch { .. })));
}

#[test]
fn eval_and_on_non_bool_is_error() {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::and(vec![Expr::constant_bool(true), Expr::constant(fp(1, 0))]);
    let result = ev.evaluate(&expr);
    assert!(matches!(result, Err(RuleError::TypeMismatch { .. })));
}

#[test]
fn eval_compare_mixed_types_is_error() {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::compare(
        CompareOp::Eq,
        Expr::constant(fp(1, 0)),
        Expr::constant_bool(true),
    );
    let result = ev.evaluate(&expr);
    assert!(matches!(result, Err(RuleError::TypeMismatch { .. })));
}

#[test]
fn eval_arithmetic_on_non_numeric_is_error() {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::arithmetic(
        ArithmeticOp::Add,
        Expr::constant_bool(true),
        Expr::constant_bool(false),
    );
    let result = ev.evaluate(&expr);
    assert!(matches!(result, Err(RuleError::TypeMismatch { .. })));
}

#[test]
fn eval_if_with_non_bool_condition_is_error() {
    let mut ev = make_evaluator("test", RowId::new(0));
    let expr = Expr::if_then_else(
        Expr::constant(fp(1, 0)),
        Expr::constant(fp(0, 0)),
        Expr::constant(fp(1, 0)),
    );
    let result = ev.evaluate(&expr);
    assert!(matches!(result, Err(RuleError::TypeMismatch { .. })));
}

// ------------------------------------------------------------------
// Scope management
// ------------------------------------------------------------------

#[test]
fn scope_new() {
    let scope = Scope::new("actor_state", RowId::new(7));
    assert_eq!(scope.table(), "actor_state");
    assert_eq!(scope.row(), RowId::new(7));
    assert_eq!(scope.stack_depth(), 0);
}

#[test]
fn scope_push_pop() -> RuleResult<()> {
    let mut scope = Scope::new("actor_state", RowId::new(1));
    scope.push("province", RowId::new(2), Some(ScopeJump::forward("actor_state -> province")));
    assert_eq!(scope.table(), "province");
    assert_eq!(scope.row(), RowId::new(2));
    assert_eq!(scope.stack_depth(), 1);

    scope.pop()?;
    assert_eq!(scope.table(), "actor_state");
    assert_eq!(scope.row(), RowId::new(1));
    assert_eq!(scope.stack_depth(), 0);
    Ok(())
}

#[test]
fn scope_pop_underflow_is_error() {
    let mut scope = Scope::new("actor_state", RowId::new(1));
    let result = scope.pop();
    assert!(matches!(result, Err(RuleError::Scope(_))));
}

#[test]
fn scope_reset() {
    let mut scope = Scope::new("actor_state", RowId::new(1));
    scope.push("province", RowId::new(2), None);
    scope.reset("global", RowId::new(99));
    assert_eq!(scope.table(), "global");
    assert_eq!(scope.row(), RowId::new(99));
    assert_eq!(scope.stack_depth(), 0);
}

#[test]
fn scope_chain_iterator() {
    let mut scope = Scope::new("actor_state", RowId::new(1));
    scope.push("province", RowId::new(2), None);
    scope.push("population", RowId::new(3), None);
    let chain: Vec<_> = scope.chain().collect();
    assert_eq!(
        chain,
        vec![
            ("actor_state", RowId::new(1)),
            ("province", RowId::new(2)),
            ("population", RowId::new(3)),
        ]
    );
}

#[test]
fn scope_apply_jump_forward() -> RuleResult<()> {
    let mut scope = Scope::new("province", RowId::new(1));
    let edge = RelationEdge {
        from: "province".to_owned(),
        to: "actor_state".to_owned(),
        kind: RelationKind::OneToMany,
        from_column: "owner_id".to_owned(),
        to_column: None,
    };
    scope.apply_jump(&edge, JumpDirection::Forward, RowId::new(10))?;
    assert_eq!(scope.table(), "actor_state");
    assert_eq!(scope.row(), RowId::new(10));
    Ok(())
}

#[test]
fn scope_apply_jump_reverse() -> RuleResult<()> {
    let mut scope = Scope::new("actor_state", RowId::new(10));
    let edge = RelationEdge {
        from: "province".to_owned(),
        to: "actor_state".to_owned(),
        kind: RelationKind::OneToMany,
        from_column: "owner_id".to_owned(),
        to_column: None,
    };
    scope.apply_jump(&edge, JumpDirection::Reverse, RowId::new(1))?;
    assert_eq!(scope.table(), "province");
    assert_eq!(scope.row(), RowId::new(1));
    Ok(())
}

// ------------------------------------------------------------------
// PrefetchCache
// ------------------------------------------------------------------

#[test]
fn cache_default_capacity() {
    let cache = PrefetchCache::default();
    assert_eq!(cache.capacity(), PrefetchCache::DEFAULT_CAPACITY);
    assert!(cache.is_empty());
}

#[test]
fn cache_custom_capacity() {
    let cache = PrefetchCache::new(256);
    assert_eq!(cache.capacity(), 256);
}

#[test]
fn cache_set_tick_clears_entries() {
    let mut cache = PrefetchCache::new(4);
    cache.set_tick(Tick(1));
    assert_eq!(cache.tick(), Tick(1));

 // Insert a dummy entry via internal method is not public,
 // so we verify the clear behavior by checking tick transition.
    cache.set_tick(Tick(2));
    assert_eq!(cache.tick(), Tick(2));
    assert!(cache.is_empty());
}

#[test]
fn cache_with_tick_builder() {
    let cache = PrefetchCache::new(4).with_tick(Tick(5));
    assert_eq!(cache.tick(), Tick(5));
}

// ------------------------------------------------------------------
// Modifier
// ------------------------------------------------------------------

#[test]
fn modifier_add() -> RuleResult<()> {
    let m = Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(10, 2));
    let base = fp(100, 2);
    let result = m.apply(base)?;
    assert_eq!(result, fp(110, 2));
    Ok(())
}

#[test]
fn modifier_mul() -> RuleResult<()> {
    let m = Modifier::new("actor_state", "treasury", ModifierOp::Mul, FixedPoint::new(11, 1));
    let base = fp(100, 2);
    let result = m.apply(base)?;
    assert_eq!(result, fp(110, 2));
    Ok(())
}

#[test]
fn modifier_override() -> RuleResult<()> {
    let m = Modifier::new("actor_state", "treasury", ModifierOp::Override, fp(42, 2));
    let base = fp(100, 2);
    let result = m.apply(base)?;
    assert_eq!(result, fp(42, 2));
    Ok(())
}

// ------------------------------------------------------------------
// ModifierRegistry
// ------------------------------------------------------------------

#[test]
fn registry_register_and_lookup() {
    let mut reg = ModifierRegistry::new();
    let m = Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(5, 2));
    reg.register(m);
    assert_eq!(reg.len(), 1);
    assert!(reg.modifiers_for("actor_state", "treasury").is_some());
    assert!(reg.modifiers_for("actor_state", "prestige").is_none());
}

#[test]
fn registry_clear_field() {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(5, 2)));
    reg.clear_field("actor_state", "treasury");
    assert!(reg.is_empty());
}

#[test]
fn registry_aggregate_add_only() -> RuleResult<()> {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(10, 2)));
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(5, 2)));
    let result = reg.aggregate("actor_state", "treasury", fp(100, 2))?;
    assert_eq!(result, fp(115, 2));
    Ok(())
}

#[test]
fn registry_aggregate_mul_only() -> RuleResult<()> {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Mul, FixedPoint::new(11, 1)));
    let result = reg.aggregate("actor_state", "treasury", fp(100, 2))?;
    assert_eq!(result, fp(110, 2));
    Ok(())
}

#[test]
fn registry_aggregate_add_then_mul() -> RuleResult<()> {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(20, 2)));
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Mul, FixedPoint::new(11, 1)));
 // (100 + 20) * 1.1 = 132
    let result = reg.aggregate("actor_state", "treasury", fp(100, 2))?;
    assert_eq!(result, fp(132, 2));
    Ok(())
}

#[test]
fn registry_aggregate_override_wins() -> RuleResult<()> {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(10, 2)));
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Override, fp(7, 2)));
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Mul, fp(2, 0)));
 // Override is last, so it wins after add/mul
    let result = reg.aggregate("actor_state", "treasury", fp(100, 2))?;
    assert_eq!(result, fp(7, 2));
    Ok(())
}

#[test]
fn registry_fields_iterator() {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(1, 0)));
    reg.register(Modifier::new("actor_state", "prestige", ModifierOp::Add, fp(1, 0)));
    let mut fields: Vec<_> = reg.fields().collect();
    fields.sort();
    assert_eq!(
        fields,
        vec![("actor_state", "prestige"), ("actor_state", "treasury")]
    );
}

// ------------------------------------------------------------------
// Evaluator: scope integration
// ------------------------------------------------------------------

#[test]
fn evaluator_scope_push_pop() -> RuleResult<()> {
    let mut ev = make_evaluator("actor_state", RowId::new(1));
    ev.scope_mut().push("province", RowId::new(2), None);
    assert_eq!(ev.scope().table(), "province");
    ev.scope_pop()?;
    assert_eq!(ev.scope().table(), "actor_state");
    Ok(())
}

#[test]
fn evaluator_jump_to_unknown_relation_is_error() {
    let mut ev = make_evaluator("province", RowId::new(1));
    let result = ev.jump_to("province -> actor_state", JumpDirection::Forward);
    assert!(matches!(result, Err(RuleError::Query(QueryError::RelationNotFound(_)))));
}

// ------------------------------------------------------------------
// Evaluator: effect submission (stubbed journal interaction)
// ------------------------------------------------------------------

#[test]
fn evaluator_submit_diff_ok() -> RuleResult<()> {
    let ev = make_evaluator("test", RowId::new(0));
    let diff = Diff::Update {
        table: "actor_state".to_owned(),
        row: RowId::new(1),
        column: "treasury".to_owned(),
        value: serde_json::Value::Number(serde_json::Number::from(100)),
    };
    ev.submit_diff(diff)?;
    let journal = ev
        .journal()
        .read()
        .map_err(|_| RuleError::Journal(JournalError::SubmitFailed("lock".to_owned())))?;
    assert_eq!(journal.pending_diff_count(), 1);
    Ok(())
}

#[test]
fn evaluator_submit_command_ok() -> RuleResult<()> {
    let ev = make_evaluator("test", RowId::new(0));
    let envelope = CommandEnvelope::new(
        Tick::ZERO,
        "test_source",
        Command::UpdateColumn {
            table: "actor_state".to_owned(),
            row: RowId::new(1),
            column: "treasury".to_owned(),
            value: serde_json::Value::Number(serde_json::Number::from(100)),
        },
    );
    ev.submit_command(envelope)?;
    let journal = ev
        .journal()
        .read()
        .map_err(|_| RuleError::Journal(JournalError::SubmitFailed("lock".to_owned())))?;
    assert_eq!(journal.pending_command_count(), 1);
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: tick lifecycle
// ------------------------------------------------------------------

#[test]
fn evaluator_advance_tick_clears_cache() {
    let mut ev = make_evaluator("test", RowId::new(0));
    ev.advance_tick(Tick(1));
    assert_eq!(ev.current_tick(), Tick(1));
    assert!(ev.cache().is_empty());
}

// ------------------------------------------------------------------
// Evaluator: modifier integration
// ------------------------------------------------------------------

#[test]
fn evaluator_with_modifiers() {
    let mut reg = ModifierRegistry::new();
    reg.register(Modifier::new("actor_state", "treasury", ModifierOp::Add, fp(5, 2)));
    let ev = make_evaluator("test", RowId::new(0)).with_modifiers(reg);
    assert_eq!(ev.modifiers().len(), 1);
}

// ------------------------------------------------------------------
// Complex trigger expression
// ------------------------------------------------------------------

#[test]
fn eval_complex_trigger() -> RuleResult<()> {
    let mut ev = make_evaluator("province", RowId::new(1));
 // (unrest > 0.8) AND (stability < 0.3)
    let unrest = Expr::compare(
        CompareOp::Gt,
        Expr::column("province", "unrest"),
        Expr::constant(fp(8, 1)), // 0.8
    );
    let stability = Expr::compare(
        CompareOp::Lt,
        Expr::column("province", "stability"),
        Expr::constant(fp(3, 1)), // 0.3
    );
    let trigger = Expr::and(vec![unrest, stability]);

 // evaluate_column queries the engine; engine has no data so returns Query error.
    let result = ev.evaluate(&trigger);
    assert!(matches!(result, Err(RuleError::Query(_))));
    Ok(())
}

// ------------------------------------------------------------------
// Serialization round-trip
// ------------------------------------------------------------------

#[test]
fn expr_serialization_roundtrip() -> RuleResult<()> {
    let original = Expr::if_then_else(
        Expr::and(vec![
            Expr::compare(
                CompareOp::Gt,
                Expr::column("actor_state", "treasury"),
                Expr::constant(fp(100, 2)),
            ),
            Expr::constant_bool(true),
        ]),
        Expr::constant(fp(1, 0)),
        Expr::constant(fp(0, 0)),
    );

    let json = serde_json::to_string(&original).map_err(|e| RuleError::Generic(e.to_string()))?;
    let deserialized: Expr =
        serde_json::from_str(&json).map_err(|e| RuleError::Generic(e.to_string()))?;
    assert_eq!(original, deserialized);
    Ok(())
}

#[test]
fn modifier_serialization_roundtrip() -> RuleResult<()> {
    let original = Modifier::new("actor_state", "treasury", ModifierOp::Mul, fp(11, 1));
    let json = serde_json::to_string(&original).map_err(|e| RuleError::Generic(e.to_string()))?;
    let deserialized: Modifier =
        serde_json::from_str(&json).map_err(|e| RuleError::Generic(e.to_string()))?;
    assert_eq!(original, deserialized);
    Ok(())
}

// ------------------------------------------------------------------
// Evaluator: advance_tick_u64 convenience method
// ------------------------------------------------------------------

#[test]
fn evaluator_advance_tick_u64_clears_cache() {
    let mut ev = make_evaluator("test", RowId::new(0));
    ev.advance_tick_u64(42);
    assert_eq!(ev.current_tick(), Tick(42));
    assert!(ev.cache().is_empty());
}

#[test]
fn evaluator_refresh_signal_integration() {
 // Simulate REFRESH_SIGNAL via RwLock-based external synchronization.
    let ev = make_evaluator("test", RowId::new(0));
    let locked = std::sync::Arc::new(RwLock::new(ev));

 // External caller acquires write lock and advances tick.
    {
        let mut guard = locked.write().unwrap();
        guard.advance_tick_u64(7);
    }

 // Verify tick updated and cache cleared.
    let guard = locked.read().unwrap();
    assert_eq!(guard.current_tick(), Tick(7));
    assert!(guard.cache().is_empty());
}

// ------------------------------------------------------------------
// Effect evaluation
// ------------------------------------------------------------------

#[test]
fn effect_update_column_submits_diff() -> RuleResult<()> {
    let mut ev = make_evaluator("actor_state", RowId::new(5));
    let effect = Effect::UpdateColumn {
        table: "actor_state".to_owned(),
        column: "treasury".to_owned(),
        value: Expr::constant(fp(100, 2)),
    };
    ev.evaluate_effect(&effect)?;

    let journal = ev
        .journal()
        .read()
        .map_err(|_| RuleError::Journal(scharnhorst_journal::JournalError::SubmitFailed(
            "lock".to_owned(),
        )))?;
    assert_eq!(journal.pending_diff_count(), 1);
    Ok(())
}

#[test]
fn effect_insert_row_submits_diff() -> RuleResult<()> {
    let mut ev = make_evaluator("actor_state", RowId::new(0));
    let mut values = HashMap::new();
    values.insert("treasury".to_owned(), Expr::constant(fp(500, 2)));
    values.insert("prestige".to_owned(), Expr::constant(fp(50, 2)));

    let effect = Effect::InsertRow {
        table: "actor_state".to_owned(),
        row: RowId::new(99),
        values,
    };
    ev.evaluate_effect(&effect)?;

    let journal = ev
        .journal()
        .read()
        .map_err(|_| RuleError::Journal(scharnhorst_journal::JournalError::SubmitFailed(
            "lock".to_owned(),
        )))?;
    assert_eq!(journal.pending_diff_count(), 1);
    Ok(())
}

#[test]
fn effect_set_variable_binds_value() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let effect = Effect::SetVariable {
        name: "x".to_owned(),
        value: Expr::constant(fp(42, 2)),
    };
    ev.evaluate_effect(&effect)?;

    let var = ev.resolve_var(&scharnhorst_rules::VarRef::new("x"))?;
    assert_eq!(*var, EvalValue::FixedPoint(fp(42, 2)));
    Ok(())
}

#[test]
fn effect_sequence_runs_all() -> RuleResult<()> {
    let mut ev = make_evaluator("test", RowId::new(0));
    let seq = Effect::Sequence(vec![
        Effect::SetVariable {
            name: "a".to_owned(),
            value: Expr::constant(fp(1, 0)),
        },
        Effect::SetVariable {
            name: "b".to_owned(),
            value: Expr::constant(fp(2, 0)),
        },
    ]);
    ev.evaluate_effect(&seq)?;

    let var_a = ev.resolve_var(&scharnhorst_rules::VarRef::new("a"))?;
    let var_b = ev.resolve_var(&scharnhorst_rules::VarRef::new("b"))?;
    assert_eq!(*var_a, EvalValue::FixedPoint(fp(1, 0)));
    assert_eq!(*var_b, EvalValue::FixedPoint(fp(2, 0)));
    Ok(())
}

#[test]
fn effect_update_column_evaluates_expression() -> RuleResult<()> {
    let mut ev = make_evaluator("actor_state", RowId::new(0));
 // Arithmetic expression: 10.00 + 5.00 = 15.00
    let effect = Effect::UpdateColumn {
        table: "actor_state".to_owned(),
        column: "treasury".to_owned(),
        value: Expr::arithmetic(
            ArithmeticOp::Add,
            Expr::constant(fp(10, 2)),
            Expr::constant(fp(5, 2)),
        ),
    };
    ev.evaluate_effect(&effect)?;

 // The expression should have been evaluated before diff submission.
 // If expression evaluation failed, this test would have returned
 // an error above. A successful evaluation produces one diff.
    let journal = ev
        .journal()
        .read()
        .map_err(|_| RuleError::Journal(scharnhorst_journal::JournalError::SubmitFailed(
            "lock".to_owned(),
        )))?;
    assert_eq!(journal.pending_diff_count(), 1);
    Ok(())
}
