use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use scharnhorst_core::{FixedPoint, RowId};

/// A path to a column value, optionally scoped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnPath {
 pub table: String,
 pub column: String,
}

impl ColumnPath {
 pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
 Self {
 table: table.into(),
 column: column.into(),
 }
 }
}

/// A variable reference resolved at evaluation time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VarRef {
 pub name: String,
}

impl VarRef {
 pub fn new(name: impl Into<String>) -> Self {
 Self { name: name.into() }
 }
}

/// Comparison operators used in trigger expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
 Eq,
 Ne,
 Lt,
 Le,
 Gt,
 Ge,
}

/// Arithmetic operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArithmeticOp {
 Add,
 Sub,
 Mul,
 Div,
 Rem,
}

/// The core expression AST for triggers and effects.
///
/// All expressions are pure: they read state exclusively through the
/// query-engine and never mutate world state directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
 /// A constant fixed-point value.
 Const(FixedPoint),

 /// A constant boolean value.
 Bool(bool),

 /// A constant string value.
 String(String),

 /// A row identifier literal.
 RowId(RowId),

 /// Reference to a column value in the current or named scope.
 Column(ColumnPath),

 /// Reference to a bound variable.
 Var(VarRef),

 /// Logical negation.
 Not(Box<Expr>),

 /// Logical conjunction.
 And(Vec<Expr>),

 /// Logical disjunction.
 Or(Vec<Expr>),

 /// Comparison between two expressions.
 Compare {
 op: CompareOp,
 lhs: Box<Expr>,
 rhs: Box<Expr>,
 },

 /// Arithmetic operation between two expressions.
 Arithmetic {
 op: ArithmeticOp,
 lhs: Box<Expr>,
 rhs: Box<Expr>,
 },

 /// Conditional expression.
 If {
 cond: Box<Expr>,
 then_branch: Box<Expr>,
 else_branch: Box<Expr>,
 },

 /// A function call with named arguments.
 Call {
 func: String,
 args: HashMap<String, Expr>,
 },

 /// A list literal (heterogeneous, validated at eval time).
 List(Vec<Expr>),

 /// A map literal (heterogeneous, validated at eval time).
 Map(HashMap<String, Expr>),
}

impl Expr {
 pub fn constant(value: FixedPoint) -> Self {
 Self::Const(value)
 }

 pub fn constant_bool(value: bool) -> Self {
 Self::Bool(value)
 }

 pub fn column(table: impl Into<String>, column: impl Into<String>) -> Self {
 Self::Column(ColumnPath::new(table, column))
 }

 pub fn var(name: impl Into<String>) -> Self {
 Self::Var(VarRef::new(name))
 }

 pub fn new_not(inner: Expr) -> Self {
 Self::Not(Box::new(inner))
 }

 pub fn and(items: Vec<Expr>) -> Self {
 Self::And(items)
 }

 pub fn or(items: Vec<Expr>) -> Self {
 Self::Or(items)
 }

 pub fn compare(op: CompareOp, lhs: Expr, rhs: Expr) -> Self {
 Self::Compare {
 op,
 lhs: Box::new(lhs),
 rhs: Box::new(rhs),
 }
 }

 pub fn arithmetic(op: ArithmeticOp, lhs: Expr, rhs: Expr) -> Self {
 Self::Arithmetic {
 op,
 lhs: Box::new(lhs),
 rhs: Box::new(rhs),
 }
 }

 pub fn if_then_else(cond: Expr, then_branch: Expr, else_branch: Expr) -> Self {
 Self::If {
 cond: Box::new(cond),
 then_branch: Box::new(then_branch),
 else_branch: Box::new(else_branch),
 }
 }

 pub fn call(func: impl Into<String>, args: HashMap<String, Expr>) -> Self {
 Self::Call {
 func: func.into(),
 args,
 }
 }

 pub fn list(items: Vec<Expr>) -> Self {
 Self::List(items)
 }

 pub fn map(entries: HashMap<String, Expr>) -> Self {
 Self::Map(entries)
 }
}

#[cfg(test)]
mod tests {
 use super::*;

 #[test]
 fn column_path_new() {
 let path = ColumnPath::new("province", "unrest");
 assert_eq!(path.table, "province");
 assert_eq!(path.column, "unrest");
 }

 #[test]
 fn var_ref_new() {
 let v = VarRef::new("stability");
 assert_eq!(v.name, "stability");
 }

 #[test]
 fn compare_op_equality() {
 assert_eq!(CompareOp::Eq, CompareOp::Eq);
 assert_ne!(CompareOp::Eq, CompareOp::Ne);
 assert_ne!(CompareOp::Lt, CompareOp::Gt);
 }

 #[test]
 fn arithmetic_op_equality() {
 assert_eq!(ArithmeticOp::Add, ArithmeticOp::Add);
 assert_ne!(ArithmeticOp::Add, ArithmeticOp::Sub);
 assert_ne!(ArithmeticOp::Mul, ArithmeticOp::Div);
 }

 #[test]
 fn expr_const_serialization() {
 let e = Expr::constant(FixedPoint::from_i64(42, 0).unwrap());
 let json = serde_json::to_string(&e).unwrap();
 let back: Expr = serde_json::from_str(&json).unwrap();
 assert_eq!(e, back);
 }

 #[test]
 fn expr_and_single_element() {
 let e1 = Expr::constant(FixedPoint::from_i64(1, 0).unwrap());
 let and_expr = Expr::and(vec![e1.clone()]);
 let json1 = serde_json::to_value(&e1).unwrap();
 let json2 = serde_json::to_value(&and_expr).unwrap();
 assert_ne!(json1, json2);
 assert_eq!(json2["And"].as_array().unwrap().len(), 1);
 }

 #[test]
 fn expr_compare_serialization() {
 let e = Expr::compare(
 CompareOp::Gt,
 Expr::column("t", "c"),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 );
 let json = serde_json::to_value(&e).unwrap();
 assert_eq!(json["Compare"]["op"], "Gt");
 }

 // ==================================================================
 // A. Expr Construction Correctness
 // ==================================================================

 /// rule-ir spec nested And/Or trigger expression for
 /// RelationGraph scope-jump. Verifies that Expr::And containing
 /// two Expr::Compare nodes serializes with correct structure.
 #[test]
 fn expr_nested_and_or_rule_ir_spec() {
 // Build: (province.unrest > 0.8) AND (actor.stability < 0.3)
 let unrest_gt = Expr::compare(
 CompareOp::Gt,
 Expr::column("province", "unrest"),
 Expr::constant(FixedPoint::from_i64(8, 1).unwrap()),
 );
 let stability_lt = Expr::compare(
 CompareOp::Lt,
 Expr::column("actor", "stability"),
 Expr::constant(FixedPoint::from_i64(3, 1).unwrap()),
 );
 let trigger = Expr::and(vec![unrest_gt, stability_lt]);

 let json_result = serde_json::to_value(&trigger);
 assert!(json_result.is_ok(), "And serialization should succeed");
 if let Ok(json) = json_result {
 let arr = json["And"].as_array();
 assert!(arr.is_some(), "And should serialize as array");
 if let Some(a) = arr {
 assert_eq!(a.len(), 2);
 assert_eq!(a[0]["Compare"]["op"], "Gt");
 assert_eq!(a[1]["Compare"]["op"], "Lt");
 }
 }

 // Round-trip via string
 let json_str_result = serde_json::to_string(&trigger);
 assert!(json_str_result.is_ok(), "to_string should succeed");
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok(), "deserialization should succeed");
 if let Ok(back) = back_result {
 assert_eq!(trigger, back);
 }
 }
 }

 /// rule-ir spec: all six CompareOp variants (Eq, Ne, Lt, Le, Gt, Ge)
 /// produce correct serialized operator names.
 #[test]
 fn expr_compare_all_six_operators() {
 let lhs = Expr::constant(FixedPoint::from_i64(1, 0).unwrap());
 let rhs = Expr::constant(FixedPoint::from_i64(2, 0).unwrap());
 let expected_ops = [
 (CompareOp::Eq, "Eq"),
 (CompareOp::Ne, "Ne"),
 (CompareOp::Lt, "Lt"),
 (CompareOp::Le, "Le"),
 (CompareOp::Gt, "Gt"),
 (CompareOp::Ge, "Ge"),
 ];

 for (op, op_str) in &expected_ops {
 let e = Expr::compare(*op, lhs.clone(), rhs.clone());
 let json_result = serde_json::to_value(&e);
 assert!(json_result.is_ok(), "serialization should succeed for {:?}", op);
 if let Ok(json) = json_result {
 assert_eq!(json["Compare"]["op"], *op_str, "op serialization mismatch for {:?}", op);
 assert!(json["Compare"]["lhs"].is_object(), "lhs should be object");
 assert!(json["Compare"]["rhs"].is_object(), "rhs should be object");
 }
 }
 }

 /// rule-ir spec: If-then-else expression with nested branches
 /// roundtrips correctly via serde.
 #[test]
 fn expr_if_then_else_nested_roundtrip() {
 // Build: If (a > 0) then (If (b > 0) then Const(1) else Const(0)) else Const(-1)
 let inner_if = Expr::if_then_else(
 Expr::compare(
 CompareOp::Gt,
 Expr::var("b"),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ),
 Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 );
 let outer_if = Expr::if_then_else(
 Expr::compare(
 CompareOp::Gt,
 Expr::var("a"),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ),
 inner_if,
 Expr::constant(FixedPoint::from_i64(-1, 0).unwrap()),
 );

 let json_result = serde_json::to_string(&outer_if);
 assert!(json_result.is_ok(), "nested If serialization should succeed");
 if let Ok(json_str) = json_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok(), "nested If deserialization should succeed");
 if let Ok(back) = back_result {
 assert_eq!(outer_if, back);
 }
 }
 }

 /// rule-ir spec: Call expression with named arguments serializes
 /// both function name and argument map correctly.
 #[test]
 fn expr_call_named_args_serialization() {
 let mut args = HashMap::new();
 args.insert("x".to_string(), Expr::constant(FixedPoint::from_i64(10, 0).unwrap()));
 args.insert("y".to_string(), Expr::constant(FixedPoint::from_i64(20, 0).unwrap()));
 let call = Expr::call("add", args);

 let json_result = serde_json::to_value(&call);
 assert!(json_result.is_ok(), "Call serialization should succeed");
 if let Ok(json) = json_result {
 let call_obj = json["Call"].as_object();
 assert!(call_obj.is_some(), "Call should be a JSON object");
 if let Some(obj) = call_obj {
 assert_eq!(obj["func"], "add", "function name should be 'add'");
 let args_map = obj["args"].as_object();
 assert!(args_map.is_some(), "args should be a JSON object");
 if let Some(map) = args_map {
 assert!(map.contains_key("x"), "args should contain 'x'");
 assert!(map.contains_key("y"), "args should contain 'y'");
 }
 }
 }

 // Round-trip
 let json_str_result = serde_json::to_string(&call);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(call, back);
 }
 }
 }

 /// rule-ir spec: List literals with mixed types preserve element
 /// order during serialization.
 #[test]
 fn expr_list_mixed_types_preserve_order() {
 let items = vec![
 Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::Bool(true),
 Expr::String("hello".to_string()),
 Expr::constant(FixedPoint::from_i64(-5, 0).unwrap()),
 ];
 let list = Expr::list(items);

 let json_result = serde_json::to_value(&list);
 assert!(json_result.is_ok(), "List serialization should succeed");
 if let Ok(json) = json_result {
 let arr = json["List"].as_array();
 assert!(arr.is_some(), "List should be a JSON array");
 if let Some(a) = arr {
 assert_eq!(a.len(), 4, "List should have 4 elements");
 // Verify order: Const(1), Bool(true), String("hello"), Const(-5)
 assert!(a[0]["Const"].is_object(), "first element should be Const (FixedPoint object)");
 assert_eq!(a[1]["Bool"], true, "second element should be Bool(true)");
 assert_eq!(a[2]["String"], "hello", "third element should be String(\"hello\")");
 assert!(a[3]["Const"].is_object(), "fourth element should be Const (FixedPoint object)");
 }
 }

 // Round-trip
 let json_str_result = serde_json::to_string(&list);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(list, back);
 }
 }
 }

 /// rule-ir spec: Map literals preserve key-value structure during
 /// serialization.
 #[test]
 fn expr_map_key_value_preserve_structure() {
 let mut entries = HashMap::new();
 entries.insert("health".to_string(), Expr::constant(FixedPoint::from_i64(100, 0).unwrap()));
 entries.insert("name".to_string(), Expr::String("orc".to_string()));
 let map = Expr::map(entries);

 let json_result = serde_json::to_value(&map);
 assert!(json_result.is_ok(), "Map serialization should succeed");
 if let Ok(json) = json_result {
 let map_obj = json["Map"].as_object();
 assert!(map_obj.is_some(), "Map should be a JSON object");
 if let Some(obj) = map_obj {
 assert!(obj.contains_key("health"), "Map should contain 'health'");
 assert!(obj.contains_key("name"), "Map should contain 'name'");
 assert!(obj["health"]["Const"].is_object(), "health value should be Const (FixedPoint object)");
 assert_eq!(obj["name"]["String"], "orc", "name value should be String(\"orc\")");
 }
 }

 // Round-trip
 let json_str_result = serde_json::to_string(&map);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(map, back);
 }
 }
 }

 // ==================================================================
 // B. Query Expression Patterns
 // ==================================================================

 /// column path references join table.column for RelationGraph
 /// lookup through the query-engine. Verifies ColumnPath stores
 /// both table and column correctly for scope resolution.
 #[test]
 fn dc10_query_engine_column_path() {
 let col = Expr::column("province", "unrest");
 match &col {
 Expr::Column(path) => {
 assert_eq!(path.table, "province");
 assert_eq!(path.column, "unrest");
 }
 _ => panic!("expected Expr::Column variant"),
 }

 // Verify serialization preserves both fields for query-engine lookup
 let json_result = serde_json::to_value(&col);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 let col_obj = json["Column"].as_object();
 assert!(col_obj.is_some());
 if let Some(obj) = col_obj {
 assert_eq!(obj["table"], "province");
 assert_eq!(obj["column"], "unrest");
 }
 }
 }

 /// scope-jump trigger expression combining two scoped
 /// column comparisons via And. Verifies that province.unrest > 0.8
 /// AND actor.stability < 0.3 serializes as a valid trigger.
 #[test]
 fn dc6_relationgraph_scope_jump_trigger() {
 // province.unrest > 0.8
 let province_check = Expr::compare(
 CompareOp::Gt,
 Expr::column("province", "unrest"),
 Expr::constant(FixedPoint::from_i64(800, 3).unwrap()),
 );
 // actor.stability < 0.3
 let actor_check = Expr::compare(
 CompareOp::Lt,
 Expr::column("actor", "stability"),
 Expr::constant(FixedPoint::from_i64(300, 3).unwrap()),
 );
 let trigger = Expr::and(vec![province_check, actor_check]);

 let json_result = serde_json::to_value(&trigger);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 let arr = json["And"].as_array();
 assert!(arr.is_some());
 if let Some(a) = arr {
 assert_eq!(a.len(), 2);
 // Verify first compare: province.unrest > 0.8
 assert_eq!(a[0]["Compare"]["op"], "Gt");
 assert_eq!(a[0]["Compare"]["lhs"]["Column"]["table"], "province");
 assert_eq!(a[0]["Compare"]["lhs"]["Column"]["column"], "unrest");
 // Verify second compare: actor.stability < 0.3
 assert_eq!(a[1]["Compare"]["op"], "Lt");
 assert_eq!(a[1]["Compare"]["lhs"]["Column"]["table"], "actor");
 assert_eq!(a[1]["Compare"]["lhs"]["Column"]["column"], "stability");
 }
 }
 }

 /// VarRef expressions for bound scoped variables preserve
 /// their name through construction, equality, and serialization.
 #[test]
 fn dc6_varref_bound_scoped_variables() {
 let var_expr = Expr::var("current_stability");

 match &var_expr {
 Expr::Var(vr) => {
 assert_eq!(vr.name, "current_stability");
 }
 _ => panic!("expected Expr::Var variant"),
 }

 // Verify serialization preserves the variable name
 let json_result = serde_json::to_value(&var_expr);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 assert_eq!(json["Var"]["name"], "current_stability");
 }

 // Round-trip
 let json_str_result = serde_json::to_string(&var_expr);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(var_expr, back);
 }
 }
 }

 // ==================================================================
 // C. Effect Expression Patterns
 // ==================================================================

 /// effect expression using If-then-else to compute a
 /// modifier value: If cond then Arithmetic(Mul, base, modifier)
 /// else Const(0). Verifies the expression structure roundtrips.
 #[test]
 fn dc1_journal_diff_effect_if_then_else() {
 // If (unrest > threshold) then base * modifier else 0
 let cond = Expr::compare(
 CompareOp::Gt,
 Expr::column("province", "unrest"),
 Expr::constant(FixedPoint::from_i64(5, 1).unwrap()),
 );
 let then_val = Expr::arithmetic(
 ArithmeticOp::Mul,
 Expr::column("province", "base_tax"),
 Expr::constant(FixedPoint::from_i64(15, 1).unwrap()), // 1.5x modifier
 );
 let else_val = Expr::constant(FixedPoint::from_i64(0, 0).unwrap());
 let effect = Expr::if_then_else(cond, then_val, else_val);

 let json_result = serde_json::to_value(&effect);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 assert_eq!(json["If"]["cond"]["Compare"]["op"], "Gt");
 assert_eq!(
 json["If"]["then_branch"]["Arithmetic"]["op"],
 "Mul"
 );
 let else_const = json["If"]["else_branch"]["Const"].as_object();
 assert!(else_const.is_some(), "else_branch Const should be an object");
 if let Some(obj) = else_const {
 assert!(obj.contains_key("raw"), "FixedPoint Const should have 'raw'");
 }
 }

 // Round-trip
 let json_str_result = serde_json::to_string(&effect);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(effect, back);
 }
 }
 }

 /// multi-modifier chain expression where Add and Mul
 /// modifiers are tracked as separate Expr::Arithmetic nodes.
 /// Verifies that (Add modifier1) + (Mul modifier2) produces
 /// distinct serialized Arithmetic nodes.
 #[test]
 fn dc1_multi_modifier_chain_separate_nodes() {
 let base = Expr::column("province", "base_value");

 // modifier1: base + add_bonus
 let add_mod = Expr::arithmetic(
 ArithmeticOp::Add,
 base.clone(),
 Expr::constant(FixedPoint::from_i64(10, 0).unwrap()),
 );

 // modifier2: (base + add_bonus) * mul_factor
 let mul_mod = Expr::arithmetic(
 ArithmeticOp::Mul,
 add_mod,
 Expr::constant(FixedPoint::from_i64(2, 0).unwrap()),
 );

 let json_result = serde_json::to_value(&mul_mod);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 // Outer node is Mul
 assert_eq!(json["Arithmetic"]["op"], "Mul");
 // Inner (lhs) is Add
 assert_eq!(
 json["Arithmetic"]["lhs"]["Arithmetic"]["op"],
 "Add"
 );
 // The Add's lhs is the column reference
 assert_eq!(
 json["Arithmetic"]["lhs"]["Arithmetic"]["lhs"]["Column"]["table"],
 "province"
 );
 assert_eq!(
 json["Arithmetic"]["lhs"]["Arithmetic"]["lhs"]["Column"]["column"],
 "base_value"
 );
 // The Add's rhs is Const(10)
 let add_rhs = json["Arithmetic"]["lhs"]["Arithmetic"]["rhs"]["Const"].as_object();
 assert!(add_rhs.is_some(), "Add rhs Const should be an object");
 if let Some(obj) = add_rhs {
 assert!(obj.contains_key("raw"), "FixedPoint Const should have 'raw'");
 }
 // The Mul's rhs is Const(2)
 let mul_rhs = json["Arithmetic"]["rhs"]["Const"].as_object();
 assert!(mul_rhs.is_some(), "Mul rhs Const should be an object");
 if let Some(obj) = mul_rhs {
 assert!(obj.contains_key("raw"), "FixedPoint Const should have 'raw'");
 }
 }

 // Round-trip
 let json_str_result = serde_json::to_string(&mul_mod);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(mul_mod, back);
 }
 }
 }

 // ==================================================================
 // D. Serialization Edge Cases
 // ==================================================================

 /// Serialization edge case: every Expr variant round-trips
 /// correctly through serde JSON. Covers all 15 variants:
 /// Const, Bool, String, RowId, Column, Var, Not, And, Or,
 /// Compare, Arithmetic, If, Call, List, Map.
 #[test]
 fn expr_roundtrip_all_variants() {
 // Helper to round-trip a single expression
 fn roundtrip(e: &Expr) -> bool {
 let json_str = match serde_json::to_string(e) {
 Ok(s) => s,
 Err(_) => return false,
 };
 let back: Result<Expr, _> = serde_json::from_str(&json_str);
 match back {
 Ok(b) => b == *e,
 Err(_) => false,
 }
 }

 // 1. Const
 assert!(roundtrip(&Expr::Const(FixedPoint::from_i64(42, 0).unwrap())));
 // 2. Bool
 assert!(roundtrip(&Expr::Bool(true)));
 assert!(roundtrip(&Expr::Bool(false)));
 // 3. String
 assert!(roundtrip(&Expr::String("test".to_string())));
 // 4. RowId
 assert!(roundtrip(&Expr::RowId(RowId::new(7))));
 // 5. Column
 assert!(roundtrip(&Expr::column("tbl", "col")));
 // 6. Var
 assert!(roundtrip(&Expr::var("x")));
 // 7. Not
 assert!(roundtrip(&Expr::new_not(Expr::Bool(true))));
 // 8. And
 assert!(roundtrip(&Expr::and(vec![
 Expr::Bool(true),
 Expr::Bool(false),
 ])));
 // 9. Or
 assert!(roundtrip(&Expr::or(vec![
 Expr::Bool(false),
 Expr::Bool(true),
 ])));
 // 10. Compare
 assert!(roundtrip(&Expr::compare(
 CompareOp::Eq,
 Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(2, 0).unwrap()),
 )));
 // 11. Arithmetic
 assert!(roundtrip(&Expr::arithmetic(
 ArithmeticOp::Add,
 Expr::constant(FixedPoint::from_i64(3, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(4, 0).unwrap()),
 )));
 // 12. If
 assert!(roundtrip(&Expr::if_then_else(
 Expr::Bool(true),
 Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 )));
 // 13. Call
 let mut args = HashMap::new();
 args.insert("x".to_string(), Expr::constant(FixedPoint::from_i64(5, 0).unwrap()));
 assert!(roundtrip(&Expr::call("add", args)));
 // 14. List
 assert!(roundtrip(&Expr::list(vec![
 Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::String("a".to_string()),
 ])));
 // 15. Map
 let mut entries = HashMap::new();
 entries.insert("k".to_string(), Expr::Bool(true));
 assert!(roundtrip(&Expr::map(entries)));
 }

 /// Serialization edge case: deeply nested Expr (depth 5) serializes
 /// and deserializes correctly without truncation or data loss.
 #[test]
 fn expr_deeply_nested_depth5_serialization() {
 // Build: Not(And([Not(Or([Not(Const(1))]))]))
 // Depth chain: Not -> And -> Not -> Or -> Not -> Const = 6 layers
 let inner_not = Expr::new_not(Expr::Const(scharnhorst_core::FixedPoint::new(1, 0)));
 let inner_or = Expr::or(vec![inner_not]);
 let mid_not = Expr::new_not(inner_or);
 let inner_and = Expr::and(vec![mid_not]);
 let outer_not = Expr::new_not(inner_and);

 let json_result = serde_json::to_string(&outer_not);
 assert!(json_result.is_ok(), "deeply nested serialization should succeed");
 if let Ok(json_str) = json_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok(), "deeply nested deserialization should succeed");
 if let Ok(back) = back_result {
 assert_eq!(outer_not, back, "deeply nested round-trip should preserve structure");
 }
 }

 // Verify structural depth by checking nested JSON path
 let json_val_result = serde_json::to_value(&outer_not);
 assert!(json_val_result.is_ok());
 if let Ok(json) = json_val_result {
 // Path: Not -> And[0] -> Not -> Or[0] -> Not -> Const
 let leaf = &json["Not"]["And"][0]["Not"]["Or"][0]["Not"]["Const"];
 assert!(leaf.is_object(), "deepest leaf should be Const (a FixedPoint object)");
 }
 }

 /// Serialization edge case: empty And/Or list serializes correctly.
 /// An empty list is a valid boundary case for logical combinators.
 #[test]
 fn expr_empty_and_or_list_edge_case() {
 // Empty And
 let empty_and = Expr::and(vec![]);
 let json_result = serde_json::to_value(&empty_and);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 let arr = json["And"].as_array();
 assert!(arr.is_some(), "empty And should be an array");
 if let Some(a) = arr {
 assert_eq!(a.len(), 0, "empty And should have 0 elements");
 }
 }

 // Round-trip empty And
 let json_str_result = serde_json::to_string(&empty_and);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(empty_and, back);
 }
 }

 // Empty Or
 let empty_or = Expr::or(vec![]);
 let json_result2 = serde_json::to_value(&empty_or);
 assert!(json_result2.is_ok());
 if let Ok(json) = json_result2 {
 let arr = json["Or"].as_array();
 assert!(arr.is_some(), "empty Or should be an array");
 if let Some(a) = arr {
 assert_eq!(a.len(), 0, "empty Or should have 0 elements");
 }
 }

 // Round-trip empty Or
 let json_str_result2 = serde_json::to_string(&empty_or);
 assert!(json_str_result2.is_ok());
 if let Ok(json_str) = json_str_result2 {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(empty_or, back);
 }
 }
 }

 /// Serialization edge case: Expr with empty Map and empty List
 /// serializes and deserializes correctly.
 #[test]
 fn expr_empty_map_empty_list_serialization() {
 // Empty List
 let empty_list = Expr::list(vec![]);
 let json_result = serde_json::to_value(&empty_list);
 assert!(json_result.is_ok());
 if let Ok(json) = json_result {
 let arr = json["List"].as_array();
 assert!(arr.is_some());
 if let Some(a) = arr {
 assert_eq!(a.len(), 0);
 }
 }
 let json_str_result = serde_json::to_string(&empty_list);
 assert!(json_str_result.is_ok());
 if let Ok(json_str) = json_str_result {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(empty_list, back);
 }
 }

 // Empty Map
 let empty_map = Expr::map(HashMap::new());
 let json_result2 = serde_json::to_value(&empty_map);
 assert!(json_result2.is_ok());
 if let Ok(json) = json_result2 {
 let obj = json["Map"].as_object();
 assert!(obj.is_some());
 if let Some(o) = obj {
 assert_eq!(o.len(), 0);
 }
 }
 let json_str_result2 = serde_json::to_string(&empty_map);
 assert!(json_str_result2.is_ok());
 if let Ok(json_str) = json_str_result2 {
 let back_result: Result<Expr, _> = serde_json::from_str(&json_str);
 assert!(back_result.is_ok());
 if let Ok(back) = back_result {
 assert_eq!(empty_map, back);
 }
 }
 }

 // ==================================================================
 // E. Utility / Constructor Methods
 // ==================================================================

 /// Utility method coverage: all Expr::new constructors produce
 /// the correct variant type. Verifies constant, constant_bool,
 /// column, var, not, and, or, compare, arithmetic,
 /// if_then_else, call, list, map.
 #[test]
 fn expr_new_constructors_all_variants() {
 // constant -> Const
 match Expr::constant(FixedPoint::from_i64(1, 0).unwrap()) {
 Expr::Const(_) => {}
 _ => panic!("constant() should produce Const"),
 }

 // constant_bool -> Bool
 match Expr::constant_bool(true) {
 Expr::Bool(_) => {}
 _ => panic!("constant_bool() should produce Bool"),
 }

 // column -> Column
 match Expr::column("t", "c") {
 Expr::Column(_) => {}
 _ => panic!("column() should produce Column"),
 }

 // var -> Var
 match Expr::var("x") {
 Expr::Var(_) => {}
 _ => panic!("var() should produce Var"),
 }

 // not -> Not
 match Expr::new_not(Expr::Bool(true)) {
 Expr::Not(_) => {}
 _ => panic!("not() should produce Not"),
 }

 // and -> And
 match Expr::and(vec![Expr::Bool(true)]) {
 Expr::And(_) => {}
 _ => panic!("and() should produce And"),
 }

 // or -> Or
 match Expr::or(vec![Expr::Bool(false)]) {
 Expr::Or(_) => {}
 _ => panic!("or() should produce Or"),
 }

 // compare -> Compare
 match Expr::compare(
 CompareOp::Eq,
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ) {
 Expr::Compare {.. } => {}
 _ => panic!("compare() should produce Compare"),
 }

 // arithmetic -> Arithmetic
 match Expr::arithmetic(
 ArithmeticOp::Add,
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ) {
 Expr::Arithmetic {.. } => {}
 _ => panic!("arithmetic() should produce Arithmetic"),
 }

 // if_then_else -> If
 match Expr::if_then_else(
 Expr::Bool(true),
 Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ) {
 Expr::If {.. } => {}
 _ => panic!("if_then_else() should produce If"),
 }

 // call -> Call
 let args = HashMap::new();
 match Expr::call("f", args) {
 Expr::Call {.. } => {}
 _ => panic!("call() should produce Call"),
 }

 // list -> List
 match Expr::list(vec![]) {
 Expr::List(_) => {}
 _ => panic!("list() should produce List"),
 }

 // map -> Map
 match Expr::map(HashMap::new()) {
 Expr::Map(_) => {}
 _ => panic!("map() should produce Map"),
 }
 }

 /// Coverage: exhaustive pattern match on all 15 Expr enum variants
 /// ensures no arm is accidentally omitted in match expressions.
 /// Variants: Const, Bool, String, RowId, Column, Var, Not, And,
 /// Or, Compare, Arithmetic, If, Call, List, Map.
 #[test]
 fn expr_pattern_match_all_15_variants() {
 // Construct one of each variant and match on it to verify
 // exhaustive coverage in user code patterns.

 let variants: Vec<Expr> = vec![
 Expr::Const(FixedPoint::from_i64(1, 0).unwrap()),
 Expr::Bool(true),
 Expr::String("s".to_string()),
 Expr::RowId(RowId::new(1)),
 Expr::column("t", "c"),
 Expr::var("v"),
 Expr::new_not(Expr::Bool(true)),
 Expr::and(vec![]),
 Expr::or(vec![]),
 Expr::compare(
 CompareOp::Eq,
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ),
 Expr::arithmetic(
 ArithmeticOp::Add,
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ),
 Expr::if_then_else(
 Expr::Bool(true),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 Expr::constant(FixedPoint::from_i64(0, 0).unwrap()),
 ),
 Expr::call("f", HashMap::new()),
 Expr::list(vec![]),
 Expr::map(HashMap::new()),
 ];

 assert_eq!(variants.len(), 15, "should have exactly 15 Expr variants");

 let mut matched_count = 0usize;
 for expr in &variants {
 let discriminant = match expr {
 Expr::Const(_) => "Const",
 Expr::Bool(_) => "Bool",
 Expr::String(_) => "String",
 Expr::RowId(_) => "RowId",
 Expr::Column(_) => "Column",
 Expr::Var(_) => "Var",
 Expr::Not(_) => "Not",
 Expr::And(_) => "And",
 Expr::Or(_) => "Or",
 Expr::Compare {.. } => "Compare",
 Expr::Arithmetic {.. } => "Arithmetic",
 Expr::If {.. } => "If",
 Expr::Call {.. } => "Call",
 Expr::List(_) => "List",
 Expr::Map(_) => "Map",
 };
 // Ensure each variant maps to a unique discriminant string
 assert!(!discriminant.is_empty());
 matched_count += 1;
 }
 assert_eq!(matched_count, 15, "all 15 variants should be matched");
 }
}
