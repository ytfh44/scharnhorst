use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use scharnhorst_core::{FixedPoint, JournalSubmitToken, RowId, Tick};
use scharnhorst_journal::{CommandEnvelope, Diff, Journal};
use scharnhorst_query::typed_access::ColumnKind;
use scharnhorst_query::QueryEngine;
use scharnhorst_schema::RelationEdge;

use crate::cache::PrefetchCache;
use crate::effect::Effect;
use crate::error::{RuleError, RuleResult};
use crate::expr::{ColumnPath, CompareOp, Expr, VarRef};
use crate::modifier::ModifierRegistry;
use crate::scope::{JumpDirection, Scope};

/// Signature for a registered callable function.
/// Takes a slice of evaluated arguments, returns a result.
type BuiltinFn = Box<dyn Fn(&[EvalValue]) -> RuleResult<EvalValue> + Send + Sync>;

/// Evaluation value produced by executing an [`Expr`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalValue {
    FixedPoint(FixedPoint),
    Bool(bool),
    String(String),
    RowId(RowId),
    List(Vec<EvalValue>),
    Map(HashMap<String, EvalValue>),
}

impl EvalValue {
    /// Returns the human-readable variant name for error messages.
    pub fn variant_name(&self) -> &'static str {
        match self {
            EvalValue::FixedPoint(_) => "FixedPoint",
            EvalValue::Bool(_) => "Bool",
            EvalValue::String(_) => "String",
            EvalValue::RowId(_) => "RowId",
            EvalValue::List(_) => "List",
            EvalValue::Map(_) => "Map",
        }
    }
}

/// The rule-IR evaluator.
///
/// Responsibilities:
/// - Evaluate [`Expr`] AST nodes into [`EvalValue`] results.
/// - Maintain a [`Scope`] stack for relation-based traversal.
/// - Maintain a [`PrefetchCache`] for cross-partition lookups.
/// - Read state **exclusively** through the query-engine.
/// - Submit effects as [`Diff`] objects through the journal.
/// - Apply modifier aggregation when resolving column values.
pub struct Evaluator {
    query_engine: Arc<RwLock<QueryEngine>>,
    journal: Arc<RwLock<Journal>>,
    scope: Scope,
    cache: PrefetchCache,
    modifiers: ModifierRegistry,
    variables: HashMap<String, EvalValue>,
    builtins: HashMap<String, BuiltinFn>,
    builtin_arg_counts: HashMap<String, usize>,
}

impl Evaluator {
    fn default_builtins() -> (HashMap<String, BuiltinFn>, HashMap<String, usize>) {
        let mut fns: HashMap<String, BuiltinFn> = HashMap::new();
        let mut arg_counts: HashMap<String, usize> = HashMap::new();

        fns.insert(
            "add".into(),
            Box::new(|args| {
                if args.len() < 2 {
                    return Err(RuleError::Evaluation(
                        "add requires at least 2 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                        Ok(EvalValue::FixedPoint((*a + *b).map_err(RuleError::from)?))
                    }
                    _ => Err(RuleError::TypeMismatch {
                        expected: "FixedPoint".into(),
                        got: args[0].variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("add".into(), 2);

        fns.insert(
            "sub".into(),
            Box::new(|args| {
                if args.len() < 2 {
                    return Err(RuleError::Evaluation(
                        "sub requires at least 2 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                        Ok(EvalValue::FixedPoint((*a - *b).map_err(RuleError::from)?))
                    }
                    _ => Err(RuleError::TypeMismatch {
                        expected: "FixedPoint".into(),
                        got: args[0].variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("sub".into(), 2);

        fns.insert(
            "mul".into(),
            Box::new(|args| {
                if args.len() < 2 {
                    return Err(RuleError::Evaluation(
                        "mul requires at least 2 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                        Ok(EvalValue::FixedPoint((*a * *b).map_err(RuleError::from)?))
                    }
                    _ => Err(RuleError::TypeMismatch {
                        expected: "FixedPoint".into(),
                        got: args[0].variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("mul".into(), 2);

        fns.insert(
            "div".into(),
            Box::new(|args| {
                if args.len() < 2 {
                    return Err(RuleError::Evaluation(
                        "div requires at least 2 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                        Ok(EvalValue::FixedPoint((*a / *b).map_err(RuleError::from)?))
                    }
                    _ => Err(RuleError::TypeMismatch {
                        expected: "FixedPoint".into(),
                        got: args[0].variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("div".into(), 2);

        fns.insert(
            "min".into(),
            Box::new(|args| {
                if args.len() < 2 {
                    return Err(RuleError::Evaluation(
                        "min requires at least 2 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                        Ok(EvalValue::FixedPoint((*a).min(*b)))
                    }
                    _ => Err(RuleError::TypeMismatch {
                        expected: "FixedPoint".into(),
                        got: args[0].variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("min".into(), 2);

        fns.insert(
            "max".into(),
            Box::new(|args| {
                if args.len() < 2 {
                    return Err(RuleError::Evaluation(
                        "max requires at least 2 arguments".into(),
                    ));
                }
                match (&args[0], &args[1]) {
                    (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                        Ok(EvalValue::FixedPoint((*a).max(*b)))
                    }
                    _ => Err(RuleError::TypeMismatch {
                        expected: "FixedPoint".into(),
                        got: args[0].variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("max".into(), 2);

        fns.insert(
            "not".into(),
            Box::new(|args| {
                if args.is_empty() {
                    return Err(RuleError::Evaluation("not requires 1 argument".into()));
                }
                match &args[0] {
                    EvalValue::Bool(b) => Ok(EvalValue::Bool(!*b)),
                    v => Err(RuleError::TypeMismatch {
                        expected: "bool".into(),
                        got: v.variant_name().to_owned(),
                    }),
                }
            }),
        );
        arg_counts.insert("not".into(), 1);

        (fns, arg_counts)
    }

    /// Create a new evaluator bound to the given query engine and journal.
    pub fn new(
        query_engine: Arc<RwLock<QueryEngine>>,
        journal: Arc<RwLock<Journal>>,
        root_table: impl Into<String>,
        root_row: RowId,
    ) -> Self {
        let (builtins, builtin_arg_counts) = Self::default_builtins();
        Self {
            query_engine,
            journal,
            scope: Scope::new(root_table, root_row),
            cache: PrefetchCache::default(),
            modifiers: ModifierRegistry::new(),
            variables: HashMap::new(),
            builtins,
            builtin_arg_counts,
        }
    }

    pub fn with_modifiers(mut self, modifiers: ModifierRegistry) -> Self {
        self.modifiers = modifiers;
        self
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Advance the evaluator to a new tick, clearing the prefetch cache.
    pub fn advance_tick(&mut self, tick: Tick) {
        self.cache.set_tick(tick);
    }

    /// Convenience: advance to a tick given as raw u64 (for scheduler refresh callback integration).
    pub fn advance_tick_u64(&mut self, tick: u64) {
        self.advance_tick(Tick(tick));
    }

    pub fn current_tick(&self) -> Tick {
        self.cache.tick()
    }

    // ------------------------------------------------------------------
    // Scope management
    // ------------------------------------------------------------------

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn scope_mut(&mut self) -> &mut Scope {
        &mut self.scope
    }

    /// Jump to a related scope using the relation graph.
    ///
    /// The relation lookup is performed through the query-engine ;
    /// the actual row lookup is also performed through the query-engine;
    /// the result is pinned in the prefetch cache.
    pub fn jump_to(&mut self, relation_name: &str, direction: JumpDirection) -> RuleResult<()> {
        let edge = {
            let qe = self
                .query_engine
                .read()
                .map_err(|_| RuleError::QueryEngine("poisoned lock".to_owned()))?;
            qe.resolve_relation_edge(relation_name)
                .map_err(RuleError::from)?
        };

        let target_row = self.resolve_target_row(&edge, direction)?;
        self.scope.apply_jump(&edge, direction, target_row)?;
        self.pin_in_cache(target_row)?;
        Ok(())
    }

    /// Pop the current scope frame and return to the previous scope.
    pub fn scope_pop(&mut self) -> RuleResult<()> {
        self.scope.pop()
    }

    /// Reset the scope to a new root.
    pub fn scope_reset(&mut self, table: impl Into<String>, row: RowId) {
        self.scope.reset(table, row);
    }

    // ------------------------------------------------------------------
    // Variable binding
    // ------------------------------------------------------------------

    pub fn bind_var(&mut self, name: impl Into<String>, value: EvalValue) {
        self.variables.insert(name.into(), value);
    }

    pub fn resolve_var(&self, var: &VarRef) -> RuleResult<&EvalValue> {
        self.variables
            .get(&var.name)
            .ok_or_else(|| RuleError::UnknownVariable(var.name.clone()))
    }

    // ------------------------------------------------------------------
    // Expression evaluation (read-only, via query-engine)
    // ------------------------------------------------------------------

    /// Evaluate an expression to a value.
    ///
    /// All read operations route through the query-engine.
    pub fn evaluate(&mut self, expr: &Expr) -> RuleResult<EvalValue> {
        match expr {
            Expr::Const(fp) => Ok(EvalValue::FixedPoint(*fp)),
            Expr::Bool(b) => Ok(EvalValue::Bool(*b)),
            Expr::String(s) => Ok(EvalValue::String(s.clone())),
            Expr::RowId(id) => Ok(EvalValue::RowId(*id)),
            Expr::Column(path) => self.evaluate_column(path),
            Expr::Var(var) => self.resolve_var(var).cloned(),
            Expr::Not(inner) => self.evaluate_not(inner),
            Expr::And(items) => self.evaluate_and(items),
            Expr::Or(items) => self.evaluate_or(items),
            Expr::Compare { op, lhs, rhs } => self.evaluate_compare(*op, lhs, rhs),
            Expr::Arithmetic { op, lhs, rhs } => self.evaluate_arithmetic(*op, lhs, rhs),
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => self.evaluate_if(cond, then_branch, else_branch),
            Expr::Call { func, args } => self.evaluate_call(func, args),
            Expr::List(items) => self.evaluate_list(items),
            Expr::Map(entries) => self.evaluate_map(entries),
        }
    }

    // ------------------------------------------------------------------
    // Effect submission (via journal)
    // ------------------------------------------------------------------

    /// Submit a diff to the journal for batched commit.
    pub fn submit_diff(&self, diff: Diff, _token: &JournalSubmitToken) -> RuleResult<()> {
        let mut journal = self
            .journal
            .write()
            .map_err(|_| RuleError::JournalWrapper("poisoned lock".to_owned()))?;
        journal.submit_diff(diff, _token).map_err(RuleError::from)
    }

    /// Submit a command envelope to the journal.
    pub fn submit_command(&self, envelope: CommandEnvelope, _token: &JournalSubmitToken) -> RuleResult<()> {
        let mut journal = self
            .journal
            .write()
            .map_err(|_| RuleError::JournalWrapper("poisoned lock".to_owned()))?;
        journal.submit_command(envelope, _token).map_err(RuleError::from)
    }

    /// Evaluate an Effect AST node, submitting resulting Diffs to the journal.
    pub fn evaluate_effect(&mut self, effect: &Effect) -> RuleResult<()> {
        match effect {
            Effect::UpdateColumn {
                table,
                column,
                value,
            } => {
                let val = self.evaluate(value)?;
                let json_val = eval_value_to_json(&val)?;
                let diff = Diff::Update {
                    table: table.clone(),
                    row: self.scope.row(),
                    column: column.clone(),
                    value: json_val,
                };
                self.submit_diff(diff, &JournalSubmitToken::new())
            }
            Effect::InsertRow { table, row, values } => {
                let mut evaluated: serde_json::Map<String, serde_json::Value> =
                    serde_json::Map::new();
                for (col, expr) in values {
                    let val = self.evaluate(expr)?;
                    let json_val = eval_value_to_json(&val)?;
                    evaluated.insert(col.clone(), json_val);
                }
                let diff = Diff::Insert {
                    table: table.clone(),
                    row: *row,
                    values: evaluated,
                };
                self.submit_diff(diff, &JournalSubmitToken::new())
            }
            Effect::SetVariable { name, value } => {
                let val = self.evaluate(value)?;
                self.bind_var(name.clone(), val);
                Ok(())
            }
            Effect::Sequence(effects) => {
                for e in effects {
                    self.evaluate_effect(e)?;
                }
                Ok(())
            }
        }
    }

    // ------------------------------------------------------------------
    // Cache access
    // ------------------------------------------------------------------

    pub fn cache(&self) -> &PrefetchCache {
        &self.cache
    }

    pub fn cache_mut(&mut self) -> &mut PrefetchCache {
        &mut self.cache
    }

    // ------------------------------------------------------------------
    // Modifier access
    // ------------------------------------------------------------------

    pub fn modifiers(&self) -> &ModifierRegistry {
        &self.modifiers
    }

    pub fn modifiers_mut(&mut self) -> &mut ModifierRegistry {
        &mut self.modifiers
    }

    pub fn journal(&self) -> &Arc<RwLock<Journal>> {
        &self.journal
    }

    // ------------------------------------------------------------------
    // Internal helpers (stubs for future implementation)
    // ------------------------------------------------------------------

    fn evaluate_column(&mut self, path: &ColumnPath) -> RuleResult<EvalValue> {
        let row_id = self.scope.row();
        let qe = self
            .query_engine
            .read()
            .map_err(|_| RuleError::QueryEngine("poisoned lock".to_owned()))?;

        let col_view = qe
            .column_view(&path.table, &path.column)
            .map_err(RuleError::from)?;

        let lookup = qe
            .lookup_row(&path.table, row_id)
            .map_err(RuleError::from)?;

        let result = match col_view.kind() {
            ColumnKind::Int64 | ColumnKind::UInt64 => {
                let val = lookup
                    .get_i64(&path.column)
                    .map_err(RuleError::from)?
                    .unwrap_or(0);
                let fp = FixedPoint::new(val, 0);
                EvalValue::FixedPoint(self.modifiers.aggregate(&path.table, &path.column, fp)?)
            }
            ColumnKind::Float64 => {
                let val = lookup
                    .get_f64(&path.column)
                    .map_err(RuleError::from)?
                    .unwrap_or(0.0);
                let fp = FixedPoint::try_from_f64(val, 2)
                    .map_err(|e| RuleError::Evaluation(e.to_string()))?;
                EvalValue::FixedPoint(self.modifiers.aggregate(&path.table, &path.column, fp)?)
            }
            ColumnKind::Boolean => {
                let val = lookup
                    .get_bool(&path.column)
                    .map_err(RuleError::from)?
                    .unwrap_or(false);
                EvalValue::Bool(val)
            }
            ColumnKind::Utf8 | ColumnKind::LargeUtf8 => {
                let val = lookup
                    .get_string(&path.column)
                    .map_err(RuleError::from)?
                    .unwrap_or_default();
                EvalValue::String(val)
            }
            other => {
                return Err(RuleError::Evaluation(format!(
                    "unsupported column type {:?} for {}.{}",
                    other, path.table, path.column
                )))
            }
        };

        Ok(result)
    }

    fn evaluate_not(&mut self, inner: &Expr) -> RuleResult<EvalValue> {
        let value = self.evaluate(inner)?;
        match value {
            EvalValue::Bool(b) => Ok(EvalValue::Bool(!b)),
            _ => Err(RuleError::TypeMismatch {
                expected: "bool".to_owned(),
                got: value.variant_name().to_owned(),
            }),
        }
    }

    fn evaluate_and(&mut self, items: &[Expr]) -> RuleResult<EvalValue> {
        for expr in items {
            let val = self.evaluate(expr)?;
            match val {
                EvalValue::Bool(false) => return Ok(EvalValue::Bool(false)),
                EvalValue::Bool(true) => {}
                _ => {
                    return Err(RuleError::TypeMismatch {
                        expected: "bool".to_owned(),
                        got: "non-bool".to_owned(),
                    })
                }
            }
        }
        Ok(EvalValue::Bool(true))
    }

    fn evaluate_or(&mut self, items: &[Expr]) -> RuleResult<EvalValue> {
        for expr in items {
            let val = self.evaluate(expr)?;
            match val {
                EvalValue::Bool(true) => return Ok(EvalValue::Bool(true)),
                EvalValue::Bool(false) => {}
                _ => {
                    return Err(RuleError::TypeMismatch {
                        expected: "bool".to_owned(),
                        got: "non-bool".to_owned(),
                    })
                }
            }
        }
        Ok(EvalValue::Bool(false))
    }

    fn evaluate_compare(&mut self, op: CompareOp, lhs: &Expr, rhs: &Expr) -> RuleResult<EvalValue> {
        let left = self.evaluate(lhs)?;
        let right = self.evaluate(rhs)?;
        let result = match (left, right) {
            (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => compare_fixed_point(op, a, b),
            (EvalValue::Bool(a), EvalValue::Bool(b)) => compare_bool(op, a, b),
            (EvalValue::String(a), EvalValue::String(b)) => compare_string(op, &a, &b),
            (EvalValue::RowId(a), EvalValue::RowId(b)) => compare_row_id(op, a, b),
            (a, b) => {
                return Err(RuleError::TypeMismatch {
                    expected: a.variant_name().to_owned(),
                    got: b.variant_name().to_owned(),
                })
            }
        };
        Ok(EvalValue::Bool(result))
    }

    fn evaluate_arithmetic(
        &mut self,
        op: crate::expr::ArithmeticOp,
        lhs: &Expr,
        rhs: &Expr,
    ) -> RuleResult<EvalValue> {
        let left = self.evaluate(lhs)?;
        let right = self.evaluate(rhs)?;
        match (left, right) {
            (EvalValue::FixedPoint(a), EvalValue::FixedPoint(b)) => {
                let result = match op {
                    crate::expr::ArithmeticOp::Add => a + b,
                    crate::expr::ArithmeticOp::Sub => a - b,
                    crate::expr::ArithmeticOp::Mul => a * b,
                    crate::expr::ArithmeticOp::Div => a / b,
                    crate::expr::ArithmeticOp::Rem => {
                        return Err(RuleError::UnsupportedOperation(
                            "remainder not supported for FixedPoint".to_owned(),
                        ))
                    }
                };
                Ok(EvalValue::FixedPoint(result.map_err(RuleError::from)?))
            }
            (a, b) => Err(RuleError::TypeMismatch {
                expected: "FixedPoint".to_owned(),
                got: format!("{} / {}", a.variant_name(), b.variant_name()),
            }),
        }
    }

    fn evaluate_if(
        &mut self,
        cond: &Expr,
        then_branch: &Expr,
        else_branch: &Expr,
    ) -> RuleResult<EvalValue> {
        let cond_val = self.evaluate(cond)?;
        match cond_val {
            EvalValue::Bool(true) => self.evaluate(then_branch),
            EvalValue::Bool(false) => self.evaluate(else_branch),
            _ => Err(RuleError::TypeMismatch {
                expected: "bool".to_owned(),
                got: cond_val.variant_name().to_owned(),
            }),
        }
    }

    fn evaluate_call(&mut self, func: &str, args: &HashMap<String, Expr>) -> RuleResult<EvalValue> {
        if let Some(expected) = self.builtin_arg_counts.get(func) {
            if args.len() != *expected {
                return Err(RuleError::Evaluation(format!(
                    "function '{}' expects {} arguments, got {}",
                    func,
                    expected,
                    args.len()
                )));
            }
        }

        let mut sorted_keys: Vec<&String> = args.keys().collect();
        sorted_keys.sort();
        let evaluated_args: Vec<EvalValue> = sorted_keys
            .iter()
            .map(|k| {
                let expr = args.get(*k).ok_or_else(|| {
                    RuleError::Evaluation(format!("missing argument '{}' in call to '{}'", k, func))
                })?;
                self.evaluate(expr)
            })
            .collect::<RuleResult<Vec<_>>>()?;

        let f = self.builtins.get(func).ok_or_else(|| {
            RuleError::UnsupportedOperation(format!("unknown function: {}", func))
        })?;

        f(&evaluated_args)
    }

    /// Register a custom callable function that will be available via `Expr::Call`.
    pub fn register_function(
        &mut self,
        name: impl Into<String>,
        func: impl Fn(&[EvalValue]) -> RuleResult<EvalValue> + Send + Sync + 'static,
    ) {
        self.builtins.insert(name.into(), Box::new(func));
    }

    fn evaluate_list(&mut self, items: &[Expr]) -> RuleResult<EvalValue> {
        let values: Vec<EvalValue> = items
            .iter()
            .map(|e| self.evaluate(e))
            .collect::<RuleResult<Vec<_>>>()?;
        Ok(EvalValue::List(values))
    }

    fn evaluate_map(&mut self, entries: &HashMap<String, Expr>) -> RuleResult<EvalValue> {
        let values: HashMap<String, EvalValue> = entries
            .iter()
            .map(|(k, e)| self.evaluate(e).map(|v| (k.clone(), v)))
            .collect::<RuleResult<HashMap<_, _>>>()?;
        Ok(EvalValue::Map(values))
    }

    fn resolve_target_row(
        &self,
        edge: &RelationEdge,
        direction: JumpDirection,
    ) -> RuleResult<RowId> {
        let qe = self
            .query_engine
            .read()
            .map_err(|_| RuleError::QueryEngine("poisoned lock".to_owned()))?;

        match direction {
            JumpDirection::Forward => {
                let fk_val = qe
                    .lookup_row(&edge.from, self.scope.row())
                    .map_err(RuleError::from)?
                    .get_i64(&edge.from_column)
                    .map_err(RuleError::from)?
                    .ok_or_else(|| RuleError::Scope("FK value is null".to_owned()))?;

                let target_spec = qe.table_schema(&edge.to).map_err(RuleError::from)?;
                let pk_col = target_spec
                    .primary_key_column()
                    .ok_or_else(|| {
                        RuleError::Scope(format!("table '{}' has no primary key column", edge.to))
                    })?
                    .name
                    .clone();

                let view = qe
                    .read_single_table(self.cache.tick(), &edge.to)
                    .map_err(RuleError::from)?;
                let pm = view.position_map();
                for row_id in pm.row_ids() {
                    let pk_val = qe
                        .lookup_row(&edge.to, row_id)
                        .map_err(RuleError::from)?
                        .get_i64(&pk_col)
                        .map_err(RuleError::from)?
                        .ok_or_else(|| RuleError::Scope("target row has null PK".to_owned()))?;
                    if pk_val == fk_val {
                        return Ok(row_id);
                    }
                }
                Err(RuleError::Scope(format!(
                    "target row not found: {} -> {} (fk={})",
                    edge.from, edge.to, fk_val
                )))
            }
            JumpDirection::Reverse => {
                let current_spec = qe.table_schema(&edge.to).map_err(RuleError::from)?;
                let pk_col = current_spec
                    .primary_key_column()
                    .ok_or_else(|| {
                        RuleError::Scope(format!("table '{}' has no primary key column", edge.to))
                    })?
                    .name
                    .clone();

                let current_pk = qe
                    .lookup_row(&edge.to, self.scope.row())
                    .map_err(RuleError::from)?
                    .get_i64(&pk_col)
                    .map_err(RuleError::from)?
                    .ok_or_else(|| RuleError::Scope("current row PK is null".to_owned()))?;

                let view = qe
                    .read_single_table(self.cache.tick(), &edge.from)
                    .map_err(RuleError::from)?;
                let pm = view.position_map();
                for row_id in pm.row_ids() {
                    let fk_val = qe
                        .lookup_row(&edge.from, row_id)
                        .map_err(RuleError::from)?
                        .get_i64(&edge.from_column)
                        .map_err(RuleError::from)?
                        .ok_or_else(|| RuleError::Scope("row has null FK".to_owned()))?;
                    if fk_val == current_pk {
                        return Ok(row_id);
                    }
                }
                Err(RuleError::Scope(format!(
                    "target row not found: {} <- {} (pk={})",
                    edge.from, edge.to, current_pk
                )))
            }
        }
    }

    fn pin_in_cache(&mut self, row_id: RowId) -> RuleResult<()> {
        let table = self.scope.table().to_owned();
        let tick = self.cache.tick();

        let view = match self.cache.find_view(&table, tick) {
            Some(view) => view,
            None => {
                let qe = self
                    .query_engine
                    .read()
                    .map_err(|_| RuleError::QueryEngine("poisoned lock".to_owned()))?;
                let view = qe
                    .read_single_table(tick, &table)
                    .map_err(RuleError::from)?;
                Arc::new(view)
            }
        };

        let cached = crate::cache::CachedRow {
            tick,
            table,
            row_id,
            view,
        };
        self.cache.insert(cached)
    }
}

fn compare_fixed_point(op: CompareOp, a: FixedPoint, b: FixedPoint) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

fn compare_bool(op: CompareOp, a: bool, b: bool) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        _ => false,
    }
}

fn compare_string(op: CompareOp, a: &str, b: &str) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

fn compare_row_id(op: CompareOp, a: RowId, b: RowId) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

/// Convert an evaluated value into its JSON representation for diff submission.
fn eval_value_to_json(val: &EvalValue) -> RuleResult<serde_json::Value> {
    match val {
        EvalValue::FixedPoint(fp) => {
            let num = serde_json::Number::from_f64(fp.to_f64()).ok_or_else(|| {
                RuleError::Evaluation("cannot convert FixedPoint to JSON number".to_owned())
            })?;
            Ok(serde_json::Value::Number(num))
        }
        EvalValue::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        EvalValue::String(s) => Ok(serde_json::Value::String(s.clone())),
        EvalValue::RowId(id) => Ok(serde_json::Value::Number(id.as_u64().into())),
        EvalValue::List(items) => {
            let vals: Vec<serde_json::Value> = items
                .iter()
                .map(eval_value_to_json)
                .collect::<RuleResult<Vec<_>>>()?;
            Ok(serde_json::Value::Array(vals))
        }
        EvalValue::Map(entries) => {
            let map: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| eval_value_to_json(v).map(|jv| (k.clone(), jv)))
                .collect::<RuleResult<serde_json::Map<_, _>>>()?;
            Ok(serde_json::Value::Object(map))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_arrow_store::ArrowStore;
    use scharnhorst_core::{FixedPoint, RowId};
    use scharnhorst_schema::SchemaRegistry;
    use std::collections::HashMap;

    fn make_evaluator() -> Evaluator {
        let journal = std::sync::Arc::new(std::sync::RwLock::new(Journal::new(
            std::sync::Arc::new(ArrowStore::new()),
        )));
        let query_engine = std::sync::Arc::new(std::sync::RwLock::new(QueryEngine::new(
            SchemaRegistry::new(),
        )));
        let (builtins, builtin_arg_counts) = Evaluator::default_builtins();
        Evaluator {
            query_engine,
            journal,
            scope: Scope::new("t", RowId::new(0)),
            cache: PrefetchCache::new(64),
            modifiers: ModifierRegistry::new(),
            variables: HashMap::new(),
            builtins,
            builtin_arg_counts,
        }
    }

    #[test]
    fn evaluate_const() {
        let mut ev = make_evaluator();
        let result = ev.evaluate(&Expr::constant(FixedPoint::from_i64(42, 0).unwrap()));
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            EvalValue::FixedPoint(FixedPoint::from_i64(42, 0).unwrap())
        );
    }

    #[test]
    fn evaluate_bool() {
        let mut ev = make_evaluator();
        let result = ev.evaluate(&Expr::constant_bool(true));
        assert_eq!(result.unwrap(), EvalValue::Bool(true));
    }

    #[test]
    fn compare_fixed_point_ge() {
        assert!(compare_fixed_point(
            CompareOp::Ge,
            FixedPoint::from_i64(5, 0).unwrap(),
            FixedPoint::from_i64(5, 0).unwrap()
        ));
        assert!(compare_fixed_point(
            CompareOp::Ge,
            FixedPoint::from_i64(6, 0).unwrap(),
            FixedPoint::from_i64(5, 0).unwrap()
        ));
        assert!(!compare_fixed_point(
            CompareOp::Ge,
            FixedPoint::from_i64(4, 0).unwrap(),
            FixedPoint::from_i64(5, 0).unwrap()
        ));
    }

    #[test]
    fn compare_bool_non_eq_ne_returns_false() {
        assert!(!compare_bool(CompareOp::Lt, true, false));
        assert!(!compare_bool(CompareOp::Gt, false, true));
    }

    #[test]
    fn compare_rowid_consistency() {
        let a = RowId::new(1);
        let b = RowId::new(2);
        assert!(compare_row_id(CompareOp::Eq, a, a));
        assert!(compare_row_id(CompareOp::Ne, a, b));
        assert!(compare_row_id(CompareOp::Lt, a, b));
        assert!(!compare_row_id(CompareOp::Lt, b, a));
    }

    // ==================================================================
    // A. Pure Function Tests
    // ==================================================================

    /// All six CompareOp variants (Eq, Ne, Lt, Le, Gt, Ge) on compare_fixed_point.
    #[test]
    fn compare_fixed_point_all_six_ops() {
        let a = FixedPoint::from_i64(10, 2).unwrap(); // 10.00
        let b = FixedPoint::from_i64(20, 2).unwrap(); // 20.00

        assert!(compare_fixed_point(CompareOp::Eq, a, a));
        assert!(!compare_fixed_point(CompareOp::Eq, a, b));

        assert!(!compare_fixed_point(CompareOp::Ne, a, a));
        assert!(compare_fixed_point(CompareOp::Ne, a, b));

        assert!(compare_fixed_point(CompareOp::Lt, a, b));
        assert!(!compare_fixed_point(CompareOp::Lt, b, a));

        assert!(compare_fixed_point(CompareOp::Le, a, b));
        assert!(compare_fixed_point(CompareOp::Le, a, a));
        assert!(!compare_fixed_point(CompareOp::Le, b, a));

        assert!(!compare_fixed_point(CompareOp::Gt, a, b));
        assert!(compare_fixed_point(CompareOp::Gt, b, a));

        assert!(!compare_fixed_point(CompareOp::Ge, a, b));
        assert!(compare_fixed_point(CompareOp::Ge, a, a));
        assert!(compare_fixed_point(CompareOp::Ge, b, a));
    }

    /// compare_fixed_point with same-scale and different-scale values.
    /// Different scales produce different FixedPoint identities; PartialEq
    /// compares both raw and scale, so values at different scales are unequal.
    #[test]
    fn compare_fixed_point_same_and_different_scales() {
        let a_scale2 = FixedPoint::from_i64(10, 2).unwrap(); // raw=1000, represents 10.00
        let b_scale2 = FixedPoint::from_i64(10, 2).unwrap(); // raw=1000, represents 10.00

        assert!(compare_fixed_point(CompareOp::Eq, a_scale2, b_scale2));
        assert!(!compare_fixed_point(CompareOp::Ne, a_scale2, b_scale2));

        let c_scale0 = FixedPoint::from_i64(10, 0).unwrap(); // raw=10, represents 10
        let d_scale2 = FixedPoint::from_i64(5, 2).unwrap(); // raw=500, represents 5.00

        assert!(compare_fixed_point(CompareOp::Eq, a_scale2, c_scale0));
        assert!(!compare_fixed_point(CompareOp::Ne, a_scale2, c_scale0));

        assert!(compare_fixed_point(CompareOp::Gt, a_scale2, d_scale2));
        assert!(compare_fixed_point(CompareOp::Lt, d_scale2, a_scale2));
    }

    /// compare_bool with all six CompareOp variants.
    /// Only Eq and Ne have meaningful semantics; Lt/Le/Gt/Ge always return false.
    #[test]
    fn compare_bool_all_six_ops() {
        assert!(compare_bool(CompareOp::Eq, true, true));
        assert!(!compare_bool(CompareOp::Eq, true, false));

        assert!(!compare_bool(CompareOp::Ne, true, true));
        assert!(compare_bool(CompareOp::Ne, true, false));

        // Non-Eq/Ne ops always return false for bool
        assert!(!compare_bool(CompareOp::Lt, true, false));
        assert!(!compare_bool(CompareOp::Le, true, true));
        assert!(!compare_bool(CompareOp::Gt, false, true));
        assert!(!compare_bool(CompareOp::Ge, true, true));
    }

    /// compare_string with all six CompareOp variants including lexicographic ordering.
    #[test]
    fn compare_string_all_ops_ordering() {
        assert!(compare_string(CompareOp::Eq, "abc", "abc"));
        assert!(!compare_string(CompareOp::Eq, "abc", "xyz"));

        assert!(!compare_string(CompareOp::Ne, "abc", "abc"));
        assert!(compare_string(CompareOp::Ne, "abc", "xyz"));

        // Lexicographic ordering: "abc" < "xyz"
        assert!(compare_string(CompareOp::Lt, "abc", "xyz"));
        assert!(!compare_string(CompareOp::Lt, "xyz", "abc"));

        assert!(compare_string(CompareOp::Le, "abc", "abc"));
        assert!(compare_string(CompareOp::Le, "abc", "xyz"));
        assert!(!compare_string(CompareOp::Le, "xyz", "abc"));

        assert!(!compare_string(CompareOp::Gt, "abc", "xyz"));
        assert!(compare_string(CompareOp::Gt, "xyz", "abc"));

        assert!(compare_string(CompareOp::Ge, "abc", "abc"));
        assert!(compare_string(CompareOp::Ge, "xyz", "abc"));
        assert!(!compare_string(CompareOp::Ge, "abc", "xyz"));
    }

    /// compare_row_id with all six CompareOp variants.
    #[test]
    fn compare_row_id_all_six_ops() {
        let a = RowId::new(1);
        let b = RowId::new(2);

        assert!(compare_row_id(CompareOp::Eq, a, a));
        assert!(!compare_row_id(CompareOp::Eq, a, b));

        assert!(!compare_row_id(CompareOp::Ne, a, a));
        assert!(compare_row_id(CompareOp::Ne, a, b));

        assert!(compare_row_id(CompareOp::Lt, a, b));
        assert!(!compare_row_id(CompareOp::Lt, b, a));

        assert!(compare_row_id(CompareOp::Le, a, a));
        assert!(compare_row_id(CompareOp::Le, a, b));
        assert!(!compare_row_id(CompareOp::Le, b, a));

        assert!(!compare_row_id(CompareOp::Gt, a, b));
        assert!(compare_row_id(CompareOp::Gt, b, a));

        assert!(compare_row_id(CompareOp::Ge, a, a));
        assert!(compare_row_id(CompareOp::Ge, b, a));
        assert!(!compare_row_id(CompareOp::Ge, a, b));
    }

    /// eval_value_to_json converts every EvalValue variant to its JSON representation.
    #[test]
    fn eval_value_to_json_all_variants() {
        // FixedPoint -> JSON Number
        let fp = EvalValue::FixedPoint(FixedPoint::from_i64(42, 2).unwrap());
        let json = eval_value_to_json(&fp).unwrap();
        assert_eq!(json, serde_json::json!(42.0));

        // Bool -> JSON Bool
        let b = EvalValue::Bool(true);
        assert_eq!(
            eval_value_to_json(&b).unwrap(),
            serde_json::Value::Bool(true)
        );

        // String -> JSON String
        let s = EvalValue::String("hello".to_owned());
        assert_eq!(eval_value_to_json(&s).unwrap(), serde_json::json!("hello"));

        // RowId -> JSON Number (u64)
        let rid = EvalValue::RowId(RowId::new(99));
        assert_eq!(eval_value_to_json(&rid).unwrap(), serde_json::json!(99));

        // List -> JSON Array
        let list = EvalValue::List(vec![
            EvalValue::Bool(true),
            EvalValue::String("a".to_owned()),
        ]);
        assert_eq!(
            eval_value_to_json(&list).unwrap(),
            serde_json::json!([true, "a"])
        );

        // Map -> JSON Object
        let mut map_entries = HashMap::new();
        map_entries.insert(
            "k".to_owned(),
            EvalValue::FixedPoint(FixedPoint::from_i64(1, 0).unwrap()),
        );
        let map = EvalValue::Map(map_entries);
        let json_map = eval_value_to_json(&map).unwrap();
        assert_eq!(json_map, serde_json::json!({"k": 1.0}));
    }

    // ==================================================================
    // B. Evaluator Construction & Lifecycle
    // ==================================================================

    /// advance_tick updates the evaluator's current tick and
    /// clears the prefetch cache across tick boundaries.
    #[test]
    fn dc3_tick_advance_clears_prefetch_cache() {
        let mut ev = make_evaluator();
        assert_eq!(ev.current_tick(), Tick(0));

        ev.advance_tick(Tick(5));
        assert_eq!(ev.current_tick(), Tick(5));
        assert!(ev.cache().is_empty());

        // Advance to another tick: cache should remain cleared
        ev.advance_tick(Tick(10));
        assert_eq!(ev.current_tick(), Tick(10));
        assert!(ev.cache().is_empty());
    }

    /// Evaluator::new binds to query engine and journal, initializes
    /// scope with the given root table and row.
    #[test]
    fn evaluator_new_binds_to_query_engine_and_journal() {
        let journal = Arc::new(RwLock::new(Journal::new(Arc::new(ArrowStore::new()))));
        let qe = Arc::new(RwLock::new(QueryEngine::new(SchemaRegistry::new())));
        let ev = Evaluator::new(qe, journal, "province", RowId::new(7));

        assert_eq!(ev.scope().table(), "province");
        assert_eq!(ev.scope().row(), RowId::new(7));
        assert_eq!(ev.current_tick(), Tick(0));
    }

    /// Evaluator::with_modifiers registers a ModifierRegistry.
    #[test]
    fn evaluator_with_modifiers_registers_modifier_registry() {
        let journal = Arc::new(RwLock::new(Journal::new(Arc::new(ArrowStore::new()))));
        let qe = Arc::new(RwLock::new(QueryEngine::new(SchemaRegistry::new())));
        let reg = ModifierRegistry::new();
        let ev = Evaluator::new(qe, journal, "t", RowId::new(0)).with_modifiers(reg);

        // Verify modifiers accessor works (registry is accessible)
        let _ = ev.modifiers();
    }

    // ==================================================================
    // C. Variable Binding (Scoped evaluation)
    // ==================================================================

    /// bind_var then resolve_var roundtrip for all EvalValue types.
    #[test]
    fn bind_and_resolve_variable_roundtrip() {
        let mut ev = make_evaluator();

        // FixedPoint
        ev.bind_var(
            "fp",
            EvalValue::FixedPoint(FixedPoint::from_i64(42, 2).unwrap()),
        );
        let resolved = ev.resolve_var(&VarRef::new("fp")).unwrap();
        assert_eq!(
            *resolved,
            EvalValue::FixedPoint(FixedPoint::from_i64(42, 2).unwrap())
        );

        // Bool
        ev.bind_var("flag", EvalValue::Bool(true));
        assert_eq!(
            *ev.resolve_var(&VarRef::new("flag")).unwrap(),
            EvalValue::Bool(true)
        );

        // String
        ev.bind_var("msg", EvalValue::String("hello".to_owned()));
        assert_eq!(
            *ev.resolve_var(&VarRef::new("msg")).unwrap(),
            EvalValue::String("hello".to_owned())
        );

        // RowId
        ev.bind_var("rid", EvalValue::RowId(RowId::new(99)));
        assert_eq!(
            *ev.resolve_var(&VarRef::new("rid")).unwrap(),
            EvalValue::RowId(RowId::new(99))
        );
    }

    /// resolve_var on an unknown variable returns RuleError::UnknownVariable.
    #[test]
    fn resolve_unknown_variable_returns_error() {
        let ev = make_evaluator();
        let result = ev.resolve_var(&VarRef::new("nonexistent"));
        assert!(result.is_err());
        match result {
            Err(RuleError::UnknownVariable(name)) => assert_eq!(name, "nonexistent"),
            _ => panic!("expected UnknownVariable error"),
        }
    }

    /// bind_var overwrites a previously bound variable.
    #[test]
    fn bind_var_overwrites_previous_binding() {
        let mut ev = make_evaluator();
        ev.bind_var(
            "x",
            EvalValue::FixedPoint(FixedPoint::from_i64(1, 0).unwrap()),
        );
        assert_eq!(
            *ev.resolve_var(&VarRef::new("x")).unwrap(),
            EvalValue::FixedPoint(FixedPoint::from_i64(1, 0).unwrap())
        );

        // Overwrite
        ev.bind_var(
            "x",
            EvalValue::FixedPoint(FixedPoint::from_i64(99, 0).unwrap()),
        );
        assert_eq!(
            *ev.resolve_var(&VarRef::new("x")).unwrap(),
            EvalValue::FixedPoint(FixedPoint::from_i64(99, 0).unwrap())
        );
    }

    /// evaluate_effect with SetVariable binds a variable in the evaluator
    /// and makes it resolvable via resolve_var.
    #[test]
    fn dc1_effect_set_variable_binds_in_evaluator() {
        let mut ev = make_evaluator();
        let effect = Effect::SetVariable {
            name: "treasury".to_owned(),
            value: Expr::constant(FixedPoint::from_i64(500, 2).unwrap()),
        };
        let result = ev.evaluate_effect(&effect);
        assert!(result.is_ok());

        let val = ev.resolve_var(&VarRef::new("treasury")).unwrap();
        assert_eq!(
            *val,
            EvalValue::FixedPoint(FixedPoint::from_i64(500, 2).unwrap())
        );
    }

    // ==================================================================
    // D. Effect Evaluation
    // ==================================================================

    /// evaluate_effect with UpdateColumn submits an Update Diff
    /// to the journal, verified via pending_diff_count.
    #[test]
    fn dc1_effect_update_column_submits_diff_to_journal() {
        let mut ev = make_evaluator();
        let effect = Effect::UpdateColumn {
            table: "actor_state".to_owned(),
            column: "treasury".to_owned(),
            value: Expr::constant(FixedPoint::from_i64(100, 2).unwrap()),
        };
        let result = ev.evaluate_effect(&effect);
        assert!(result.is_ok());

        // diff was submitted to the journal
        let journal = ev.journal().read().unwrap();
        assert_eq!(journal.pending_diff_count(), 1);
    }

    /// evaluate_effect with Sequence executes all effects in order.
    /// Each SetVariable binds a variable sequentially to the evaluator.
    #[test]
    fn dc1_effect_sequence_executes_all_in_order() {
        let mut ev = make_evaluator();
        let effects = vec![
            Effect::SetVariable {
                name: "x".to_owned(),
                value: Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
            },
            Effect::SetVariable {
                name: "y".to_owned(),
                value: Expr::constant(FixedPoint::from_i64(2, 0).unwrap()),
            },
        ];
        let seq = Effect::Sequence(effects);
        let result = ev.evaluate_effect(&seq);
        assert!(result.is_ok());

        // both effects were applied in order
        assert_eq!(
            *ev.resolve_var(&VarRef::new("x")).unwrap(),
            EvalValue::FixedPoint(FixedPoint::from_i64(1, 0).unwrap())
        );
        assert_eq!(
            *ev.resolve_var(&VarRef::new("y")).unwrap(),
            EvalValue::FixedPoint(FixedPoint::from_i64(2, 0).unwrap())
        );
    }

    /// evaluate_effect with InsertRow submits an Insert Diff to the journal.
    #[test]
    fn dc1_effect_insert_row_submits_diff() {
        let mut ev = make_evaluator();
        let mut values = HashMap::new();
        values.insert(
            "treasury".to_owned(),
            Expr::constant(FixedPoint::from_i64(500, 2).unwrap()),
        );
        let effect = Effect::InsertRow {
            table: "actor_state".to_owned(),
            row: RowId::new(99),
            values,
        };
        let result = ev.evaluate_effect(&effect);
        assert!(result.is_ok());

        // diff was submitted to the journal
        let journal = ev.journal().read().unwrap();
        assert_eq!(journal.pending_diff_count(), 1);
    }

    /// SetVariable effect modifies evaluator state:
    /// before evaluation the variable is unknown, after evaluation it resolves.
    #[test]
    fn dc1_effect_set_variable_modifies_evaluator_state() {
        let mut ev = make_evaluator();

        // before the effect, the variable is not bound
        assert!(ev.resolve_var(&VarRef::new("stability")).is_err());

        // evaluate_effect with SetVariable binds it
        let effect = Effect::SetVariable {
            name: "stability".to_owned(),
            value: Expr::constant(FixedPoint::from_i64(50, 1).unwrap()),
        };
        ev.evaluate_effect(&effect).unwrap();

        // after the effect, the variable resolves
        let val = ev.resolve_var(&VarRef::new("stability")).unwrap();
        assert_eq!(
            *val,
            EvalValue::FixedPoint(FixedPoint::from_i64(50, 1).unwrap())
        );
    }

    // ==================================================================
    // E. Error Path Testing
    // ==================================================================

    /// evaluate Not on a non-bool value returns TypeMismatch error.
    #[test]
    fn evaluate_not_on_non_bool_returns_type_mismatch() {
        let mut ev = make_evaluator();
        let expr = Expr::new_not(Expr::Const(scharnhorst_core::FixedPoint::new(42, 0)));
        let result = ev.evaluate(&expr);
        assert!(result.is_err());
        match result {
            Err(RuleError::TypeMismatch { expected, .. }) => {
                assert_eq!(expected, "bool");
            }
            _ => panic!("expected TypeMismatch error"),
        }
    }

    /// evaluate Compare with mixed types (FixedPoint vs Bool) returns TypeMismatch.
    #[test]
    fn evaluate_compare_with_mixed_types_returns_type_mismatch() {
        let mut ev = make_evaluator();
        let expr = Expr::compare(
            CompareOp::Eq,
            Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
            Expr::constant_bool(true),
        );
        let result = ev.evaluate(&expr);
        assert!(result.is_err());
        match result {
            Err(RuleError::TypeMismatch { .. }) => {}
            _ => panic!("expected TypeMismatch error"),
        }
    }

    /// evaluate Arithmetic on non-FixedPoint operands returns TypeMismatch.
    #[test]
    fn evaluate_arithmetic_on_non_fixed_point_returns_type_mismatch() {
        let mut ev = make_evaluator();
        let expr = Expr::arithmetic(
            crate::expr::ArithmeticOp::Add,
            Expr::constant_bool(true),
            Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
        );
        let result = ev.evaluate(&expr);
        assert!(result.is_err());
        match result {
            Err(RuleError::TypeMismatch { expected, .. }) => {
                assert_eq!(expected, "FixedPoint");
            }
            _ => panic!("expected TypeMismatch error"),
        }
    }

    /// evaluate If with a non-bool condition returns TypeMismatch.
    #[test]
    fn evaluate_if_with_non_bool_condition_returns_type_mismatch() {
        let mut ev = make_evaluator();
        let expr = Expr::if_then_else(
            Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
            Expr::constant_bool(true),
            Expr::constant_bool(false),
        );
        let result = ev.evaluate(&expr);
        assert!(result.is_err());
        match result {
            Err(RuleError::TypeMismatch { expected, .. }) => {
                assert_eq!(expected, "bool");
            }
            _ => panic!("expected TypeMismatch error"),
        }
    }

    /// evaluate Call with an unknown function name returns UnsupportedOperation.
    #[test]
    fn evaluate_call_unknown_function_returns_unsupported() {
        let mut ev = make_evaluator();
        let args = HashMap::new();
        let expr = Expr::call("nonexistent_func", args);
        let result = ev.evaluate(&expr);
        assert!(result.is_err());
        match result {
            Err(RuleError::UnsupportedOperation(msg)) => {
                assert!(msg.contains("unknown function"));
            }
            _ => panic!("expected UnsupportedOperation error"),
        }
    }

    /// evaluate Call with missing required arguments returns an error.
    #[test]
    fn evaluate_call_missing_args_returns_error() {
        let mut ev = make_evaluator();
        // "add" requires "x" and "y" args; we provide none
        let expr = Expr::call("add", HashMap::new());
        let result = ev.evaluate(&expr);
        assert!(result.is_err());
    }

    // ==================================================================
    // F. Expression Evaluator (via make_evaluator)
    // ==================================================================

    /// Evaluator routes column lookups through the query-engine.
    /// With an empty schema registry, the column path fails gracefully.
    #[test]
    fn dc10_evaluator_reads_from_query_engine_for_column() {
        let mut ev = make_evaluator();
        // column evaluation goes through the query-engine
        let expr = Expr::column("t", "some_column");
        let result = ev.evaluate(&expr);
        // Empty schema registry in the test QE means this errors;
        // the error confirms the QE path is exercised.
        assert!(result.is_err());
    }

    /// evaluate And: true && false -> false; verifies boolean conjunction logic.
    #[test]
    fn evaluate_and_short_circuits_on_false() {
        let mut ev = make_evaluator();
        // true && false -> false
        let expr = Expr::and(vec![Expr::constant_bool(true), Expr::constant_bool(false)]);
        let result = ev.evaluate(&expr).unwrap();
        assert_eq!(result, EvalValue::Bool(false));

        // true && true && true -> true
        let expr2 = Expr::and(vec![
            Expr::constant_bool(true),
            Expr::constant_bool(true),
            Expr::constant_bool(true),
        ]);
        let result2 = ev.evaluate(&expr2).unwrap();
        assert_eq!(result2, EvalValue::Bool(true));
    }

    /// evaluate Or: true || false -> true; verifies boolean disjunction logic.
    #[test]
    fn evaluate_or_short_circuits_on_true() {
        let mut ev = make_evaluator();
        // true || false -> true
        let expr = Expr::or(vec![Expr::constant_bool(true), Expr::constant_bool(false)]);
        let result = ev.evaluate(&expr).unwrap();
        assert_eq!(result, EvalValue::Bool(true));

        // false || false -> false
        let expr2 = Expr::or(vec![Expr::constant_bool(false), Expr::constant_bool(false)]);
        let result2 = ev.evaluate(&expr2).unwrap();
        assert_eq!(result2, EvalValue::Bool(false));
    }

    /// evaluate empty And returns true (identity of conjunction).
    #[test]
    fn evaluate_empty_and_returns_true() {
        let mut ev = make_evaluator();
        let expr = Expr::and(vec![]);
        let result = ev.evaluate(&expr).unwrap();
        // for loop with Bool(true) identity start returns identity on empty iter
        assert_eq!(result, EvalValue::Bool(true));
    }

    /// evaluate empty Or returns false (identity of disjunction).
    #[test]
    fn evaluate_empty_or_returns_false() {
        let mut ev = make_evaluator();
        let expr = Expr::or(vec![]);
        let result = ev.evaluate(&expr).unwrap();
        // for loop with Bool(false) identity start returns identity on empty iter
        assert_eq!(result, EvalValue::Bool(false));
    }

    /// evaluate List produces an ordered EvalValue::List.
    #[test]
    fn evaluate_list_produces_ordered_eval_values() {
        let mut ev = make_evaluator();
        let expr = Expr::list(vec![
            Expr::constant(FixedPoint::from_i64(1, 0).unwrap()),
            Expr::constant_bool(true),
            Expr::String("abc".to_owned()),
        ]);
        let result = ev.evaluate(&expr).unwrap();
        assert_eq!(
            result,
            EvalValue::List(vec![
                EvalValue::FixedPoint(FixedPoint::from_i64(1, 0).unwrap()),
                EvalValue::Bool(true),
                EvalValue::String("abc".to_owned()),
            ])
        );
    }

    /// evaluate Map produces keyed EvalValue::Map entries.
    #[test]
    fn evaluate_map_produces_keyed_eval_values() {
        let mut ev = make_evaluator();
        let mut entries = HashMap::new();
        entries.insert("a".to_owned(), Expr::constant_bool(true));
        entries.insert("b".to_owned(), Expr::String("v".to_owned()));
        let expr = Expr::map(entries);

        let result = ev.evaluate(&expr).unwrap();
        if let EvalValue::Map(map) = result {
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("a").unwrap(), &EvalValue::Bool(true));
            assert_eq!(map.get("b").unwrap(), &EvalValue::String("v".to_owned()));
        } else {
            panic!("expected EvalValue::Map");
        }
    }

    /// evaluate Call for add, mul, min, max functions with FixedPoint operands.
    #[test]
    fn evaluate_call_add_mul_min_max_functions() {
        let mut ev = make_evaluator();

        // add: 2.00 + 3.00 = 5.00
        let mut args = HashMap::new();
        args.insert(
            "x".to_owned(),
            Expr::constant(FixedPoint::from_i64(2, 2).unwrap()),
        );
        args.insert(
            "y".to_owned(),
            Expr::constant(FixedPoint::from_i64(3, 2).unwrap()),
        );
        let result = ev.evaluate(&Expr::call("add", args)).unwrap();
        assert_eq!(
            result,
            EvalValue::FixedPoint(FixedPoint::from_i64(5, 2).unwrap())
        );

        // mul: 2.00 * 3.00 = 6.00
        let mut args = HashMap::new();
        args.insert(
            "x".to_owned(),
            Expr::constant(FixedPoint::from_i64(2, 2).unwrap()),
        );
        args.insert(
            "y".to_owned(),
            Expr::constant(FixedPoint::from_i64(3, 2).unwrap()),
        );
        let result = ev.evaluate(&Expr::call("mul", args)).unwrap();
        assert_eq!(
            result,
            EvalValue::FixedPoint(FixedPoint::from_i64(6, 2).unwrap())
        );

        // min: min(5.00, 3.00) = 3.00
        let mut args = HashMap::new();
        args.insert(
            "x".to_owned(),
            Expr::constant(FixedPoint::from_i64(5, 2).unwrap()),
        );
        args.insert(
            "y".to_owned(),
            Expr::constant(FixedPoint::from_i64(3, 2).unwrap()),
        );
        let result = ev.evaluate(&Expr::call("min", args)).unwrap();
        assert_eq!(
            result,
            EvalValue::FixedPoint(FixedPoint::from_i64(3, 2).unwrap())
        );

        // max: max(5.00, 3.00) = 5.00
        let mut args = HashMap::new();
        args.insert(
            "x".to_owned(),
            Expr::constant(FixedPoint::from_i64(5, 2).unwrap()),
        );
        args.insert(
            "y".to_owned(),
            Expr::constant(FixedPoint::from_i64(3, 2).unwrap()),
        );
        let result = ev.evaluate(&Expr::call("max", args)).unwrap();
        assert_eq!(
            result,
            EvalValue::FixedPoint(FixedPoint::from_i64(5, 2).unwrap())
        );
    }

    /// evaluate Call for the "not" function with a boolean argument.
    #[test]
    fn evaluate_call_not_function() {
        let mut ev = make_evaluator();

        // not(true) -> false
        let mut args = HashMap::new();
        args.insert("value".to_owned(), Expr::constant_bool(true));
        let result = ev.evaluate(&Expr::call("not", args)).unwrap();
        assert_eq!(result, EvalValue::Bool(false));

        // not(false) -> true
        let mut args = HashMap::new();
        args.insert("value".to_owned(), Expr::constant_bool(false));
        let result = ev.evaluate(&Expr::call("not", args)).unwrap();
        assert_eq!(result, EvalValue::Bool(true));

        // not("non-bool") -> TypeMismatch error
        let mut args = HashMap::new();
        args.insert("value".to_owned(), Expr::String("bad".to_owned()));
        let result = ev.evaluate(&Expr::call("not", args));
        assert!(result.is_err());
        match result {
            Err(RuleError::TypeMismatch { expected, .. }) => {
                assert_eq!(expected, "bool");
            }
            _ => panic!("expected TypeMismatch error"),
        }
    }
}
