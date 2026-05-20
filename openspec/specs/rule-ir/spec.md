## Requirements

### Requirement: Rule Intermediate Representation
The system SHALL compile modded logic into a stable Rule IR consisting of expressions, triggers, and effects.

#### Scenario: Evaluating a trigger
- **WHEN** checking if a revolt should start
- **THEN** the evaluator processes the `Expr::And` of "unrest > 0.8" and "stability < 0.3".

### Requirement: Scope Traversal
The system MUST support scope jumping (e.g., `Actor` -> `OwnedProvince` -> `LocalPopulation`) via the Relation Graph.

#### Scope Jump Column Resolution

Forward jump (`from -> to`):
1. Read FK value from `edge.from_column` in the current row (in `from` table)
2. Find matching row in `to` table where its PK equals the FK value
3. PK column is determined by calling `TableSpec::primary_key_column()` on the target table

Reverse jump (`to -> from`):
1. Read PK value of the current row (in `to` table) using the `to` table's `primary_key_column()`
2. Use `edge.from_column` (if present) to search for matching rows in `from` table
3. If `from_column` is absent, fall back to searching by `from` table's `primary_key_column()` matching the PK value

**Implementation requirements**:
- PK column resolution MUST NOT hardcode column name `"id"` -- always use `TableSpec::primary_key_column()`
- `lookup_row` MUST be used for single-row FK->PK lookups
- The scan-based fallback (linear search through all rows) is valid for initial implementation but MUST be documented as a performance anti-pattern

#### Scenario: Jumping to owner
- **WHEN** a rule is executing in the scope of a `SpatialNode` and calls `jump_to(Relation::Owner)`
- **THEN** the execution scope shifts to the `Actor` who owns that node.

### Requirement: Modifier Aggregation
The system SHALL support additive, multiplicative, and override modifiers that affect field values without permanently changing the base state.

#### Scenario: Applying a bonus
- **WHEN** an actor has a "Tax Efficiency" modifier of +10%
- **THEN** the query for "tax_income" applies the `Mul(1.1)` operator to the base value.

### Requirement: Built-in Function Catalog
The evaluator SHALL support the following built-in functions accessible via `Expr::Call`. All functions are pure -- they read state exclusively through query-engine and produce no side-effects.

| Function | Arguments | Input type | Output type | Semantics |
|----------|-----------|------------|-------------|-----------|
| `add` | `x`, `y` | FixedPoint | FixedPoint | `x + y` |
| `sub` | `x`, `y` | FixedPoint | FixedPoint | `x - y` |
| `mul` | `x`, `y` | FixedPoint | FixedPoint | `x * y` |
| `div` | `x`, `y` | FixedPoint | FixedPoint | `x / y` (error if y=0) |
| `min` | `x`, `y` | FixedPoint | FixedPoint | `x < y ? x : y` |
| `max` | `x`, `y` | FixedPoint | FixedPoint | `x > y ? x : y` |
| `not` | `value` | Bool | Bool | `!value` |

#### Function argument validation:
All arithmetic functions (`add`, `sub`, `mul`, `div`, `min`, `max`) SHALL accept **exactly 2 arguments**. `not` SHALL accept exactly 1. Calling a function with a mismatched argument count SHALL return `RuleError::Evaluation` with a descriptive message including the function name and expected/actual counts.

The `evaluate_call` method SHALL validate argument count against a per-function registry before evaluation. This prevents silent ignoring of extra arguments.

**Note**: Argument count validation applies only to built-in functions registered via `default_builtins()`. Custom functions registered via `register_function()` are responsible for their own argument validation.

#### Extensibility:
The evaluator SHOULD support registering custom functions at system startup via:
```
evaluator.register_function(name, impl Fn(&[EvalValue]) -> RuleResult<EvalValue>)
```
This replaces the hardcoded `match` with a pluggable registry. Built-in functions are registered the same way custom ones would be. Custom functions are responsible for validating their own arguments inside their closure. Built-in functions additionally benefit from a separate arg-count registry that enables early-exit validation in `evaluate_call()`.

### Requirement: Read-Only Evaluation with Effect Submission (journal-system, query-engine)
The Rule-IR evaluator SHALL be a pure reader over committed Authority state during expression and trigger evaluation. Expressions and triggers read state exclusively through `query-engine`; they MUST NOT mutate snapshots, access raw Arrow tables, or bypass `journal-system`.

Only the effect phase of rule execution may produce state changes. Effects MUST be submitted through a journal submission capability and MUST NOT be applied directly to `ArrowStore`.

**Unified Read Interface Contract:**

| Operation | Allowed Path | Forbidden |
|-----------|--------------|-----------|
| Column value lookup | query-engine typed read | raw Arrow column access |
| Row iteration | query-engine scan/read API | raw snapshot table iteration |
| Cross-partition jump | query-engine lookup | direct partition access |
| Filtered query | query-engine filter/read API | manual Arrow filter over store internals |
| State change | journal submission capability | direct `ArrowStore` mutation |

**Evaluation lifecycle:**

1. Read: evaluator issues queries via query-engine against the current committed generation.
2. Trigger: evaluator computes conditions without side effects.
3. Effect: triggered effects are converted into diffs or commands.
4. Submit: effects are submitted to journal-system for batched commit.

#### Scenario: Modifier chain evaluation
- **WHEN** evaluating `tax_income` for actor FRA
- **THEN** the evaluator reads base values and modifiers through query-engine or modifier registry reads
- **AND** it submits any resulting treasury update through journal-system
- **AND** it does not write to Arrow tables directly

#### Scenario: Rule effect cannot apply store diff
- **WHEN** a rule effect wants to create a new event row
- **THEN** it submits an insert diff through the journal submission capability
- **AND** the event row appears in Authority state only after journal commit

### Requirement: RelationGraph for Scope Traversal (schema-registry, query-engine)
Scope traversal (`jump_to`) SHALL use the `RelationGraph` maintained by `schema-registry` to resolve cross-table jumps. The RelationGraph SHALL store:

- Source table and column
- Target table and column
- Target table's partition key (e.g., `region_id`)

**Authority**: The `schema-registry` is the sole maintainer of the RelationGraph. The `rule-ir` evaluator accesses it **exclusively through** `query-engine`.

**DC-10 Compliance**: Cross-partition lookups MUST route through `query-engine`. The evaluator does NOT directly access Arrow partitions or query the RelationGraph directly.

When a scope jump crosses partition boundaries, the evaluator SHALL:
1. Call `query-engine.lookup_row(target_table, row_id)`
2. The query-engine internally queries the RelationGraph to resolve the target partition
3. The query-engine performs an indexed lookup within that partition
4. The evaluator pins the result in the prefetch cache for subsequent accesses (within the same tick)

#### Scenario: Cross-partition scope jump
- **WHEN** evaluating a rule in the scope of province P (partition: `region_12`)
- **AND** the rule calls `jump_to(Relation::Owner)` targeting actor A (partition: `global`)
- **THEN** the evaluator (a) queries `RelationGraph` via `query-engine` for the Owner relation, (b) identifies actor A is in the `global` partition, (c) calls `query-engine.lookup_row("actor_state", actor_id)`, (d) pins the result in the prefetch cache.

### Requirement: Prefetch Cache with LRU Eviction
The evaluator SHALL maintain a bounded in-memory prefetch cache for cross-partition scope lookups. The cache is Derived state. It SHALL be rebuilt or cleared from committed Authority state and MUST NOT be persisted or included in replay hashes.

Default capacity: 1024 pinned rows. Eviction policy: LRU.

**CachedRow restriction:**
The `CachedRow` struct SHALL store logical row identity and resolve physical position through query-engine or `RowPositionMap` at access time. It MUST NOT treat `RowId.0` as a raw physical array index.

**Cache lifecycle:**
The evaluator registers a refresh handler with `sim-scheduler`. On `REFRESH_SIGNAL`, the evaluator discards all cached rows for the previous generation before the next tick's rule evaluation.

#### Scenario: Cache cleared at tick boundary
- **WHEN** `sim-scheduler` triggers `journal.commit()` at end of tick T
- **THEN** the evaluator receives `REFRESH_SIGNAL` and clears prefetch cache entries from snapshot T
- **AND** the first rule of tick T+1 performs fresh lookups against generation T+1

### Requirement: No Raw Diff Return for External Application
Rule APIs SHALL NOT expose raw diffs in a way that invites arbitrary external store application. If rule evaluation produces effects, those effects SHALL be submitted through an explicit journal-facing capability or returned only to a scheduler/journal adapter that cannot bypass the journal commit path.

#### Scenario: Rule API returns effect result
- **WHEN** a caller evaluates a rule set that produces an Authority update
- **THEN** the update is submitted to journal-system or returned in a type accepted only by the journal adapter
- **AND** no caller can apply it directly to `ArrowStore`

### Requirement: Short-Circuit Boolean Evaluation
`evaluate_and` SHALL short-circuit on the first `Bool(false)` result: subsequent expressions in the AND chain MUST NOT be evaluated once a `false` result is reached. `evaluate_or` SHALL short-circuit on the first `Bool(true)` result.

This prevents evaluation of expressions that reference non-existent columns or tables in branches that are already logically determined to be unreachable. Without short-circuit evaluation, a `false && unknown_column` expression would incorrectly fail with a column-not-found error instead of returning `false`.

#### Scenario: AND short-circuits on false
- **WHEN** evaluating `false && column("missing_table", "col")`
- **THEN** the evaluator returns `Bool(false)` without attempting to resolve `missing_table`
- **AND** no column-not-found error is raised

#### Scenario: OR short-circuits on true
- **WHEN** evaluating `true || column("missing_table", "col")`
- **THEN** the evaluator returns `Bool(true)` without attempting to resolve `missing_table`

### Requirement: CachedRow TableReadView Sharing
Within a single prefetch cache population for a given table, all `CachedRow` entries SHALL share a single `Arc<TableReadView>` rather than cloning the `TableReadView` per row. This prevents memory amplification when caching many rows from the same table (e.g., 1024 pinned rows each holding a copy of all column arrays).

The `CachedRow` struct SHALL store `Arc<TableReadView>` and resolve physical position through query-engine or `RowPositionMap` at access time. It MUST NOT treat `RowId.0` as a raw physical array index.

#### Scenario: Multiple rows share same TableReadView
- **WHEN** prefetching 100 rows from the `actor_state` table
- **THEN** all 100 `CachedRow` entries reference the same `Arc<TableReadView>`
- **AND** total memory usage for the table's column arrays is only one copy

---

## Invariants

### I-RR-READ-ONLY-EVAL
During expression and trigger evaluation, the evaluator is a pure reader over committed Authority state via `query-engine`. No Arrow tables, snapshots, or `RelationGraph` are mutated during evaluation. Only the effect phase produces state changes, and these MUST be submitted through a journal submission capability.

### I-RR-CACHE-LIFECYCLE
The prefetch cache MUST be empty at the start of each tick's rule evaluation. On `REFRESH_SIGNAL`, the evaluator discards all `CachedRow` entries from the previous generation. This is enforced by the scheduler's synchronous `REFRESH_SIGNAL` protocol.

### I-RR-CACHED-ROW-ARC
Within a single prefetch cache population for a given table, all `CachedRow` entries share a single `Arc<TableReadView>`. This prevents memory amplification when caching many rows from the same table. `CachedRow` resolves physical position through `query-engine` or `RowPositionMap` at access time — it MUST NOT treat `RowId.0` as a raw physical array index.

### I-RR-SHORT-CIRCUIT
`evaluate_and` SHALL short-circuit on `Bool(false)`: subsequent expressions after a `false` result MUST NOT be evaluated. `evaluate_or` SHALL short-circuit on `Bool(true)`. This prevents evaluation of expressions that reference non-existent fields in unreachable branches.