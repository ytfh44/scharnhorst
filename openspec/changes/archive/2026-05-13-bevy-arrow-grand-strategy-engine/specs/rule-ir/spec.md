**Crate**: `scharnhorst_rules`

## ADDED Requirements

### Requirement: Rule Intermediate Representation
The system SHALL compile modded logic into a stable Rule IR consisting of expressions, triggers, and effects.

#### Scenario: Evaluating a trigger
- **WHEN** checking if a revolt should start
- **THEN** the evaluator processes the `Expr::And` of "unrest > 0.8" and "stability < 0.3".

### Requirement: Scope Traversal
The system MUST support scope jumping (e.g., `Actor` -> `OwnedProvince` -> `LocalPopulation`) via the Relation Graph.

#### Scope Jump Column Resolution

Forward jump (`from → to`):
1. Read FK value from `edge.from_column` in the current row (in `from` table)
2. Find matching row in `to` table where its PK equals the FK value
3. PK column is determined by calling `TableSpec::primary_key_column()` on the target table

Reverse jump (`to → from`):
1. Read PK value of the current row (in `to` table) using the `to` table's `primary_key_column()`
2. Use `edge.from_column` (if present) to search for matching rows in `from` table
3. If `from_column` is absent, fall back to searching by `from` table's `primary_key_column()` matching the PK value

**Implementation requirements**:
- PK column resolution MUST NOT hardcode column name `"id"` — always use `TableSpec::primary_key_column()`
- `lookup_row` MUST be used for single-row FK→PK lookups (see DC-4 Row-by-Row Access)
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
The evaluator SHALL support the following built-in functions accessible via `Expr::Call`. All functions are pure — they read state exclusively through query-engine and produce no side-effects.

| Function | Arguments | Input type | Output type | Semantics |
|----------|-----------|------------|-------------|-----------|
| `add` | `x`, `y` | FixedPoint | FixedPoint | `x + y` |
| `sub` | `x`, `y` | FixedPoint | FixedPoint | `x - y` |
| `mul` | `x`, `y` | FixedPoint | FixedPoint | `x * y` |
| `div` | `x`, `y` | FixedPoint | FixedPoint | `x / y` (error if y=0) |
| `min` | `x`, `y` | FixedPoint | FixedPoint | `x < y ? x : y` |
| `max` | `x`, `y` | FixedPoint | FixedPoint | `x > y ? x : y` |
| `not` | `value` | Bool | Bool | `!value` |

#### Function argument validation (CLARIFIED):
All arithmetic functions (`add`, `sub`, `mul`, `div`, `min`, `max`) SHALL accept **exactly 2 arguments**. `not` SHALL accept exactly 1. Calling a function with a mismatched argument count SHALL return `RuleError::Evaluation` with a descriptive message including the function name and expected/actual counts.

The `evaluate_call` method SHALL validate argument count against a per-function registry before evaluation. This prevents silent ignoring of extra arguments.

**Note**: Argument count validation applies only to built-in functions registered via `default_builtins()`. Custom functions registered via `register_function()` are responsible for their own argument validation.

#### Extensibility:
The evaluator SHOULD support registering custom functions at system startup via:
```
evaluator.register_function(name, impl Fn(&[EvalValue]) -> RuleResult<EvalValue>)
```
This replaces the hardcoded `match` with a pluggable registry. Built-in functions are registered the same way custom ones would be. Custom functions are responsible for validating their own arguments inside their closure. Built-in functions additionally benefit from a separate arg-count registry that enables early-exit validation in `evaluate_call()`.

## MODIFIED Requirements

### (Compatibility) Requirement: Read-Only Evaluation with Effect Submission (↔ journal-system, ↔ query-engine)

The Rule-IR evaluator SHALL be a **pure function** over the current `WorldSnapshot`. Expressions and triggers read state exclusively through the `query-engine`; they MUST NOT mutate the snapshot and MUST NOT bypass the journal-system to write diffs. Only the **effect** phase of rule execution may produce state changes, and these MUST be submitted as `Diff` objects through the `journal-system`.

**Unified Read Interface Contract (DC-10)**:
All read operations in the evaluator MUST route through `query-engine`. Direct access to Arrow `RecordBatch` or `Table` objects is **strictly forbidden**:

| Operation | Allowed Path | Forbidden |
|-----------|-------------|-----------|
| Column value lookup | `query_engine.get_column(...)` | `snapshot.arrow_table().column()` |
| Row iteration | `query_engine.scan_table(...)` | `snapshot.arrow_table().iter()` |
| Cross-partition jump | `query_engine.lookup_row(table, row_id)` | Direct partition access |
| Filtered query | `query_engine.filter(...)` | Manual Arrow filter application |

Read path: `rule-ir evaluator → query-engine → WorldSnapshot → ArrowStore`

#### Evaluation lifecycle:
```
1. Read → evaluator issues queries via query-engine against current snapshot + modifiers (pure query)
2. Trigger → evaluates conditions via query-engine (pure computation, no side-effects)
3. Effect → for each triggered effect, emits Diff::Update/Diff::Insert
4. Submit → diffs submitted to journal-system for batched commit
```

#### Scenario: Modifier chain evaluation
- **WHEN** evaluating "tax_income" for actor FRA
- **THEN** the evaluator (a) queries base treasury from snapshot via `query-engine`, (b) queries modifier chain from the modifier registry, (c) computes `base * 1.1 * 0.95 = final_value`, (d) emits `Diff::Update { table: "actor_state", row: FRA, column: "treasury", value: final_value }` — **but does NOT write to the Arrow table directly**.

### (Compatibility) Requirement: RelationGraph for Scope Traversal (↔ schema-registry, ↔ query-engine)
Scope traversal (`jump_to`) SHALL use the `RelationGraph` maintained by `schema-registry` to resolve cross-table jumps. The RelationGraph SHALL store:

- Source table and column
- Target table and column
- Target table's partition key (e.g., `region_id`)

**Authority**: The `schema-registry` is the sole maintainer of the RelationGraph. The `rule-ir` evaluator accesses it **exclusively through** `query-engine` (DC-10 compliance).

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

### (Compatibility) Requirement: Prefetch Cache with LRU Eviction

The evaluator SHALL maintain a bounded in-memory prefetch cache for cross-partition scope lookups. Default capacity: 1024 pinned rows. Eviction policy: **LRU** (Least Recently Used).

#### CachedRow restriction — no raw RowId indexing:
The `CachedRow` struct SHALL store the `RowId` and resolve physical position via `RowPositionMap` at access time. It MUST NOT store `row_index: usize` derived from `RowId.0`. Rationale:
- Rows can be deleted, creating gaps in physical storage that would make a cached `row_index` stale
- Multiple `RecordBatch` objects per table mean a RowId's physical `(batch_idx, offset)` is not a simple linear index
- Tick boundaries invalidate all cached positions (handled by REFRESH_SIGNAL)

#### Cache lifecycle — REFRESH_SIGNAL protocol

The `rule-ir` evaluator registers a refresh handler with the `sim-scheduler` (see `sim-scheduler` DC-10). The lifecycle is:

```
T+0 (end of PostTick):
1. sim-scheduler broadcasts REFRESH_SIGNAL
2. rule-ir evaluator receives signal

T+1 (before first rule evaluation):
3. Evaluator discards all cached rows from snapshot T
4. Evaluator obtains new snapshot reference via query-engine.get_snapshot()
5. First rule of tick T+1 executes with fresh lookups against snapshot T+1
```

**Invariant**: The prefetch cache MUST be empty at the start of each tick's rule evaluation. This is enforced by the `sim-scheduler`'s synchronous REFRESH_SIGNAL protocol (all consumers acknowledge before any proceeds).

#### Scenario: Cache cleared at tick boundary
- **WHEN** the `sim-scheduler` triggers `journal.commit()` at end of tick T
- **THEN** the evaluator receives REFRESH_SIGNAL, discards all cached rows from snapshot T
- **AND** when the first rule of tick T+1 executes, it performs fresh lookups against snapshot T+1

#### Cache safety guarantee

Because the evaluator operates exclusively on an **immutable** `WorldSnapshot`, the prefetch cache is inherently safe from stale reads **within** a single tick. `ArrowStore` mutation modes (`Patchable`, `RebuildPerTick`) never mutate tables in-place — they produce new snapshot generations. Since the evaluator pins rows from a single immutable snapshot for the duration of one tick, cached cross-partition references cannot become invalid mid-evaluation.

#### Scenario: Prefetch hit during scope traversal
- **WHEN** the evaluator resolves `jump_to(Relation::Owner)` for province P and pins the result (actor A) in the cache
- **AND** another rule in the same tick also traverses the `OwnerOf` relation for a different province that also targets actor A
- **THEN** the evaluator serves the second lookup from the prefetch cache, avoiding a redundant cross-partition index lookup.

#### Scenario: Cache cleared at tick boundary
- **WHEN** the `sim-scheduler` triggers `journal.commit()` at end of tick T
- **THEN** the evaluator discards all cached rows from snapshot T
- **AND** when the first rule of tick T+1 executes, it performs fresh lookups against snapshot T+1
