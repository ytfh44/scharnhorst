## MODIFIED Requirements

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

## ADDED Requirements

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
