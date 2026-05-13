# Scharnhorst ROADMAP

> **A deterministic simulation engine — first an invariant-driven world ledger,
> then an observable simulation platform, then an editable content ecosystem.**

Scharnhorst's ultimate ambition is to be the engine behind moddable grand-strategy
games where save files are proof artifacts, multiplayer desyncs are forensic evidence,
and content authors get compiler-grade error messages instead of silent misbehaviour.

This roadmap is a sequence of **capability milestones**, not calendar deadlines.
Each phase gates what can be reliably built in the next. The ordering is deliberate:
skip nothing, permute nothing.

---

## Guiding Philosophy

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                   │
│  1. Make the world ledger immutable (type-system enforced)        │
│  2. Make the simulation observable (query, inspect, debug)        │
│  3. Make content authorable (rules, mods, diagnostics)            │
│  4. Build the playable ecosystem (domains, vertical slice)       │
│                                                                   │
│  Do not pour content into a foundation that cannot prove          │
│  its own determinism. Do not optimise what you cannot observe.    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 0 — Fortify the Invariants (Now)

**Theme**: Turn conventions into type-system guarantees.

### 0.1 Enforce the Single-Write-Entry-Point Invariant

The most important architectural rule is: **during simulation, all world-state
mutations go through `scharnhorst_journal`. Nothing writes to `ArrowStore`
directly.** Today this is enforced by convention and code review — it must
become a type-system guarantee.

- [ ] Split `ArrowStore` into a **read-only public interface** (`WorldView` / `Snapshot`-like)
      and an **internal mutation implementation** accessible only to `Journal`.
- [ ] Move all `apply_diffs`, `create_table`, `drop_table` methods behind a
      trait or module that is `pub(crate)` to the journal crate, or otherwise
      inaccessible from simulation systems.
- [ ] In `scharnhorst_bevy`, route all input -> command -> diff through the journal.
      The bridge must never hold a write handle to the store.
- [ ] In `scharnhorst_rules`, the evaluator must never return a `Diff` — it
      submits to journal, period.
- [ ] Introduce a `SimPhase` enum in the type system that gates write access:
      `Initialization` (can create tables, load content), `Simulation` (can only
      read + submit diffs via journal). The store asserts against writes in
      `Simulation` phase unless they arrive through the journal's commit path.

**Verification**: A compile-time error if any crate besides `scharnhorst_journal`
imports `ArrowStore`'s mutation methods. A panic (in debug builds) if `apply_diffs`
is called outside `atomic_commit`.

### 0.2 Split World State into Three Tiers

Arrow's columnar format excels at cache-local scans and analytical queries,
but its immutability-per-generation makes it expensive for transient data.
We need explicit tiers:

| Tier | Lifetime | Examples | Storage | Serializable? |
|------|----------|---------|---------|-------------|
| **Authority** | Tick boundaries, persisted | Economy tables, diplomacy relations, military units | Arrow `RecordBatch` via `VersionedTable` | Yes — save/load, replay, hash |
| **Derived** | Rebuildable from authority | Map-mode heatmaps, AI score grids, materialised views | Custom structures, recomputed on tick | No — reconstructed |
| **Ephemeral** | Within one tick or frame | UI hover, pathfinding queues, animation state, AI planning trees | Bevy ECS components, local heaps | No — discarded |

- [ ] Audit every table in the ArrowStore. Classify as Authority, Derived, or
      Ephemeral. Move non-Authority data out of ArrowStore.
- [ ] Define a `DerivedState` trait: `fn rebuild(&mut self, snapshot: &WorldSnapshot)`.
      Register derived-state systems that run after commit.
- [ ] Define an `EphemeralStorage` concept in the Bevy bridge for frame-local data.
- [ ] Document the classification so new contributors know where each kind of
      data lives.

**Rationale**: Stuffing game-loop ephemera into Arrow tables would cause
fragmentation pressure in `RebuildPerTick` mode and bloat save files.
Keeping only Authority data in Arrow preserves the determinism-replay-hash
chain for what matters, while leaving the engine free to use fast mutable
structures for per-frame work.

### 0.3 Formalise Schema Immutability After Initialisation

The schema (tables, columns, relations) must be frozen before the first tick.
No `create_table` or `add_relation` during simulation.

- [ ] The `SchemaRegistry` exposes an `is_frozen()` check (already exists in
      the six-phase lifecycle).
- [ ] All mutation methods on `SchemaRegistry` assert `!is_frozen()`.
- [ ] `SchemaRegistry::freeze()` is called at the end of the content-loading
      phase (Phase 5) and cannot be unfrozen.
- [ ] Content loading and mod application happen strictly in Phase 0–4 of the
      lifecycle.

---

## Phase 1 — Make the World Observable

**Theme**: The `QueryEngine` becomes the universal lens into world state.

### 1.1 Productionise the QueryEngine

The query engine exists as a typed columnar read path with `lookup_row`,
`batch_reader`, `TypedColumnAccess`, and `RowCursor`. It needs to become
the **exclusive** read interface — no consumer holds a direct `WorldSnapshot`
reference.

- [ ] Audit all read paths (scheduler systems, rule evaluator, bevy bridge,
      save system). Route every one through `QueryEngine`.
- [ ] Remove all public `WorldSnapshot::table()` or `snapshot().arrow_table()`
      accessors. Consumers who need bulk data use `batch_reader` or
      `unified_read::ReadRequest`.
- [ ] Add a `QueryEngine::snapshot()` method that returns a read-only borrowed
      view (not the raw snapshot struct). This is the only way to get a
      point-in-time view.
- [ ] Verify that `RulePath::from_table_column` (and similar) is the structured
      path through `QueryEngine` rather than raw string-based lookups.
- [ ] Add `QueryEngine::validate()` that checks all registered paths against
      the current schema — fails early, not at first rule evaluation.

### 1.2 Debug SQL Console (Debug Build)

DataFusion is already a dependency. Use it for ad-hoc developer queries.

- [ ] Expose a `QueryEngine::sql(&self, sql: &str) -> Result<RecordBatch>` that
      runs SELECT queries against the current snapshot via DataFusion.
- [ ] Wire this into the Bevy inspector as a console (debug builds only).
- [ ] Support `DESCRIBE TABLE`, `SHOW TABLES`, basic `SELECT` with WHERE/filters.
- [ ] In debug builds, route `UPDATE`/`INSERT`/`DELETE` SQL through
      `DebugWriteJournal` (already designed, partially implemented).

### 1.3 Inspector Console and Table Browser

- [ ] Build a `bevy_egui` panel showing all registered tables, their schemas,
      row counts, and a paginated table viewer.
- [ ] Add a relation graph visualiser: show tables as nodes, relations as edges.
      Click a node to see its rows, click an edge to follow a foreign key jump.
- [ ] Add a diff inspector: for any tick, show which diffs were applied, to
      which tables/rows/columns, with before/after values.
- [ ] Add a snapshot browser: list all retained snapshots, compare ticks,
      show state hash differences.

### 1.4 Runtime Telemetry

- [ ] Instrument the scheduler: per-phase timing, per-system timing, diff counts.
- [ ] Instrument the query engine: query latency histogram, cache hit ratio,
      most-accessed tables/columns.
- [ ] Instrument the journal: commit latency, diff batch size distribution.
- [ ] Export via `tracing` metrics (already a dependency) to a live console or
      file.

---

## Phase 2 — Content Language and Rule Diagnostics

**Theme**: The Rule IR transforms from an internal AST to a proper content
language with compiler-grade error reporting.

### 2.1 Source-Anchored Error Reporting

Today a rule evaluation error yields an opaque `EvalError`. Content authors
need to know the file, line, column, scope, and expected vs actual types.

- [ ] Add `source_span: SourceSpan` to every `Expr` node (file path, start line,
      start col, end line, end col).
- [ ] Add a `SourceMap` to `Evaluator` that maps paths to source text for
      error snippets.
- [ ] Implement a layered error formatter:
  ```
  Error[E002]: Type mismatch in rule "tax_calculation"
    ┌─ mods/tax_reform/economic_rules.toml:14:22
    │
  14│   if pop.wealth > "rich" then ...
    │                    ^^^^^^^ expected FixedPoint, found String
    │
    ┌─ In scope: Province(42) -> Population(prosperity_eval)
  ```

- [ ] Add scope path to error diagnostics: which entity, which relation chain,
      which rule file.

### 2.2 Static Analysis for Rules

- [ ] Scope validation: verify that every `jump_to` target and `lookup_row`
      table exists in the RelationGraph at compile time (not evaluation time).
- [ ] Type checking: verify that every expression's inferred type matches its
      context (comparison operands are same type, arithmetic on FixedPoint only,
      etc.)
- [ ] Column existence: verify that every `ColumnPath` references a real table
      and column in the SchemaRegistry.
- [ ] Modifier target validation: verify modifier targets match existing tables
      and columns.
- [ ] Dead code detection (warn on unreachable branches).

### 2.3 Rule Debugger

- [ ] Step-through evaluation: debug builds support single-stepping through a
      rule's expression tree, inspecting intermediate values at each node.
- [ ] Breakpoint by rule name, table, or column.
- [ ] Live modifier chain view: show the base value + all applied modifiers
      for a given (table, row, column), with the rule file and line that
      produced each modifier.
- [ ] Record and replay rule evaluation: log all rule evaluations for a tick
      and replay them in the inspector for forensic analysis.

### 2.4 Rich Scope Traversal

- [ ] Verify current `ScopeJump` / `RelationEdge` implementation handles
      forward, reverse, and multi-hop jumps correctly.
- [ ] Add named scope chains: `from Province -> Actor -> Capital -> Building`
      as a first-class path expression, not nested `jump_to` calls.
- [ ] Add bounded iteration: `all_owned_provinces()`, `all_neighbours()`,
      `all_units_in_province()` as scope queries.
- [ ] Add filter scopes: `owned_provinces.where(tax_income > 10)`.

---

## Phase 3 — Mod and Content Pipeline

**Theme**: Mods become a first-class contractual interface, not a file-system
convention.

### 3.1 Manifest and Dependency System

- [ ] Define a mod manifest format (TOML) with:
  - `id` (namespace), `version` (semver), `display_name`
  - `dependencies` (mod ID + version range)
  - `conflicts` (mod ID + version range)
  - `overrides` (explicit list of tables/definitions this mod replaces)
- [ ] Implement dependency resolution: topological sort, conflict detection,
      version constraint satisfaction.
- [ ] Error on missing or unsatisfied dependencies with actionable diagnostics.
      No "dependency X not found" — instead "mod 'trade_expansion' requires
      'economy_base >= 1.2', but installed version is 1.1".

### 3.2 Overlay and Conflict Resolution

The `OverlayResolver`, `MergeStrategy`, and `OverlayLayer` exist. They need
testing and hardening.

- [ ] Test every `MergeStrategy` variant (Replace, Merge, Append, Error) with
      realistic conflict scenarios.
- [ ] Add a `conflict_report` command that shows, for a given mod set:
  - Which definitions conflict
  - Which mod "won" each conflict
  - The merge strategy applied
- [ ] Add three-way comparison: base content + mod A + mod B. Show what each
      mod changed relative to base, and where they intersect.
- [ ] Add override declarations: a mod can declare "I intentionally override
      def X" to suppress spurious conflict warnings.

### 3.3 Fingerprint and Mod Tolerance

- [ ] The `FingerprintRegistry` and `ModToleranceChecker` exist in design.
      Implement and integrate into the load lifecycle (Phase 2).
- [ ] Load-time decision matrix:
  | Save Fingerprint | Installed Mod | Policy | Outcome |
  |---|---|---|---|
  | Match | Same version | Accept | Normal load |
  | Match | Different version | Accept | Load, warn of version change |
  | Mismatch | Present | Accept | Rebuild derived state |
  | Mismatch | Absent | Accept | Mark as "detached", preserve data, warn |
  | Mismatch | Present | Reject | Error with conflicted definitions |
  | Mismatch | Absent | Reject | Error with missing expected mod |

- [ ] Support semantic migration: when a mod changes a table name, column name,
      or column type between versions, a migration script can be bundled with
      the mod.

### 3.4 Semantic ID Stability and Content References

- [ ] Define a `ContentId` type that is stable across mod versions (unlike
      auto-increment IDs that shift when content is added/removed).
- [ ] Validate all content references at load time: if mod A's rule references
      `pop_type: "scholar"` and mod B removes that pop type, produce an error
      with the reference chain.
- [ ] Add `ContentId` -> `RowId` resolution as a post-migration step.

### 3.5 Localisation and Asset Validation

- [ ] Define a localisation format (key-value pairs per language).
- [ ] Validate that all localisation keys referenced by content actually exist.
- [ ] Validate asset references (textures, models, sounds) — no broken paths
      at content-compile time.
- [ ] Generate content documentation from manifests: list all defined tables,
      columns, rules, relations, events, and modifiers with their descriptions.

---

## Phase 4 — Proof: Save, Replay, and Multiplayer as One System

**Theme**: Every tick should be provable: input commands -> systems -> diffs ->
state hash. The journal is not a log; it is a forensic record.

### 4.1 Deterministic Replay

- [ ] Record the full command sequence for a session to a replay file.
- [ ] Replay: start from the initial checkpoint, replay every command through
      the scheduler, verify that the state hash matches at every tick.
- [ ] On mismatch, report: tick, table, row, column, the actual diff and the
      expected diff, and the command that seeded the divergent tick.
- [ ] The replay must be byte-identical across platforms (Windows, Linux, macOS).
  - Test with `FixedPoint` arithmetic (platform-independent by construction).
  - Test with the `DeterministicRng` stream.
  - Test with string sorting (locale-independent).
  - Test with hash map iteration (seeded or BTree-based).
- [ ] Add a `--replay` CLI mode that verifies a replay file against a recorded
      hash chain and exits with a report.

### 4.2 Tick Proof Chain

- [ ] Compute a cumulative state hash after each `atomic_commit`.
- [ ] Hash = `SHA256(tick_number || input_command_hash || diff_hash || parent_hash)`.
- [ ] The hash chain makes it computationally infeasible to have two different
      world states with the same chain.
- [ ] Store the hash chain in the save file. On load, verify the chain from
      checkpoint to final tick.
- [ ] Export the hash chain for multiplayer: peers exchange tick hashes after
      every N ticks. On mismatch, enter desync forensic mode:
  - Find the first divergent tick via binary search
  - Dump the divergent command + diff + hash at that tick
  - Report to both peers

### 4.3 Desync Forensics (Multiplayer)

- [ ] Implement a `compare_snapshot` function that finds the exact first
      difference between two `WorldSnapshot` instances: table, row, column,
      value_actual, value_expected.
- [ ] When a desync is detected, peers dump the divergent tick's full journal
      (all commands + diffs) to a file.
- [ ] The desync report includes: tick, system, table, row, column,
      actual platform, expected platform, the command that was being processed.
- [ ] Build a desync replay tool: given two peer dumps, replay both side by
      side and highlight the first divergence.

### 4.4 Save-Load Hardening

- [ ] The six-phase load lifecycle is specified. Full integration test:
      save -> load -> replay N ticks -> save again -> load again -> compare
      state hashes at every tick.
- [ ] Edge cases:
  - Load a save with a different mod set (mod tolerance policies)
  - Load a save from an older game version (schema migration)
  - Load a save with truncated journal (checkpoint-only recovery)
  - Load a save into a different platform endianness
- [ ] Corruption resilience: save files should have checksums at the chunk
      level (not just file-level). Detect corruption early, report which chunk
      is damaged.
- [ ] Write-ahead journaling for save files: crash during save should not
      corrupt the last known-good save. Always write to a temp file, then
      atomically rename.

---

## Phase 5 — Developer Tooling in the Bevy Bridge

**Theme**: The Bevy bridge transforms from a rendering projector into a
full development environment.

### 5.1 In-Game Inspector Panels

All of these are Bevy UI panels (debug builds):

- [ ] **Table Viewer**: select a table from dropdown, see its rows in a table
      widget, paginate, sort, filter by column value.
- [ ] **Diff Stream**: live-scrolling view of diffs as they commit. Filter by
      table, row, system.
- [ ] **Rule Debugger**: select a rule, step through its expression tree,
      see intermediate values. Set breakpoints on table/column writes.
- [ ] **Modifier Chain Inspector**: for a given (table, row, column), show the
      base value and every modifier applied to it, with the rule file and line
      that produced each.
- [ ] **Relation Graph Visualiser**: interactive graph of tables and relations.
      Click a node to inspect, click an edge to follow.
- [ ] **Performance Dashboard**: per-system timing, per-table access counts,
      query latency, diff volume per tick.
- [ ] **Snapshot Timeline**: slider across retained snapshots. Drag to see
      state at tick T vs tick T+50.

### 5.2 Map Mode Query System

- [ ] Define a `MapModeQuery` interface: a function that takes a
      `&WorldSnapshot` and returns a `MapModeResult` (coloured overlay).
- [ ] Ship map modes as presets (political, terrain, development, religion,
      culture, etc.) each backed by a `MapModeQuery`.
- [ ] Mods can register custom map modes via a simple API.
- [ ] The map mode is not a static texture — it is a live query against the
      current snapshot.

### 5.3 Content Editor (Future)

- [ ] In-game table editor: double-click a cell to edit its value (writes via
      `DebugWriteJournal`).
- [ ] New row inserter for any table.
- [ ] Rule editor with syntax highlighting and live error checking.
- [ ] Mod conflict visualiser: see two mods' changes side by side, choose
      which to apply.

---

## Phase 6 — Domain Modules

**Theme**: Build the domain crates that will populate the simulation, but do
not hardcode them into a single monolithic game object.

Each domain module should be a self-contained crate (or well-defined module)
that:

1. Declares its tables and relations in a `domain_setup()` function
2. Declares its rules, systems, and modifiers
3. Reads through `QueryEngine`, writes through `Journal`
4. Registers its systems with the `Scheduler`
5. Does not depend on other domain modules' internals — only on shared schema

### 6.1 Spatial (Map) Module

- [ ] Table: `spatial_nodes` (ID, province_id, region_id, adjacency list,
      position, terrain type, climate)
- [ ] Table: `regions` (ID, name, parent_region, area_type)
- [ ] Table: `adjacency_matrix` (from_node, to_node, relation_type)
- [ ] Relation: `SpatialNode -> Region` (owner territory)
- [ ] Relation: `SpatialNode -> [SpatialNode]` (adjacency)
- [ ] Systems: adjacency recalculation, region hierarchy maintenance

### 6.2 Polity Module

- [ ] Table: `actors` (ID, name, tag, primary_color, capital_id,
      government_type, legitimacy)
- [ ] Table: `actor_relations` (actor_id, target_id, relation_value,
      truce_until, alliance_id)
- [ ] Relation: `Actor -> [SpatialNode]` (owned territory)
- [ ] Relation: `Actor -> [Actor]` (diplomatic relations)
- [ ] Not a single `Country` struct with all methods. Tables store what they
      are; rules encode what they can do.

### 6.3 Population Module

- [ ] Table: `pops` (ID, node_id, pop_type, size, wealth, religion, culture,
      literacy, militancy, consciousness)
- [ ] Table: `pop_groups` (node_id, pop_type, count)
- [ ] Relation: `Pop -> SpatialNode` (location)
- [ ] Relation: `Pop -> Actor` (primary culture group)
- [ ] Systems: growth, migration, promotion, assimilation, radicalisation
- [ ] Rules: unrest calculation, tax contribution by pop type

### 6.4 Economy Module

- [ ] Table: `goods` (ID, name, category, base_price, tradeability)
- [ ] Table: `market` (node_id or region_id, good_id, supply, demand,
      market_price)
- [ ] Table: `actor_treasury` (actor_id, gold, income_breakdown,
      expense_breakdown)
- [ ] Table: `buildings` (ID, node_id, type, level, input_goods, output_goods)
- [ ] Systems: production, consumption, trade, taxation, budgeting
- [ ] Rules: price formation (supply/demand model), tax policy modifiers,
      trade route efficiency

### 6.5 Diplomacy Module

- [ ] Table: `diplomatic_pacts` (ID, actor_a, actor_b, pact_type, start_tick,
      end_tick, terms)
- [ ] Table: `wars` (ID, attacker, defender, start_tick, war_goal,
      war_score)
- [ ] Table: `war_participants` (war_id, actor_id, side, contribution)
- [ ] Systems: war score calculation, peace negotiation, alliance obligations
- [ ] Rules: casus belli validation, truce enforcement, call-to-arms logic

### 6.6 Military Module

- [ ] Table: `units` (ID, actor_id, type, size, experience, equipment_level,
      node_id, morale, organisation)
- [ ] Table: `unit_stats` (unit_type, base_attack, base_defence, speed,
      supply_consumption)
- [ ] Relation: `Unit -> SpatialNode` (location)
- [ ] Relation: `Unit -> Actor` (commander)
- [ ] Systems: movement, combat, attrition, reinforcement, supply
- [ ] Rules: combat resolution (dice + modifiers + unit stats), supply
      consumption, terrain effects

### 6.7 Events Module

- [ ] Table: `event_queue` (event_id, trigger_tick, event_type, scope,
      params_json)
- [ ] Table: `event_history` (event_id, tick, event_type, actor_id,
      description_key, effects_summary)
- [ ] Systems: event processing, random event generation, event chain
      resolution
- [ ] Rules: event trigger conditions, event effect application, MTTH
      (mean time to happen) modifier

### 6.8 AI Module

- [ ] The AI is not a monolithic "AI system". It is a set of systems that
      produce `Command` objects submitted to the journal, same as a human
      player.
- [ ] Table: `ai_priorities` (actor_id, goal_type, target, weight,
      evaluation_tick)
- [ ] Table: `ai_plans` (actor_id, plan_id, goal_type, steps, status)
- [ ] Systems: threat assessment, opportunity evaluation, plan construction,
      plan execution
- [ ] AI is deterministic given the same RNG stream and snapshot.

---

## Phase 7 — Vertical Slice

**Theme**: A small but complete playable loop that exercises every layer of the
stack. The goal is not fun — it is proving that the architecture sustains a
real simulation cycle.

### 7.1 Minimal Scenario

- [ ] Map: 50–100 provinces, 3–5 regions, adjacency relationships
- [ ] Actors: 2–4 polities with territory
- [ ] Population: distributed across provinces, 2 pop types (rural, urban)
- [ ] Economy: model basic good (grain), tax income from pops, treasury
- [ ] Diplomacy: improve relations, declare war, make peace
- [ ] Military: units that move, fight simple battles (dice + modifiers)
- [ ] Events: random events that fire based on MTTH, show a popup, apply
      effects
- [ ] AI: at least 1 AI actor that sets priorities and issues commands

### 7.2 Full-Layer Exercise

Each tick in the slice must exercise:

1. **Input**: bevy bridge receives commands (keyboard or AI) -> command buffer
2. **Scheduling**: tick start -> pull commands -> submit to journal -> run
   phase systems
3. **Rules**: rule IR evaluation with scope jumps, modifiers, effects -> diffs
4. **Journal commit**: atomic_diff -> ingest_snapshot -> query engine update
5. **Refresh**: REFRESH_SIGNAL broadcast -> consumers acknowledge
6. **Query**: next tick's systems read fresh state through query engine
7. **Bevy materialisation**: snapshot -> entity materialization -> rendered
8. **Save/Load**: full save and reload with state hash verification
9. **Replay**: record commands, replay from initial checkpoint, verify hashes

### 7.3 Test Coverage

- [ ] Golden replay test: fixed command sequence produces identical tick-by-tick
      hashes across N runs (and across platforms).
- [ ] Save/load round-trip: save at tick T, reload, replay to T+N, compare
      hashes against a from-scratch run of the same command sequence.
- [ ] Mod load test: apply a mod that overrides a table and a rule, verify the
      modded values take effect.
- [ ] Rollback test: given a checkpoint, apply commands 1..N, then roll back to
      checkpoint and verify state matches.
- [ ] Desync detection test: introduce a deliberate non-determinism, verify
      that the desync forensics system catches it and reports the right
      tick/table/row/column.

---

## The Rule of Thumb

For every feature proposed to this project, ask:

> *"Does this weaken determinism, complicate replay forensics, or make the
> single-write-entry-point harder to enforce?"*

If the answer is yes, redesign before merging. Scharnhorst's value proposition
is **provable simulation**. Every line of code either serves that proposition
or is technical debt against it.

---

## Current Status (2026-05)

| Area | Status |
|------|--------|
| scharnhorst_core (types, FixedPoint, Diff) | ✅ Stable |
| scharnhorst_schema (tables, fields, relations) | ✅ Stable |
| scharnhorst_arrow_store (versioned tables, snapshots, index) | ✅ Stable |
| scharnhorst_journal (commands, diffs, commit, save journal) | ✅ Stable |
| scharnhorst_query (DataFusion SQL, typed access, inspector) | ✅ Stable |
| scharnhorst_content (compilation, lifecycle, overlay, name resolution) | ✅ Stable |
| scharnhorst_save (snapshot persistence, load reconstruction, checkpoint) | ✅ Stable |
| scharnhorst_scheduler (phase-based, deterministic RNG, refresh signal) | ✅ Stable |
| scharnhorst_rules (expression evaluator, modifiers, scope jumps) | ✅ Stable |
| scharnhorst_bevy (entity materialisation, sync, refresh handler) | ✅ Stable |
| scharnhorst_integration_tests | ✅ Exists |
| **Phase 0 — Invariants** | ⬜ Not started |
| **Phase 1 — Observability** | ⬜ Not started |
| **Phase 2 — Rule Diagnostics** | ⬜ Not started |
| **Phase 3 — Mod Pipeline** | ⬜ Not started |
| **Phase 4 — Proof System** | ⬜ Not started |
| **Phase 5 — Dev Tooling** | ⬜ Not started |
| **Phase 6 — Domain Modules** | ⬜ Not started |
| **Phase 7 — Vertical Slice** | ⬜ Not started |
