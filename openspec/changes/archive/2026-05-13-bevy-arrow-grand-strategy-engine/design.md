## Context

Traditional grand strategy engines often blend simulation state with rendering logic, leading to "spaghetti state" where a UI change can accidentally trigger a simulation side-effect, or a simulation change breaks the renderer. Moreover, row-based object models make massive-scale simulations (thousands of provinces/pops) inefficient and make save-game migrations a nightmare.

This design proposes a "World Ledger" approach: the simulation is a series of commits to a columnar database (Apache Arrow), and the game engine (Bevy) is simply a specialized view of that database.

## Goals / Non-Goals

**Goals:**
- **Absolute Determinism**: Given the same seed and input commands, the world state must be identical across all platforms.
- **Columnar Efficiency**: Leverage Apache Arrow for cache-friendly simulation and fast queries.
- **Deep Moddability**: Logic defined in a Rule IR, allowing mods to change triggers and effects without recompiling the engine.
- **Perfect Replay/Sync**: All state changes captured in a command-diff journal.
- **Schema Evolution**: First-class support for versioned tables and migration scripts.

**Non-Goals:**
- **General Purpose DB**: We are not building a general database; the store is optimized for tick-based simulation.
- **Real-time Physics**: This is for strategic scales; high-fidelity physics are handled by Bevy or separate modules.
- **Immediate-mode Simulation**: We explicitly avoid updating state mid-tick; we use a Read-Snapshot -> Write-Journal -> Commit cycle.

## Decisions

### 1. Arrow as the Primary State Store
- **Decision**: Use `arrow-rs` for the `ArrowStore`.
- **Rationale**: Grand strategy games involve scanning thousands of entities for specific conditions (e.g., "all provinces with unrest > 0.5"). Columnar storage is orders of magnitude faster for these scans.
- **Alternatives**: Row-based ECS (Bevy) for everything. *Rejected because Bevy ECS is not optimized for the specific relational queries and snapshotting required for deterministic grand strategy saves.*

### 2. The "Snapshot & Journal" Loop
- **Decision**: Systems read from a `WorldSnapshot` (immutable) and write to a `DiffJournal`.
- **Rationale**: This eliminates race conditions during parallel execution and allows for trivial "undo" or "rollback" during OOS (Out-of-Sync) detection.
- **Alternatives**: Direct mutable access to state. *Rejected due to non-determinism and complexity in parallelization.*

### 3. Rule IR over Embedded Scripting
- **Decision**: Compile DSL/Files into a custom Rule IR (Intermediate Representation).
- **Rationale**: Provides a stable contract between content and engine. Allows the engine to optimize rule execution (e.g., batching similar triggers) and provides better error reporting for modders.
- **Alternatives**: Lua/Rhai. *Rejected as primary logic drivers because they are harder to serialize, version, and analyze for dependencies.*

### 4. Bevy as a "Projection"
- **Decision**: Bevy entities are "View Models" of Arrow rows.
- **Rationale**: Keeps the simulation headless. The game can run on a server without Bevy. Bevy only materializes entities that are currently visible or interactive.

## Risks / Trade-offs

- **[Risk] Arrow Update Overhead** → **Mitigation**: Use a "Patchable" mutation mode. Small updates go to a diff buffer; large updates trigger a batch rebuild of the specific Arrow chunk.
- **[Risk] Complexity of Rule IR** → **Mitigation**: Start with a simple AST-based evaluator before moving to a bytecode VM.
- **[Risk] Memory Usage** → **Mitigation**: Use Arrow's memory mapping and partitioned tables to keep only active regions in RAM.

## Resolved Compatibility Decisions

These decisions were derived from cross-spec compatibility analysis to eliminate tensions between the 9 capability specs:

### DC-1: Single Write Entry Point (journal-system is the gatekeeper)

All world state mutations — from player input, AI, Rule-IR effects, and debug tools — MUST flow through `journal-system`. No component may write directly to Arrow tables during simulation.

| Source | Path |
|--------|------|
| Player input | `bevy-bridge InputCommandBuffer` → `sim-scheduler` → `journal-system` |
| AI / internal systems | Direct `journal.submit()` call |
| Rule-IR effects | Evaluator emits `Diff` → `journal-system` |
| Debug writes (debug build only) | `DebugWriteJournal` → `journal-system` |

**Why**: Preserves deterministic replay, enables OOS detection, and simplifies save/load.

#### Temporal Scope

DC-1 applies from **Phase 6 (Simulation Start)** of the [Six-Phase Load Lifecycle](#dc-13-six-phase-load-lifecycle) onward — once `sim-scheduler` begins its first tick. During Phases 1–5 (snapshot deserialization, mod coordination, schema migration, content compilation, and schema freeze), the `ArrowStore` may be mutated directly via its public API to bootstrap the initial world state. Any post-freeze mutation outside the journal is a DC-1 violation.

The transition to DC-1 enforcement is marked by the call to `schema-registry.freeze()` at the end of Phase 5. After `freeze()` returns, all ArrowStore mutations MUST route through Journal.

#### API Visibility

`ArrowStore` mutation methods (`create_table`, `drop_table`, `append_batches`, `patch_rows`, `rebuild_table`, `apply_diffs`, `truncate_before`) are **public** — they are accessible to any crate for bootstrap tooling during Phases 1–5 (including content-loader, integration test harnesses, and save-system reconstruction). During simulation (Phase 6+), these methods SHALL only be invoked through Journal. This constraint is enforced by convention and code review, not by Rust visibility modifiers.

#### RowId Allocation Contract

`RowId(u64)` is defined in `scharnhorst_core` as a **monotonically-assigned logical identifier** for a single row within a table. RowId values support total ordering (`PartialOrd` + `Ord`) by their assigned value, reflecting insertion order within the scope of a single table. RowId is NOT a physical array offset; the inner `u64` SHOULD be accessed through `as_u64()`. Direct field access (`row_id.0`) is soft-deprecated — existing code (tests, debug formatting, serialization) may continue using it, but new production code SHOULD use the accessor.

1. **Monotonically increasing but NOT gapless**: `RowId` values are assigned in ascending order as rows are inserted. When a row is deleted, its `RowId` is NOT reused, and existing `RowId` values are NOT re-indexed. The ordering of RowId values IS semantically meaningful within a table scope.
2. **NOT a physical array index**: RowId values MUST NOT be used as a direct index into any `RecordBatch`, `ColumnView`, or `Vec`. Translation from `RowId` to physical storage position MUST go through a `RowPositionMap`. Conversely, a physical row offset or loop index MUST NOT be used as a RowId value — RowId and physical position are decoupled.
3. **Scoped per-table**: `RowId(5)` in table `actor_state` and `RowId(5)` in table `spatial_nodes` refer to distinct entities. Each table has its own independent RowId sequence.
4. **Scoped per-partition**: `RowId(5)` in partition `north` and `RowId(5)` in partition `south` refer to distinct entities within a partitioned table.
5. **Position map**: Each table SHALL maintain a `RowPositionMap` (defined in `scharnhorst_core`) that translates `RowId → (batch_index, row_offset)`. This map is updated on insert, delete, and batch compaction.
6. **lookup_row() API**: The `query-engine` SHALL expose `fn lookup_row(table, row_id)` as the exclusive mechanism for row-by-row access. The evaluator, cache, and all consumers MUST use this method — direct indexing via RowId is FORBIDDEN. See `query-engine/spec.md` for the complete API contract.
7. **Dual position map architecture**: The authoritative `RowPositionMap` is stored on `VersionedTable` and updated synchronously during diff application (`apply_insert`, `apply_delete`, `apply_replace`). A read-only copy SHALL be stored on `TableReadView` (the query-engine's cached table view) during snapshot ingestion. The authoritative map ensures `lookup_row` correctness immediately after diff application; the read-only copy optimizes bulk read access inside the query-engine without lock contention.
8. **RowId derivation from physical position is FORBIDDEN**: Code MUST NOT construct `RowId::new(i)` where `i` is a physical row offset, loop index, batch index, or column scan position. The only valid RowId sources are: (a) existing RowId values retrieved from tables, (b) RowIds freshly allocated by a sequence generator (see rule 9), or (c) RowId literals in test/data fixtures. When scanning a column to find a matching row, the code MUST retrieve the actual RowId from the table (not assume `RowId(offset)`).
9. **RowId ↔ Primary Key separation**: `RowId` is an internal storage-level identifier; a table's logical Primary Key (defined by `TableSpec::primary_key_column()`) is a separate concept. When `apply_update` or `apply_delete` resolves a row location, it SHALL look up the row's physical position via the `RowPositionMap` keyed by `RowId` — NOT by constructing a string from `RowId::as_u64()` and querying the PrimaryKeyIndex with it. The PrimaryKeyIndex maps business-level PK values (e.g., `"FRA"`) to physical positions; the RowPositionMap maps RowId values to physical positions. These are distinct indices.
10. **RowId sequence generation**: Fresh RowId values are generated through **three authorized paths**:
   - **`apply_replace`** (Diff::ReplaceTable): replaces entire table content. RowIds are reassigned **sequentially from 0** using a **per-table counter** reset to 0 at the start of each ReplaceTable invocation. The counter iterates across all new batches in batch-major order. Prior RowIds for this table are entirely invalidated.
   - **`rebuild_table`**: follows the identical strategy as `apply_replace` — per-table counter from 0 across all batches. Every row receives a UNIQUE RowId value within its table.
   - **`NameResolver`** (content system): assigns RowIds sequentially via a per-type counter during cold-start content loading. Because NameResolver runs before simulation-assigned RowIds, its allocations form the base range, and simulation-assigned RowIds continue from where NameResolver left off (or from 0 if content uses ReplaceTable to set initial state).
   - **`apply_insert`** (Diff::Insert): uses the RowId value provided in the `Diff::Insert` payload, which was generated by one of the authorized paths above. The implementation MAY validate that the RowId is consistent with the table's RowId sequence, but validation is deferred — the invariant is maintained by restricting Diff::Insert construction to authorized paths only.

### DC-2: Tick-Aligned Command Flow
- `bevy-bridge` buffers player commands per frame, exposes them to `sim-scheduler` once per tick at the tick boundary.
- Mid-tick arrivals are held until the next tick.
- AI commands bypass the bridge entirely.

**Why**: Clean separation of UI responsiveness from simulation determinism.

### DC-3: Tick Lifecycle

```
T+0:  scheduler pulls bridge commands → journal.submit(commands)
T+0:  scheduler runs [PreTick → Economy → Diplomacy → ... → PostTick]
      → systems emit Diffs → journal accumulates
T+1:  scheduler triggers journal.commit()
      → diffs applied atomically to ArrowStore
      → new snapshot generated
      → snapshot published to query-engine via ingest_snapshot()
      → SaveJournal appended with commit record
      → REFRESH_SIGNAL broadcast to all consumers
```

#### Snapshot Publication Protocol

`Journal::commit()` SHALL execute the following steps atomically from the caller's perspective:

```
1. Phase → Committing
2. Apply all pending diffs to ArrowStore
3. Generate new WorldSnapshot (tick N+1)
4. For each table in the snapshot:
   a. Extract RecordBatch data
   b. Extract the authoritative RowPositionMap from VersionedTable
   c. Call query_engine.ingest_snapshot(tick, table_name, batches, position_map)
5. Append commit record to SaveJournal (if configured)
6. Broadcast REFRESH_SIGNAL to scheduler
```

Step 4 MUST complete for ALL tables before step 6 begins — no consumer may observe a partially-updated query-engine cache.

**Journal ↔ Query-Engine coupling**: `Journal` SHALL hold an `Arc<QueryEngine>` reference (or equivalent shared handle). The `ingest_snapshot` call is synchronous — it writes directly into the query-engine's internal `view_cache` and `latest_tick`. The REFRESH_SIGNAL broadcast is the scheduler's responsibility; Journal signals completion to the scheduler via its return value (`CommitResult`), and the scheduler then broadcasts to consumers.

### DC-4: Query-Engine is Read-Only

`query-engine` operates exclusively on immutable `WorldSnapshot` references. All writes go through journal.

#### Debug Write Path

A `DebugWriteJournal` component SHALL be hosted by the `query-engine` crate (canonical definition in `scharnhorst_query::debug_write`). It intercepts SQL `UPDATE`/`INSERT`/`DELETE` statements from developer tooling and translates them into `Diff` objects.

The `scharnhorst_journal` crate provides the **SQL parser module** (`sql_parser`) — available only in debug builds (`#[cfg(debug_assertions)]`) — that handles parsing and `Diff` translation. The `DebugWriteJournal` struct in `query-engine` consumes this parser.

| Component | Crate | Responsibility |
|-----------|-------|---------------|
| `SqlParser`, `SqlStatement`, `SqlValue` | `scharnhorst_journal::sql_parser` | Parse SQL text, translate to `Diff` |
| `DebugWriteJournal` struct + ring buffer | `scharnhorst_query::debug_write` | Host the write journal, record operations |

**Constraints**:
- **Debug builds only**: The `#[cfg(debug_assertions)]` gate on `scharnhorst_journal::sql_parser` ensures the parser module compiles only in debug builds
- **No direct writes**: Still routes through journal, preserving single-write invariant
- **Tick-aligned**: Changes committed at next tick boundary, not immediately
- **Logged**: All debug writes are logged for debugging reproducibility

### DC-5: Static Content Loading

Content loading (base + mod overlays) completes **before** the first tick. Schema-registry is frozen at simulation start. Hot-reload is explicitly out of scope (deferred).

### DC-6: RelationGraph for Cross-Partition Lookups

`schema-registry` is the **sole maintainer** of the `RelationGraph` with partition metadata. All consumers access the RelationGraph **exclusively through** `query-engine` — direct access is forbidden. This enables O(log n) cross-partition scope traversal instead of O(n) scans. Partition key = `region_id`.

**Access Pattern**: `rule-ir` → `query-engine.lookup_row()` → (internally queries RelationGraph) → indexed lookup in target partition.

### DC-7: Prefetch Cache with LRU Eviction

Rule-IR evaluator maintains a bounded in-memory prefetch cache for cross-partition scope lookups. This cache stores `CachedRow` entries that reference `RowId` values (not physical positions — positions are resolved at access time via `RowPositionMap`).

**Configuration**:
- **Capacity**: configurable via `EvaluatorConfig`, default 1024 rows
- **Eviction policy**: LRU (Least Recently Used) — on insert when full, the least recently accessed entry is evicted
- **Eviction is silent**: evicted entries are simply dropped; no error is raised

**Lifecycle**: The cache is **cleared at each tick boundary** when the `sim-scheduler` broadcasts the REFRESH_SIGNAL, ensuring no stale references persist across ticks.

### DC-8: Checkpoint & Diff Replay

SaveJournal accumulates diffs per tick. Auto-checkpoint triggers when the SaveJournal spans **N ticks** without an intervening full save. The threshold N SHALL default to 1000, with configuration deferred to a future revision.

3 historical snapshots retained for rollback. Loading = latest snapshot + replay of subsequent diffs.

### DC-9: Multiplayer Authority Model

Only the authoritative server maintains the SaveJournal. Clients send `InputCommand` and reconstruct state from snapshot + journal replay.

### DC-10: Unified Read Interface via Query-Engine

All world state consumers — simulation systems, `rule-ir` evaluator, and `bevy-bridge` — MUST access the `WorldSnapshot` exclusively through the `query-engine`. No component outside `scharnhorst_arrow_store` or `scharnhorst_query` may directly import or use Arrow crate types (`arrow::*`, `arrow_array::*`, `arrow_schema::*`, `arrow::ipc::*`). All read operations SHALL use query-engine abstractions (`ColumnView`, `RowCursor`, `BatchColumnReader`, `RowLookupView`).

**Temporal exception — Content Compilation (Phases 1-5)**: During the Six-Phase Load Lifecycle (DC-13), the `scharnhorst_content` crate may import and use Arrow types (`arrow_array::RecordBatch`, `arrow_array::StringArray`, `arrow_array::ArrayRef`, `arrow_schema::Schema`, `arrow_schema::Field`, `arrow_schema::DataType`) for the purpose of compiling TOML content definitions into `RecordBatch` instances. This is a necessary tradeoff: content compilation is the bridge between human-authored TOML data and the columnar storage layer, and Arrow type construction is inherent to this task. This exception applies exclusively during Phases 1-5 (before `schema-registry.freeze()`). After Phase 5, no further Arrow type imports or direct Arrow type construction is permitted from `scharnhorst_content`.

**Structural exception — Bevy Bridge Entity Materialization**: The `scharnhorst_bevy` crate may import `WorldSnapshot` (from `scharnhorst_arrow_store`) for entity materialization purposes. The bevy-bridge SHALL obtain the `WorldSnapshot` reference exclusively through `query_engine.snapshot()` (which returns the cached snapshot produced during `journal.commit()`), NOT by calling `ArrowStore::get_snapshot()` directly. Entity materialization requires direct access to `VersionedTable` internals (via `snapshot.get_table()`) to iterate over batches and spawn Bevy entities, which is not practical through the query-engine's typed read APIs. However, all general-purpose data reads (column scans, row lookups) from the bevy-bridge SHALL still use query-engine typed read APIs. The bevy-bridge SHALL NOT import raw Arrow types (`arrow_array::*`, `arrow_schema::*`).

**Forbidden outside `scharnhorst_arrow_store` and `scharnhorst_query`:**
- `use arrow_array::RecordBatch;` — use `TableReadView` via query-engine (except: `scharnhorst_content` during Phases 1-5)
- `use arrow_schema::DataType;` — use `ColumnView::data_type()` and query-engine type abstractions (except: `scharnhorst_content` during Phases 1-5)
- `use arrow::ipc::*;` — IPC serialization is the exclusive responsibility of `scharnhorst_arrow_store`
- Direct construction of `RecordBatch::try_new(...)` — use `ArrowStore::append_batches()` or query-engine factory methods (except: `scharnhorst_content` during Phases 1-5)
- `batch.column(i).as_any().downcast_ref::<T>()` — use `query-engine.lookup_row()` or typed accessors
- Direct calls to `ArrowStore::get_snapshot()` from outside `journal-system` — use `query_engine.snapshot()`

**IPC Serialization Boundary**: `scharnhorst_save` MUST NOT depend on `arrow` or `arrow-array` crates. It SHALL accept and return only `Vec<u8>` byte buffers. The translation between byte buffers and Arrow types is the exclusive responsibility of `scharnhorst_arrow_store::ipc_serialization`.

Internally, `query-engine` dispatches to different Arrow access strategies (direct column scan, dictionary-indexed lookup, DataFusion SQL plan) transparently to callers, enabling parallel optimizations at the Arrow layer.

**RelationGraph Access**: The `schema-registry` maintains the `RelationGraph`, but it is accessed **exclusively through** `query-engine` — direct queries from `rule-ir` or other consumers are forbidden. The `query-engine` SHALL expose `resolve_relation_edge(key)` for edge lookup and SHALL internally consult the `RelationGraph` when performing cross-partition `lookup_row` calls.

**Snapshot delivery model**: The query-engine does NOT expose a pull-based `get_snapshot()` method that returns a raw snapshot handle. Instead:
- Snapshot data is **pushed** into the query-engine via `ingest_snapshot(tick, table_name, batches, position_map)` (called by Journal during commit — see DC-3)
- Consumers access snapshot data through the query-engine's typed read APIs: `lookup_row`, `column_view`, `batch_reader`, `read`
- Each `TableReadView` stored in the query-engine's internal cache SHALL contain a `RowPositionMap` copy for fast RowId→position resolution without locking the ArrowStore

**REFRESH_SIGNAL Protocol**: At each tick boundary, `sim-scheduler` broadcasts a refresh signal after `journal.commit()` publishes the new snapshot. All consumers MUST:
1. Discard cached references to the old snapshot
2. Clear prefetch caches (`rule-ir`)
3. Re-obtain data from the query-engine (which now serves the new generation's data from its internal cache)
4. Acknowledge to scheduler before proceeding to next tick

The scheduler SHALL wait for all registered consumers to acknowledge before advancing the tick counter. If a consumer's acknowledgement callback fails, the scheduler SHALL preserve the consumer's specific error (not replace it with a generic message) and propagate it through `SchedulerResult`.

**Affected specs**: `sim-scheduler` (removed "or direct Arrow access" from registration, added synchronous REFRESH_SIGNAL protocol with ack), `rule-ir` (read path updated to go through query-engine, prefetch cache cleared on refresh), `query-engine` (Unified Read Interface, sole RelationGraph consumer, ingest_snapshot push model, TableReadView with position_map, store_world_snapshot / snapshot() for bevy-bridge), `bevy-bridge` (reads snapshot via query_engine.snapshot(), direct ArrowStore coupling removed, refresh handler registered; may import WorldSnapshot for entity materialization).

### DC-11: Schema Manifest Embedding in Snapshots

Every snapshot file MUST embed a `SchemaManifest` in its header, containing all `TableSpec` definitions, field semantics, `RelationGraph` edges, and `ModFingerprint` entries. This makes each saved game **self-contained**: a client or process receiving only the snapshot file can reconstruct the full `schema-registry` without external configuration or additional network round-trips.

**Affected specs**: `content-loader` (serializes SchemaManifest during cold-start, merges migrated schema with compiled content), `save-system` (snapshot header structure, six-phase load reconstruction), `schema-registry` (ModFingerprint tracking, cold-start registration contract).

### DC-12: Mod Fingerprint Tracking & Load Tolerance

The `schema-registry` maintains a `ModFingerprint` per loaded mod (mod ID, version, registered TableSpec names, content hash). On save load, the system compares stored fingerprints against currently available mods:

| Scenario | Behavior |
|----------|---------|
| All mods match | Normal load |
| Version mismatch | Warn; attempt load with graceful degradation |
| Mod in save but missing on disk | Warn; mark affected tables "degraded", skip dependent rules |
| Base-game mod missing | Reject load with clear error |

**Affected specs**: `content-loader` (generates fingerprints, merges non-save mods into compilation), `schema-registry` (stores fingerprints, cold-start registration contract), `save-system` (compares on load, applies tolerance policy, six-phase lifecycle).

### DC-13: Six-Phase Load Lifecycle

To resolve the cross-spec loading order ambiguity between `save-system` (schema migration), `content-loader` (mod compilation), and `schema-registry` (freeze), we define a strict six-phase load lifecycle. All three specs MUST conform to this sequence.

```
PHASE 1 — Snapshot Deserialize
│ Locate snapshot_gen_N.arrow, parse header → SchemaManifest + ModFingerprint list
│ Output: SchemaManifest (saved version)                [scharnhorst_schema::manifest]

PHASE 2 — Mod Coordination
│ Compare ModFingerprint vs mods on disk → produce final mod list
│ - Mod in save, missing on disk → warn, mark tables "degraded"
│ - Mod on disk, not in save → include in compilation
│ - Version mismatch → warn, attempt graceful degradation
│ - Base-game mod missing → REJECT
│ The final mod list is stored in SchemaRegistry via store_mod_fingerprints()

PHASE 3 — Schema Migration
│ Run Migration functions on SchemaManifest → MigratedSchemaManifest (in memory)
│ The MigratedSchemaManifest is stored in SchemaRegistry.
│ Output: MigratedSchemaManifest                [scharnhorst_schema::manifest]

PHASE 4 — Content Compilation
│ content-loader reads MigratedSchemaManifest from SchemaRegistry
│ content-loader compiles base + available mods into Arrow tables
│ schema-registry registers all TableSpecs via load_from_manifest()
│ RelationGraph is built from declared relations
│ Global cycle detection runs on complete RelationGraph
│ Output: Populated SchemaRegistry with RelationGraph

PHASE 5 — Schema Freeze
│ schema-registry.freeze() is called
│ schema-registry.is_frozen() = true
│ No further TableSpec registrations accepted

PHASE 6 — Simulation Start
│ sim-scheduler begins first tick
```

**Phase transition protocol**: The `SchemaRegistry` serves as the **shared state holder** across phases:
- Phase 1 → output is a standalone `SchemaManifest` (not stored in SchemaRegistry)
- Phase 2 → final mod list is pushed into SchemaRegistry via `store_mod_fingerprints()`
- Phase 3 → `MigratedSchemaManifest` is produced by save-system and pushed into SchemaRegistry (the registry holds it in memory)
- Phase 4 → content-loader reads the `MigratedSchemaManifest` from SchemaRegistry, compiles content, and calls `load_from_manifest()` which registers tables, adds relations, and performs cycle detection
- Phase 5 → `freeze()` is called directly on SchemaRegistry
- Phase 6 → scheduler starts

Each phase transition is triggered by external orchestration code (the save-system's `LoadReconstruction`), not automatically. A phase SHALL NOT begin until the previous phase's return value confirms success.

**Error handling**: If any phase fails, the load SHALL be aborted. There is no partial-load recovery. The caller receives the phase-specific error.

**Invariant**: Phase 5 MUST occur after Phase 4 and before the first tick. Phase 3 MUST occur before Phase 4 so that migrations affect compiled content. Phase 2 MUST occur before Phase 4 so that the final mod list determines which mods are compiled.

**Type Authority Note**: All shared types referenced in the lifecycle (`SchemaManifest`, `ModFingerprint`, `MigratedSchemaManifest`) SHALL be defined in `scharnhorst_schema::manifest` and re-exported by other crates. No crate may define its own version of these types.

**Affected specs**: `save-system` (load reconstruction follows phases 1→6), `content-loader` (compilation occurs at Phase 4, reads MigratedSchemaManifest and final mod list from SchemaRegistry), `schema-registry` (holds intermediate state across phases, registers mods at Phase 4, freezes at Phase 5).

### DC-14: FixedPoint Parsing Specification

`FixedPoint::from_str` parses decimal strings into `FixedPoint { raw: i64, scale: u32 }` values. The parsing grammar is:

```
input  = [sign] integer-part ['.' fractional-part]
sign   = '+' | '-'
integer-part = digit+
fractional-part = digit*
```

**Parsing Rules**:
1. **Leading zeros are preserved in the raw value**, NOT in scale: the scale is determined by the fractional part's digit count. Leading zeros in the integer part are part of the integer value. Example: `"0.05"` → `raw = 5, scale = 2` (parsed as `005` → `5`, NOT as `005` → `500`).
2. **Missing integer part** (e.g., `".5"`): the integer part is implicitly `0`. Result: `raw = 5, scale = 1`.
3. **Missing fractional part** (e.g., `"5."`): the fractional part is implicitly empty. `scale = 0`. Result: `raw = 5, scale = 0`.
4. **Negative zero** (`"-0"`): `raw = 0, scale = 0`.
5. **Whitespace**: Leading and trailing whitespace is trimmed before parsing.
6. **Overflow**: If the parsed `i64` value exceeds `i64::MAX` or `i64::MIN`, return `CoreError::ArithmeticOverflow`.
7. **Invalid input** (empty, non-numeric characters): return `CoreError::InvalidId`.

**Implementation Note**: The parser must strip the sign before filtering the decimal point to avoid losing the sign character. Leading zeros are accepted by Rust's `i64::parse()` and represent the correct integer value.

## Migration Plan

1. **Core Foundation**: Implement `scharnhorst_core` and `scharnhorst_schema`.
2. **Store Layer**: Implement `scharnhorst_arrow_store` and basic snapshotting.
3. **Query Engine**: Implement `scharnhorst_query` with unified read interface (DC-10) — required before any consumer can read state. This is the **sole read path** for all world state access.
4. **Journal System**: Implement `scharnhorst_journal` as single write entry point (DC-1).
5. **Simulation Loop**: Implement `scharnhorst_scheduler` with phase-based execution and synchronous snapshot refresh signal protocol (DC-3, DC-10).
6. **Rule System**: Implement `scharnhorst_rules` (IR and Evaluator) with query-engine read path and prefetch cache (DC-7).
7. **Content Pipeline**: Implement `scharnhorst_content` for mod loading with fingerprint generation (DC-12).
8. **Save System**: Implement `scharnhorst_save` with checkpoint/replay and six-phase load lifecycle (DC-13), using SchemaRegistry as shared-state holder.
9. **Bevy Integration**: Build the `scharnhorst_bevy` bridge with tick-aligned command buffering (DC-2) and snapshot refresh handling.
10. **Load Lifecycle Integration**: Wire the six-phase lifecycle across `save-system`, `content-loader`, and `schema-registry`.