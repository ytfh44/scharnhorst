## 1. Project Foundation & Core
- [x] 1.1 Initialize workspace crates: `scharnhorst_core`, `scharnhorst_schema`, `scharnhorst_arrow_store`, `scharnhorst_scheduler`, `scharnhorst_journal`, `scharnhorst_query`, `scharnhorst_save`, `scharnhorst_content`, `scharnhorst_bevy`, `scharnhorst_rules`
- [x] 1.2 Implement `scharnhorst_core`: ID types (TableId, RowId, Tick), FixedPoint math, and basic error handling.
- [x] 1.3 Implement `scharnhorst_schema`: `TableSpec`, `FieldSemantic`, and `SchemaRegistry`.
- [x] 1.4 Implement `RelationGraph` in schema-registry (DC-6)

## 2. Arrow Store & State Management
- [x] 2.1 Implement `scharnhorst_arrow_store`: Basic `ArrowStore` and `VersionedTable` structures.
- [x] 2.2 Implement `WorldSnapshot` generation and retrieval.
- [x] 2.3 Implement Table Mutation modes (`AppendOnly`, `Patchable`).
- [x] 2.4 Implement Primary Key and Foreign Key indexing for Arrow tables.
- [x] 2.5 Implement immutable snapshot expose for read-only consumers (DC-4)
- [x] 2.6 Implement partitioned table access by `region_id` (DC-6)

## 3. Query Engine (Unified Read Interface)
**Rationale**: All consumers (sim-scheduler, rule-ir, bevy-bridge) depend on query-engine for world state access. Must be implemented before any read operations.
- [x] 3.1 Implement `scharnhorst_query`: **read-only** columnar access API with typed column access (DC-4, DC-10)
- [x] 3.2 Implement **Unified Read Interface** — all consumers must route through query-engine (DC-10)
- [x] 3.3 Implement schema-registry metadata caching in query engine for semantic-aware queries (DC-4)
- [x] 3.4 Integrate DataFusion for SQL-based analysis and complex queries (DC-4)
- [x] 3.5 Implement `DebugWriteJournal` interception layer (debug build only, disabled in multiplayer) (DC-4, DC-10)
- [x] 3.6 Build basic Inspector/Console for table browsing (developer tooling)

## 4. Journal System (Single Write Entry Point)
- [x] 4.1 Implement `scharnhorst_journal`: `CommandEnvelope`, `Diff` types, and **sole write entry point** invariant (DC-1)
- [x] 4.2 Implement tick-aligned atomic commit cycle (DC-3)
- [x] 4.3 Implement SaveJournal append interface for incremental persistence (DC-8)
- [x] 4.4 Implement `DebugWriteJournal` path for debug builds (routes SQL UPDATE/INSERT to journal) (DC-4)

## 5. Deterministic Simulation Loop
- [x] 5.1 Implement `scharnhorst_scheduler`: Phase-based execution, `SimSystem` trait, and **tick-boundary command consumption** (DC-2, DC-3)
- [x] 5.2 Implement the Read-Snapshot → Write-Journal → Atomic Commit cycle (DC-3)
- [x] 5.3 Implement deterministic RNG stream per system/tick.
- [x] 5.4 Implement system dependency registration via **query-engine** (DC-10)
- [x] 5.5 Implement snapshot refresh signal protocol for consumers (DC-3, DC-10)

## 6. Rule System
- [x] 6.1 Implement `scharnhorst_rules`: `Expr` AST for triggers and effects.
- [x] 6.2 Implement **read-only evaluator** with effect submission through journal (DC-1)
- [x] 6.3 Implement evaluator read path exclusively via **query-engine** (DC-10)
- [x] 6.4 Implement Scope management and **RelationGraph-based traversal** (DC-6)
- [x] 6.5 Implement **prefetch cache with LRU eviction** (1024 rows, DC-7) + cache lifecycle management (DC-10)
- [x] 6.6 Implement Modifier aggregation (Add, Mul, Override) as pure functions

## 7. Content Pipeline & Schema Freeze
- [x] 7.1 Implement `scharnhorst_content`: Overlay resolver and name resolution (String -> ID).
- [x] 7.2 Implement Content Compilation to binary Arrow format.
- [x] 7.3 Implement **cold-start-only loading** with schema-registry freeze (DC-5)
- [x] 7.4 Implement `ModFingerprint` generation in content-loader (DC-12)
- [x] 7.5 Implement `SchemaManifest` serialization to snapshot file header (DC-11)
- [x] 7.6 Implement six-phase load lifecycle coordination (DC-13)

## 8. Bevy Bridge & Rendering
- [x] 8.1 Implement `scharnhorst_bevy`: `ViewOf` component and entity materialization logic.
- [x] 8.2 Implement ViewModel synchronization reading `WorldSnapshot` via **query-engine** (DC-10)
- [x] 8.3 Implement the `InputCommandBuffer` with **tick-aligned drain** (DC-2)
- [x] 8.4 Enforce **player commands only** in bridge; AI bypasses bridge (DC-2)
- [x] 8.5 Implement snapshot refresh handler for tick-boundary updates (DC-3, DC-10)

## 9. Save System & Persistence
- [x] 9.1 Implement `scharnhorst_save`: Snapshot persistence, IPC serialization, **checkpoint & journal replay** (DC-8)
- [x] 9.2 Implement Schema Migration pipeline for save-game updates.
- [x] 9.3 Implement **3-snapshot retention** and journal truncation (DC-8)
- [x] 9.4 Implement **Mod-Aware Load Tolerance** with fingerprint comparison (DC-12)
- [x] 9.5 Implement six-phase load reconstruction (DC-13)

## 10. MVP Integration & Verification
- [x] 10.1 Create MVP scenario: 2 Actors, 10 Spatial Nodes, simple ownership transfer.
- [x] 10.2 Verify determinism: Same commands result in identical world hashes.
- [x] 10.3 Verify save/load cycle and schema migration.
- [x] 10.4 Verify Bevy view updates correctly when simulation state changes.

## 11. Cross-Cutting: Compatibility Guarantees
- [x] 11.1 Enforce `schema-registry.is_frozen()` after cold start (DC-5)
- [x] 11.2 Verify single-write-entry-point invariant in integration tests
- [x] 11.3 Implement SaveJournal auto-checkpoint at 1000 diffs (configurable) (DC-8)
- [x] 11.4 Implement multiplayer authority model: server-only Journal, client input replay (DC-9)
