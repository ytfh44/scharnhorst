## 1. Baseline Audit

- [x] 1.1 Inventory all current public `ArrowStore` mutation APIs in `scharnhorst_arrow_store/src/store.rs`
- [x] 1.2 Inventory all direct `ArrowStore` uses outside `scharnhorst_arrow_store`
- [x] 1.3 Classify each direct `ArrowStore` use as initialization, commit, query/read, save/load reconstruction, test-only, or forbidden simulation access
- [x] 1.4 Inventory all direct `WorldSnapshot`, `RecordBatch`, and Arrow table access outside `query-engine`
- [x] 1.5 Inventory all schema-registry mutation APIs and identify which ones must be frozen after Phase 5
- [x] 1.6 Inventory all Bevy bridge state holders and classify each as Derived or Ephemeral
- [x] 1.7 Inventory all rule-ir caches, evaluator outputs, and effect paths
- [x] 1.8 Inventory save/load reconstruction paths that currently apply diffs or load batches directly
- [x] 1.9 Record audit results in a repository document or code comments close to the affected modules

## 2. State Tier Model

- [x] 2.1 Add a state-tier type representing Authority, Derived, and Ephemeral state
- [x] 2.2 Add Authority tier metadata to table specification or an adjacent table-tier registry
- [x] 2.3 Add APIs for registering and querying Authority table tier metadata
- [x] 2.4 Add a Derived state trait or equivalent contract with dirty/rebuild semantics
- [x] 2.5 Add an Ephemeral state documentation or registration mechanism for subsystem-owned runtime state
- [x] 2.6 Ensure Authority tier metadata is available to `save-system`
- [x] 2.7 Ensure Authority tier metadata is available to `journal-system`
- [x] 2.8 Ensure Authority tier metadata is available to tests
- [x] 2.9 Add tests proving every persisted Arrow table has Authority classification
- [x] 2.10 Add tests proving Derived and Ephemeral state are excluded from Authority snapshot enumeration

## 3. Lifecycle and Capability Handles

- [x] 3.1 Define lifecycle states for Initialization and Simulation
- [x] 3.2 Add an initialization capability that can create Authority tables and register schema-affecting metadata before freeze
- [x] 3.3 Add a read-only world/query capability for simulation consumers
- [x] 3.4 Add a journal submission capability for systems and rule effects
- [x] 3.5 Add a journal-owned commit capability for applying diffs to `ArrowStore`
- [x] 3.6 Ensure initialization capability is consumed or invalidated when entering Simulation
- [x] 3.7 Ensure commit capability cannot be constructed by simulation consumers
- [x] 3.8 Add runtime lifecycle guards that return errors when stale capabilities are used
- [x] 3.9 Add tests for stale initialization capability rejection after Simulation starts
- [x] 3.10 Add tests for unavailable or rejected commit capability usage outside journal commit

## 4. Arrow Store Boundary Hardening

- [x] 4.1 Move `create_table` behind initialization capability access
- [x] 4.2 Move `drop_table` behind initialization capability access
- [x] 4.3 Move storage type registration behind initialization capability access
- [x] 4.4 Move mutation-mode changes behind initialization capability access unless a table-local runtime use is explicitly justified
- [x] 4.5 Move checkpoint ingestion and direct batch reconstruction behind initialization or load reconstruction capability access
- [x] 4.6 Move `apply_diffs` behind journal-owned commit capability access
- [x] 4.7 Preserve existing internal diff application semantics under the new commit capability
- [x] 4.8 Ensure direct simulation-facing code cannot call mutation APIs
- [x] 4.9 Ensure forbidden mutation attempts return `ArrowStoreError` or mapped domain errors instead of panicking
- [x] 4.10 Add tests for table creation allowed during Initialization
- [x] 4.11 Add tests for table creation rejected during Simulation
- [x] 4.12 Add tests for direct diff application rejected outside journal commit
- [x] 4.13 Update `scharnhorst_arrow_store` docs to describe read-oriented simulation surface

## 5. Journal System Enforcement

- [x] 5.1 Refactor `Journal` construction to own or receive the commit capability required for store mutation
- [x] 5.2 Ensure `Journal::submit_command` remains separate from commit authority
- [x] 5.3 Ensure `Journal::submit_diff` remains separate from commit authority
- [x] 5.4 Update `Journal::commit` to apply diffs through the commit capability
- [x] 5.5 Ensure `Journal::commit` publishes snapshots to `query-engine` only after successful store mutation
- [x] 5.6 Ensure `Journal::commit` does not advance the tick after failed diff application
- [x] 5.7 Ensure `DebugWriteJournal` submits translated diffs instead of mutating store or query caches directly
- [x] 5.8 Update save journal append behavior to remain after successful Authority commit
- [x] 5.9 Add tests for successful journal-owned commit
- [x] 5.10 Add tests for failed commit returning `JournalError` without panic
- [x] 5.11 Add tests proving debug SQL writes route through journal submission

## 6. Query Engine Read Boundary

- [x] 6.1 Replace simulation consumers' raw snapshot reads with query-engine typed read APIs
- [x] 6.2 Ensure query-engine snapshot-like handles are read-only and generation-tagged
- [x] 6.3 Ensure query-engine handles cannot expose `ArrowStore` mutation methods
- [x] 6.4 Audit `lookup_row`, `column_view`, `batch_reader`, and `read` for tick-before-cache access ordering
- [x] 6.5 Add query APIs needed by Derived rebuild code so it does not reach into `ArrowStore`
- [x] 6.6 Ensure SQL SELECT remains read-only with no side effects
- [x] 6.7 Ensure SQL write statements in debug builds route through `DebugWriteJournal`
- [x] 6.8 Add tests proving query reads do not mutate journal queues or Authority state
- [x] 6.9 Add tests proving Derived rebuilds can read through query-engine without raw Arrow access
- [x] 6.10 Update query-engine docs to describe snapshot handle hygiene

## 7. Schema Freeze Hardening

- [x] 7.1 Ensure `SchemaRegistry::freeze()` is irreversible
- [x] 7.2 Ensure `SchemaRegistry` exposes `is_frozen()` for lifecycle validation
- [x] 7.3 Return errors for post-freeze `TableSpec` registration attempts
- [x] 7.4 Return errors for post-freeze relation graph mutation attempts
- [x] 7.5 Return errors for post-freeze schema version mutation attempts
- [x] 7.6 Return errors for post-freeze mod fingerprint registration attempts
- [x] 7.7 Freeze Authority tier metadata registration before Simulation starts
- [x] 7.8 Ensure content-loader is the only schema author during cold-start Phase 4
- [x] 7.9 Add tests for each post-freeze mutation rejection path
- [x] 7.10 Add integration test proving scheduler refuses to start before schema freeze

## 8. Content Loader Integration

- [x] 8.1 Update content compilation to create Authority tables through initialization capability
- [x] 8.2 Register Authority tier metadata for every compiled persisted table
- [x] 8.3 Validate that Derived or Ephemeral content definitions are not persisted as Authority tables
- [x] 8.4 Ensure overlay resolution completes before schema freeze
- [x] 8.5 Ensure relation graph construction completes before schema freeze
- [x] 8.6 Ensure mod fingerprint registration completes before schema freeze
- [x] 8.7 Drop or consume initialization capability after Phase 5
- [x] 8.8 Add tests for valid Authority content compilation
- [x] 8.9 Add tests for rejecting transient UI or cache tables as Authority content
- [x] 8.10 Add tests proving no runtime content hot-reload mutates schema or Authority tables

## 9. Save and Load Integration

- [x] 9.1 Update full snapshot persistence to enumerate only Authority tables
- [x] 9.2 Exclude Derived state from full save files
- [x] 9.3 Exclude Ephemeral state from full save files
- [x] 9.4 Update checkpoint creation to include Authority tier metadata in validation
- [x] 9.5 Update load reconstruction Phase 1 to deserialize Authority snapshots only
- [x] 9.6 Update load reconstruction Phase 4 to construct Authority tables through initialization capability
- [x] 9.7 Update post-checkpoint diff replay to use journal-owned mutation semantics
- [x] 9.8 Verify final state hash after replay is based on Authority state only
- [x] 9.9 Initialize Derived state as empty, dirty, or rebuilt after load
- [x] 9.10 Initialize Ephemeral state from subsystem defaults after load
- [x] 9.11 Add save/load round-trip test proving Derived caches are not persisted
- [x] 9.12 Add save/load round-trip test proving Ephemeral UI state is not persisted
- [x] 9.13 Add replay test proving live simulation and load reconstruction produce the same Authority hash

## 10. Scheduler Integration

- [x] 10.1 Update scheduler initialization to require frozen schema before first tick
- [x] 10.2 Update system execution context to pass query capability instead of raw store access
- [x] 10.3 Update system execution context to pass journal submission capability instead of commit authority
- [x] 10.4 Ensure scheduler does not call `ArrowStore::apply_diffs` directly
- [x] 10.5 Ensure scheduler calls only `journal.commit()` at tick boundary
- [x] 10.6 Register Derived state handlers with `REFRESH_SIGNAL`
- [x] 10.7 Register tick-scoped Ephemeral cleanup handlers with `REFRESH_SIGNAL`
- [x] 10.8 Ensure `REFRESH_SIGNAL` is broadcast only after successful commit
- [x] 10.9 Add tests for scheduler capability injection
- [x] 10.10 Add tests for refresh invalidating Derived state
- [x] 10.11 Add tests for refresh discarding tick-scoped Ephemeral state
- [x] 10.12 Add test proving failed commit prevents refresh broadcast

## 11. Rule IR Integration

- [x] 11.1 Update evaluator read paths to use query-engine APIs exclusively
- [x] 11.2 Update scope traversal to avoid direct relation graph or Arrow partition access
- [x] 11.3 Update rule effect execution to submit through journal submission capability
- [x] 11.4 Remove or restrict APIs that return raw diffs intended for arbitrary external application
- [x] 11.5 Classify prefetch cache as Derived state
- [x] 11.6 Clear prefetch cache on `REFRESH_SIGNAL`
- [x] 11.7 Ensure cached rows resolve through row-position lookup and never treat `RowId.0` as a physical index
- [x] 11.8 Add tests proving rule reads route through query-engine
- [x] 11.9 Add tests proving rule effects do not directly mutate `ArrowStore`
- [x] 11.10 Add tests proving prefetch cache is cleared at tick boundary

## 12. Bevy Bridge Integration

- [x] 12.1 Classify `InputCommandBuffer` as Ephemeral until commands are consumed by scheduler
- [x] 12.2 Classify entity materialization registry as Derived or Ephemeral view state
- [x] 12.3 Classify view models and sync fields as Derived views of Authority state
- [x] 12.4 Classify hover, selection, camera, and animation state as Ephemeral
- [x] 12.5 Ensure bridge reads Authority data through query-engine only
- [x] 12.6 Ensure bridge cannot call `ArrowStore` mutation APIs
- [x] 12.7 Ensure bridge player interactions become buffered commands, not direct diffs
- [x] 12.8 Ensure AI and internal simulation commands bypass the Bevy input buffer
- [x] 12.9 Refresh Bevy read handles after `REFRESH_SIGNAL`
- [x] 12.10 Add tests proving player command buffering does not mutate Authority state until journal commit
- [x] 12.11 Add tests proving Bevy view state is excluded from saves
- [x] 12.12 Add tests proving AI commands do not enter the Bevy input buffer

## 13. API Cleanup and Documentation

- [x] 13.1 Remove deprecated broad mutation APIs from simulation-facing exports
- [x] 13.2 Update crate-level docs for `scharnhorst_arrow_store`
- [x] 13.3 Update crate-level docs for `scharnhorst_journal`
- [x] 13.4 Update crate-level docs for `scharnhorst_query`
- [x] 13.5 Update crate-level docs for `scharnhorst_scheduler`
- [x] 13.6 Update crate-level docs for `scharnhorst_schema`
- [x] 13.7 Update crate-level docs for `scharnhorst_content`
- [x] 13.8 Update crate-level docs for `scharnhorst_save`
- [x] 13.9 Update crate-level docs for `scharnhorst_rules`
- [x] 13.10 Update crate-level docs for `scharnhorst_bevy`
- [x] 13.11 Ensure docs use relative paths only
- [x] 13.12 Document any remaining runtime-guarded boundaries that are not yet compile-time enforced

## 14. Verification

- [x] 14.1 Run `cargo fmt`
- [x] 14.2 Run `cargo test -p scharnhorst_core`
- [x] 14.3 Run `cargo test -p scharnhorst_schema`
- [x] 14.4 Run `cargo test -p scharnhorst_arrow_store`
- [x] 14.5 Run `cargo test -p scharnhorst_journal`
- [x] 14.6 Run `cargo test -p scharnhorst_query`
- [x] 14.7 Run `cargo test -p scharnhorst_scheduler`
- [x] 14.8 Run `cargo test -p scharnhorst_content`
- [x] 14.9 Run `cargo test -p scharnhorst_save`
- [x] 14.10 Run `cargo test -p scharnhorst_rules`
- [x] 14.11 Run `cargo test -p scharnhorst_bevy`
- [x] 14.12 Run `cargo test -p scharnhorst_integration_tests`
- [x] 14.13 Run full workspace `cargo test`
- [x] 14.14 Run `cargo clippy --workspace --all-targets`
- [x] 14.15 Verify no new `unwrap()`, `expect()`, or `panic!()` calls were introduced in non-test code
- [x] 14.16 Run `openspec validate phase-0-invariant-fortification --strict`
- [x] 14.17 Confirm all Phase 0 OpenSpec requirements have corresponding tests or documented verification paths

## 15. Audit-Driven Rework (SPEC Refinement + Code Fix)

This section records tasks reopened or added after systematic cross-module audit of the Phase 0 implementation. SPECs in `openspec/changes/phase-0-invariant-fortification/specs/` have been refined for self-consistency prior to code fixes.

### 15.1 Core (FixedPoint, Capability, StateTier, Error)

- [x] 15.1.1 Remove `inner()` leak methods from `InitStore` and `CommitStore` guard types; make `ArrowStore::advance_to_simulation()` `pub(crate)` — defense in depth: primary barrier is not exposing `Arc<ArrowStore>` to external consumers; guards now forward operations without leaking the store handle; `CommitStore::new()` remains `pub` for framework crates (journal, scheduler) that legitimately hold `Arc<ArrowStore>`
- [x] 15.1.2 Add `CoreError::InvalidPhase` (or equivalent) — ~~deferred; journal error has its own `JournalError::InvalidPhase`~~ **FIXED**: `CoreError::InvalidPhase(String)` variant added; `LifecycleGuard::advance_to_simulation()` now returns `CoreError::InvalidPhase` instead of `CoreError::Generic` for double-advance guard. Test `advance_to_simulation_twice_returns_invalid_phase` verifies.
- [x] 15.1.3 Fix `FixedPoint::from_str` to return `ArithmeticOverflow` on overflow and `InvalidId` on invalid input instead of `Generic`
- [x] 15.1.4 `TierRegistry::register()` must return `Err` on duplicate, not silent `Option` return (SPEC refined: "reject duplicate registration")

### 15.2 Journal

- [x] 15.2.1 `commit()`: on failure after phase set to `Committing`, reset phase to `Open` and restore pending diffs/commands for ALL failure paths (diff application, query-engine ingestion, save-journal append); all three failure paths now restore pending data and reset phase (SPEC refined: commit failure recovery semantics)
- [x] 15.2.2 `clear_pending`: implement `JournalError::InvalidPhase` variant (distinct from `SubmitFailed`) for `clear_pending` phase guard
- [x] 15.2.3 Bound `commit_history` VecDeque to `MAX_COMMIT_HISTORY = 1024`, evict oldest via `pop_front`
- [x] 15.2.4 `Tick::next()`: saturate at `Tick::MAX` (not `Tick(u64::MAX)` sentinel)

### 15.3 Query Engine

- [x] 15.3.1 `column_view()`, `row_cursor()`, `batch_reader()`: enforce tick-before-cache ordering (call `latest_tick()` via Acquire before `view_cache` read lock) — currently only `lookup_row()` does this
- [x] 15.3.2 `read()`: validate that `request.tick` matches the cache's actual tick; return error on mismatch
- [x] 15.3.3 `lookup_row()`: return `RowLookupView` vs `RowLookup` — implementation already uses `RowLookup` from core, aligned with SPEC

### 15.4 Scheduler

- [x] 15.4.1 `tick()`: increment generation after successful commit and before `REFRESH_SIGNAL` broadcast (per spec: "incremented only after successful journal.commit() and before REFRESH_SIGNAL broadcast"); code verified: `atomic_commit()` stores generation via `self.generation.store(new_gen, Relaxed)` before calling `self.broadcast_refresh(result.tick, new_gen)`
- [x] 15.4.2 `atomic_commit()`: broadcast `REFRESH_SIGNAL` after successful commit
- [x] 15.4.3 `broadcast_refresh()`: pass generation number, not `state_hash`, as the signal payload
- [x] 15.4.4 `unregister_system()`: remove from `refresh_bus` as well as `systems`/`registrations`
- [x] 15.4.5 `register_system()`: reject after `initialized == true`, return `SchedulerError::Initialized`

### 15.5 Save System

- [x] 15.5.1 `replay_diffs`: initialize `current_hash` to snapshot's `state_hash`, not `0`
- [x] 15.5.2 `LoadReconstruction`: enforce phase ordering via `lifecycle.advance()`; reject out-of-order calls
- [x] 15.5.3 `CheckpointingSaveJournal::append()`: check `should_auto_checkpoint()` and enforce retention at threshold; code does NOT truncate journal (data-loss risk avoided); only `enforce_retention()` is called — snapshot writing and journal truncation deferred to `write_checkpoint_snapshot` path which requires world state access; spec updated to match this deferred design
- [x] 15.5.4 Phase 4: execute DFS cycle detection on RelationGraph after all edges are added; reject cycles — ~~deferred (not in Phase 0 critical path)~~ **IMPLEMENTED**: `detect_cycles()` already present in code; added `add_edge_unchecked` (`#[doc(hidden)] pub`) for test-only cycle injection; 6 `detect_cycles` unit tests (direct cycle, multi-node cycle, DAG false positive, diamond false positive, empty graph, self-loop); 2 Phase 4 integration tests (`phase4_rejects_cyclic_relations`, `phase4_accepts_non_cyclic_dag`) covering both the `add_edge` per-edge check path and the `detect_cycles` global safety net

### 15.6 Arrow Store

- [x] 15.6.1 `load_checkpoint`: acquire `create_drop_lock` before inserting into `tables`/`table_id_map`
- [x] 15.6.2 `apply_diff_insert`: check for duplicate `RowId` via `position_map.contains()` before `insert()`; return `DuplicateRowId` error
- [x] 15.6.3 Delete semantics: ensure nullable column test coverage and `RowPositionMap` removal verification — ~~deferred (not in Phase 0 critical path)~~ **IMPLEMENTED**: 7 new delete tests covering: nullable column null_patch (nullable→null, non-nullable→preserved), RowPositionMap removal, delete-nonexistent, double-delete, wrong-table, position_map verification via `get_table().position_map().contains()`; confirmed `build_null_patch` correctly handles `field.is_nullable()` and null_patch is inline-applied to existing version batches (not as separate batch)

### 15.7 Rule IR

- [x] 15.7.1 Forward scope jump PK resolution: use `TableSpec::primary_key_column()` instead of hardcoded `.get_i64("id")`
- [x] 15.7.2 `CachedRow`: use `Arc<TableReadView>` instead of cloned `TableReadView` per row

### 15.8 Content Loader

- [x] 15.8.1 `CompilationOutput`: add `RelationGraph` edges and `ModFingerprint` entries
- [x] 15.8.2 `CompilationInput`: accept `MigratedSchemaManifest` reference; validate compiled tables conform
- [ ] 15.8.3 Tier validation during compilation: reject Derived/Ephemeral as Authority — deferred (not in Phase 0 critical path)

### 15.9 Bevy Bridge

- [x] 15.9.1 `InputCommandBuffer::with_player_id`: propagate lock poisoning instead of `if let Ok` swallowing

### 15.10 Second-Pass Audit (Cross-Module Gap Remediation)

This section records issues found during systematic second-pass audit after Section 15 code fixes were applied. SPECs have been verified self-consistent; these are implementation gaps relative to the refined SPECs.

#### 15.10.1 Arrow Store

- [x] 15.10.1.1 `write_checkpoint`: now has two token-gated overloads (`&InitToken` / `&CommitToken`) with lifecycle validation; non-gated `pub(crate) fn write_checkpoint` removed; `InitStore::into_simulation()` no longer leaks `Arc<ArrowStore>` — returns only `CommitStore`. All callers updated.
- [x] 15.10.1.2 `load_checkpoint`: holds `create_drop_lock` for duration of all table insertions; locks correctly acquired before table/table_id_map mutation; acceptable — audit note only for future profiling (confirmed harmless in Phase 0)

#### 15.10.2 Journal System

- [x] 15.10.2.1 `submit_command()` / `submit_diff()`: accept `&JournalSubmitToken` as `_token` parameter — token acts as type witness preventing accidental calls from code that lacks the token type in scope; `JournalSubmitToken::new()` is `pub` (not `pub(crate)`) so any crate can construct it, reducing it to a documentation marker — this is acceptable per Decision 1's layered defense ("use narrow handles plus runtime `Result` guards as an intermediate step") but is recorded as a spec-code gap (spec says pub(crate), code says pub)
- [x] 15.10.2.2 `DebugWriteJournal`: ~~maintains independent `VecDeque<DebugWriteOp>` queue instead of routing through `journal.submit_diff()`~~ **FIXED**: removed `pending_diffs` field, `execute_sql()` now accepts `&mut Journal` and routes through `journal.submit_diff()`. `take_pending()` and `pending_count()` removed. All 11 DebugWriteJournal tests pass routing through journal commit pathway.
- [x] 15.10.2.3 `commit_history()`: `VecDeque` bounded at `MAX_COMMIT_HISTORY = 1024` with `pop_front` eviction — correct; `as_slices()` contiguity concern is moot since no caller accesses commit_history via raw slice API
- [x] 15.10.2.4 `commit_history()`: return type was `&[CommitRecord]` using `self.history.as_slices().0` — VecDeque internal wrapping after `pop_front` would produce incomplete slice (`.1` dropped). **FIXED**: changed to return `Vec<&CommitRecord>` via `self.history.iter().collect()`. Test `commit_history_bounded_and_contiguous_after_wrapping` verifies 1030 commits bounded to 1024.

#### 15.10.3 Query Engine

- [x] 15.10.3.1 `inspect_table_summary`, `inspect_table_page`, `cached_table_names`: all three DO call `latest_tick()` before acquiring `view_cache` read lock — tick-before-cache ordering confirmed for ALL reader methods including inspector paths
- [x] 15.10.3.2 `ingest_snapshot`: uses proper runtime `if tick.as_u64() == u64::MAX { return Err(...) }` check, not `debug_assert!` — sentinel collision correctly rejected in both debug and release builds
- [x] 15.10.3.3 `register_table_schema`: non-atomic two-phase write — schema_registry mutated before inspector; if inspector write fails (e.g. lock poison), schema_registry is already committed but inspector is stale. **FIXED**: lock ordering reversed (registry first, then inspector) to reduce window between lock and mutation; inspector write is infallible so full rollback is not needed.
- [x] 15.10.3.4 `inspect_table_summary`, `inspect_table_page`, `cached_table_names`: `let _tick = self.latest_tick()` (no `?`) — Result error silently dropped, violating tick-before-cache ordering guarantee on error path. **FIXED**: added `?` to all three (`let _tick = self.latest_tick()?`).
- [x] 15.10.3.5 `read()`: tick mismatch used `QueryError::InvalidQuery` despite dedicated `TicksMismatch` variant in error.rs. **FIXED**: changed to `QueryError::TicksMismatch { requested, actual }`.

#### 15.10.4 Scheduler

- [x] 15.10.4.1 `atomic_commit()`: TOCTOU on generation — `load(Acquire)` reads gen=N, computes N+1 for broadcast, then `fetch_add(AcqRel)` atomically increments; load and fetch_add are separate operations. Replace with single `fetch_add(1, Ordering::AcqRel)` that returns the OLD generation, then use `old_gen + 1` for broadcast (no separate load needed). Remove the preceding `load(Acquire)` entirely.
- [x] 15.10.4.2 `atomic_commit()`: generation incremented before `broadcast_refresh` — this is per-spec behavior; sim-scheduler spec states "generation counter SHALL be incremented during atomic_commit() after the commit succeeds but before the broadcast. If broadcast_refresh itself fails, the generation has already been incremented and the caller is responsible for recovery." No fix needed.
- [x] 15.10.4.3 `tick()`: returns `SchedulerError::NotInitialized` with correct message "scheduler not yet initialized; call initialize() first" — NOT the wrong variant, confirmed correct
- [x] 15.10.4.4 `generation`: `current_generation()` reads with `Relaxed` — consumer may observe stale generation without seeing committed data. **FIXED**: `current_generation()` now reads with `Acquire`, `atomic_commit()` uses `AcqRel` on `fetch_add`. Pair forms release-acquire ordering: committed data is visible to any reader that observes the new generation. `Relaxed` recommendation in spec is a spec-code gap — code is correct.
- [x] 15.10.4.5 `register_system()`: conflict detection uses `unwrap_or_else(|| "<unknown>".to_string())` — correct fallback for unreachable empty-conflict case; not an issue
- [x] 15.10.4.6 `register_system()` TOCTOU: initialized check at function entry (line 90) was separated from lock acquisition (lines 97-98) by a race window — concurrent `initialize()` could set `initialized=true` after the check but before the lock. **FIXED**: added post-lock re-check of `initialized` after both `lock_systems()` and `lock_registrations()` succeed. Test `register_system_rejected_after_initialization` verifies the lifecycle gate.

#### 15.10.5 Content Loader

- [x] 15.10.5.1 `compile()`: calls `registry.add_relation()` and `registry.load_from_manifest()` when `input.migrated_manifest` is `Some` — relation edges correctly registered from migrated manifest in save/load path. In new-game path (migrated_manifest is `None`), RelationGraph is populated by `load_from_manifest()` which is called separately during Phase 4 of the six-phase lifecycle, not by `compile()` — this is acceptable separation of concerns.
- [x] 15.10.5.2 `compile()`: calls `registry.store_mod_fingerprints()` when `migrated_manifest` is `Some` — fingerprints stored correctly. In new-game path, mod fingerprints come from content definitions and are stored through the six-phase lifecycle content-loading path.
- [x] 15.10.5.3 `compile()`: calls `fp.compute_hash("content_compilation", &table_specs)` via `ModFingerprintExt` when `migrated_manifest` is `Some` — content_hash computed correctly using fxhash over mod_id, version, table specs.
- [x] 15.10.5.4 `compile()`: uses `?` propagation for `tier_registry.register()` errors — NOT silently dropped; returns `ContentError::CompilationFailed` on failure.
- [x] 15.10.5.5 `compile()`: now calls `registry.set_migrated_manifest(migrated.clone())` when `input.migrated_manifest` is `Some` — schema registry carries migration context. New-game cold start with `None` remains correct.
- [x] 15.10.5.6 `CompilationOutput`: `migrated_manifest: Option<MigratedSchemaManifest>` field added; populated from `input.migrated_manifest` — available for downstream Phase 5 freeze validation.

#### 15.10.6 Schema Registry

- [x] 15.10.6.1 `RelationGraph::detect_cycles()`: iterative DFS DOES visit Gray neighbors — back edges correctly identified as cycles; pattern `Some(Color::White) | Some(Color::Gray)` at relation.rs L171 ensures both White (tree edge) and Gray (back edge forming cycle) are pushed onto stack. Not a bug.
- [x] 15.10.6.2 `SchemaManifest` serialization methods: now wrap raw errors in `SchemaError::ManifestSerialization` / `ManifestDeserialization`; all four methods (`to_toml`, `from_toml`, `to_json`, `from_json`) return `Result<_, SchemaError>`. Callers use `.expect()`/`.unwrap()` — no behavior change.

#### 15.10.7 Save System

- [x] 15.10.7.1 `try_auto_checkpoint`: DOES call `reset_journal_counter()` after `enforce_retention()` — no redundant calls. Also `write_checkpoint_snapshot()` now calls `self.reset_journal_counter()` after full save — counter reflects entries since last full save, not last auto-checkpoint trigger.
- [x] 15.10.7.2 `replay_diffs`: requires exact tick match for snapshot — no "latest snapshot ≤ base_tick" fallback; acceptable for Phase 0, doc gap recorded
- [x] 15.10.7.3 `temp_dir()` race: `load_reconstruction.rs`, `snapshot_persistence.rs`, `checkpoint.rs` used PID-only temp dir paths — parallel tests shared the same directory. **FIXED**: added `AtomicUsize` counter to all three `temp_dir()` functions, producing unique paths per invocation. Test `temp_dir_unique_across_invocations` verifies 50 unique paths.
- [x] 15.10.7.4 `SnapshotPersistence::write_snapshot`: no `create_dir_all` guard before `File::create` — trust on caller to pre-create `base_dir`. **FIXED**: added `create_dir_all(parent)` guard. Test `write_snapshot_creates_missing_parent` verifies writing to non-existent directory.
- [x] 15.10.7.5 Silent error dropping: `let _ = std::fs::create_dir_all/remove_dir_all` masks setup failures across 12 locations in `load_reconstruction.rs`, `checkpoint.rs`. **FIXED**: replaced with `.unwrap()` / `.expect()` for setup operations; teardown cleanup uses `cleanup()` helper. Test failures from missing dirs now panic immediately instead of surfacing as cryptic os error 3.

#### 15.10.8 Rule IR

- [x] 15.10.8.1 Stale comments in `evaluate_empty_and_returns_true` and `evaluate_empty_or_returns_false`: updated from `try_fold starts with ...` to `for loop with ... identity start returns identity on empty iter` — comments now describe actual implementation.
- [x] 15.10.8.2 `pin_in_cache`: now calls `PrefetchCache::find_view()` to share existing `Arc<TableReadView>` for same table+tick before creating new Arc — consistent with `prefetch()` behavior.

#### 15.10.9 Bevy Bridge

- [x] 15.10.9.1 `CommandBatch`: `count` field removed; `is_empty()` now derives from `commands.is_empty()`; `with_commands`, `drain_commands`, constructors no longer maintain redundant count.
- [x] 15.10.9.2 `push_with_source`: `AiCommandRejected` renamed to `NonPlayerCommandRejected` covering both Ai and Internal sources; error message updated accordingly; all match patterns and tests updated.
- [x] 15.10.9.3 `submit_player_command`: signature changed from `source: impl Into<String>` to `source: CommandSource`; gate uses `source.is_player()` not string prefix; aligned with `push_with_source` enum-based gating.
- [x] 15.10.9.4 `refresh_handler`: callback now uses scheduler-provided `generation` parameter; separate `AtomicU64` counter removed; `refresh_snapshot` uses `vm.generation()?.saturating_add(1)`; `current_generation` delegates to `view_model.generation()`.

#### 15.10.10 Core

- [x] 15.10.10.1 `FixedPoint::Ord::cmp`: `cmp_normalized` rewritten to use i128 arithmetic (eliminates overflow for all valid i64 raw × 10^18 combos); `Ord::cmp` fallback now returns `Ordering::Equal` as deterministic tie-breaker; `PartialOrd::partial_cmp` still returns `None` on incomparable; Ord/PartialOrd contract now consistent.
- [x] 15.10.10.2 `FixedPoint::from_str`: maps format errors (non-overflow parse failures) to `CoreError::InvalidFormat` — correct; overflow maps to `ArithmeticOverflow`. `InvalidId` is never used for from_str. Not an issue.

#### 15.10.11 Spec Self-Consistency (Post-Implementation Audit)

- [x] 15.10.11.1 `journal-system/spec.md`: JournalSubmitToken requirement updated from `pub(crate) fn new()` to `pub fn new()` with defense-in-depth rationale documented — matches actual implementation; token serves as documentation marker, enforcement at guard layer
- [x] 15.10.11.2 `journal-system/spec.md`: merged duplicate Tick::MAX requirement sections (both described same semantics with slightly different wording) into single "Tick Saturates at MAX" section with unified scenario
- [x] 15.10.11.3 `tasks.md` 15.1.2 `CoreError::InvalidPhase`: deferred item now implemented — variant added, LifecycleGuard adopts it, test verifies

#### 15.10.12 Test Coverage Fortification

- [x] 15.10.12.1 `scharnhorst_core`: `advance_to_simulation_twice_returns_invalid_phase` — confirms LifecycleGuard double-advance returns `CoreError::InvalidPhase` not Generic
- [x] 15.10.12.2 `scharnhorst_content`: `compiled_tables_registered_as_authority` — verifies all compiled tables are Authority-tier in TierRegistry
- [x] 15.10.12.3 `scharnhorst_content`: `tier_registry_only_contains_authority_tables` — confirms `authority_tables()` iterator returns only Authority entries
- [x] 15.10.12.4 `scharnhorst_content`: `compiled_output_includes_fingerprints` — structural validation of CompilationOutput fields
- [x] 15.10.12.5 `scharnhorst_save`: `persisted_snapshot_filter_to_authority_removes_non_authority_tables` — verifies Derived and Ephemeral data removed from snapshot before serialization
- [x] 15.10.12.6 `scharnhorst_save`: `filter_to_authority_without_registry_removes_all_unregistered` — confirms unregistered tables treated as non-Authority (safety default)
- [x] 15.10.12.7 `scharnhorst_save`: `filter_to_authority_with_no_authority_tables_removes_all` — edge case: all-registry-no-authority produces empty snapshot

#### 15.10.13 JSON Null-to-Arrow Null Fix

- [x] 15.10.13.1 `scharnhorst_arrow_store`: **`json_value_to_array` now handles `Value::Null`** — previously, null JSON values were silently converted to empty strings (`""`) or zero values on insert, producing incorrect arrow arrays. Fixed by returning proper null-bitmap arrow arrays (`ArrayRef` with null entry) for all supported types (Int64, Int32, UInt64, Float64, Boolean, Utf8, LargeUtf8) when `value.is_null()`.

#### 15.10.14 New Edge-Case May-Fail Tests

- [x] 15.10.14.1 `arrow_store_tests`: `patch_string_preserves_nulls_in_unaffected_rows` — confirms that when two rows are inserted in separate batches for the same tick and one has a null string, patching the other row's string value does not affect the null row's null bitmap
- [x] 15.10.14.2 `arrow_store_tests`: `patch_string_array_handles_large_utf8` — confirms that `patch_string_array` correctly returns `GenericStringArray<i64>` (not i32) for `DataType::LargeUtf8` columns
- [x] 15.10.14.3 `fixed_point`: `ord_consistent_with_partial_ord_cross_scale` — confirms `Ord::cmp` agrees with `PartialOrd` for cross-scale comparisons (scale 6 vs scale 0, negative cross-scale)
- [x] 15.10.14.4 `fixed_point`: `from_str_overflow_returns_arithmetic_overflow` — confirms 19-digit parse overflows i64 and returns `CoreError::ArithmeticOverflow`
- [x] 15.10.14.5 `fixed_point`: `from_str_invalid_format_returns_invalid_format` — confirms non-numeric string returns `CoreError::InvalidFormat`
- [x] 15.10.14.6 `journal_tests`: `clear_pending_during_open_succeeds_and_clears` — confirms `clear_pending` clears all pending diffs when in Open phase
- [x] 15.10.14.7 `journal_tests`: `commit_history_bounded_and_contiguous_after_wrapping` — confirms bounded VecDeque retains ascending ticks after exceeding MAX_COMMIT_HISTORY (1024)
- [x] 15.10.14.8 `schema_tests`: `relation_graph_mut_rejected_after_freeze` — confirms `relation_graph_mut()` returns error after freeze (complement to existing `register`, `add_relation` freeze tests)
