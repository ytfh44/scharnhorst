## 1. Phase 1 — Scheduler Atomic Replacements

- [x] 1.1 Replace `Scheduler::current_tick: Arc<Mutex<Tick>>` with `Arc<AtomicU64>` in `scharnhorst_scheduler/src/scheduler.rs`
- [x] 1.2 Replace `Scheduler::generation: Arc<Mutex<u64>>` with `Arc<AtomicU64>` in `scharnhorst_scheduler/src/scheduler.rs`
- [x] 1.3 Replace `Scheduler::initialized: Arc<Mutex<bool>>` with `Arc<AtomicBool>` in `scharnhorst_scheduler/src/scheduler.rs`
- [x] 1.4 Update `Scheduler::new()` to initialize atomics instead of Mutex wrappers
- [x] 1.5 Rewrite `advance_tick()` to use `fetch_add(1, Relaxed)` instead of `lock_tick()` + `*tick = tick.next()`
- [x] 1.6 Rewrite `current_tick()` to use `load(Relaxed)` instead of `lock_tick()` + deref
- [x] 1.7 Rewrite `current_generation()` to use `load(Relaxed)` instead of `lock_generation()` + deref
- [x] 1.8 Rewrite `atomic_commit()` generation increment to use `fetch_add(1, Relaxed)`
- [x] 1.9 Rewrite `initialize()` to use two-phase pattern: `load(Acquire)` fast-path check → validation (under Mutex serialization) → `store(Release)` after success (per Amendment E / S-CA-001; compare_exchange approach rejected because it would permanently mask validation failures)
- [x] 1.10 Rewrite `is_initialized()` to use `load(Acquire)` (pairs with `store(Release)` in two-phase initialize), replacing `lock_initialized()` + deref
- [x] 1.11 Remove private lock helper methods: `lock_tick`, `lock_generation`, `lock_initialized`
- [x] 1.12 Update `std::fmt::Debug` impl for `Scheduler` to use atomic loads instead of `lock()` calls
- [x] 1.13 Run `scharnhorst_scheduler` tests (`cargo test -p scharnhorst_scheduler`) and verify all pass

## 2. Phase 1 — QueryEngine Atomic Replacement

- [x] 2.1 Replace `QueryEngine::latest_tick: Arc<RwLock<Option<Tick>>>` with `Arc<AtomicU64>` in `scharnhorst_query/src/engine.rs` (use `u64::MAX` as sentinel for `None`)
- [x] 2.2 Update `QueryEngine::new()` to initialize `AtomicU64::new(u64::MAX)` (None sentinel)
- [x] 2.3 Rewrite `ingest_snapshot()` to use `store(tick.as_u64(), Release)` instead of write lock
- [x] 2.4 Rewrite `latest_tick()` to translate `load(Acquire)` back to `Option<Tick>` (sentinel → None)
- [x] 2.5 Run `scharnhorst_query` tests (`cargo test -p scharnhorst_query`) and verify all pass

## 3. Phase 1 — Bevy Bridge Atomic Replacements

- [x] 3.1 Split `RefreshHandlerState { signal_received: bool, last_snapshot_generation: u64 }` into two atomics in `scharnhorst_bevy/src/refresh_handler.rs`
- [x] 3.2 Update `SnapshotRefreshHandler` struct: replace `state: Arc<Mutex<RefreshHandlerState>>` with `signal_received: Arc<AtomicBool>` and `last_snapshot_generation: Arc<AtomicU64>`
- [x] 3.3 Rewrite `on_refresh_signal()` to use `store(true, Release)`
- [x] 3.4 Rewrite `should_refresh()` to use `load(Acquire)`
- [x] 3.5 Rewrite `refresh_snapshot()` to use `fetch_add(1, Relaxed)` for generation, then `store(false, Release)` for signal
- [x] 3.6 Rewrite `current_generation()` to use `load(Relaxed)`
- [x] 3.7 Rewrite `callback()` closure to use atomic operations instead of `state.lock()`
- [x] 3.8 Split `ViewModelState::generation` from the Mutex: add `generation: Arc<AtomicU64>` to `ViewModel` in `scharnhorst_bevy/src/sync.rs`
- [x] 3.9 Rewrite `ViewModel::refresh()` to use `store(generation, Relaxed)` for the generation field (snapshot/latest_tick remain under Mutex)
- [x] 3.10 Rewrite `ViewModel::generation()` to use `load(Relaxed)`
- [x] 3.11 Run `scharnhorst_bevy` tests (`cargo test -p scharnhorst_bevy`) and verify all pass

## 4. Phase 2 — RefreshSignalBus Broadcast Snapshotting

- [x] 4.1 In `RefreshSignalBus::broadcast()` (`scharnhorst_scheduler/src/refresh_signal.rs`), clone `consumers.values()` into a `Vec<RefreshCallback>` under the lock, then release lock before iterating and invoking callbacks
- [x] 4.2 Ensure error propagation semantics unchanged: if any callback returns `Err`, return immediately with `RefreshSignalFailed`
- [x] 4.3 Run `scharnhorst_scheduler` tests and verify `broadcast` tests pass (especially `broadcast_error_propagates`, `unregister_stops_broadcast`)

## 5. Phase 3 — ArrowStore DashMap Preparation

- [x] 5.1 Add `dashmap` dependency to `scharnhorst_arrow_store/Cargo.toml`
- [x] 5.2 Add `use dashmap::DashMap` import in `scharnhorst_arrow_store/src/store.rs`

## 6. Phase 3 — ArrowStore Struct Restructuring

- [x] 6.1 Replace `ArrowStoreInner` struct: convert `tables: HashMap<String, VersionedTable>` to `DashMap<String, Arc<RwLock<VersionedTable>>>`
- [x] 6.2 Convert `table_id_map: HashMap<TableId, String>` to `DashMap<TableId, String>`
- [x] 6.3 Convert `snapshots: HashMap<Tick, Arc<WorldSnapshot>>` to `DashMap<Tick, Arc<WorldSnapshot>>`
- [x] 6.4 Convert `next_table_id: u64` to `AtomicU64`
- [x] 6.5 Convert `generation: u64` to `AtomicU64`
- [x] 6.6 Keep `type_registry: TypeRegistry` under its own `RwLock<TypeRegistry>` (since TypeRegistry is already Clone but used via &mut, wrap in RwLock for concurrent read access)
- [x] 6.7 Update `ArrowStore` struct: remove `inner: Arc<RwLock<ArrowStoreInner>>`, add direct fields for each sharded component
- [x] 6.8 Remove `ArrowStoreInner` struct (flatten into `ArrowStore`)

## 7. Phase 3 — ArrowStore Method Rewrites

- [x] 7.1 Rewrite `read()` and `write()` helpers: remove them; access per-field directly
- [x] 7.2 Rewrite `create_table()`: use `next_table_id.fetch_add(1, Relaxed)` for ID allocation; insert into `tables` and `table_id_map` DashMaps
- [x] 7.3 Rewrite `drop_table()`: remove from `tables` first, then `table_id_map`; handle orphan cleanup
- [x] 7.4 Rewrite `get_table()`: acquire read lock on the `Arc<RwLock<VersionedTable>>` entry from DashMap
- [x] 7.5 Rewrite `table_names()`: iterate `tables` DashMap keys
- [x] 7.6 Rewrite `register_type()`: acquire write lock on `type_registry` RwLock
- [x] 7.7 Rewrite `resolve_type()`: acquire read lock on `type_registry` RwLock
- [x] 7.8 Rewrite `generate_snapshot()`: iterate `tables` DashMap, acquire per-table read locks, build snapshot; use `generation.fetch_add(1, Relaxed)`
- [x] 7.9 Rewrite `get_snapshot()`: lookup in `snapshots` DashMap
- [x] 7.10 Rewrite `latest_snapshot()`: iterate `snapshots` DashMap keys
- [x] 7.11 Rewrite `load_checkpoint()`: remove inlined snapshot generation (no longer needed since no re-entrancy constraint); call `generate_snapshot()` instead
- [x] 7.12 Rewrite `truncate_before()`: iterate `snapshots` DashMap, retain by tick condition
- [x] 7.13 Rewrite all remaining methods that previously accessed `inner.read()?.tables` or `inner.write()?.tables` to use the new per-field access patterns
- [x] 7.14 Update `Default` impl for `ArrowStore` to construct the new structure

## 8. Phase 3 — ArrowStore Index Methods

- [x] 8.1 Rewrite primary key index methods (`build_primary_index`, `lookup_by_primary_key`) to acquire table read locks from DashMap
- [x] 8.2 Rewrite foreign key index methods (`build_foreign_index`, `lookup_by_foreign_key`) to acquire table read locks from DashMap
- [x] 8.3 Rewrite partition scan methods (`scan_region`, `get_partition`) to acquire table read locks from DashMap

## 9. Phase 3 — ArrowStore IPC and Schema Methods

- [x] 9.1 Rewrite IPC serialization methods (`serialize_table`, `deserialize_table`) to access tables via DashMap
- [x] 9.2 Rewrite schema-related methods (`get_table_schema`, `get_table_schema_by_id`) to access tables and type_registry via per-field locks

## 10. Phase 3 — Verification

- [x] 10.1 Run `scharnhorst_arrow_store` tests (`cargo test -p scharnhorst_arrow_store`) and fix any failures
- [x] 10.2 Run `scharnhorst_integration_tests` (`cargo test -p scharnhorst_integration_tests`) and fix any failures

## 11. Critical Issue Remediation (Phase 4)

These tasks address issues discovered during deep review of Phases 1-3. They should be implemented AFTER Phases 1-3 are complete but BEFORE the final verification.

### 11.1 QueryEngine — Tick Sentinel Protection (Amendment A, QE-CA-001)

- [x] 11.1.1 Add `const MAX: Tick = Tick(u64::MAX - 1)` to `scharnhorst_core/src/id.rs` (or wherever `Tick` is defined) to reserve `u64::MAX` as sentinel at the type level
- [x] 11.1.2 In `ingest_snapshot()` in `scharnhorst_query/src/engine.rs`, add `debug_assert!(tick.as_u64() != u64::MAX, "Tick sentinel collision")` before the atomic store
- [x] 11.1.3 Document the sentinel invariant on `latest_tick`: add doc comment stating `u64::MAX` is reserved as `None`
- [x] 11.1.4 Run `scharnhorst_query` tests to verify tick sentinel assertion does not break existing tests

### 11.2 QueryEngine — Document Access Ordering (Amendment B, QE-CA-002)

- [x] 11.2.1 Add doc-comment on `ingest_snapshot()` (and/or `latest_tick()`) stating the ordering invariant: readers MUST load `latest_tick()` BEFORE reading `view_cache`
- [x] 11.2.2 Verify `lookup_row()` and all other reader sites maintain the required access order (load tick first, then cache)

### 11.3 ArrowStore — create_table/drop_table Atomic Pairing (Amendment C, AS-CA-001)

- [x] 11.3.1 Add `create_drop_lock: Mutex<()>` field to `ArrowStore` struct in `scharnhorst_arrow_store/src/store.rs`
- [x] 11.3.2 In `create_table()`, acquire `create_drop_lock` before the two DashMap inserts (tables, then table_id_map); release after both complete
- [x] 11.3.3 In `drop_table()`, acquire `create_drop_lock` before the two DashMap removes; release after both complete
- [x] 11.3.4 Update `ArrowStore::new()` and `Default` impl to initialize the new `create_drop_lock`
- [x] 11.3.5 Run `scharnhorst_arrow_store` tests and verify create/drop tests pass

### 11.4 ArrowStore — Document Snapshot Consistency Constraints (Amendment D, AS-CA-002)

- [x] 11.4.1 Add doc-comment on `generate_snapshot()` stating that it must not be called concurrently with mutating operations when cross-table consistency is required
- [x] 11.4.2 Run `scharnhorst_arrow_store` tests (documentation-only change, no behavioral impact)

### 11.5 ArrowStore — Document truncate_before Constraint (Amendment F, AS-CA-003)

- [x] 11.5.1 Add doc-comment on `truncate_before()` stating that it must not run concurrently with `generate_snapshot()`
- [x] 11.5.2 Run `scharnhorst_arrow_store` tests (documentation-only change)

### 11.6 ArrowStore — Restructure apply_diffs Atomicity (Amendment G, AS-CA-004)

- [x] 11.6.1 Extract diff application logic from `ArrowStoreInner` into standalone functions that accept `&mut VersionedTable` + `&TypeRegistry` instead of `&mut ArrowStoreInner`
- [x] 11.6.2 Rewrite `ArrowStore::apply_diffs()`: for each diff, acquire per-table write lock ONCE via `tables.get_mut(table_name)?.write()`, then call the extracted function
- [x] 11.6.3 Ensure `apply_update`, `apply_delete`, `apply_insert`, `apply_replace` all operate on the same locked `VersionedTable` reference without releasing it mid-operation
- [x] 11.6.4 Run `scharnhorst_arrow_store` tests and verify diff application tests pass (especially `patch_rows_updates_values`)
- [x] 11.6.5 Run `scharnhorst_integration_tests` and verify determinism tests pass

### 11.7 ArrowStore — Document DashMap Iteration Best-Effort Semantics (Amendment H, AS-CA-005)

- [x] 11.7.1 Add doc-comment on `table_names()`, `snapshot_ticks()`, and `latest_snapshot()` stating they return best-effort snapshots (may be inconsistent under concurrent mutations)
- [x] 11.7.2 Run `scharnhorst_arrow_store` tests (documentation-only change)

### 11.8 Scheduler — Fix initialize() Two-Phase Pattern (Amendment E, S-CA-001)

- [x] 11.8.1 In `scharnhorst_scheduler/src/scheduler.rs`, rewrite `initialize()`: replace `compare_exchange` with `load(Acquire)` fast-path check → validation → `store(Release)` after success
- [x] 11.8.2 Ensure the `Release` ordering pairs with readers' `Acquire` in `is_initialized()`
- [x] 11.8.3 Run `scharnhorst_scheduler` tests and verify all initialization tests pass (especially `initialize_is_idempotent`, `initialize_succeeds_with_no_systems`, `initialize_fails_when_system_has_no_tables`)

## 12. Final Verification

- [x] 12.1 Run full project test suite (`cargo test`) and verify all tests pass
- [x] 12.2 Run `cargo clippy` for all crates and fix any warnings introduced by the changes
- [x] 12.3 Verify no new `unwrap()`, `expect()`, or `panic!()` calls (per AGENTS.md error handling rule)
