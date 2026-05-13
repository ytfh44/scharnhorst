# Query Engine — Lock-Free Optimization Delta

## Overview

This spec describes the behavioral implications of Phase 1 (atomic replacement of `latest_tick: Arc<RwLock<Option<Tick>>>` with `Arc<AtomicU64>` using a sentinel value). No other QueryEngine fields are modified.

**Baseline**: [openspec/specs/query-engine/spec.md](openspec/specs/query-engine/spec.md)

---

## QE-001: Schema Registry Access

### Baseline Requirement

The query engine owns the `SchemaRegistry` behind an `RwLock`, accessible via `schema_registry()` (read) and `schema_registry_mut()` (write).

### Behavioral Impact

**None.** The `schema_registry: Arc<RwLock<SchemaRegistry>>` at [engine.rs:L28](scharnhorst_query/src/engine.rs#L28) is unchanged. Schema operations remain behind the same RwLock.

---

## QE-002: Unified Read Interface

### Baseline Requirement

`read(request)` returns a `ReadResponse` with cached `TableReadView`s for requested tables. Results are served from `view_cache` populated by snapshot ingestion.

### Behavioral Impact

**None.** The `view_cache: Arc<RwLock<HashMap<String, TableReadView>>>` at [engine.rs:L35](scharnhorst_query/src/engine.rs#L35) is unchanged. All read paths (`read()` at [engine.rs:L111-L126](scharnhorst_query/src/engine.rs#L111-L126), `column_view()` at [engine.rs:L206-L222](scharnhorst_query/src/engine.rs#L206-L222), `row_cursor()` at [engine.rs:L224-L233](scharnhorst_query/src/engine.rs#L224-L233)) continue to acquire read locks on `view_cache`. These are unaffected.

---

## QE-003: Snapshot Ingestion

### Baseline Requirement

`ingest_snapshot(tick, table_name, batches, position_map)` creates a `TableReadView` and inserts it into `view_cache`, while recording `tick` as the latest ingested tick.

### Behavioral Impact — Atomic latest_tick

**Current code** at [engine.rs:L138-L161](scharnhorst_query/src/engine.rs#L138-L161):
```rust
pub fn ingest_snapshot(&self, tick: Tick, table_name: &str, ...) -> QueryResult<()> {
    let schema = self.table_schema(table_name)?;
    let view = TableReadView::new(table_name, tick, batches, Arc::new(schema), Some(position_map));

    let mut cache = self.view_cache.write()...?;
    cache.insert(table_name.to_owned(), view);    // (A)

    let mut latest = self.latest_tick.write()...?; // (B) - write lock
    *latest = Some(tick);                          // (C)
    Ok(())
}
```
Lines (A)-(C) write to `view_cache` and `latest_tick` under two separate write locks. After the change, `latest_tick` is `AtomicU64`:
```rust
    self.latest_tick.store(tick.as_u64(), Ordering::Release); // (B') - atomic store
```
Line (B') is a lock-free store. The write lock on `view_cache` at line (A) is still held, providing ordering: the view is inserted into cache before the tick is advanced.

**Sentinel value**: `Option<Tick>` is encoded as `u64`. `None` is represented by `u64::MAX`. `Tick::MAX = Tick(u64::MAX - 1)` reserves the sentinel at the type level. `Tick::next()` uses `saturating_add`, so `Tick::MAX.next()` saturates to `Tick(u64::MAX)` — reaching the sentinel requires 2^64-1 increments, a physically unreachable bound. When `latest_tick` is read at [engine.rs:L176-L182](scharnhorst_query/src/engine.rs#L176-L182):
```rust
pub fn latest_tick(&self) -> QueryResult<Option<Tick>> {
    let raw = self.latest_tick.load(Ordering::Acquire);
    Ok(if raw == u64::MAX { None } else { Some(Tick(raw)) })
}
```

**Ordering pair**: `ingest_snapshot` uses `Release` on the store; `latest_tick()` uses `Acquire` on the load. This forms a happens-before edge: if `latest_tick()` observes tick T, all view_cache writes up to and including T's insertion are visible.

**Tick space reduction**: `u64::MAX` is reserved as the None sentinel. This reduces the usable tick range from `[0, 2^64-1]` to `[0, 2^64-2]`. At 1 billion ticks/second (impossibly fast), this is ~585 years of ticks. The reduction is harmless.

**Evidence of read sites**: `latest_tick()` is read in `lookup_row()` at [engine.rs:L247-L248](scharnhorst_query/src/engine.rs#L247-L248):
```rust
let tick = self.latest_tick()?
    .ok_or_else(|| QueryError::InvalidQuery("no tick available".to_owned()))?;
```
This call site observes the tick to validate that data is available before reading. The atomic load provides a strictly more recent value than the old RwLock read, since the Mutex could be held while reads proceed (write-priority RwLocks may starve readers).

---

## QE-004: World Snapshot Storage

### Baseline Requirement

`store_world_snapshot(snapshot)` stores the latest `WorldSnapshot` produced during `journal.commit()`. `snapshot()` retrieves it for consumers like the bevy-bridge.

### Behavioral Impact

**None.** `latest_snapshot: Arc<RwLock<Option<Arc<WorldSnapshot>>>>` at [engine.rs:L38](scharnhorst_query/src/engine.rs#L38) is unchanged. `store_world_snapshot()` at [engine.rs:L175-L182](scharnhorst_query/src/engine.rs#L175-L182) and `snapshot()` at [engine.rs:L194-L200](scharnhorst_query/src/engine.rs#L194-L200) continue to use RwLock guards.

---

## QE-005: Debug Write Journal

### Baseline Requirement

`DebugWriteJournal` records all write operations in a ring buffer for debugging purposes. Only compiled in debug builds.

### Behavioral Impact

**None.** `debug_journal: Arc<DebugWriteJournal>` at [engine.rs:L31](scharnhorst_query/src/engine.rs#L31) is unchanged. The internal `Mutex<JournalInner>` is preserved because the journal is a minor overhead only present in debug builds ([debug_write.rs:L36-L38](scharnhorst_query/src/debug_write.rs#L36-L38)).

---

## QE-006: Typed Columnar Access

### Baseline Requirement

`column_view`, `row_cursor`, `batch_reader` provide typed access to Arrow data in the view cache.

### Behavioral Impact

**None.** These methods at [engine.rs:L206-L244](scharnhorst_query/src/engine.rs#L206-L244) access only `view_cache` (unchanged). They do not interact with `latest_tick`.

---

## QE-007: SQL Interface

### Baseline Requirement

`SqlExecutionContext` manages registered SQL tables. The implementation uses an `Arc<Mutex<HashSet<String>>>`.

### Behavioral Impact

**None.** The `sql_context: Arc<RwLock<SqlExecutionContext>>` at [engine.rs:L29](scharnhorst_query/src/engine.rs#L29) is unchanged. This spec does not currently address lock-free optimization of the SQL interface, which is a separate concern.

---

## New Invariants Introduced

1. **I-QE-TICK-SENTINEL**: `u64::MAX` is reserved as the None sentinel for `latest_tick`. No valid tick may have this value. `Tick::MAX = Tick(u64::MAX - 1)` encodes this at the type level. `Tick::next()` uses `saturating_add(1)`, so calling `Tick::MAX.next()` saturates to `Tick(u64::MAX)` — the sentinel. In normal operation this is physically unreachable (~585 billion years at 1 billion ticks/sec). A `debug_assert!` in `ingest_snapshot` guards against accidental sentinel ingestion from test code, deserialization, or replay.

2. **I-QE-TICK-ORDERING**: `latest_tick.store(Release)` in `ingest_snapshot` is paired with `latest_tick.load(Acquire)` in `latest_tick()`. This ordering pair ensures that **when a reader calls `latest_tick()` (Acquire) BEFORE reading `view_cache`**, the `view_cache` insertion performed *in the `ingest_snapshot` call that stored T* is visible to that reader. The precondition is critical: a reader that reads `view_cache` first and `latest_tick()` second may observe stale cache data associated with a newer tick (see [QE-CA-002](#qe-ca-002-ingest_snapshot-ordering--fragile-stated-invariant-critical)). All current reader sites in `lookup_row()`, `column_view()`, `row_cursor()`, and `batch_reader()` maintain the correct access order (tick first, then cache). During multi-table snapshot ingestion (a sequence of `ingest_snapshot` calls within the journal commit), each call stores tick T independently — intermediate stores expose T with a partial `view_cache` (only tables ingested so far). Since all ingestion and the final `store_world_snapshot` happen within a single sequential `atomic_commit()` phase, no concurrent reader observes the intermediate states.

## Invariants Preserved

- All `view_cache` operations remain transactional (single RwLock protects the entire HashMap).
- `latest_snapshot` operations remain transactional (single RwLock protects the `Option<Arc<...>>`).
- `DebugWriteJournal` operations remain serialized (single Mutex per journal).
- Public API signatures unchanged (`latest_tick()` still returns `QueryResult<Option<Tick>>`).
- Error propagation: the atomic load in `latest_tick()` cannot fail, but the return type `QueryResult<Option<Tick>>` is preserved with an `Ok()` wrapper.

---

## Critical Analysis Amendments

This section documents issues discovered during deep review of the QueryEngine atomic replacement design.

---

### QE-CA-001: Tick Sentinel `u64::MAX` — Silent Data Corruption (CRITICAL)

**Location**: QE-003 (Snapshot Ingestion), I-QE-TICK-SENTINEL

**Problem**: `u64::MAX` is reserved as the None sentinel for `latest_tick`. `Tick` is a public newtype over `u64` with `pub u64` field access. Any code path — test code, checkpoint deserialization, deterministic replay, or future extensions — can construct `Tick(u64::MAX)`. If passed to `ingest_snapshot`, the atomic store of `u64::MAX` is indistinguishable from `None`. The result: `latest_tick()` returns `None`, and `lookup_row()` at `engine.rs:L262-L264` returns `Err("no tick available")` despite data being present.

Additionally, `Tick::next()` uses `saturating_add(1)`, so `Tick::MAX = Tick(u64::MAX - 1)` followed by `.next()` produces `Tick(u64::MAX)` — exactly the sentinel. While unreachable in normal operation, this path is formally possible.

```rust
pub struct Tick(pub u64);
```

Any code path — test code, checkpoint deserialization, deterministic replay, or future extensions — can construct `Tick(u64::MAX)`. If passed to `ingest_snapshot`, the atomic store of `u64::MAX` is indistinguishable from `None`. The result: `latest_tick()` returns `None`, and `lookup_row()` at `engine.rs:L246-L249` returns `Err("no tick available")` despite data being present.

**Impact**: Silent logical corruption — data exists but is invisible.

**Recommended Correction**:

1. Add a public const `Tick::MAX = Tick(u64::MAX - 1)` to reserve the sentinel value at the type level.
2. In `ingest_snapshot`, add a debug-only assertion:
   ```rust
   debug_assert!(tick.as_u64() != u64::MAX, "Tick sentinel collision");
   ```
3. Document the sentinel invariant on `latest_tick`:
   ```
   /// INVARIANT: u64::MAX is reserved as the None sentinel.
   /// Tick::as_u64() must never equal u64::MAX.
   ```

---

### QE-CA-002: `ingest_snapshot` Ordering — Fragile Stated Invariant (CRITICAL)

**Location**: QE-003 (Snapshot Ingestion), I-QE-TICK-ORDERING

**Problem**: I-QE-TICK-ORDERING claims:
> "Any reader observing tick T also sees all view_cache insertions performed prior to T's store."

This is achieved via `store(Release)` in `ingest_snapshot` paired with `load(Acquire)` in `latest_tick()`. However, the invariant **only holds if the reader loads `latest_tick` BEFORE reading `view_cache`**. A reader that reads `view_cache` first, then `latest_tick`, can observe:

1. Reader: acquires `view_cache.read()` → sees old data
2. Writer: completes both `cache.insert()` and `latest_tick.store(Release)`
3. Reader: `latest_tick.load(Acquire)` → sees new tick T
4. Reader: uses old cache data under the belief it corresponds to tick T

**Impact**: A future code path that reads cache before tick will silently attribute stale data to a newer tick.

**Recommended Correction**: Document the required access order on both methods:

On `ingest_snapshot`:
```
ORDERING INVARIANT: Readers MUST load `latest_tick()` BEFORE
acquiring the `view_cache` read lock. Violating this order may
cause stale cache data to be attributed to a newer tick.
```

Verify `lookup_row()` and all other reader sites maintain this order.