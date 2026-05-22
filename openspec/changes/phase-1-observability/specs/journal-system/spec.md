## ADDED Requirements

### Requirement: Commit Latency Instrumentation
The journal SHALL record the wall-clock duration of each `commit()` call and emit the data as `tracing` events. Duration SHALL include: `ingest_snapshot()` for all modified tables, `ArrowStore::apply_diffs()`, `QueryEngine::update_cache()`, and `WorldSnapshot` generation. Instrumentation SHALL be gated behind `#[cfg(feature = "metrics")]`.

#### Scenario: Successful commit emits latency span
- **WHEN** `journal.commit()` completes successfully with metrics enabled
- **THEN** a `tracing::info_span!("journal_commit")` is emitted with fields `tick`, `diff_count`, and `commit_duration_micros`
- **AND** the span covers the entire commit operation from `commit()` entry to successful return

#### Scenario: Failed commit emits warning with latency
- **WHEN** `journal.commit()` fails partway through (e.g., `apply_diffs` error, snapshot generation error) with metrics enabled
- **THEN** a `tracing::warn!` event is emitted with fields `tick`, `diff_count`, `commit_duration_micros`, and `error`
- **AND** the duration is measured up to the failure point (the span is closed before error propagation)
- **AND** the journal phase resets to `Open` and pending diffs are restored per existing recovery invariants
- **AND** the journal is ready to accept new diffs for the next commit attempt

### Requirement: Commit Diff Batch Size
The journal SHALL record the number of diffs in each `commit()` call and emit the count as a `tracing` event.

#### Scenario: Batch size emitted on commit
- **WHEN** `journal.commit()` applies `N` diffs with metrics enabled
- **THEN** a `tracing::debug!` event is emitted with fields `tick` and `diff_count: N`

#### Scenario: Zero-diff commit still emits batch size
- **WHEN** `journal.commit()` is called with zero pending diffs
- **THEN** a `tracing::debug!` event IS emitted with `diff_count: 0`
- **AND** the commit proceeds normally (generates an identity snapshot — same state hash as previous tick, and updates `query-engine` generation counter)

### Requirement: Metrics Feature Flag
The `scharnhorst_journal` crate SHALL define a `metrics` Cargo feature. Instrumentation code SHALL be compiled only when this feature is enabled. All `#[cfg(feature = "metrics")]` blocks are compile-time eliminated. There is no runtime cost when the feature is disabled.

#### Scenario: Feature flag controls compilation
- **WHEN** the crate is a dependency with `features = ["metrics"]`
- **THEN** `Instant::now()` calls and `tracing` events are compiled and execute during commit
- **WHEN** the crate is a dependency without `features = ["metrics"]`
- **THEN** no instrumentation code is compiled — zero runtime cost, zero binary size impact

### Requirement: Telemetry Excluded from Journal History
Commit latency and diff batch size data SHALL NOT be stored in `CommitRecord`, `SaveJournal`, or any persisted journal structure. Telemetry is push-only to `tracing` subscribers and SHALL NOT affect journal history, replay, or state hashes.

#### Scenario: CommitRecord unchanged
- **WHEN** a commit completes with metrics enabled
- **THEN** the `CommitRecord` appended to journal history contains only `tick`, `diffs`, and `commands` — no timing or count metadata
- **AND** replaying the `CommitRecord` produces identical state hashes regardless of whether metrics were enabled during the original run

#### Scenario: Replay from journal with metrics produces same hashes
- **WHEN** a simulation is run with metrics enabled, producing a journal file
- **AND** the simulation is replayed from that journal file with metrics enabled
- **THEN** the state hash at every tick is identical between original and replay
- **AND** the per-tick `CommitRecord` data is byte-identical
- **AND** only the `tracing` events differ (they are push-side, not recorded in journal)

### ADDED Invariants

#### Invariant: Journal Telemetry Non-Blocking (I-JRN-TELEMETRY-NON-BLOCKING)
Journal commit telemetry SHALL NOT block the commit operation for unbounded durations. If a `tracing` subscriber's write buffer is full, events SHALL be dropped. The journal SHALL NOT wait for subscriber acknowledgment before returning from `commit()`. This invariant extends I-TELEMETRY-NON-BLOCKING (from runtime-telemetry) to the journal crate.