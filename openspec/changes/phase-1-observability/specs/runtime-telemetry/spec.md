## ADDED Requirements

### Requirement: Zero-Cost Telemetry Gating
All telemetry instrumentation SHALL be gated behind a Cargo `metrics` feature on each instrumented crate. When disabled, no instrumentation code SHALL be compiled. When enabled, `tracing` spans and events emit to the subscriber layer, which SHALL use a non-blocking writer.

#### Scenario: Metrics feature off — zero binary impact
- **WHEN** the project is compiled without `--features metrics`
- **THEN** all `tracing` macro calls are `#[cfg]`-eliminated at compile time
- **AND** binary size and runtime performance are identical to a build with no instrumentation code

#### Scenario: Metrics feature on — subscriber handles events
- **WHEN** the project is compiled with `--features metrics`
- **THEN** all telemetry spans and events are active
- **AND** a `tracing-subscriber` with non-blocking writer can collect and export data

### Requirement: Telemetry Does Not Affect Determinism
Instrumentation data SHALL NOT be included in state hashes, save files, replay files, or any deterministic computation. Telemetry is purely push-side observational data.

#### Scenario: Same seed, same hash, with or without metrics
- **WHEN** the same simulation runs twice with identical inputs and RNG seeds, once with metrics enabled and once disabled
- **THEN** the state hash at every tick is identical between the two runs
- **AND** the replay file does not contain timing data

#### Scenario: Replay mode disables metrics
- **WHEN** the simulation enters replay mode (reconstructing from journal)
- **THEN** all telemetry events are suppressed (Feature flag off, or catch-all `tracing` subscriber teardown at replay entry)
- **AND** replay timing is measured by the replay harness, not internal scheduler spans
- **AND** no `tracing` event from the replayed simulation interferes with the replay analysis

### Requirement: Non-Blocking Tracing Subscriber
The `tracing` subscriber used in production and debug builds SHALL use a non-blocking writer (`tracing_subscriber::fmt::Layer::with_writer(std::io::sink)` or equivalent). If the subscriber's internal buffer is full, events SHALL be silently dropped rather than blocking the simulation thread.

#### Scenario: Subscriber buffer full — event dropped, simulation continues
- **WHEN** the tracing subscriber's internal ring buffer is full (e.g., high-frequency events during rapid simulation)
- **THEN** new `tracing` events are silently dropped
- **AND** the simulation thread does NOT block waiting for buffer space
- **AND** the subscriber emits a `tracing_subscriber::fmt::Layer::on_event_dropped` callback (if configured) but does not propagate to simulation code

#### Scenario: No subscriber attached — events silently dropped
- **WHEN** the `metrics` feature is enabled but no `tracing` subscriber is registered (e.g., user compiled but never called `tracing_subscriber::init()`)
- **THEN** all `tracing` events are silently dropped by the global dispatcher
- **AND** there is zero performance penalty beyond the `Instant::now()` calls for duration measurement

### Requirement: Stable Span and Field Names
All `tracing` span names and event field keys SHALL use `const` definitions. Span names SHALL be stable across code changes to support downstream tooling contracts.

#### Scenario: Field keys are documented and stable
- **WHEN** a downstream tool reads telemetry data
- **THEN** the following field keys are guaranteed stable:
  - `tick: u64`, `phase: &str`, `system_name: &str`
  - `duration_micros: u64`, `diff_count: u64`
  - `method: &str`, `table: &str`, `column: &str`
  - `cache_hits: u64`, `cache_misses: u64`, `hit_ratio: f64`
  - `commit_duration_micros: u64`, `error: &str`

#### Scenario: Span names are stable
- **WHEN** a downstream tool reads telemetry data
- **THEN** the following span names are guaranteed:
  - `"scheduler_tick"`, `"scheduler_phase"`, `"query_operation"`, `"journal_commit"`
- **AND** changing the internal representation of these operations does not change the span name

### ADDED Invariants

#### Invariant: Telemetry Non-Blocking (I-TELEMETRY-NON-BLOCKING)
Telemetry SHALL NOT block the simulation thread for unbounded durations. `tracing` subscribers SHALL use non-blocking writers. If a subscriber's buffer is full, events SHALL be dropped. Replay mode SHALL disable all telemetry to prevent subscriber overhead from affecting replay timing analysis.

#### Invariant: Telemetry Determinism (I-TELEMETRY-DETERMINISM)
The presence or absence of telemetry SHALL NOT affect state hashes, diff computation, RNG streams, or any deterministic computation. Telemetry data is excluded from save files, replay files, journal records, and hash inputs. A simulation run with `metrics` enabled produces byte-identical `CommitRecord` data to the same run with `metrics` disabled.