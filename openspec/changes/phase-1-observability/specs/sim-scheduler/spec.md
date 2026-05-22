## ADDED Requirements

### Requirement: Scheduler Phase Timing Instrumentation
The scheduler SHALL record wall-clock duration for each simulation phase (`PreTick`, `Economy`, `Diplomacy`, `Military`, `PostTick`) and emit the data as `tracing` events. Timing SHALL use a guard-based approach (`DurationGuard` with `Drop` impl) to ensure spans are closed even if a phase panics. Instrumentation SHALL be gated behind `#[cfg(feature = "metrics")]`.

#### Scenario: Phase timing span emitted for all phases
- **WHEN** the scheduler completes a full tick with metrics enabled
- **THEN** five spans are emitted in order for `PreTick`, `Economy`, `Diplomacy`, `Military`, `PostTick`
- **AND** each span contains fields `tick` and `phase`
- **AND** the `tracing` subscriber receives `duration_micros` from the span's `elapsed()`

#### Scenario: Phase timing guard closes on panic
- **WHEN** a phase's system panics during execution
- **THEN** the phase timing guard's `Drop` implementation closes the `tracing` span before the panic unwinds
- **AND** the span records the duration up to the point of panic
- **AND** the `tracing` event is still emitted (the subscriber may or may not receive it depending on panic propagation)

#### Scenario: No timing overhead without metrics feature
- **WHEN** the project is compiled without `--features metrics`
- **THEN** no scheduling instrumentation code exists in the scheduler binary
- **AND** tick execution wall-clock time is identical to a build without the instrumentation code
- **AND** no `Instant::now()` calls or `tracing` macro expansions are compiled

### Requirement: Per-System Timing Instrumentation
The scheduler SHALL record wall-clock duration for each registered system's execution within its phase and emit the data as `tracing` events. The duration SHALL be measured as time between `system.run()` start and `system.run()` completion.

#### Scenario: System timing event emitted
- **WHEN** a simulation system `TaxCollectionSystem` completes execution in the `Economy` phase with metrics enabled
- **THEN** a `tracing::debug!` event is emitted with fields `tick`, `phase: "Economy"`, `system_name: "TaxCollectionSystem"`, and `duration_micros`

#### Scenario: Multiple systems in one phase each emit timing
- **WHEN** the `Economy` phase has three systems
- **THEN** three distinct per-system timing events are emitted, one after each system completes
- **AND** each event references its system's registered name
- **AND** events are emitted in system execution order

#### Scenario: Zero-system phase produces no system events but phase span still emitted
- **WHEN** the `Diplomacy` phase has zero registered systems
- **THEN** no per-system timing events are emitted for that phase
- **AND** the phase span still records total phase duration (which will be near-zero)
- **AND** the scheduler continues to the next phase normally

#### Scenario: System timing on system panic
- **WHEN** a system panics mid-execution with metrics enabled
- **THEN** the per-system timing guard closes the `tracing::debug!` event in its `Drop` impl
- **AND** the event records `duration_micros` up to the panic point (if `Instant::elapsed()` is not affected by stack unwinding)
- **NOTE**: If the panic causes `Instant::elapsed()` to fail, the event is not emitted — this is acceptable because the panic is the primary failure; timing is secondary

### Requirement: Diff Count Tracking Per Tick
The scheduler SHALL track the total number of diffs accumulated across all systems in a tick and emit the count at tick end. The count SHALL include diffs from all systems in all phases, captured from `Journal::pending_diff_count()` after all phases complete and before `journal.commit()`.

#### Scenario: Diff count emitted at PostTick end
- **WHEN** the scheduler completes `PostTick` and `journal.commit()` succeeds with metrics enabled
- **THEN** a `tracing::info!` event is emitted with fields `tick` and `diff_count`
- **AND** the count is the total number of diffs applied in `journal.commit()`

#### Scenario: Diff count accuracy with multiple phases
- **WHEN** `TaxCollectionSystem` emits 5 diffs, `PopulationGrowthSystem` emits 3 diffs, and `MilitaryMovementSystem` emits 2 diffs during a tick
- **THEN** the emitted `diff_count` is `10` (sum across all diff-producing systems)

#### Scenario: Zero-diff tick still emits count
- **WHEN** a tick completes with zero diffs produced (e.g., idle tick where no system modifies state)
- **THEN** a `tracing::info!` event is still emitted with `diff_count: 0`

#### Scenario: Failed commit does not emit diff count
- **WHEN** `journal.commit()` fails (returns error)
- **THEN** the scheduler's diff count event is NOT emitted for that tick
- **AND** the journal itself may emit a warning event with the attempted diff count (see journal-system spec)

### Requirement: Metrics Feature Flag
The `scharnhorst_scheduler` crate SHALL define a `metrics` Cargo feature. Instrumentation code SHALL be compiled only when this feature is enabled. All `#[cfg(feature = "metrics")]` blocks are compile-time eliminated; there is no runtime branching.

#### Scenario: Feature flag controls compilation
- **WHEN** the crate is a dependency with `features = ["metrics"]`
- **THEN** instrumentation code is compiled and active — `Instant::now()` calls and `tracing` events execute
- **WHEN** the crate is a dependency without `features = ["metrics"]`
- **THEN** no instrumentation code is compiled — zero runtime cost, zero binary size impact

#### Scenario: Duration measurement uses saturating conversion
- **WHEN** a single phase execution hypothetically exceeds `u64::MAX` microseconds
- **THEN** `duration_micros` is saturated to `u64::MAX` via `saturating_as_u64()` conversion
- **AND** the `tracing` event still emits (with the saturated value) — no panic on duration overflow

### ADDED Invariants

#### Invariant: Scheduler Telemetry Non-Blocking (I-SCHED-TELEMETRY-NON-BLOCKING)
Telemetry spans and events emitted by the scheduler SHALL NOT block the simulation thread for unbounded durations. If a `tracing` subscriber's write buffer is full, events SHALL be dropped. The scheduler SHALL NOT wait for subscriber acknowledgment. This invariant extends I-TELEMETRY-NON-BLOCKING (from runtime-telemetry) to the scheduler crate.

#### Invariant: Timing Spans Close on Drop (I-SCHED-SPAN-DROP-CLOSE)
All scheduler timing spans SHALL be entered via guard types with `Drop` implementations that close the span. If a phase or system panics, the `Drop` impl SHALL close the span during stack unwinding. This prevents leaked spans from accumulating in `tracing`'s subscriber state.