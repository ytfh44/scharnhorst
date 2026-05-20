## Context

Scharnhorst is built around a deterministic ledger model: simulation reads current world state, emits commands/effects/diffs, and commits those diffs atomically at tick boundaries. The current specs already describe this model, but several enforcement points are still structural conventions rather than API guarantees:

- `ArrowStore` exposes mutation-oriented methods as ordinary public APIs.
- Some consumers can still be wired with raw store or snapshot access instead of narrow read/query handles.
- Schema freeze is specified, but the lifecycle boundary is not represented strongly enough in the access model.
- Authority, Derived, and Ephemeral state are described in roadmap prose but not yet represented as a capability with testable requirements.
- Runtime guards are useful diagnostics, but they cannot be the primary way to protect determinism.

Phase 0 is therefore a boundary-hardening change. It should reduce the number of ways future gameplay/domain modules can accidentally bypass the ledger architecture.

The main stakeholders are:

- Simulation system authors, who need a small API surface: read state, emit diffs/effects, no direct storage writes.
- Content/mod authors, whose schema/content compilation must finish before simulation.
- Save/replay/multiplayer code, which depends on Authority state and journal history being complete proof artifacts.
- Bevy/UI code, which needs fast synchronized views but must not become a state authority.

## Goals / Non-Goals

**Goals:**

- Make journal-only writes enforceable by API shape, not just comments.
- Make schema freeze irreversible after cold-start initialization.
- Define the state-tiering contract for Authority, Derived, and Ephemeral state.
- Ensure only Authority state participates in persistence, replay, and state hashes.
- Ensure Derived state rebuilds from the post-commit query snapshot and never becomes an untracked mutation source.
- Ensure Ephemeral state stays outside `ArrowStore`, save files, and replay hashes.
- Pass narrow capabilities into systems: query/read handles for reads, journal submission handles for writes, initialization handles for setup.
- Prefer compile-time unavailability of forbidden operations; use `Result` errors for recoverable lifecycle violations.
- Keep the existing crate split and avoid introducing broad new dependencies.

**Non-Goals:**

- Implement runtime mod hot-reload.
- Implement mid-simulation schema migration.
- Replace Arrow as the Authority storage backend.
- Replace DataFusion or the existing query-engine typed access model.
- Build gameplay domain modules.
- Implement Phase 1 observability panels or SQL inspector UI beyond preserving their future access path.
- Make every architectural misuse compile-fail immediately if Rust visibility cannot express it cleanly across crates; where needed, use narrow handles plus runtime `Result` guards as an intermediate step.

## Decisions

### Decision 1: Introduce Capability Handles Instead of Passing Raw Store Access

Simulation-facing code should receive capability handles, not raw `ArrowStore`.

Proposed capability families:

- Initialization capability: can register schema, create authority tables, load content, register storage types, and build initial snapshots before simulation starts.
- Query capability: can read through `query-engine` typed APIs and SQL SELECT/debug reads where allowed.
- Journal submission capability: can submit commands/effects/diffs but cannot apply them.
- Commit capability: owned by `journal-system` and used only inside the tick commit path to apply diffs to `ArrowStore`.

Rationale: Rust visibility cannot make a method public to only one sibling crate unless the crate graph changes or the API moves behind an owned type. Capability handles express the authorization boundary without relying on caller discipline.

Alternative considered: keep public methods and add manual checks. Rejected as the primary design because it preserves the broad API surface and lets future code compile while violating the architecture.

Alternative considered: move `ArrowStore` entirely inside `journal-system`. Rejected for Phase 0 because it would create a large dependency inversion and disrupt save/content/query integration more than necessary.

### Decision 2: Make `ArrowStore` Publicly Read-Oriented During Simulation

`ArrowStore` should expose stable read/snapshot support for infrastructure that owns the storage layer, but world mutation methods should move behind initialization or commit-only access.

Mutation operations include:

- Table creation and deletion.
- Type registration that affects table decoding.
- Mutation mode changes.
- Diff application.
- Checkpoint ingestion or reconstruction writes.
- Any direct batch replacement or authority data insertion.

During simulation, ordinary consumers should not be able to call these operations. They should read through `query-engine` and write by submitting to `journal-system`.

Rationale: this preserves the Arrow storage implementation while preventing storage from becoming a second write API.

Alternative considered: use debug-only assertions on all mutation methods. Rejected because release builds and tests still compile forbidden callers, and the AGENTS.md error handling policy forbids panic-centered enforcement.

### Decision 3: Represent Lifecycle as Types and Runtime State

The engine should distinguish at least:

- `Initialization`: schema/content/storage construction is allowed.
- `Simulation`: schema is frozen, authority tables exist, and only journal commits mutate authority state.

The implementation can use a combination of:

- Distinct handle types, such as `InitializationWorld`, `SimulationWorld`, `WorldView`, and `CommitWorld`.
- Internal lifecycle flags that return errors if a stale or forged path attempts mutation after freeze.
- Builder/finalize methods that consume initialization handles and return simulation handles.

Rationale: compile-time handles prevent accidental use in normal code paths, while runtime lifecycle checks protect against integration mistakes and deserialization/reconstruction edge cases.

Alternative considered: a single `SimPhase` enum passed to every method. This is useful for diagnostics but insufficient alone because callers can still call the wrong method and only fail at runtime.

### Decision 4: Make State Tiering a First-Class Spec

Authority, Derived, and Ephemeral state need their own capability because the rules cut across storage, query, save, scheduler, rules, and Bevy.

Tier definitions:

- Authority: persisted Arrow-backed world facts at tick boundaries. Included in saves, replay, and hashes.
- Derived: deterministic or cache-like structures rebuildable from Authority state and current content/schema. Not persisted as proof data.
- Ephemeral: frame-local, UI-local, animation, input staging, temporary planning, or other non-authority runtime state. Not persisted and not replay-authoritative.

Rationale: without a tier contract, later domain modules may put fast-changing frame or cache data into Arrow tables just because Arrow is globally available.

Alternative considered: document tiers only in `README.md` or `ROADMAP.md`. Rejected because implementation and tests need normative requirements.

### Decision 5: Use Refresh Signals to Rebuild and Invalidate Non-Authority State

After `journal.commit()` publishes a new query-engine snapshot, the scheduler broadcasts `REFRESH_SIGNAL`. Phase 0 extends this signal as the boundary where:

- Query consumers discard stale read caches.
- Derived state rebuilds or marks itself dirty.
- Rule prefetch caches clear.
- Bevy view state refreshes from the query-engine snapshot.
- Ephemeral state that is tick-scoped is discarded.

Rationale: the refresh signal is already the tick boundary synchronization mechanism. Reusing it keeps Derived/Ephemeral invalidation aligned with authoritative commits.

Alternative considered: let each subsystem decide its own cache invalidation timing. Rejected because it creates stale-read risks and makes replay behavior harder to reason about.

### Decision 6: Schema Freeze Covers All Schema-Affecting Data

Freeze should cover:

- `TableSpec` registration.
- Column changes.
- Relation graph mutation.
- Schema version mutation.
- Mod fingerprint registration.
- Storage type registrations that affect authority table decoding.

After freeze, mutation attempts return descriptive errors. The registry cannot be unfrozen in the same runtime.

Rationale: schema changes during simulation would need journaled schema diffs, replay semantics, and migration semantics. That is explicitly out of Phase 0.

Alternative considered: allow additive table registration during simulation for debug tools. Rejected because debug convenience would weaken the production contract and multiplayer determinism.

### Decision 7: Save/Load Replays Through the Same Authority Mutation Gate

Cold-start loading may construct Authority state through initialization capabilities. Once the schema is frozen and replay begins, post-checkpoint diffs should be applied through the same journal-owned commit path used during normal simulation.

Rationale: replay and live simulation should share the same mutation semantics. Divergent load-only mutation paths are common sources of save/load drift.

Alternative considered: let save-system call `ArrowStore::apply_diffs` directly during replay. Rejected because it creates a privileged second write path outside the journal.

### Decision 8: Rule Effects Submit, They Do Not Apply

The rule evaluator may compute effects and submit them to the journal-facing capability. It must not apply effects to storage, return a batch for arbitrary application, or access raw `ArrowStore` mutation APIs.

Rationale: rules are a major source of future content complexity; their output needs the same auditing and replay guarantees as system-generated diffs.

Alternative considered: keep rule evaluation pure and let callers convert returned effects into diffs. Acceptable only if the caller is the journal/scheduler path and the API does not expose a direct store-apply shortcut. The stronger design is to give rules an explicit submission capability.

### Decision 9: Bevy Owns View/Frame State, Not Authority State

Bevy ECS components, view models, hover/selection state, animation state, and input buffers are Ephemeral or Derived. The Bevy bridge can:

- Buffer player commands.
- Read synchronized state through `query-engine`.
- Materialize and dematerialize entities.
- Maintain view-local caches.

It cannot:

- Mutate Authority state directly.
- Create or drop Authority tables.
- Submit AI/internal simulation commands through player input buffers.

Rationale: Bevy must stay a presentation/input layer. Making it a storage writer would undermine deterministic replay and multiplayer authority.

### Decision 10: Verification Must Include Boundary Tests

The implementation should add tests for:

- Direct mutation methods unavailable or inaccessible to simulation-facing crates.
- Schema mutation rejected after freeze.
- Scheduler systems receive query and journal capabilities, not raw store handles.
- Rule effects route through journal submission.
- Bevy input becomes commands and does not mutate store state.
- Derived state rebuilds after refresh and is excluded from snapshots.
- Ephemeral state is excluded from saves and replay hashes.
- Save/load reconstruction produces the same hash as normal journal replay.

Where compile-fail testing is practical, use it for API boundaries. Where not practical, use integration tests that prove unauthorized mutation attempts return errors.

## Risks / Trade-offs

- Public API churn -> Mitigation: introduce compatibility adapters only for initialization/test harnesses, then remove raw simulation access in a controlled task sequence.
- Rust crate visibility limitations -> Mitigation: use capability types and private fields instead of trying to make methods visible to exactly one sibling crate.
- Test harness disruption -> Mitigation: provide explicit test builders for initialization and commit scenarios rather than reusing production simulation handles.
- Save/load path complexity -> Mitigation: split cold-start Authority construction from post-checkpoint journal replay and test both paths.
- Derived state rebuild cost -> Mitigation: allow lazy invalidation for expensive derived views, but require the dirty/rebuild semantics to be tied to refresh signals.
- Debug tooling friction -> Mitigation: keep debug writes available only through `DebugWriteJournal`, with SQL write statements translated to journal diffs and committed at tick boundaries.
- Incomplete compile-time enforcement in the first implementation pass -> Mitigation: document all remaining runtime-guarded boundaries in tasks and specs, then tighten them iteratively.

## Migration Plan

1. Add the state-tiering types/spec contract without changing behavior.
2. Introduce initialization and commit capability handles around existing `ArrowStore` mutation methods.
3. Update content-loading and save reconstruction to use initialization capabilities.
4. Update journal commit to use the commit-only mutation capability.
5. Update scheduler, rules, and Bevy bridge APIs to receive query/journal capabilities instead of raw store access.
6. Harden schema freeze and return errors for post-freeze mutation attempts.
7. Classify existing Authority/Derived/Ephemeral data and move non-Authority state out of `ArrowStore`.
8. Add boundary and replay tests.
9. Remove or restrict deprecated broad mutation APIs after all internal callers migrate.

Rollback strategy: because this is a source-level API hardening change, rollback is a normal code rollback of the change set. Data format rollback is not expected if Authority snapshot schemas are unchanged. If schema metadata is extended to include tier classifications, loaders must either default missing tier metadata to Authority for older snapshots or reject unknown/malformed tier metadata with a descriptive error.

## Open Questions

- Should tier classification live in `TableSpec`, a separate registry, or both? Preferred direction: `TableSpec` carries Authority table intent while a separate Derived/Ephemeral registry covers non-Arrow runtime state.
- Should compile-fail tests use `trybuild`, or should the project avoid a new dev dependency and rely on crate-boundary integration tests? Preferred direction: use integration tests first; add `trybuild` only if the boundary cannot be proven otherwise.
- Should storage type registration freeze with schema freeze, or can it stay initialization-only but outside `schema-registry`? Preferred direction: freeze it with the world initialization boundary because it affects Arrow decoding.
- Should Derived state rebuild eagerly on every refresh or lazily on first read after dirty marking? Preferred direction: allow both, but require deterministic rebuild input and explicit refresh invalidation.
