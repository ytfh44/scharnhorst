## Why

Phase 0 turns Scharnhorst's core determinism rules from conventions into enforceable architecture. The current specs already state the intended invariants, but public mutation APIs, mixed state lifetimes, and lifecycle boundaries still leave too much correctness dependent on caller discipline.

This change fortifies the engine before domain gameplay expands: world writes must be impossible outside the journal path, schema changes must be impossible after initialization, and non-authority state must not leak into persisted Arrow world state.

## What Changes

- **BREAKING**: Split public world access into explicit read-only and mutation-capable surfaces. Simulation consumers receive read/query capabilities and journal submission capabilities, not raw mutable `ArrowStore` access.
- **BREAKING**: Restrict `ArrowStore` mutation APIs (`create_table`, `drop_table`, `apply_diffs`, checkpoint ingestion, mutation-mode changes, type registration) to initialization or journal-owned commit contexts.
- **BREAKING**: Introduce a type-level lifecycle boundary between `Initialization` and `Simulation`. Initialization may construct schema and authority tables; simulation may only read through `query-engine` and submit effects/commands/diffs through `journal-system`.
- Add a dedicated state-tiering capability defining Authority, Derived, and Ephemeral state, including persistence, rebuild, ownership, and refresh rules.
- Require all persisted Arrow tables to be classified as Authority state and reject Derived/Ephemeral data from `ArrowStore` save/replay hashes.
- Require Derived state to be rebuilt from a query-engine snapshot after commits and invalidated by refresh signals.
- Require Ephemeral state to live outside `ArrowStore`, outside save files, and outside deterministic replay hashes.
- Harden `schema-registry` freeze semantics: table specs, relations, schema versions, type registrations, and mod fingerprints are finalized before the first tick and cannot be mutated during simulation.
- Harden `content-loader` and `save-system` lifecycle sequencing so all schema/table construction occurs before freeze, while replayed diffs after load go through the same journal-owned mutation path as normal simulation.
- Align `bevy-bridge`, `rule-ir`, and `sim-scheduler` around capability-passing: they may read through `query-engine`, submit to `journal-system`, and maintain Derived/Ephemeral state, but may not hold or call world mutation APIs.
- Replace panic-based enforcement with compile-time API boundaries and `Result`-based runtime guards. Debug assertions may document impossible states, but they are not the primary enforcement mechanism.

## Capabilities

### New Capabilities

- `state-tiering`: Defines Authority, Derived, and Ephemeral world-state tiers, including ownership, persistence, rebuild, hash participation, and invalidation rules.

### Modified Capabilities

- `arrow-store`: Restrict mutation APIs to initialization and journal-owned commit contexts; store only Authority state during simulation.
- `journal-system`: Make the journal the only simulation write capability and define the exclusive commit token/path used to mutate `ArrowStore`.
- `query-engine`: Strengthen the unified read interface so simulation consumers cannot bypass it with raw `ArrowStore`, `WorldSnapshot`, `RecordBatch`, or relation graph access.
- `sim-scheduler`: Carry the initialization-vs-simulation lifecycle boundary through scheduler startup, tick execution, refresh signals, and system capability injection.
- `schema-registry`: Make freeze irreversible for all schema-affecting state and require mutation attempts after freeze to return descriptive errors.
- `content-loader`: Limit content compilation, overlay resolution, table creation, relation construction, and mod fingerprint registration to cold-start phases before schema freeze.
- `save-system`: Persist and replay only Authority state, restore schema through the six-phase lifecycle, and replay post-checkpoint diffs through the journal-owned mutation path.
- `rule-ir`: Ensure evaluators only read through `query-engine` and submit effects through `journal-system`, never returning or applying raw store mutations directly.
- `bevy-bridge`: Treat Bevy ECS and view models as Ephemeral or Derived state; route player input to journal commands and forbid direct world writes.

## Impact

- Affected crates: `scharnhorst_arrow_store`, `scharnhorst_journal`, `scharnhorst_query`, `scharnhorst_scheduler`, `scharnhorst_schema`, `scharnhorst_content`, `scharnhorst_save`, `scharnhorst_rules`, `scharnhorst_bevy`, and integration tests.
- Public API impact: callers that currently receive `ArrowStore` or `WorldSnapshot` directly will need narrower capability types such as read-only world views, query-engine handles, journal submission handles, initialization builders, or commit-only mutation tokens.
- Test impact: add compile-fail or API-boundary tests where practical, plus integration tests proving schema freeze, journal-only writes, Derived rebuild, Ephemeral exclusion from saves, and replay determinism.
- Documentation impact: OpenSpec specs and developer-facing docs must describe lifecycle phases, state tiers, and allowed access paths using relative repository paths only.
- Runtime policy impact: invalid lifecycle access must return domain errors (`Result`) rather than panic, unwrap, or expect.
