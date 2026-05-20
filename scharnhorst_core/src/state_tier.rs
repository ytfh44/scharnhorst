use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{CoreError, CoreResult};
use crate::id::Tick;

/// The state-tiering model for the Scharnhorst engine.
///
/// Every table in the engine belongs to exactly one tier, which determines
/// how its data is persisted, rebuilt, and propagated across ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StateTier {
    /// The canonical source of truth. Authority tables are the only tables
    /// written directly by simulation systems; all other tiers derive from them.
    /// Persisted across ticks and cannot be rebuilt from other tables.
    Authority,

    /// Derived tables are computed from authority data at the end of each tick.
    /// They are read-only during simulation and are rebuilt atomically after
    /// all authority writes have been applied. Not directly persisted; can be
    /// regenerated from authority data on replay.
    Derived,

    /// Ephemeral tables hold transient scratch data that exists only for the
    /// duration of a single tick. They are neither persisted nor rebuilt across
    /// ticks and are discarded at tick end.
    Ephemeral,
}

/// Metadata describing a registered table in the tier registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierMetadata {
    /// The tier classification for this table.
    pub tier: StateTier,
    /// The name of the subsystem that owns this table.
    pub owner_subsystem: String,
    /// A human-readable description of the table's purpose.
    pub description: String,
}

/// A registry mapping table names to their tier metadata.
///
/// `TierRegistry` is the single point of truth for which tier each table
/// belongs to. All engine subsystems consult the registry to determine
/// correct handling of reads, writes, and rebuilds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierRegistry {
    entries: HashMap<String, TierMetadata>,
}

impl TierRegistry {
    /// Creates an empty tier registry.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers a table with its tier, owning subsystem, and description.
    ///
    /// Returns `Ok(())` on first registration, or
    /// `Err(CoreError::DuplicateRegistration(...))` if a table with the
    /// same name is already registered.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        tier: StateTier,
        owner: impl Into<String>,
        desc: impl Into<String>,
    ) -> CoreResult<()> {
        let name = name.into();
        if let Some(existing) = self.entries.get(&name) {
            return Err(CoreError::DuplicateRegistration(format!(
                "table '{}' already registered as {:?}",
                name, existing.tier
            )));
        }
        self.entries.insert(
            name,
            TierMetadata {
                tier,
                owner_subsystem: owner.into(),
                description: desc.into(),
            },
        );
        Ok(())
    }

    /// Returns the `TierMetadata` for a table, if registered.
    pub fn get(&self, name: &str) -> Option<&TierMetadata> {
        self.entries.get(name)
    }

    /// Returns true if the named table is registered as `Authority` tier.
    pub fn is_authority(&self, name: &str) -> bool {
        self.entries
            .get(name)
            .is_some_and(|meta| meta.tier == StateTier::Authority)
    }

    /// Returns an iterator over the names of all `Authority`-tier tables.
    pub fn authority_tables(&self) -> impl Iterator<Item = &str> + '_ {
        self.entries.iter().filter_map(|(name, meta)| {
            if meta.tier == StateTier::Authority {
                Some(name.as_str())
            } else {
                None
            }
        })
    }

    /// Returns the total number of registered tables.
    pub fn table_count(&self) -> usize {
        self.entries.len()
    }
}

/// Trait for tables that are computed from authority data.
///
/// Implementors track a dirty flag and provide a rebuild method that
/// recomputes the entire table from the current authority state.
pub trait DerivedState {
    /// Marks this derived table as needing a rebuild.
    ///
    /// Called whenever any authority table this derivation depends on
    /// has been written to during the current tick.
    fn mark_dirty(&mut self);

    /// Returns true if this derived table is dirty and needs a rebuild.
    fn is_dirty(&self) -> bool;

    /// Rebuilds this derived table from the current authority state.
    ///
    /// After a successful rebuild, the dirty flag MUST be cleared.
    /// The `tick` parameter provides the current simulation tick for
    /// timestamping and dependency tracking.
    fn rebuild_from_authority(&mut self, tick: Tick) -> CoreResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------------
    // TierMetadata
    // ---------------------------------------------------------------------------

    #[test]
    fn tier_metadata_construction() {
        let meta = TierMetadata {
            tier: StateTier::Authority,
            owner_subsystem: "physics".to_string(),
            description: "rigid body transforms".to_string(),
        };
        assert_eq!(meta.tier, StateTier::Authority);
        assert_eq!(meta.owner_subsystem, "physics");
        assert_eq!(meta.description, "rigid body transforms");
    }

    // ---------------------------------------------------------------------------
    // StateTier derives
    // ---------------------------------------------------------------------------

    #[test]
    fn state_tier_debug() {
        assert_eq!(format!("{:?}", StateTier::Authority), "Authority");
        assert_eq!(format!("{:?}", StateTier::Derived), "Derived");
        assert_eq!(format!("{:?}", StateTier::Ephemeral), "Ephemeral");
    }

    #[test]
    fn state_tier_clone_copy() {
        let a = StateTier::Authority;
        let b = a;
        assert_eq!(a, b);
        let c = a;
        assert_eq!(a, c);
    }

    #[test]
    fn state_tier_eq() {
        assert_eq!(StateTier::Authority, StateTier::Authority);
        assert_ne!(StateTier::Authority, StateTier::Derived);
        assert_ne!(StateTier::Derived, StateTier::Ephemeral);
        assert_ne!(StateTier::Ephemeral, StateTier::Authority);
    }

    #[test]
    fn state_tier_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = StateTier::Authority;
        let b = StateTier::Authority;
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());

        let c = StateTier::Derived;
        let mut hc = DefaultHasher::new();
        c.hash(&mut hc);
        assert_ne!(ha.finish(), hc.finish());
    }

    #[test]
    fn state_tier_serialize_roundtrip() {
        for tier in [
            StateTier::Authority,
            StateTier::Derived,
            StateTier::Ephemeral,
        ] {
            let json = serde_json::to_string(&tier).unwrap();
            let back: StateTier = serde_json::from_str(&json).unwrap();
            assert_eq!(tier, back);
        }
    }

    // ---------------------------------------------------------------------------
    // TierRegistry: new / table_count
    // ---------------------------------------------------------------------------

    #[test]
    fn registry_new_is_empty() {
        let reg = TierRegistry::new();
        assert_eq!(reg.table_count(), 0);
    }

    #[test]
    fn registry_default_is_empty() {
        let reg = TierRegistry::default();
        assert_eq!(reg.table_count(), 0);
    }

    // ---------------------------------------------------------------------------
    // TierRegistry: register + get
    // ---------------------------------------------------------------------------

    #[test]
    fn register_and_get() {
        let mut reg = TierRegistry::new();
        let _ = reg.register(
            "positions",
            StateTier::Authority,
            "physics",
            "world-space positions",
        );
        assert_eq!(reg.table_count(), 1);

        let meta = reg.get("positions").unwrap();
        assert_eq!(meta.tier, StateTier::Authority);
        assert_eq!(meta.owner_subsystem, "physics");
        assert_eq!(meta.description, "world-space positions");
    }

    #[test]
    fn get_missing_returns_none() {
        let reg = TierRegistry::new();
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn register_duplicate_returns_error() {
        let mut reg = TierRegistry::new();
        let result = reg.register("table_a", StateTier::Derived, "sys1", "original desc");
        assert!(result.is_ok());
        let result = reg.register("table_a", StateTier::Authority, "sys2", "overwritten desc");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("already registered"));
        assert_eq!(reg.table_count(), 1);

        let meta = reg.get("table_a").unwrap();
        assert_eq!(meta.tier, StateTier::Derived);
        assert_eq!(meta.owner_subsystem, "sys1");
        assert_eq!(meta.description, "original desc");
    }

    #[test]
    fn register_multiple_independent() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("a", StateTier::Authority, "s1", "d1");
        let _ = reg.register("b", StateTier::Derived, "s2", "d2");
        let _ = reg.register("c", StateTier::Ephemeral, "s3", "d3");
        assert_eq!(reg.table_count(), 3);
        assert_eq!(reg.get("a").unwrap().tier, StateTier::Authority);
        assert_eq!(reg.get("b").unwrap().tier, StateTier::Derived);
        assert_eq!(reg.get("c").unwrap().tier, StateTier::Ephemeral);
    }

    // ---------------------------------------------------------------------------
    // TierRegistry: is_authority
    // ---------------------------------------------------------------------------

    #[test]
    fn is_authority_true() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("auth1", StateTier::Authority, "sys", "desc");
        assert!(reg.is_authority("auth1"));
    }

    #[test]
    fn is_authority_false_for_derived() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("der1", StateTier::Derived, "sys", "desc");
        assert!(!reg.is_authority("der1"));
    }

    #[test]
    fn is_authority_false_for_ephemeral() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("eph1", StateTier::Ephemeral, "sys", "desc");
        assert!(!reg.is_authority("eph1"));
    }

    #[test]
    fn is_authority_false_for_missing() {
        let reg = TierRegistry::new();
        assert!(!reg.is_authority("no_such_table"));
    }

    // ---------------------------------------------------------------------------
    // TierRegistry: authority_tables
    // ---------------------------------------------------------------------------

    #[test]
    fn authority_tables_empty_registry() {
        let reg = TierRegistry::new();
        let names: Vec<&str> = reg.authority_tables().collect();
        assert!(names.is_empty());
    }

    #[test]
    fn authority_tables_only_authority_returned() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("a", StateTier::Authority, "s1", "d1");
        let _ = reg.register("b", StateTier::Derived, "s2", "d2");
        let _ = reg.register("c", StateTier::Ephemeral, "s3", "d3");
        let _ = reg.register("d", StateTier::Authority, "s4", "d4");
        let _ = reg.register("e", StateTier::Derived, "s5", "d5");

        let mut names: Vec<&str> = reg.authority_tables().collect();
        names.sort();
        assert_eq!(names, vec!["a", "d"]);
    }

    #[test]
    fn authority_tables_no_authority_registered() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("x", StateTier::Derived, "s", "d");
        let _ = reg.register("y", StateTier::Ephemeral, "s", "d");
        let names: Vec<&str> = reg.authority_tables().collect();
        assert!(names.is_empty());
    }

    #[test]
    fn authority_tables_all_authority() {
        let mut reg = TierRegistry::new();
        let _ = reg.register("a1", StateTier::Authority, "s", "d");
        let _ = reg.register("a2", StateTier::Authority, "s", "d");
        let _ = reg.register("a3", StateTier::Authority, "s", "d");
        assert_eq!(reg.authority_tables().count(), 3);
    }

    // ---------------------------------------------------------------------------
    // TierRegistry: table_count with mixed tiers
    // ---------------------------------------------------------------------------

    #[test]
    fn table_count_reflects_all_registrations() {
        let mut reg = TierRegistry::new();
        assert_eq!(reg.table_count(), 0);
        let _ = reg.register("t1", StateTier::Authority, "s", "d");
        assert_eq!(reg.table_count(), 1);
        let _ = reg.register("t2", StateTier::Derived, "s", "d");
        assert_eq!(reg.table_count(), 2);
        let _ = reg.register("t1", StateTier::Ephemeral, "s", "d");
        assert_eq!(reg.table_count(), 2);
    }

    // ---------------------------------------------------------------------------
    // DerivedState mock
    // ---------------------------------------------------------------------------

    /// Simple mock implementing `DerivedState` for test purposes.
    struct MockDerived {
        dirty: bool,
        rebuild_count: u64,
        last_tick: Option<Tick>,
    }

    impl MockDerived {
        fn new() -> Self {
            Self {
                dirty: false,
                rebuild_count: 0,
                last_tick: None,
            }
        }

        fn rebuild_count(&self) -> u64 {
            self.rebuild_count
        }

        fn last_tick(&self) -> Option<Tick> {
            self.last_tick
        }
    }

    impl DerivedState for MockDerived {
        fn mark_dirty(&mut self) {
            self.dirty = true;
        }

        fn is_dirty(&self) -> bool {
            self.dirty
        }

        fn rebuild_from_authority(&mut self, tick: Tick) -> CoreResult<()> {
            self.dirty = false;
            self.rebuild_count += 1;
            self.last_tick = Some(tick);
            Ok(())
        }
    }

    #[test]
    fn derived_state_initial_not_dirty() {
        let d = MockDerived::new();
        assert!(!d.is_dirty());
        assert_eq!(d.rebuild_count(), 0);
        assert_eq!(d.last_tick(), None);
    }

    #[test]
    fn derived_state_mark_dirty() {
        let mut d = MockDerived::new();
        d.mark_dirty();
        assert!(d.is_dirty());
    }

    #[test]
    fn derived_state_rebuild_clears_dirty() -> CoreResult<()> {
        let mut d = MockDerived::new();
        d.mark_dirty();
        assert!(d.is_dirty());
        d.rebuild_from_authority(Tick(7))?;
        assert!(!d.is_dirty());
        assert_eq!(d.rebuild_count(), 1);
        assert_eq!(d.last_tick(), Some(Tick(7)));
        Ok(())
    }

    #[test]
    fn derived_state_multiple_rebuilds() -> CoreResult<()> {
        let mut d = MockDerived::new();
        for tick_val in 0..5 {
            d.mark_dirty();
            d.rebuild_from_authority(Tick(tick_val))?;
            assert!(!d.is_dirty());
        }
        assert_eq!(d.rebuild_count(), 5);
        assert_eq!(d.last_tick(), Some(Tick(4)));
        Ok(())
    }

    #[test]
    fn derived_state_rebuild_without_dirty() -> CoreResult<()> {
        let mut d = MockDerived::new();
        d.rebuild_from_authority(Tick(0))?;
        assert!(!d.is_dirty());
        assert_eq!(d.rebuild_count(), 1);
        Ok(())
    }

    #[test]
    fn derived_state_double_mark_is_idempotent() {
        let mut d = MockDerived::new();
        d.mark_dirty();
        d.mark_dirty();
        assert!(d.is_dirty());
    }

    // ---------------------------------------------------------------------------
    // TierMetadata serialization
    // ---------------------------------------------------------------------------

    #[test]
    fn tier_metadata_serialize_roundtrip() {
        let meta = TierMetadata {
            tier: StateTier::Derived,
            owner_subsystem: "render".to_string(),
            description: "gpu instance buffer".to_string(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: TierMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    // ---------------------------------------------------------------------------
    // TierRegistry serialization
    // ---------------------------------------------------------------------------

    #[test]
    fn tier_registry_serialize_roundtrip() {
        let mut reg = TierRegistry::new();
        let _ = reg.register(
            "health",
            StateTier::Authority,
            "combat",
            "unit health values",
        );
        let _ = reg.register("hud", StateTier::Derived, "ui", "rendered health bars");
        let _ = reg.register("scratch", StateTier::Ephemeral, "ai", "pathfinding temp");

        let json = serde_json::to_string(&reg).unwrap();
        let back: TierRegistry = serde_json::from_str(&json).unwrap();
        assert_eq!(reg.table_count(), back.table_count());
        assert!(back.is_authority("health"));
        assert!(!back.is_authority("hud"));
        assert!(!back.is_authority("scratch"));
        assert_eq!(back.get("health").unwrap().tier, StateTier::Authority);
        assert_eq!(back.get("hud").unwrap().tier, StateTier::Derived);
        assert_eq!(back.get("scratch").unwrap().tier, StateTier::Ephemeral);
    }
}
