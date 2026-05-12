use std::collections::HashSet;
use std::sync::Arc;

use scharnhorst_journal::diff::Diff;
use scharnhorst_query::engine::QueryEngine;

use crate::error::SchedulerResult;
use crate::phase::Phase;
use crate::refresh_signal::RefreshCallback;
use crate::rng::DeterministicRng;

/// Trait implemented by all simulation systems.
///
/// A `SimSystem` receives a deterministic RNG stream, read-only access
/// to the world via the query engine, and may emit [`Diff`] objects
/// that are collected into the journal for atomic commit.
pub trait SimSystem: Send + Sync {
 /// Unique identifier for this system (used for RNG seeding and logging).
    fn id(&self) -> &str;

 /// The execution phase this system belongs to.
    fn phase(&self) -> Phase;

 /// Table names this system reads from.
    fn read_tables(&self) -> Vec<String>;

 /// Table names this system may write diffs to.
    fn write_tables(&self) -> Vec<String>;

 /// Optional refresh-signal callback if the system maintains caches.
    fn refresh_callback(&self) -> Option<RefreshCallback> {
        None
    }

 /// Execute the system for the current tick.
 ///
 /// * `rng` 鈥?deterministic stream seeded with `(self.id, tick)`.
 /// * `query` 鈥?read-only view of the world snapshot.
 /// * `phase` 鈥?the current simulation phase.
 /// * `tick` 鈥?the current simulation tick.
 ///
 /// Returns a vector of diffs to be applied atomically at commit time.
    fn execute(
        &self,
        rng: &mut DeterministicRng,
        query: &QueryEngine,
        phase: Phase,
        tick: u64,
    ) -> SchedulerResult<Vec<Diff>>;
}

/// Registration metadata for a simulation system.
///
/// Captures dependency information so the scheduler can order phases,
/// detect write conflicts, and build the refresh-signal recipient list.
#[derive(Clone)]
pub struct SystemRegistration {
    pub system_id: String,
    pub phase: Phase,
    pub read_tables: HashSet<String>,
    pub write_tables: HashSet<String>,
    pub refresh_callback: Option<RefreshCallback>,
}

impl std::fmt::Debug for SystemRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemRegistration")
            .field("system_id", &self.system_id)
            .field("phase", &self.phase)
            .field("read_tables", &self.read_tables)
            .field("write_tables", &self.write_tables)
            .field("has_refresh_callback", &self.refresh_callback.is_some())
            .finish()
    }
}

impl SystemRegistration {
 /// Create a new registration from a [`SimSystem`] implementation.
    pub fn from_system(system: &dyn SimSystem) -> Self {
        Self {
            system_id: system.id().to_owned(),
            phase: system.phase(),
            read_tables: system.read_tables().into_iter().collect(),
            write_tables: system.write_tables().into_iter().collect(),
            refresh_callback: system.refresh_callback(),
        }
    }

 /// Returns true if this system writes to any table.
    pub fn is_writer(&self) -> bool {
        !self.write_tables.is_empty()
    }

 /// Returns true if this system and another have disjoint write sets.
    pub fn write_sets_disjoint(&self, other: &Self) -> bool {
        self.write_tables.is_disjoint(&other.write_tables)
    }

 /// Returns true if this system has a write conflict with another in the same phase.
    pub fn has_write_conflict(&self, other: &Self) -> bool {
        self.phase == other.phase && !self.write_sets_disjoint(other)
    }

 /// Returns the set of conflicting table names with another registration.
    pub fn conflicting_tables(&self, other: &Self) -> HashSet<String> {
        self.write_tables
            .intersection(&other.write_tables)
            .cloned()
            .collect()
    }
}

/// A boxed, type-erased simulation system.
pub type BoxedSystem = Arc<dyn SimSystem>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_from_system_captures_metadata() {
        struct TestSys {
            id: String,
            phase: Phase,
            reads: Vec<String>,
            writes: Vec<String>,
            callback: Option<RefreshCallback>,
        }
        impl SimSystem for TestSys {
            fn id(&self) -> &str { &self.id }
            fn phase(&self) -> Phase { self.phase }
            fn read_tables(&self) -> Vec<String> { self.reads.clone() }
            fn write_tables(&self) -> Vec<String> { self.writes.clone() }
            fn refresh_callback(&self) -> Option<RefreshCallback> { self.callback.clone() }
            fn execute(
                &self, _rng: &mut DeterministicRng, _query: &QueryEngine, _phase: Phase, _tick: u64,
            ) -> SchedulerResult<Vec<scharnhorst_journal::diff::Diff>> {
                Ok(Vec::new())
            }
        }

        let sys = TestSys {
            id: "s1".into(),
            phase: Phase::Economy,
            reads: vec!["r1".into()],
            writes: vec!["w1".into()],
            callback: None,
        };
        let reg = SystemRegistration::from_system(&sys);
        assert_eq!(reg.system_id, "s1");
        assert_eq!(reg.phase, Phase::Economy);
        assert_eq!(reg.read_tables, HashSet::from(["r1".to_owned()]));
        assert_eq!(reg.write_tables, HashSet::from(["w1".to_owned()]));
        assert!(reg.is_writer());
    }

    #[test]
    fn is_writer_false_when_no_write_tables() {
        let reg = SystemRegistration {
            system_id: "ro".into(),
            phase: Phase::PreTick,
            read_tables: ["a".into()].into(),
            write_tables: HashSet::new(),
            refresh_callback: None,
        };
        assert!(!reg.is_writer());
    }

    #[test]
    fn write_sets_disjoint_returns_true() {
        let a = SystemRegistration {
            system_id: "a".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["x"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        let b = SystemRegistration {
            system_id: "b".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["y"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        assert!(a.write_sets_disjoint(&b));
    }

    #[test]
    fn write_sets_disjoint_returns_false_on_overlap() {
        let a = SystemRegistration {
            system_id: "a".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["x", "y"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        let b = SystemRegistration {
            system_id: "b".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["y", "z"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        assert!(!a.write_sets_disjoint(&b));
    }

    #[test]
    fn has_write_conflict_same_phase_overlap() {
        let a = SystemRegistration {
            system_id: "a".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["t1"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        let b = SystemRegistration {
            system_id: "b".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["t1"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        assert!(a.has_write_conflict(&b));
    }

    #[test]
    fn has_write_conflict_different_phase_no_conflict() {
        let a = SystemRegistration {
            system_id: "a".into(), phase: Phase::Economy,
            read_tables: HashSet::new(), write_tables: ["t1"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        let b = SystemRegistration {
            system_id: "b".into(), phase: Phase::Diplomacy,
            read_tables: HashSet::new(), write_tables: ["t1"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        assert!(!a.has_write_conflict(&b));
    }

    #[test]
    fn conflicting_tables_returns_intersection() {
        let a = SystemRegistration {
            system_id: "a".into(), phase: Phase::Economy,
            read_tables: HashSet::new(),
            write_tables: ["t1", "t2", "t3"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        let b = SystemRegistration {
            system_id: "b".into(), phase: Phase::Economy,
            read_tables: HashSet::new(),
            write_tables: ["t2", "t3", "t4"].into_iter().map(String::from).collect(),
            refresh_callback: None,
        };
        let c = a.conflicting_tables(&b);
        assert_eq!(c.len(), 2);
        assert!(c.contains("t2"));
        assert!(c.contains("t3"));
    }
}
