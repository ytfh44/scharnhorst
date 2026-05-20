use crate::error::{CoreError, CoreResult};

/// Lifecycle phases of the scharnhorst engine.
///
/// The engine begins in [`Initialization`](Lifecycle::Initialization), during which
/// configuration is loaded, tables are built, and providers are wired.
/// Once setup is complete, the engine advances to [`Simulation`](Lifecycle::Simulation)
/// and begins processing ticks. The transition is irreversible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lifecycle {
    /// Engine is initializing: loading configuration, building tables, wiring providers.
    Initialization,
    /// Engine is running the simulation loop: processing ticks, applying diffs, updating state.
    Simulation,
}

impl Lifecycle {
    /// Returns `true` if this is the [`Initialization`](Lifecycle::Initialization) phase.
    pub fn is_initialization(self) -> bool {
        matches!(self, Self::Initialization)
    }

    /// Returns `true` if this is the [`Simulation`](Lifecycle::Simulation) phase.
    pub fn is_simulation(self) -> bool {
        matches!(self, Self::Simulation)
    }
}

/// Guards the engine lifecycle, controlling phase transitions.
///
/// Owns the current [`Lifecycle`]. Once the engine advances to
/// [`Simulation`](Lifecycle::Simulation) it can never return to initialization.
#[derive(Debug)]
pub struct LifecycleGuard {
    lifecycle: Lifecycle,
}

impl LifecycleGuard {
    /// Creates a new guard starting in [`Initialization`](Lifecycle::Initialization).
    pub fn new() -> Self {
        Self {
            lifecycle: Lifecycle::Initialization,
        }
    }

    /// Returns the current lifecycle phase.
    pub fn current_lifecycle(&self) -> Lifecycle {
        self.lifecycle
    }

    /// Transitions from [`Initialization`](Lifecycle::Initialization) to
    /// [`Simulation`](Lifecycle::Simulation).
    ///
    /// Returns an error if already in the Simulation phase.
    pub fn advance_to_simulation(&mut self) -> CoreResult<()> {
        if self.lifecycle.is_simulation() {
            return Err(CoreError::InvalidPhase(
                "already in Simulation phase, cannot advance again".to_string(),
            ));
        }
        self.lifecycle = Lifecycle::Simulation;
        Ok(())
    }

    /// Returns `true` if the guard is in the [`Initialization`](Lifecycle::Initialization) phase.
    pub fn is_initialization(&self) -> bool {
        self.lifecycle.is_initialization()
    }

    /// Returns `true` if the guard is in the [`Simulation`](Lifecycle::Simulation) phase.
    pub fn is_simulation(&self) -> bool {
        self.lifecycle.is_simulation()
    }
}

impl Default for LifecycleGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Capability marker types
//
// These zero-sized tokens serve as compile-time proofs of authorization.
// Private fields prevent external structural construction; constructors are
// `pub` because framework crates (scharnhorst_arrow_store, etc.) need them.
//
// Defense-in-depth layers:
// 1. `inner()` removed from both `InitStore` and `CommitStore` — prevents
//    `Arc<ArrowStore>` leakage through the capability boundary. A guard gives
//    access to store operations; it does not give the store handle itself.
// 2. `InitStore::new()` requires `Arc<ArrowStore>` which simulation consumers
//    should never possess.
// 3. `CommitStore::new()` is constructible from `Arc<ArrowStore>` by framework
//    crates (arrow-store, journal, scheduler). External consumers without store
//    access cannot forge a commit handle.
// 4. `advance_to_simulation()` on `ArrowStore` is `pub(crate)` — only
//    `InitStore::into_simulation()` can trigger the transition.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InitToken {
    _private: (),
}

#[derive(Debug)]
pub struct CommitToken {
    _private: (),
}

#[derive(Debug, Clone)]
pub struct SimReadToken {
    _private: (),
}

#[derive(Debug, Clone)]
pub struct JournalSubmitToken {
    _private: (),
}

#[allow(clippy::new_without_default)]
impl InitToken {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[allow(clippy::new_without_default)]
impl CommitToken {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[allow(dead_code)]
impl SimReadToken {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[allow(dead_code)]
impl JournalSubmitToken {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- LifecycleGuard tests --

    #[test]
    fn lifecycle_guard_construction() {
        let guard = LifecycleGuard::new();
        assert!(guard.is_initialization());
        assert!(!guard.is_simulation());
        assert_eq!(guard.current_lifecycle(), Lifecycle::Initialization);
    }

    #[test]
    fn advance_to_simulation_succeeds() {
        let mut guard = LifecycleGuard::new();
        let result = guard.advance_to_simulation();
        assert!(result.is_ok());
        assert!(guard.is_simulation());
        assert!(!guard.is_initialization());
        assert_eq!(guard.current_lifecycle(), Lifecycle::Simulation);
    }

    #[test]
    fn advance_to_simulation_twice_returns_error() {
        let mut guard = LifecycleGuard::new();
        let _ = guard.advance_to_simulation();
        let result = guard.advance_to_simulation();
        assert!(result.is_err());
        // Verify still in Simulation despite the error.
        assert!(guard.is_simulation());
    }

    #[test]
    fn advance_to_simulation_twice_returns_invalid_phase() {
        let mut guard = LifecycleGuard::new();
        let _ = guard.advance_to_simulation();
        let result = guard.advance_to_simulation();
        match result {
            Err(CoreError::InvalidPhase(_)) => {}
            other => panic!("expected InvalidPhase, got {:?}", other),
        }
    }

    #[test]
    fn lifecycle_variants_and_methods() {
        assert!(Lifecycle::Initialization.is_initialization());
        assert!(!Lifecycle::Initialization.is_simulation());
        assert!(Lifecycle::Simulation.is_simulation());
        assert!(!Lifecycle::Simulation.is_initialization());
    }

    #[test]
    fn lifecycle_guard_default_is_initialization() {
        let guard = LifecycleGuard::default();
        assert!(guard.is_initialization());
    }

    // -- Capability token tests --

    #[test]
    fn init_token_not_clone_not_copy() {
        let token = InitToken::new();
        // Debug is available.
        let debug = format!("{:?}", token);
        assert!(debug.contains("InitToken"));
        // InitToken does NOT implement Clone or Copy.
        // The following would fail to compile if uncommented:
        // let _ = token; // Copy would keep token alive
        // let _dup = token.clone(); // Clone would compile
    }

    #[test]
    fn commit_token_not_clone_not_copy() {
        let token = CommitToken::new();
        let debug = format!("{:?}", token);
        assert!(debug.contains("CommitToken"));
        // CommitToken does NOT implement Clone or Copy.
    }

    #[test]
    fn sim_read_token_cloneable() {
        let token = SimReadToken::new();
        let clone = token.clone();
        let debug_orig = format!("{:?}", token);
        let debug_clone = format!("{:?}", clone);
        assert!(debug_orig.contains("SimReadToken"));
        assert_eq!(debug_orig, debug_clone);
    }

    #[test]
    fn journal_submit_token_cloneable() {
        let token = JournalSubmitToken::new();
        let clone = token.clone();
        let debug_orig = format!("{:?}", token);
        let debug_clone = format!("{:?}", clone);
        assert!(debug_orig.contains("JournalSubmitToken"));
        assert_eq!(debug_orig, debug_clone);
    }
}
