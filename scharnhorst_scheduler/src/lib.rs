//! scharnhorst_scheduler: simulation scheduler with phase-based execution,
//! deterministic RNG, tick-boundary command consumption, and atomic commit.

pub mod error;
pub mod phase;
pub mod refresh_signal;
pub mod rng;
pub mod scheduler;
pub mod system;

pub use error::{SchedulerError, SchedulerResult};
pub use phase::Phase;
pub use refresh_signal::{RefreshCallback, RefreshSignalBus, RefreshSignalHandle};
pub use rng::DeterministicRng;
pub use scheduler::Scheduler;
pub use system::{BoxedSystem, SimSystem, SystemRegistration};
