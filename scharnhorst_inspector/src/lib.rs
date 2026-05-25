//! Debug inspector for scharnhorst world state.
//! All panels are only compiled in debug builds.

pub mod inspector;
pub mod panels;

#[cfg(debug_assertions)]
mod app;

#[cfg(debug_assertions)]
pub use app::run_inspector;
