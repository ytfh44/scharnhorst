/// Telemetry instrumentation for the journal.
/// Only compiled when the `metrics` feature is enabled.
#[cfg(feature = "metrics")]
pub mod inner {
    use std::sync::atomic::{AtomicU64, Ordering};

    // Per-tick submission counters
    pub static DIFFS_SUBMITTED: AtomicU64 = AtomicU64::new(0);
    pub static COMMANDS_SUBMITTED: AtomicU64 = AtomicU64::new(0);

    pub fn record_diff_submit(diff_type: &str, table: &str) {
        DIFFS_SUBMITTED.fetch_add(1, Ordering::Relaxed);
        tracing::trace!(
            event = "diff_submit",
            diff_type = diff_type,
            table = table,
            "diff submitted"
        );
    }

    pub fn record_command_submit(command_type: &str) {
        COMMANDS_SUBMITTED.fetch_add(1, Ordering::Relaxed);
        tracing::trace!(
            event = "command_submit",
            command_type = command_type,
            "command submitted"
        );
    }

    pub fn emit_commit_record(tick: u64, batch_size: u64, final_diff_count: u64, state_hash: &str) {
        tracing::info!(
            event = "commit",
            tick = tick,
            batch_size = batch_size,
            final_diff_count = final_diff_count,
            state_hash = state_hash,
            "commit recorded"
        );
    }

    pub fn reset_counters() {
        DIFFS_SUBMITTED.store(0, Ordering::Relaxed);
        COMMANDS_SUBMITTED.store(0, Ordering::Relaxed);
    }
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, unused_imports)]
pub(crate) mod inner {
    #[inline(always)]
    pub(crate) fn record_diff_submit(_: &str, _: &str) {}
    #[inline(always)]
    pub(crate) fn record_command_submit(_: &str) {}
    #[inline(always)]
    pub(crate) fn emit_commit_record(_: u64, _: u64, _: u64, _: &str) {}
    #[inline(always)]
    pub(crate) fn reset_counters() {}
}

#[cfg(feature = "metrics")]
pub use inner::*;
