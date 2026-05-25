/// Telemetry instrumentation for the query engine.
/// Only compiled when the `metrics` feature is enabled.
#[cfg(feature = "metrics")]
pub(crate) mod inner {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Record an API call event with latency.
    pub(crate) fn record_api_call(
        method: &str,
        table: &str,
        column: Option<&str>,
        duration_micros: u64,
    ) {
        match column {
            Some(col) => tracing::debug!(
                method = method,
                table = table,
                column = col,
                duration_micros = duration_micros,
                "query api call"
            ),
            None => tracing::debug!(
                method = method,
                table = table,
                duration_micros = duration_micros,
                "query api call"
            ),
        }
    }

    // Cache hit/miss counters
    pub(crate) static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

    pub(crate) fn record_cache_hit() {
        CACHE_HITS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_miss() {
        CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    }

    /// Emit cache summary at tick end.
    pub(crate) fn emit_cache_summary(tick: u64) {
        let hits = CACHE_HITS.load(Ordering::Relaxed);
        let misses = CACHE_MISSES.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_ratio = if total > 0 {
            hits as f64 / total as f64
        } else {
            1.0
        };
        tracing::info!(
            tick = tick,
            cache_hits = hits,
            cache_misses = misses,
            hit_ratio = hit_ratio,
            "cache summary"
        );
    }

    /// Reset cache counters (called at tick start via refresh signal).
    pub(crate) fn reset_cache_counters() {
        CACHE_HITS.store(0, Ordering::Relaxed);
        CACHE_MISSES.store(0, Ordering::Relaxed);
    }
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code, unused_imports)]
pub(crate) mod inner {
    #[inline(always)]
    pub(crate) fn record_api_call(_: &str, _: &str, _: Option<&str>, _: u64) {}
    #[inline(always)]
    pub(crate) fn record_cache_hit() {}
    #[inline(always)]
    pub(crate) fn record_cache_miss() {}
    #[inline(always)]
    pub(crate) fn emit_cache_summary(_: u64) {}
    #[inline(always)]
    pub(crate) fn reset_cache_counters() {}
}

pub(crate) use inner::*;
