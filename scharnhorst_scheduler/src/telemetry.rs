/// Telemetry instrumentation for the scheduler.
/// Only compiled when the `metrics` feature is enabled.
#[cfg(feature = "metrics")]
mod inner {
    use std::time::Instant;

    /// Guard that creates a tracing span and records duration on Drop.
    ///
    /// Ensures spans are closed even on panic (I-SCHED-SPAN-DROP-CLOSE invariant).
    pub struct DurationGuard {
        start: Instant,
        span: tracing::Span,
    }

    impl DurationGuard {
        pub fn tick_span(tick: u64) -> Self {
            let span = tracing::info_span!("scheduler_tick", tick = tick);
            Self {
                start: Instant::now(),
                span,
            }
        }

        pub fn phase_span(tick: u64, phase_name: &str) -> Self {
            let span = tracing::info_span!("scheduler_phase", tick = tick, phase = phase_name);
            Self {
                start: Instant::now(),
                span,
            }
        }
    }

    impl Drop for DurationGuard {
        fn drop(&mut self) {
            let dur = self.start.elapsed().as_micros() as u64;
            let _enter = self.span.enter();
            self.span.record("duration_micros", dur);
        }
    }

    pub fn emit_system_event(tick: u64, phase: &str, system_name: &str, duration_micros: u64) {
        tracing::debug!(
            tick = tick,
            phase = phase,
            system_name = system_name,
            duration_micros = duration_micros,
            "system execution completed"
        );
    }

    pub fn emit_diff_count(tick: u64, diff_count: u64) {
        tracing::info!(tick = tick, diff_count = diff_count, "tick diff count");
    }
}

#[cfg(not(feature = "metrics"))]
mod inner {
    /// Zero-sized no-op guard when metrics is disabled.
    pub struct DurationGuard;

    impl DurationGuard {
        #[inline(always)]
        pub fn tick_span(_tick: u64) -> Self {
            Self
        }
        #[inline(always)]
        pub fn phase_span(_tick: u64, _phase_name: &str) -> Self {
            Self
        }
    }

    #[inline(always)]
    pub fn emit_system_event(_tick: u64, _phase: &str, _system_name: &str, _duration_micros: u64) {}

    #[inline(always)]
    pub fn emit_diff_count(_tick: u64, _diff_count: u64) {}
}

pub use inner::*;
