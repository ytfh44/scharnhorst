/// SplitMix64 PRNG — guarantees identical sequence on all platforms
/// and all Rust versions. Replaces `std::collections::hash_map::DefaultHasher`
/// which does NOT provide cross-platform stability guarantees.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }

    /// Generate the next `usize` in `[0, bound)` without modulo bias.
    ///
    /// When `bound == 0`, returns `0` — the integer range `[0, 0)` is
    /// empty, so this is the only valid sentinel.
    ///
    /// Uses the multiply-shift method (Lemire) which maps a uniform
    /// `u64` to `[0, bound)` with negligible bias, avoiding the
    /// platform-dependent behavior of the modulo operator on negative
    /// values and the statistical bias of simple modulo reduction.
    fn next_usize(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        ((self.next_u64() as u128).wrapping_mul(bound as u128) >> 64) as usize
    }
}

/// A deterministic RNG stream seeded by system name and tick.
///
/// Given the same `(system_id, tick)` pair, the stream produces the
/// identical sequence of values on all platforms, enabling deterministic
/// replay.
///
/// Internally uses [`SplitMix64`] which relies only on 64-bit integer
/// arithmetic with no platform-dependent hashing or floating-point.
#[derive(Debug, Clone)]
pub struct DeterministicRng {
    rng: SplitMix64,
    tick: u64,
    system_id: String,
}

impl DeterministicRng {
    /// Create a new deterministic RNG for the given system and tick.
    pub fn new(system_id: impl Into<String>, tick: u64) -> Self {
        let system_id = system_id.into();
        let seed = Self::compute_seed(&system_id, tick);
        Self {
            rng: SplitMix64::new(seed),
            tick,
            system_id,
        }
    }

    /// Returns the tick this stream was seeded for.
    pub fn tick(&self) -> u64 {
        self.tick
    }

    /// Returns the system ID this stream was seeded for.
    pub fn system_id(&self) -> &str {
        &self.system_id
    }

    /// Generate the next `u64` in the deterministic sequence.
    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Generate the next `f64` in the range `[0.0, 1.0)`.
    pub fn next_f64(&mut self) -> f64 {
        let bits = self.next_u64();
        let mantissa = bits >> 11;
        mantissa as f64 / (1u64 << 53) as f64
    }

    /// Generate the next `bool` with 50/50 probability.
    pub fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    /// Generate the next `usize` in the range `[0, bound)`.
    pub fn next_usize(&mut self, bound: usize) -> usize {
        self.rng.next_usize(bound)
    }

    /// Compute a deterministic seed from system ID and tick.
    ///
    /// Uses SplitMix64 itself to mix the inputs — no hasher dependency,
    /// identical output on all platforms and Rust versions.
    fn compute_seed(system_id: &str, tick: u64) -> u64 {
        let mut sm = SplitMix64::new(tick);
        for byte in system_id.bytes() {
            sm.state ^= (byte as u64).wrapping_mul(0x9e3779b97f4a7c15);
            sm.next_u64();
        }
        sm.next_u64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_seed_produces_identical_stream() {
        let mut a = DeterministicRng::new("sys", 42);
        let mut b = DeterministicRng::new("sys", 42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_system_ids_produce_different_streams() {
        let mut a = DeterministicRng::new("sys_a", 42);
        let mut b = DeterministicRng::new("sys_b", 42);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn different_ticks_produce_different_streams() {
        let mut a = DeterministicRng::new("sys", 1);
        let mut b = DeterministicRng::new("sys", 2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn tick_and_system_id_accessors() {
        let rng = DeterministicRng::new("eco", 7);
        assert_eq!(rng.tick(), 7);
        assert_eq!(rng.system_id(), "eco");
    }

    #[test]
    fn next_f64_in_range() {
        let mut rng = DeterministicRng::new("sys", 0);
        for _ in 0..1000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v));
        }
    }

    #[test]
    fn next_usize_respects_bound() {
        let mut rng = DeterministicRng::new("sys", 0);
        for _ in 0..100 {
            let v = rng.next_usize(10);
            assert!(v < 10);
        }
    }

    #[test]
    fn next_usize_zero_bound() {
        let mut rng = DeterministicRng::new("sys", 0);
        assert_eq!(rng.next_usize(0), 0);
    }

    #[test]
    fn next_bool_produces_both_values() {
        let mut rng = DeterministicRng::new("sys", 0);
        let mut saw_true = false;
        let mut saw_false = false;
        for _ in 0..100 {
            if rng.next_bool() {
                saw_true = true;
            } else {
                saw_false = true;
            }
        }
        assert!(saw_true && saw_false);
    }

    #[test]
    fn rng_is_cloneable() {
        let mut a = DeterministicRng::new("sys", 1);
        let _ = a.next_u64();
        let mut b = a.clone();
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn splitmix64_is_deterministic() {
        let mut a = SplitMix64::new(12345);
        let a_val = a.next_u64();
        let mut b = SplitMix64::new(12345);
        let b_val = b.next_u64();
        assert_eq!(a_val, b_val);
    }

    #[test]
    fn compute_seed_is_deterministic() {
        let a = DeterministicRng::compute_seed("test_system", 42);
        let b = DeterministicRng::compute_seed("test_system", 42);
        assert_eq!(a, b);
    }

    #[test]
    fn compute_seed_differs_for_different_inputs() {
        let a = DeterministicRng::compute_seed("sys_a", 1);
        let b = DeterministicRng::compute_seed("sys_b", 1);
        assert_ne!(a, b);

        let c = DeterministicRng::compute_seed("sys_a", 2);
        assert_ne!(a, c);
    }
}
