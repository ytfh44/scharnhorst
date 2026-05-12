use std::collections::HashSet;

use serde::{Deserialize, Serialize};

pub use scharnhorst_schema::manifest::ModFingerprint;

/// Extension trait providing content-specific methods for [`ModFingerprint`].
pub trait ModFingerprintExt {
 /// Compute a simple hash from table spec names and a seed string.
    fn compute_hash(&mut self, seed: &str);
}

impl ModFingerprintExt for ModFingerprint {
    fn compute_hash(&mut self, seed: &str) {
        let mut names = self.table_specs.clone();
        names.sort_unstable();
        let combined = format!("{}:{}:{}", self.mod_id, self.version, seed);
        self.content_hash = format!("{:x}", fxhash::hash64(&combined));
    }
}

/// Registry of fingerprints for all loaded mods.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintRegistry {
    fingerprints: Vec<ModFingerprint>,
}

impl FingerprintRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, fp: ModFingerprint) {
        self.fingerprints.push(fp);
    }

    pub fn fingerprints(&self) -> &[ModFingerprint] {
        &self.fingerprints
    }

    pub fn find(&self, mod_id: &str) -> Option<&ModFingerprint> {
        self.fingerprints.iter().find(|fp| fp.mod_id == mod_id)
    }

    pub fn contains(&self, mod_id: &str) -> bool {
        self.find(mod_id).is_some()
    }

    pub fn len(&self) -> usize {
        self.fingerprints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
    }

 /// Compare stored fingerprints against available mods and report mismatches.
    pub fn compare<'a>(
        &'a self,
        available: &'a [ModFingerprint],
    ) -> FingerprintComparison<'a> {
        let stored_ids: HashSet<&str> =
            self.fingerprints.iter().map(|fp| fp.mod_id.as_str()).collect();
        let available_ids: HashSet<&str> =
            available.iter().map(|fp| fp.mod_id.as_str()).collect();

        let missing: Vec<&'a ModFingerprint> = self
            .fingerprints
            .iter()
            .filter(|fp| !available_ids.contains(fp.mod_id.as_str()))
            .collect();

        let extra: Vec<&'a ModFingerprint> = available
            .iter()
            .filter(|fp| !stored_ids.contains(fp.mod_id.as_str()))
            .collect();

        let mismatched: Vec<(&'a ModFingerprint, &'a ModFingerprint)> = self
            .fingerprints
            .iter()
            .filter_map(|stored| {
                available
                    .iter()
                    .find(|avail| avail.mod_id == stored.mod_id)
                    .and_then(|avail| {
                        if avail.content_hash != stored.content_hash
                            || avail.version != stored.version
                        {
                            Some((stored, avail))
                        } else {
                            None
                        }
                    })
            })
            .collect();

        FingerprintComparison {
            missing,
            extra,
            mismatched,
        }
    }
}

/// Result of comparing a stored fingerprint registry against available mods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintComparison<'a> {
    pub missing: Vec<&'a ModFingerprint>,
    pub extra: Vec<&'a ModFingerprint>,
    pub mismatched: Vec<(&'a ModFingerprint, &'a ModFingerprint)>,
}

impl FingerprintComparison<'_> {
    pub fn is_exact_match(&self) -> bool {
        self.missing.is_empty() && self.extra.is_empty() && self.mismatched.is_empty()
    }
}

/// Simple 64-bit hash function for fingerprint generation.
mod fxhash {
    pub fn hash64(data: &str) -> u64 {
        const SEED: u64 = 0x517cc1b727220a95;
        let mut hash = SEED;
        for byte in data.bytes() {
            hash = hash.wrapping_mul(0x00000100000001b3);
            hash ^= u64::from(byte);
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_serialize_roundtrip() {
        let fp = ModFingerprint::new("mod_a", "v1")
            .with_table_spec("t1")
            .with_content_hash("deadbeef");
        let json = serde_json::to_string(&fp).unwrap();
        let restored: ModFingerprint = serde_json::from_str(&json).unwrap();
        assert_eq!(fp, restored);
    }

    #[test]
    fn fingerprint_registry_len_and_is_empty() {
        let reg = FingerprintRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);

        let mut reg = FingerprintRegistry::new();
        reg.register(ModFingerprint::new("a", "v1"));
        assert!(!reg.is_empty());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn fingerprint_registry_find_miss() {
        let reg = FingerprintRegistry::new();
        assert!(reg.find("nonexistent").is_none());
        assert!(!reg.contains("nonexistent"));
    }

    #[test]
    fn fingerprint_registry_fingerprints_slice() {
        let mut reg = FingerprintRegistry::new();
        reg.register(ModFingerprint::new("a", "v1"));
        reg.register(ModFingerprint::new("b", "v2"));
        assert_eq!(reg.fingerprints().len(), 2);
    }

    #[test]
    fn fingerprint_registry_compare_version_and_hash_mismatch() {
        let mut reg = FingerprintRegistry::new();
        let stored = ModFingerprint::new("mod_a", "v1")
            .with_table_spec("t1")
            .with_content_hash("abc");
        reg.register(stored);

        let available = vec![ModFingerprint::new("mod_a", "v1")
            .with_table_spec("t2")
            .with_content_hash("xyz")];
        let comp = reg.compare(&available);
        assert!(!comp.is_exact_match());
        assert_eq!(comp.mismatched.len(), 1);
    }

    #[test]
    fn fingerprint_comparison_is_exact_match() {
        let comp = FingerprintComparison {
            missing: vec![],
            extra: vec![],
            mismatched: vec![],
        };
        assert!(comp.is_exact_match());
    }

    #[test]
    fn fxhash_deterministic() {
        let h1 = fxhash::hash64("hello");
        let h2 = fxhash::hash64("hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn fxhash_different_inputs() {
        let h1 = fxhash::hash64("hello");
        let h2 = fxhash::hash64("world");
        assert_ne!(h1, h2);
    }
}
