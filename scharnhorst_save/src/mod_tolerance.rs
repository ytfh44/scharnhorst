use std::collections::HashSet;

use scharnhorst_content::{FingerprintRegistry, ModFingerprint};

use crate::error::{SaveError, SaveResult};

/// Outcome of comparing stored mod fingerprints against available mods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModLoadOutcome {
    /// All mods match exactly.
    ExactMatch,
    /// Some mods have version or content hash mismatches; load proceeds with warnings.
    VersionMismatch { warnings: Vec<String> },
    /// Mods present in save but missing on disk; affected tables are degraded.
    MissingMods {
        missing: Vec<ModFingerprint>,
        degraded_tables: Vec<String>,
    },
    /// Extra mods on disk not present in save; they apply from cold start.
    ExtraMods { extra: Vec<ModFingerprint> },
    /// Combined mismatches and missing mods.
    Degraded {
        warnings: Vec<String>,
        degraded_tables: Vec<String>,
    },
}

/// Policy for handling mod mismatches during load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModTolerancePolicy {
    /// Strict: reject any mismatch.
    Strict,
    /// Warn but allow mismatches.
    Warn,
    /// Silent: ignore mismatches.
    Silent,
}

/// Evaluates mod fingerprint compatibility and produces a load outcome.
pub struct ModToleranceChecker {
    policy: ModTolerancePolicy,
    critical_mods: HashSet<String>,
}

impl ModToleranceChecker {
    pub fn new(policy: ModTolerancePolicy) -> Self {
        Self {
            policy,
            critical_mods: HashSet::new(),
        }
    }

    pub fn with_critical_mod(mut self, mod_id: impl Into<String>) -> Self {
        self.critical_mods.insert(mod_id.into());
        self
    }

    pub fn policy(&self) -> ModTolerancePolicy {
        self.policy
    }

    pub fn is_critical(&self, mod_id: &str) -> bool {
        self.critical_mods.contains(mod_id)
    }

    /// Compare stored fingerprints against available mods and return the load outcome.
    pub fn check(
        &self,
        stored: &FingerprintRegistry,
        available: &[ModFingerprint],
    ) -> SaveResult<ModLoadOutcome> {
        let comparison = stored.compare(available);

        for missing in &comparison.missing {
            if self.is_critical(&missing.mod_id) {
                return Err(SaveError::CriticalModMissing(missing.mod_id.clone()));
            }
        }

        if self.policy == ModTolerancePolicy::Strict {
            if available.len() != stored.len() {
                return Err(SaveError::Generic(format!(
                    "strict mode: mod count mismatch: stored={}, available={}",
                    stored.len(),
                    available.len()
                )));
            }
            if let Some(missing) = comparison.missing.first() {
                return Err(SaveError::Generic(format!(
                    "strict mode: mod mismatch detected: mod '{}' is missing from available mods",
                    missing.mod_id
                )));
            }
            if let Some((stored_fp, avail_fp)) = comparison.mismatched.first() {
                return Err(SaveError::Generic(format!(
                    "strict mode: mod mismatch detected: mod '{}' version/content differs (stored v{} vs available v{})",
                    stored_fp.mod_id, stored_fp.version, avail_fp.version
                )));
            }
            return Ok(ModLoadOutcome::ExactMatch);
        }

        if self.policy == ModTolerancePolicy::Silent {
            return Ok(ModLoadOutcome::ExactMatch);
        }

        let warnings: Vec<String> = comparison
            .mismatched
            .iter()
            .map(|(stored, avail)| {
                format!(
                    "mod {} version mismatch: save={}, disk={}",
                    stored.mod_id, stored.version, avail.version
                )
            })
            .collect();

        let degraded_tables: Vec<String> = comparison
            .missing
            .iter()
            .flat_map(|fp| fp.table_specs.iter().cloned())
            .collect();

        let extra: Vec<ModFingerprint> = comparison.extra.iter().map(|&fp| fp.clone()).collect();

        if comparison.is_exact_match() && extra.is_empty() {
            return Ok(ModLoadOutcome::ExactMatch);
        }

        if !comparison.mismatched.is_empty() && !comparison.missing.is_empty() && !extra.is_empty()
        {
            return Ok(ModLoadOutcome::Degraded {
                warnings,
                degraded_tables,
            });
        }

        if !comparison.mismatched.is_empty() {
            return Ok(ModLoadOutcome::VersionMismatch { warnings });
        }

        if !comparison.missing.is_empty() {
            return Ok(ModLoadOutcome::MissingMods {
                missing: comparison.missing.iter().map(|&fp| fp.clone()).collect(),
                degraded_tables,
            });
        }

        if !extra.is_empty() {
            return Ok(ModLoadOutcome::ExtraMods { extra });
        }

        Ok(ModLoadOutcome::ExactMatch)
    }

    /// Produce the final mod list to use for compilation.
    pub fn final_mod_list(
        &self,
        stored: &FingerprintRegistry,
        available: &[ModFingerprint],
    ) -> SaveResult<Vec<ModFingerprint>> {
        let mut result: Vec<ModFingerprint> = available.to_vec();

        for fp in stored.fingerprints() {
            if !result.iter().any(|a| a.mod_id == fp.mod_id) {
                result.push(fp.clone());
            }
        }

        result.sort_by(|a, b| a.mod_id.cmp(&b.mod_id));
        Ok(result)
    }
}

impl Default for ModToleranceChecker {
    fn default() -> Self {
        Self::new(ModTolerancePolicy::Warn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(mod_id: &str, version: &str, hash: &str) -> ModFingerprint {
        ModFingerprint::new(mod_id, version).with_content_hash(hash)
    }

    #[test]
    fn exact_match() -> SaveResult<()> {
        let mut stored = FingerprintRegistry::new();
        stored.register(fp("base", "1.0", "abc"));
        let available = vec![fp("base", "1.0", "abc")];
        let checker = ModToleranceChecker::default();
        let outcome = checker.check(&stored, &available)?;
        assert_eq!(outcome, ModLoadOutcome::ExactMatch);
        Ok(())
    }

    #[test]
    fn critical_mod_missing_rejected() {
        let mut stored = FingerprintRegistry::new();
        stored.register(fp("base", "1.0", "abc"));
        let available: Vec<ModFingerprint> = vec![];
        let checker = ModToleranceChecker::default().with_critical_mod("base");
        let result = checker.check(&stored, &available);
        assert!(matches!(result, Err(SaveError::CriticalModMissing(_))));
    }

    #[test]
    fn version_mismatch_warns() -> SaveResult<()> {
        let mut stored = FingerprintRegistry::new();
        stored.register(fp("base", "1.0", "abc"));
        let available = vec![fp("base", "1.1", "def")];
        let checker = ModToleranceChecker::default();
        let outcome = checker.check(&stored, &available)?;
        assert!(
            matches!(outcome, ModLoadOutcome::VersionMismatch { .. }),
            "expected VersionMismatch, got {:?}",
            outcome
        );
        Ok(())
    }

    #[test]
    fn final_mod_list_includes_all() -> SaveResult<()> {
        let mut stored = FingerprintRegistry::new();
        stored.register(fp("base", "1.0", "abc"));
        let available = vec![fp("extra", "1.0", "xyz")];
        let checker = ModToleranceChecker::default();
        let list = checker.final_mod_list(&stored, &available)?;
        assert_eq!(list.len(), 2);
        Ok(())
    }
}
