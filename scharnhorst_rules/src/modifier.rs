use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use scharnhorst_core::FixedPoint;

use crate::error::{RuleError, RuleResult};

/// The kind of a modifier operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModifierOp {
    /// Additive modifier: `base + value`.
    Add,
    /// Multiplicative modifier: `base * value`.
    Mul,
    /// Override modifier: replaces the base value entirely.
    Override,
}

/// A single modifier entry targeting a specific field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Modifier {
    pub target_table: String,
    pub target_column: String,
    pub op: ModifierOp,
    pub value: FixedPoint,
}

impl Modifier {
    pub fn new(
        table: impl Into<String>,
        column: impl Into<String>,
        op: ModifierOp,
        value: FixedPoint,
    ) -> Self {
        Self {
            target_table: table.into(),
            target_column: column.into(),
            op,
            value,
        }
    }

    /// Apply this modifier to a base value, returning the result.
    pub fn apply(&self, base: FixedPoint) -> RuleResult<FixedPoint> {
        match self.op {
            ModifierOp::Add => {
                let value = self.value.rescale(base.scale()).map_err(RuleError::from)?;
                (base + value).map_err(RuleError::from)
            }
            ModifierOp::Mul => {
                let value = self.value.rescale(base.scale()).map_err(RuleError::from)?;
                (base * value).map_err(RuleError::from)
            }
            ModifierOp::Override => Ok(self.value),
        }
    }
}

/// A collection of modifiers keyed by target field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModifierRegistry {
    entries: HashMap<(String, String), Vec<Modifier>>,
}

impl ModifierRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a modifier for a target field.
    pub fn register(&mut self, modifier: Modifier) {
        let key = (
            modifier.target_table.clone(),
            modifier.target_column.clone(),
        );
        self.entries.entry(key).or_default().push(modifier);
    }

    /// Remove all modifiers for a target field.
    pub fn clear_field(&mut self, table: &str, column: &str) {
        self.entries.remove(&(table.to_owned(), column.to_owned()));
    }

    /// Return all modifiers for a target field, if any.
    pub fn modifiers_for(&self, table: &str, column: &str) -> Option<&[Modifier]> {
        self.entries
            .get(&(table.to_owned(), column.to_owned()))
            .map(|v| v.as_slice())
    }

    /// Aggregate modifiers for a field into a single effective value.
    ///
    /// Aggregation order:
    /// 1. Start with the base value.
    /// 2. Apply all `Add` modifiers.
    /// 3. Apply all `Mul` modifiers.
    /// 4. If any `Override` exists, the last registered one wins.
    pub fn aggregate(&self, table: &str, column: &str, base: FixedPoint) -> RuleResult<FixedPoint> {
        let mods = self.modifiers_for(table, column).unwrap_or_default();

        let (adds, muls, overrides): (Vec<_>, Vec<_>, Vec<_>) =
            mods.iter().map(|m| m.op).zip(mods.iter()).fold(
                (Vec::new(), Vec::new(), Vec::new()),
                |(mut a, mut m, mut o), (_, mod_ref)| {
                    match mod_ref.op {
                        ModifierOp::Add => a.push(mod_ref),
                        ModifierOp::Mul => m.push(mod_ref),
                        ModifierOp::Override => o.push(mod_ref),
                    }
                    (a, m, o)
                },
            );

        let after_add = adds.iter().try_fold(base, |acc, m| m.apply(acc))?;

        let after_mul = muls.iter().try_fold(after_add, |acc, m| m.apply(acc))?;

        overrides
            .last()
            .map_or(Ok(after_mul), |m| m.apply(after_mul))
    }

    /// Return an iterator over all registered target fields.
    pub fn fields(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.entries.keys().map(|(t, c)| (t.as_str(), c.as_str()))
    }

    /// Return the total number of registered modifier entries.
    pub fn len(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modifier_op_enum_variants() {
        assert_eq!(ModifierOp::Add, ModifierOp::Add);
        assert_eq!(ModifierOp::Mul, ModifierOp::Mul);
        assert_ne!(ModifierOp::Add, ModifierOp::Override);
    }

    #[test]
    fn modifier_new() {
        let m = Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 0).unwrap(),
        );
        assert_eq!(m.target_table, "t");
        assert_eq!(m.target_column, "c");
        assert_eq!(m.op, ModifierOp::Add);
        assert_eq!(m.value, FixedPoint::from_i64(5, 0).unwrap());
    }

    #[test]
    fn modifier_apply_add() -> RuleResult<()> {
        let m = Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 0).unwrap(),
        );
        let result = m.apply(FixedPoint::from_i64(10, 0).unwrap())?;
        assert_eq!(result, FixedPoint::from_i64(15, 0).unwrap());
        Ok(())
    }

    #[test]
    fn registry_empty_returns_base() -> RuleResult<()> {
        let reg = ModifierRegistry::new();
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(42, 0).unwrap())?;
        assert_eq!(result, FixedPoint::from_i64(42, 0).unwrap());
        Ok(())
    }

    #[test]
    fn registry_unknown_field_returns_base() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t1",
            "c1",
            ModifierOp::Add,
            FixedPoint::from_i64(1, 0).unwrap(),
        ));
        let result = reg.aggregate("t2", "c2", FixedPoint::from_i64(100, 0).unwrap())?;
        assert_eq!(result, FixedPoint::from_i64(100, 0).unwrap());
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // A. ModifierOp Application
    // ---------------------------------------------------------------------------

    /// ModifierOp::Mul applies multiplication (10 * 1.5 = 15,
    /// accounting for FixedPoint internal scale).
    #[test]
    fn dc1_modifier_apply_mul() -> RuleResult<()> {
        // 1.5 at scale 2 is raw = 150.
        let m = Modifier::new("t", "c", ModifierOp::Mul, FixedPoint::new(150, 2));
        let base = FixedPoint::from_i64(10, 2).unwrap();
        let result = m.apply(base)?;
        // 10.00 * 1.50 = 15.00 => raw = 1500 at scale 2
        assert_eq!(result, FixedPoint::from_i64(15, 2).unwrap());
        Ok(())
    }

    /// ModifierOp::Override replaces base entirely.
    #[test]
    fn dc1_modifier_apply_override() -> RuleResult<()> {
        let m = Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(99, 2).unwrap(),
        );
        let base = FixedPoint::from_i64(10, 2).unwrap();
        let result = m.apply(base)?;
        // Override returns the modifier value as-is (preserving its own scale).
        assert_eq!(result, FixedPoint::from_i64(99, 2).unwrap());
        Ok(())
    }

    /// Add with different scales rescaled internally before the
    /// arithmetic operation.
    #[test]
    fn dc1_modifier_apply_add_with_different_scale() -> RuleResult<()> {
        // modifier value at scale 1 (raw = 50 = 5.0)
        let m = Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 1).unwrap(),
        );
        // base at scale 2 (raw = 1000 = 10.00)
        let base = FixedPoint::from_i64(10, 2).unwrap();
        let result = m.apply(base)?;
        // rescale 5.0 (scale 1) -> 5.00 (scale 2): raw 50 -> 500
        // 10.00 + 5.00 = 15.00
        assert_eq!(result, FixedPoint::from_i64(15, 2).unwrap());
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // B. Modifier Serialization
    // ---------------------------------------------------------------------------

    /// / serde roundtrip for Modifier.
    #[test]
    fn modifier_serialization_roundtrip() -> RuleResult<()> {
        let m = Modifier::new(
            "health",
            "max_hp",
            ModifierOp::Mul,
            FixedPoint::from_i64(2, 1).unwrap(),
        );
        let json = serde_json::to_string(&m).map_err(|e| RuleError::Generic(e.to_string()))?;
        let back: Modifier =
            serde_json::from_str(&json).map_err(|e| RuleError::Generic(e.to_string()))?;
        assert_eq!(m, back);
        Ok(())
    }

    /// / serde roundtrip for ModifierOp.
    #[test]
    fn modifier_op_serialization_roundtrip() -> RuleResult<()> {
        let ops = [ModifierOp::Add, ModifierOp::Mul, ModifierOp::Override];
        for op in ops {
            let json = serde_json::to_string(&op).map_err(|e| RuleError::Generic(e.to_string()))?;
            let back: ModifierOp =
                serde_json::from_str(&json).map_err(|e| RuleError::Generic(e.to_string()))?;
            assert_eq!(op, back);
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // C. ModifierRegistry Basic Operations
    // ---------------------------------------------------------------------------

    /// a brand-new registry has len == 0 and is_empty == true.
    #[test]
    fn registry_new_is_empty() {
        let reg = ModifierRegistry::new();
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
    }

    /// registering one modifier increments len and makes the registry
    /// non-empty.
    #[test]
    fn registry_register_adds_modifier() {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(1, 0).unwrap(),
        ));
        assert_eq!(reg.len(), 1);
        assert!(!reg.is_empty());
    }

    /// register two modifiers for the same field, then clear_field
    /// removes all of them.
    #[test]
    fn registry_clear_field_removes_all_modifiers() {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(150, 2),
        ));
        assert_eq!(reg.len(), 2);
        reg.clear_field("t", "c");
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
        assert!(reg.modifiers_for("t", "c").is_none());
    }

    /// register 3 modifiers for the same field, verify the slice
    /// returned by modifiers_for.
    #[test]
    fn registry_modifiers_for_returns_correct_modifiers() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        let m1 = Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(1, 2).unwrap(),
        );
        let m2 = Modifier::new("t", "c", ModifierOp::Mul, FixedPoint::new(150, 2));
        let m3 = Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(42, 2).unwrap(),
        );
        reg.register(m1.clone());
        reg.register(m2.clone());
        reg.register(m3.clone());

        let slice = reg
            .modifiers_for("t", "c")
            .ok_or_else(|| RuleError::Generic("expected modifiers".into()))?;
        assert_eq!(slice.len(), 3);
        assert_eq!(&slice[0], &m1);
        assert_eq!(&slice[1], &m2);
        assert_eq!(&slice[2], &m3);
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // D. Aggregation Chains
    // ---------------------------------------------------------------------------

    /// base=100, Add(+10), Mul(*0.5) => (100+10)*0.5 = 55.
    #[test]
    fn dc1_aggregate_add_then_mul_chain() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(10, 2).unwrap(),
        ));
        // 0.5 at scale 2 => raw = 50
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(50, 2),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 2).unwrap())?;
        assert_eq!(result, FixedPoint::from_i64(55, 2).unwrap());
        Ok(())
    }

    /// register Mul first then Add first; aggregation order is always
    /// Add -> Mul regardless of registration order, so result is still
    /// (100+10)*0.5 = 55.
    #[test]
    fn dc1_aggregate_mul_then_add_chain() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        // Mul registered BEFORE Add, but Add is always applied first.
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(50, 2),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(10, 2).unwrap(),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 2).unwrap())?;
        // Order is always Add -> Mul: (100+10)*0.5 = 55.
        assert_eq!(result, FixedPoint::from_i64(55, 2).unwrap());
        Ok(())
    }

    /// multiple Add modifiers are summed together.
    #[test]
    fn dc1_aggregate_multiple_adds() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(10, 2).unwrap(),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 2).unwrap())?;
        // 100 + 5 + 10 = 115
        assert_eq!(result, FixedPoint::from_i64(115, 2).unwrap());
        Ok(())
    }

    /// multiple Mul modifiers are multiplied in sequence:
    /// base * 1.5 * 0.5 = base * 0.75.
    #[test]
    fn dc1_aggregate_multiple_muls() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        // Add(+0) establishes the pipeline; Muls chain afterwards.
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(0, 2).unwrap(),
        ));
        // 1.5 at scale 2 => raw = 150
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(150, 2),
        ));
        // 0.5 at scale 2 => raw = 50
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(50, 2),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 2).unwrap())?;
        // (100+0)*1.5*0.5 = 75
        assert_eq!(result, FixedPoint::from_i64(75, 2).unwrap());
        Ok(())
    }

    /// Override trumps all Add/Mul modifiers.
    #[test]
    fn dc1_aggregate_override_wins() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(100, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(42, 2).unwrap(),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 2).unwrap())?;
        // Override wins => 42.
        assert_eq!(result, FixedPoint::from_i64(42, 2).unwrap());
        Ok(())
    }

    /// when multiple Overrides are registered, the last one wins.
    #[test]
    fn dc1_aggregate_last_override_wins() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(10, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(20, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(30, 2).unwrap(),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(0, 2).unwrap())?;
        // Last Override (30) wins.
        assert_eq!(result, FixedPoint::from_i64(30, 2).unwrap());
        Ok(())
    }

    /// when both Mul and Override exist, Override wins and both
    /// Add/Mul are ignored.
    #[test]
    fn dc1_aggregate_override_with_mul_ignores_both() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        // Mul *2.0 => raw = 200 at scale 2
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(200, 2),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(42, 2).unwrap(),
        ));
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 2).unwrap())?;
        // Override trumps Mul => 42.
        assert_eq!(result, FixedPoint::from_i64(42, 2).unwrap());
        Ok(())
    }

    /// modifiers for different fields do not interfere with each other.
    #[test]
    fn dc1_aggregate_aggregates_only_specific_field() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        // Field (t1, c1): Add +5
        reg.register(Modifier::new(
            "t1",
            "c1",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 2).unwrap(),
        ));
        // Field (t2, c2): Mul *2.0 => raw = 200 at scale 2
        reg.register(Modifier::new(
            "t2",
            "c2",
            ModifierOp::Mul,
            FixedPoint::new(200, 2),
        ));

        let r1 = reg.aggregate("t1", "c1", FixedPoint::from_i64(100, 2).unwrap())?;
        let r2 = reg.aggregate("t2", "c2", FixedPoint::from_i64(100, 2).unwrap())?;
        assert_eq!(r1, FixedPoint::from_i64(105, 2).unwrap());
        assert_eq!(r2, FixedPoint::from_i64(200, 2).unwrap());
        Ok(())
    }

    /// base is at scale 1 while the modifier is at scale 2; the
    /// modifier value is rescaled during apply so aggregation produces
    /// a consistent result.
    #[test]
    fn dc1_aggregate_modifier_on_different_scale_rescales() -> RuleResult<()> {
        let mut reg = ModifierRegistry::new();
        // Add +5 at scale 2: raw = 500
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(5, 2).unwrap(),
        ));
        // base at scale 1: raw = 1000 (100.0)
        let result = reg.aggregate("t", "c", FixedPoint::from_i64(100, 1).unwrap())?;
        // rescale modifier from scale 2 to scale 1: 500/10 = 50
        // 1000 + 50 = 1050 at scale 1 => represented as 105.0
        assert_eq!(result, FixedPoint::from_i64(105, 1).unwrap());
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // E. Registry Iteration
    // ---------------------------------------------------------------------------

    /// fields iterator yields every registered (table, column) pair.
    #[test]
    fn registry_fields_iterates_all_fields() {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t1",
            "c1",
            ModifierOp::Add,
            FixedPoint::from_i64(1, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t2",
            "c2",
            ModifierOp::Mul,
            FixedPoint::new(150, 2),
        ));
        reg.register(Modifier::new(
            "t3",
            "c3",
            ModifierOp::Override,
            FixedPoint::from_i64(42, 2).unwrap(),
        ));

        let mut fields: Vec<(&str, &str)> = reg.fields().collect();
        fields.sort();
        assert_eq!(fields, vec![("t1", "c1"), ("t2", "c2"), ("t3", "c3")]);
    }

    /// len reflects the current number of registered modifier
    /// entries; clearing a field drops its entries.
    #[test]
    fn registry_len_after_register_and_clear() {
        let mut reg = ModifierRegistry::new();
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Add,
            FixedPoint::from_i64(1, 2).unwrap(),
        ));
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Mul,
            FixedPoint::new(150, 2),
        ));
        assert_eq!(reg.len(), 2);
        reg.clear_field("t", "c");
        assert_eq!(reg.len(), 0);
        // re-registering should bring len back
        reg.register(Modifier::new(
            "t",
            "c",
            ModifierOp::Override,
            FixedPoint::from_i64(7, 2).unwrap(),
        ));
        assert_eq!(reg.len(), 1);
    }
}
