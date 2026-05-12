use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::{CoreError, CoreResult};

/// A monotonically increasing simulation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tick(
 /// Deprecated: use [`Tick::as_u64`] instead.
    pub u64,
);

impl Tick {
    pub const ZERO: Self = Self(0);

 /// Access the inner `u64` value.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn prev(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }
}

impl fmt::Display for Tick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque identifier for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId(
 /// Deprecated: use [`TableId::as_u64`] instead.
    pub u64,
);

impl TableId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

 /// Access the inner `u64` value.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T{}", self.0)
    }
}

impl FromStr for TableId {
    type Err = CoreError;

    fn from_str(s: &str) -> CoreResult<Self> {
        let raw = s
            .strip_prefix('T')
            .unwrap_or(s)
            .parse::<u64>()
            .map_err(|_| CoreError::InvalidId(s.to_owned()))?;
        Ok(Self(raw))
    }
}

/// Opaque identifier for a row within a table.
///
/// Soft-deprecated: existing code (tests, debug formatting, serialization)
/// may continue using direct field access, but new production code SHOULD use
/// [`RowId::as_u64`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowId(
 /// Soft-deprecated: use [`RowId::as_u64`] instead.
    pub u64,
);

impl RowId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

 /// Access the inner `u64` value.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R{}", self.0)
    }
}

impl FromStr for RowId {
    type Err = CoreError;

    fn from_str(s: &str) -> CoreResult<Self> {
        let raw = s
            .strip_prefix('R')
            .unwrap_or(s)
            .parse::<u64>()
            .map_err(|_| CoreError::InvalidId(s.to_owned()))?;
        Ok(Self(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

 // ---- Tick ----

    #[test]
    fn tick_zero() {
        assert_eq!(Tick::ZERO, Tick(0));
        assert_eq!(Tick::ZERO.0, 0);
    }

    #[test]
    fn tick_next() {
        assert_eq!(Tick(0).next(), Tick(1));
        assert_eq!(Tick(41).next(), Tick(42));
 // saturating: max + 1 = max
        assert_eq!(Tick(u64::MAX).next(), Tick(u64::MAX));
    }

    #[test]
    fn tick_prev() {
        assert_eq!(Tick(1).prev(), Some(Tick(0)));
        assert_eq!(Tick(42).prev(), Some(Tick(41)));
        assert_eq!(Tick::ZERO.prev(), None);
    }

    #[test]
    fn tick_display() {
        assert_eq!(Tick(0).to_string(), "0");
        assert_eq!(Tick(99).to_string(), "99");
        assert_eq!(Tick(u64::MAX).to_string(), u64::MAX.to_string());
    }

    #[test]
    fn tick_ordering() {
        assert!(Tick(0) < Tick(1));
        assert!(Tick(5) > Tick(3));
        assert!(Tick(7) <= Tick(7));
        assert!(Tick(7) >= Tick(7));
    }

 // ---- TableId ----

    #[test]
    fn table_id_new() {
        let t = TableId::new(42);
        assert_eq!(t.0, 42);
    }

    #[test]
    fn table_id_display() {
        assert_eq!(TableId(0).to_string(), "T0");
        assert_eq!(TableId(1).to_string(), "T1");
        assert_eq!(TableId(999).to_string(), "T999");
    }

    #[test]
    fn table_id_from_str_with_prefix() {
        let t: TableId = "T42".parse().unwrap();
        assert_eq!(t, TableId(42));
    }

    #[test]
    fn table_id_from_str_without_prefix() {
        let t: TableId = "42".parse().unwrap();
        assert_eq!(t, TableId(42));
    }

    #[test]
    fn table_id_from_str_invalid() {
        let r: CoreResult<TableId> = "not_a_number".parse();
        assert!(r.is_err());
        let r: CoreResult<TableId> = "".parse();
        assert!(r.is_err());
    }

    #[test]
    fn table_id_ordering() {
        let a = TableId(10);
        let b = TableId(20);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, TableId(10));
    }

    #[test]
    fn table_id_serialize_roundtrip() {
        let t = TableId(42);
        let json = serde_json::to_string(&t).unwrap();
        let back: TableId = serde_json::from_str(&json).unwrap();
        assert_eq!(t, back);
    }

 // ---- RowId ----

    #[test]
    fn row_id_new() {
        let r = RowId::new(42);
        assert_eq!(r.0, 42);
    }

    #[test]
    fn row_id_display() {
        assert_eq!(RowId(0).to_string(), "R0");
        assert_eq!(RowId(1).to_string(), "R1");
        assert_eq!(RowId(999).to_string(), "R999");
    }

    #[test]
    fn row_id_from_str_with_prefix() {
        let r: RowId = "R42".parse().unwrap();
        assert_eq!(r, RowId(42));
    }

    #[test]
    fn row_id_from_str_without_prefix() {
        let r: RowId = "42".parse().unwrap();
        assert_eq!(r, RowId(42));
    }

    #[test]
    fn row_id_from_str_invalid() {
        let r: CoreResult<RowId> = "abc".parse();
        assert!(r.is_err());
        let r: CoreResult<RowId> = "".parse();
        assert!(r.is_err());
    }

    #[test]
    fn row_id_ordering() {
        let a = RowId(5);
        let b = RowId(10);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, RowId(5));
    }

    #[test]
    fn row_id_serialize_roundtrip() {
        let r = RowId(42);
        let json = serde_json::to_string(&r).unwrap();
        let back: RowId = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }

 // ---- additional coverage ----

    #[test]
    fn tick_serialize_roundtrip() {
        let t = Tick(42);
        let json = serde_json::to_string(&t).unwrap();
        let back: Tick = serde_json::from_str(&json).unwrap();
        assert_eq!(t, back);

        let t_zero = Tick::ZERO;
        let json_zero = serde_json::to_string(&t_zero).unwrap();
        let back_zero: Tick = serde_json::from_str(&json_zero).unwrap();
        assert_eq!(t_zero, back_zero);
    }

    #[test]
    fn tick_debug() {
        assert_eq!(format!("{:?}", Tick(0)), "Tick(0)");
        assert_eq!(format!("{:?}", Tick(42)), "Tick(42)");
        assert_eq!(format!("{:?}", Tick(u64::MAX)), format!("Tick({})", u64::MAX));
    }

    #[test]
    fn tick_zero_identity() {
 // Tick::ZERO equals Tick(0)
        assert_eq!(Tick::ZERO, Tick(0));
 // next from ZERO yields Tick(1)
        assert_eq!(Tick::ZERO.next(), Tick(1));
 // prev from ZERO yields None
        assert!(Tick::ZERO.prev().is_none());
    }

    #[test]
    fn tick_next_n_equivalent() {
 // calling next n times on Tick(0) reaches Tick(n)
        let mut t = Tick(0);
        for i in 1..=5 {
            t = t.next();
            assert_eq!(t, Tick(i));
        }
    }

    #[test]
    fn table_id_debug() {
        assert_eq!(format!("{:?}", TableId(0)), "TableId(0)");
        assert_eq!(format!("{:?}", TableId(99)), "TableId(99)");
    }

    #[test]
    fn table_id_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = TableId(42);
        let b = TableId(42);
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());

 // different values should have different hashes (highly likely)
        let c = TableId(99);
        let mut hc = DefaultHasher::new();
        c.hash(&mut hc);
        assert_ne!(ha.finish(), hc.finish());
    }

    #[test]
    fn table_id_minimal_vs_nonzero() {
        let zero = TableId(0);
        let one = TableId(1);
        let large = TableId(u64::MAX);
        assert_eq!(zero.0, 0);
        assert_eq!(one.0, 1);
        assert_eq!(large.0, u64::MAX);
        assert_ne!(zero, one);
        assert!(zero < one);
    }

    #[test]
    fn row_id_debug() {
        assert_eq!(format!("{:?}", RowId(0)), "RowId(0)");
        assert_eq!(format!("{:?}", RowId(99)), "RowId(99)");
    }

    #[test]
    fn row_id_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let a = RowId(42);
        let b = RowId(42);
        let mut ha = DefaultHasher::new();
        let mut hb = DefaultHasher::new();
        a.hash(&mut ha);
        b.hash(&mut hb);
        assert_eq!(ha.finish(), hb.finish());

        let c = RowId(99);
        let mut hc = DefaultHasher::new();
        c.hash(&mut hc);
        assert_ne!(ha.finish(), hc.finish());
    }

    #[test]
    fn row_id_new_and_into() {
 // RowId::new wraps a u64
        let r = RowId::new(7);
        assert_eq!(r.0, 7);
 // field access is equivalent to new
        assert_eq!(r, RowId(7));
    }
}
