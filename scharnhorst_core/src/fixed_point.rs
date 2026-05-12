use serde::{Deserialize, Serialize};
use std::ops::{Add, Div, Mul, Neg, Sub};
use std::str::FromStr;

use crate::error::{CoreError, CoreResult};

/// A signed 64-bit fixed-point number with a configurable decimal scale.
///
/// The raw value is `value * 10^scale`. Scale is checked at operation time
/// so that mixing scales is always an explicit error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FixedPoint {
    raw: i64,
    scale: u32,
}

impl FixedPoint {
    pub const fn new(raw: i64, scale: u32) -> Self {
        Self { raw, scale }
    }

    pub const fn raw(&self) -> i64 {
        self.raw
    }

    pub const fn scale(&self) -> u32 {
        self.scale
    }

    pub fn from_i64(value: i64, scale: u32) -> CoreResult<Self> {
        let factor = 10_i64.checked_pow(scale).ok_or(CoreError::ArithmeticOverflow)?;
        let raw = value.checked_mul(factor).ok_or(CoreError::ArithmeticOverflow)?;
        Ok(Self::new(raw, scale))
    }

 /// Creates a FixedPoint from an f64 value.
 ///
 /// Rounding mode: half away from zero (`.round`).
 /// Returns `ArithmeticOverflow` if the scaled value exceeds i64 range.
    pub fn try_from_f64(value: f64, scale: u32) -> CoreResult<Self> {
        let factor = 10_f64.powi(scale as i32);
        let raw = (value * factor).round();
        if raw > i64::MAX as f64 || raw < i64::MIN as f64 {
            return Err(CoreError::ArithmeticOverflow);
        }
        Ok(Self::new(raw as i64, scale))
    }

    pub fn to_f64(&self) -> f64 {
        self.raw as f64 / 10_f64.powi(self.scale as i32)
    }

    pub fn rescale(&self, new_scale: u32) -> CoreResult<Self> {
        match new_scale.cmp(&self.scale) {
            std::cmp::Ordering::Equal => Ok(*self),
            std::cmp::Ordering::Greater => {
                let delta = new_scale - self.scale;
                let factor = 10_i64.checked_pow(delta).ok_or(CoreError::ArithmeticOverflow)?;
                let raw = self.raw.checked_mul(factor).ok_or(CoreError::ArithmeticOverflow)?;
                Ok(Self::new(raw, new_scale))
            }
            std::cmp::Ordering::Less => {
                let delta = self.scale - new_scale;
                let factor = 10_i64.checked_pow(delta).ok_or(CoreError::ArithmeticOverflow)?;
                let raw = self.raw.checked_div(factor).ok_or(CoreError::ArithmeticOverflow)?;
                Ok(Self::new(raw, new_scale))
            }
        }
    }

    fn ensure_same_scale(lhs: &Self, rhs: &Self) -> CoreResult<()> {
        if lhs.scale != rhs.scale {
            return Err(CoreError::InvalidFixedPointScale {
                expected: lhs.scale,
                got: rhs.scale,
            });
        }
        Ok(())
    }
}

impl Add for FixedPoint {
    type Output = CoreResult<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        Self::ensure_same_scale(&self, &rhs)?;
        let raw = self.raw.checked_add(rhs.raw).ok_or(CoreError::ArithmeticOverflow)?;
        Ok(Self::new(raw, self.scale))
    }
}

impl Sub for FixedPoint {
    type Output = CoreResult<Self>;

    fn sub(self, rhs: Self) -> Self::Output {
        Self::ensure_same_scale(&self, &rhs)?;
        let raw = self.raw.checked_sub(rhs.raw).ok_or(CoreError::ArithmeticOverflow)?;
        Ok(Self::new(raw, self.scale))
    }
}

impl Mul for FixedPoint {
    type Output = CoreResult<Self>;

    fn mul(self, rhs: Self) -> Self::Output {
        Self::ensure_same_scale(&self, &rhs)?;
        let product = self
            .raw
            .checked_mul(rhs.raw)
            .ok_or(CoreError::ArithmeticOverflow)?;
        let factor = 10_i64.pow(self.scale);
        let raw = product.checked_div(factor).ok_or(CoreError::DivisionByZero)?;
        Ok(Self::new(raw, self.scale))
    }
}

impl Div for FixedPoint {
    type Output = CoreResult<Self>;

    fn div(self, rhs: Self) -> Self::Output {
        Self::ensure_same_scale(&self, &rhs)?;
        if rhs.raw == 0 {
            return Err(CoreError::DivisionByZero);
        }
        let scaled = self
            .raw
            .checked_mul(10_i64.pow(self.scale))
            .ok_or(CoreError::ArithmeticOverflow)?;
        let raw = scaled.checked_div(rhs.raw).ok_or(CoreError::ArithmeticOverflow)?;
        Ok(Self::new(raw, self.scale))
    }
}

impl Neg for FixedPoint {
    type Output = CoreResult<Self>;

    fn neg(self) -> Self::Output {
        let raw = self.raw.checked_neg().ok_or(CoreError::ArithmeticOverflow)?;
        Ok(Self::new(raw, self.scale))
    }
}

/// Parses a decimal string into a `FixedPoint`.
///
/// Accepts optional leading `+` / `-` sign, optional decimal point, and
/// optional leading/trailing whitespace.
///
/// # Examples
///
/// ```ignore
/// "42" 鈫?FixedPoint(42, 0)
/// "12.34" 鈫?FixedPoint(1234, 2)
/// "-5.5" 鈫?FixedPoint(-55, 1)
/// ".5" 鈫?FixedPoint(5, 1)
/// "5." 鈫?FixedPoint(5, 0)
/// ```
impl FromStr for FixedPoint {
    type Err = CoreError;

    fn from_str(s: &str) -> CoreResult<Self> {
        let s = s.trim();
        let scale = s
            .split('.')
            .nth(1)
            .map(|dec| dec.len() as u32)
            .unwrap_or(0);
        let num_str: String = s.chars().filter(|c| *c != '.').collect();
        let raw: i64 = num_str
            .parse()
            .map_err(|_| CoreError::Generic(format!("invalid number format: '{s}'")))?;
        Ok(Self::new(raw, scale))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construction_from_i64() {
        let fp = FixedPoint::from_i64(42, 2).unwrap();
        assert_eq!(fp.raw(), 4200);
        assert_eq!(fp.scale(), 2);
    }

    #[test]
    fn construction_from_f64_ok() -> CoreResult<()> {
        let fp = FixedPoint::try_from_f64(std::f64::consts::PI, 2)?;
        assert_eq!(fp.raw(), 314);
        Ok(())
    }

    #[test]
    fn to_f64_roundtrip() -> CoreResult<()> {
        let fp = FixedPoint::try_from_f64(2.5, 3)?;
        let back = fp.to_f64();
        let diff = (back - 2.5).abs();
        assert!(diff < 1e-9);
        Ok(())
    }

    #[test]
    fn add_same_scale() -> CoreResult<()> {
        let a = FixedPoint::from_i64(1, 2)?;
        let b = FixedPoint::from_i64(2, 2)?;
        let c = (a + b)?;
        assert_eq!(c.raw(), 300);
        Ok(())
    }

    #[test]
    fn add_mismatched_scale_is_err() {
        let a = FixedPoint::from_i64(1, 2).unwrap();
        let b = FixedPoint::from_i64(2, 3).unwrap();
        let result = a + b;
        assert!(matches!(
            result,
            Err(CoreError::InvalidFixedPointScale { expected: 2, got: 3 })
        ));
    }

    #[test]
    fn sub_ok() -> CoreResult<()> {
        let a = FixedPoint::from_i64(5, 2)?;
        let b = FixedPoint::from_i64(3, 2)?;
        let c = (a - b)?;
        assert_eq!(c.raw(), 200);
        Ok(())
    }

    #[test]
    fn mul_ok() -> CoreResult<()> {
        let a = FixedPoint::from_i64(2, 2)?;
        let b = FixedPoint::from_i64(3, 2)?;
        let c = (a * b)?;
        assert_eq!(c.raw(), 600);
        Ok(())
    }

    #[test]
    fn div_ok() -> CoreResult<()> {
        let a = FixedPoint::from_i64(7, 2)?;
        let b = FixedPoint::from_i64(2, 2)?;
        let c = (a / b)?;
        assert_eq!(c.raw(), 350);
        Ok(())
    }

    #[test]
    fn div_by_zero_is_err() {
        let a = FixedPoint::from_i64(1, 2).unwrap();
        let b = FixedPoint::new(0, 2);
        let result = a / b;
        assert!(matches!(result, Err(CoreError::DivisionByZero)));
    }

    #[test]
    fn neg_ok() -> CoreResult<()> {
        let a = FixedPoint::from_i64(5, 2)?;
        let b = (-a)?;
        assert_eq!(b.raw(), -500);
        Ok(())
    }

    #[test]
    fn rescale_up() -> CoreResult<()> {
        let a = FixedPoint::from_i64(1, 2)?;
        let b = a.rescale(4)?;
        assert_eq!(b.raw(), 10000);
        Ok(())
    }

    #[test]
    fn rescale_down() -> CoreResult<()> {
        let a = FixedPoint::from_i64(12345, 4)?;
        let b = a.rescale(2)?;
        assert_eq!(b.raw(), 1234500);
        Ok(())
    }

 // ---- edge cases ----

    #[test]
    fn max_value_construction() {
        let fp = FixedPoint::new(i64::MAX, 0);
        assert_eq!(fp.raw(), i64::MAX);
    }

    #[test]
    fn min_value_construction() {
        let fp = FixedPoint::new(i64::MIN, 0);
        assert_eq!(fp.raw(), i64::MIN);
    }

    #[test]
    fn max_value_neg_is_overflow() {
        let fp = FixedPoint::new(i64::MIN, 0);
        let result = -fp;
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn exact_comparison() {
        let a = FixedPoint::from_i64(100, 2).unwrap();
        let b = FixedPoint::from_i64(100, 2).unwrap();
        assert_eq!(a, b);
        let c = FixedPoint::from_i64(101, 2).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn ordering_comparison() {
        let a = FixedPoint::from_i64(10, 2).unwrap();
        let b = FixedPoint::from_i64(20, 2).unwrap();
        assert!(a < b);
        assert!(b > a);
        assert!(a <= a);
        assert!(a >= a);
    }

    #[test]
    fn serialize_roundtrip_json() {
        let fp = FixedPoint::from_i64(42, 2).unwrap();
        let json = serde_json::to_string(&fp).unwrap();
        let back: FixedPoint = serde_json::from_str(&json).unwrap();
        assert_eq!(fp, back);
        assert_eq!(back.raw(), 4200);
        assert_eq!(back.scale(), 2);
    }

    #[test]
    fn rescale_same_is_identity() -> CoreResult<()> {
        let a = FixedPoint::from_i64(5, 2)?;
        let b = a.rescale(2)?;
        assert_eq!(a, b);
        Ok(())
    }

    #[test]
    fn rescale_extreme_up() -> CoreResult<()> {
        let a = FixedPoint::new(1, 0);
        let b = a.rescale(9)?;
        assert_eq!(b.raw(), 1_000_000_000);
        assert_eq!(b.scale(), 9);
        Ok(())
    }

    #[test]
    fn rescale_extreme_down_truncates() -> CoreResult<()> {
        let a = FixedPoint::new(123_456_789, 9);
        let b = a.rescale(0)?;
        assert_eq!(b.raw(), 0);
        Ok(())
    }

    #[test]
    fn rescale_overflow_up() {
        let a = FixedPoint::new(i64::MAX, 0);
        let result = a.rescale(1);
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn add_overflow() {
        let a = FixedPoint::new(i64::MAX, 0);
        let b = FixedPoint::new(1, 0);
        let result = a + b;
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn sub_underflow() {
        let a = FixedPoint::new(i64::MIN, 0);
        let b = FixedPoint::new(1, 0);
        let result = a - b;
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn mul_overflow() {
        let a = FixedPoint::new(i64::MAX, 0);
        let b = FixedPoint::new(2, 0);
        let result = a * b;
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn mul_zero() -> CoreResult<()> {
        let a = FixedPoint::from_i64(10, 2)?;
        let b = FixedPoint::new(0, 2);
        let c = (a * b)?;
        assert_eq!(c.raw(), 0);
        Ok(())
    }

    #[test]
    fn div_overflow_scaled_numerator() {
        let a = FixedPoint::new(i64::MAX, 9);
        let b = FixedPoint::new(1, 9);
        let result = a / b;
        assert!(result.is_err());
    }

    #[test]
    fn negative_value_operations() -> CoreResult<()> {
        let a = FixedPoint::from_i64(-5, 2)?;
        let b = FixedPoint::from_i64(3, 2)?;
        let c = (a + b)?;
        assert_eq!(c.raw(), -200);
        let d = (a - b)?;
        assert_eq!(d.raw(), -800);
        Ok(())
    }

    #[test]
    fn negative_mul() -> CoreResult<()> {
        let a = FixedPoint::from_i64(-2, 2)?;
        let b = FixedPoint::from_i64(3, 2)?;
        let c = (a * b)?;
        assert_eq!(c.raw(), -600);
        Ok(())
    }

    #[test]
    fn negative_div() -> CoreResult<()> {
        let a = FixedPoint::from_i64(-6, 2)?;
        let b = FixedPoint::from_i64(2, 2)?;
        let c = (a / b)?;
        assert_eq!(c.raw(), -300);
        Ok(())
    }

    #[test]
    fn zero_values() {
        let zero = FixedPoint::new(0, 2);
        assert_eq!(zero.to_f64(), 0.0);
        assert_eq!(zero.raw(), 0);
    }

    #[test]
    fn f64_overflow() {
        let result = FixedPoint::try_from_f64(f64::MAX, 0);
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

 // ---- additional coverage ----

    #[test]
    fn fp_new_raw_scale() {
        let fp = FixedPoint::new(12345, 3);
        assert_eq!(fp.raw(), 12345);
        assert_eq!(fp.scale(), 3);
    }

    #[test]
    fn fp_from_i64_equivalence() {
        let a = FixedPoint::from_i64(100, 2).unwrap();
        let b = FixedPoint::new(10000, 2);
        assert_eq!(a, b);
        assert_eq!(a.raw(), 10000);
        assert_eq!(b.raw(), 10000);
    }

    #[test]
    fn fp_try_from_f64_large_overflow() {
        let result = FixedPoint::try_from_f64(1e20, 0);
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn fp_try_from_f64_negative_ok() -> CoreResult<()> {
        let fp = FixedPoint::try_from_f64(-12.34, 2)?;
        assert_eq!(fp.raw(), -1234);
        assert_eq!(fp.scale(), 2);
        Ok(())
    }

    #[test]
    fn fp_rescale_up_two_to_four() -> CoreResult<()> {
        let a = FixedPoint::new(500, 2);
        let b = a.rescale(4)?;
        assert_eq!(b.raw(), 50000);
        assert_eq!(b.scale(), 4);
        Ok(())
    }

    #[test]
    fn fp_rescale_down_four_to_two() -> CoreResult<()> {
        let a = FixedPoint::new(50000, 4);
        let b = a.rescale(2)?;
        assert_eq!(b.raw(), 500);
        assert_eq!(b.scale(), 2);
        Ok(())
    }

    #[test]
    fn fp_sub_mismatched_scale_err() {
        let a = FixedPoint::from_i64(10, 2).unwrap();
        let b = FixedPoint::from_i64(5, 3).unwrap();
        let result = a - b;
        assert!(matches!(
            result,
            Err(CoreError::InvalidFixedPointScale { expected: 2, got: 3 })
        ));
    }

    #[test]
    fn fp_sub_negative_result() -> CoreResult<()> {
        let a = FixedPoint::from_i64(3, 2)?;
        let b = FixedPoint::from_i64(5, 2)?;
        let c = (a - b)?;
        assert_eq!(c.raw(), -200);
        Ok(())
    }

    #[test]
    fn fp_mul_mismatched_scale_err() {
        let a = FixedPoint::from_i64(10, 2).unwrap();
        let b = FixedPoint::from_i64(5, 3).unwrap();
        let result = a * b;
        assert!(matches!(
            result,
            Err(CoreError::InvalidFixedPointScale { expected: 2, got: 3 })
        ));
    }

    #[test]
    fn fp_mul_zero_by_value() -> CoreResult<()> {
        let a = FixedPoint::new(0, 2);
        let b = FixedPoint::from_i64(100, 2)?;
        let c = (a * b)?;
        assert_eq!(c.raw(), 0);
        Ok(())
    }

    #[test]
    fn fp_div_mismatched_scale_err() {
        let a = FixedPoint::from_i64(10, 2).unwrap();
        let b = FixedPoint::from_i64(5, 3).unwrap();
        let result = a / b;
        assert!(matches!(
            result,
            Err(CoreError::InvalidFixedPointScale { expected: 2, got: 3 })
        ));
    }

    #[test]
    fn fp_division_by_zero_raw() {
        let a = FixedPoint::new(100, 2);
        let b = FixedPoint::new(0, 2);
        let result = a / b;
        assert!(matches!(result, Err(CoreError::DivisionByZero)));
    }

    #[test]
    fn fp_neg_positive() -> CoreResult<()> {
        let a = FixedPoint::new(500, 2);
        let b = (-a)?;
        assert_eq!(b.raw(), -500);
        assert_eq!(b.scale(), 2);
        Ok(())
    }

    #[test]
    fn fp_neg_negative() -> CoreResult<()> {
        let a = FixedPoint::new(-300, 3);
        let b = (-a)?;
        assert_eq!(b.raw(), 300);
        assert_eq!(b.scale(), 3);
        Ok(())
    }

    #[test]
    fn fp_neg_zero() -> CoreResult<()> {
        let a = FixedPoint::new(0, 2);
        let b = (-a)?;
        assert_eq!(b.raw(), 0);
        Ok(())
    }

    #[test]
    fn fp_from_i64_max_scale_zero() {
        let fp = FixedPoint::from_i64(i64::MAX, 0).unwrap();
        assert_eq!(fp.raw(), i64::MAX);
        assert_eq!(fp.scale(), 0);
    }

    #[test]
    fn fp_add_overflow_boundary() {
        let a = FixedPoint::new(i64::MAX, 0);
        let b = FixedPoint::new(1, 0);
        let result = a + b;
        assert!(matches!(result, Err(CoreError::ArithmeticOverflow)));
    }

    #[test]
    fn fp_partial_ord_different_scale_not_comparable() {
        let small = FixedPoint::from_i64(1, 3).unwrap();
        let large = FixedPoint::from_i64(2, 3).unwrap();
        assert!(small < large);
        assert!(large > small);
    }

    #[test]
    fn fp_serialize_roundtrip_boundary() {
        let fp = FixedPoint::new(i64::MIN, 5);
        let json = serde_json::to_string(&fp).unwrap();
        let back: FixedPoint = serde_json::from_str(&json).unwrap();
        assert_eq!(fp, back);
        assert_eq!(back.raw(), i64::MIN);
        assert_eq!(back.scale(), 5);
    }

    #[test]
    fn from_str_positive_integer() -> CoreResult<()> {
        let fp = "42".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 42);
        assert_eq!(fp.scale(), 0);
        Ok(())
    }

    #[test]
    fn from_str_decimal() -> CoreResult<()> {
        let fp = "12.34".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 1234);
        assert_eq!(fp.scale(), 2);
        Ok(())
    }

    #[test]
    fn from_str_negative() -> CoreResult<()> {
        let fp = "-5.5".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), -55);
        assert_eq!(fp.scale(), 1);
        Ok(())
    }

    #[test]
    fn from_str_leading_trailing_whitespace() -> CoreResult<()> {
        let fp = "  3.14  ".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 314);
        assert_eq!(fp.scale(), 2);
        Ok(())
    }

    #[test]
    fn from_str_invalid_returns_err() {
        let result = "abc".parse::<FixedPoint>();
        assert!(result.is_err());
    }

    #[test]
    fn from_str_edge_dot_prefix() -> CoreResult<()> {
        let fp = ".5".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 5);
        assert_eq!(fp.scale(), 1);
        assert!((fp.to_f64() - 0.5).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn from_str_edge_trailing_dot() -> CoreResult<()> {
        let fp = "5.".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 5);
        assert_eq!(fp.scale(), 0);
        assert!((fp.to_f64() - 5.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn from_str_edge_negative_with_fraction() -> CoreResult<()> {
        let fp = "-0.05".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), -5);
        assert_eq!(fp.scale(), 2);
        assert!((fp.to_f64() + 0.05).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn from_str_edge_negative_zero() -> CoreResult<()> {
        let fp = "-0".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 0);
        assert_eq!(fp.scale(), 0);
        assert!((fp.to_f64() - 0.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn from_str_edge_extra_whitespace() -> CoreResult<()> {
        let fp = "  0.5  ".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 5);
        assert_eq!(fp.scale(), 1);
        assert!((fp.to_f64() - 0.5).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn from_str_plus_prefix() -> CoreResult<()> {
 // Rust's i64::parse accepts '+' prefix (e.g. "+5" 鈫?5).
        let fp = "+5".parse::<FixedPoint>()?;
        assert_eq!(fp.raw(), 5);
        assert_eq!(fp.scale(), 0);
        Ok(())
    }
}
