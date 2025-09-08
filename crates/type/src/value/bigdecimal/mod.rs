// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	hash::{Hash, Hasher},
	ops::Deref,
	str::FromStr,
};

use bigdecimal::{BigDecimal as StdBigDecimal, FromPrimitive, Zero};
use serde::{Deserialize, Serialize};

/// A wrapper type for arbitrary-precision decimal numbers
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BigDecimal(pub StdBigDecimal);

impl BigDecimal {
	/// Create a new BigDecimal from an f64
	pub fn from_f64(value: f64) -> Option<Self> {
		StdBigDecimal::from_f64(value).map(BigDecimal)
	}

	/// Create a new BigDecimal from an f32
	pub fn from_f32(value: f32) -> Option<Self> {
		StdBigDecimal::from_f32(value).map(BigDecimal)
	}

	/// Create a new BigDecimal from an i64
	pub fn from_i64(value: i64) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}

	/// Create a new BigDecimal from an i128
	pub fn from_i128(value: i128) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}

	/// Parse a BigDecimal from a string
	pub fn from_str(
		s: &str,
	) -> Result<Self, bigdecimal::ParseBigDecimalError> {
		StdBigDecimal::from_str(s).map(BigDecimal)
	}

	/// Create a BigDecimal representing zero
	pub fn zero() -> Self {
		BigDecimal(StdBigDecimal::from(0))
	}

	/// Create a BigDecimal representing one
	pub fn one() -> Self {
		BigDecimal(StdBigDecimal::from(1))
	}

	/// Round to a specific number of decimal places
	pub fn round(&self, digits: i64) -> Self {
		BigDecimal(self.0.round(digits))
	}

	/// Check if the value is zero
	pub fn is_zero(&self) -> bool {
		self.0.is_zero()
	}
}

impl Default for BigDecimal {
	fn default() -> Self {
		Self::zero()
	}
}

impl Deref for BigDecimal {
	type Target = StdBigDecimal;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for BigDecimal {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.normalized().hash(state);
	}
}

impl PartialOrd for BigDecimal {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.0.partial_cmp(&other.0)
	}
}

impl Ord for BigDecimal {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl From<i32> for BigDecimal {
	fn from(value: i32) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}
}

impl From<i64> for BigDecimal {
	fn from(value: i64) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}
}

impl From<i128> for BigDecimal {
	fn from(value: i128) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}
}

impl From<u32> for BigDecimal {
	fn from(value: u32) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}
}

impl From<u64> for BigDecimal {
	fn from(value: u64) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}
}

impl From<u128> for BigDecimal {
	fn from(value: u128) -> Self {
		BigDecimal(StdBigDecimal::from(value))
	}
}

impl FromStr for BigDecimal {
	type Err = bigdecimal::ParseBigDecimalError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		StdBigDecimal::from_str(s).map(BigDecimal)
	}
}

impl From<StdBigDecimal> for BigDecimal {
	fn from(value: StdBigDecimal) -> Self {
		BigDecimal(value)
	}
}

impl From<BigDecimal> for StdBigDecimal {
	fn from(bigdecimal: BigDecimal) -> Self {
		bigdecimal.0
	}
}

impl Display for BigDecimal {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_bigdecimal_create() {
		let bigdec = BigDecimal::from_i64(42);
		assert_eq!(format!("{}", bigdec), "42");
	}

	#[test]
	fn test_bigdecimal_from_string() {
		let bigdec = BigDecimal::from_str("3.14159").unwrap();
		assert_eq!(format!("{}", bigdec), "3.14159");
	}

	#[test]
	fn test_bigdecimal_equality() {
		let a = BigDecimal::from_str("100.00").unwrap();
		let b = BigDecimal::from_str("100.0").unwrap();
		let c = BigDecimal::from_str("200.0").unwrap();

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_bigdecimal_ordering() {
		let a = BigDecimal::from_str("10.5").unwrap();
		let b = BigDecimal::from_str("20.3").unwrap();
		let c = BigDecimal::from_str("20.3").unwrap();

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_bigdecimal_from_float() {
		let bigdec = BigDecimal::from_f64(3.14159).unwrap();
		let s = format!("{}", bigdec);
		// Float conversion may have precision differences, so we check
		// for the beginning
		assert!(s.starts_with("3.1415"));
	}

	#[test]
	fn test_bigdecimal_round() {
		let bigdec = BigDecimal::from_str("3.14159").unwrap();
		let rounded = bigdec.round(2);
		assert_eq!(format!("{}", rounded), "3.14");
	}

	#[test]
	fn test_bigdecimal_display() {
		let bigdec = BigDecimal::from_str("-123.456").unwrap();
		assert_eq!(format!("{}", bigdec), "-123.456");
	}

	#[test]
	fn test_bigdecimal_zero() {
		let zero = BigDecimal::zero();
		assert!(zero.is_zero());
		assert_eq!(format!("{}", zero), "0");
	}

	#[test]
	fn test_bigdecimal_hash() {
		use std::collections::HashSet;

		let a = BigDecimal::from_str("42.0").unwrap();
		let b = BigDecimal::from_str("42.00").unwrap();

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}
}
