// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	hash::{Hash, Hasher},
	ops::Deref,
};

use num_bigint::BigInt as StdBigInt;
use num_traits::Signed;
use serde::{Deserialize, Serialize};

pub mod parse;
pub use parse::parse_varuint;

/// A wrapper type for arbitrary-precision unsigned integers (VarUint)
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VarUint(pub StdBigInt);

impl VarUint {
	/// Create a new VarUint from a u64
	pub fn from_u64(value: u64) -> Self {
		VarUint(StdBigInt::from(value))
	}

	/// Create a new VarUint from a u128
	pub fn from_u128(value: u128) -> Self {
		VarUint(StdBigInt::from(value))
	}

	/// Create a VarUint representing zero
	pub fn zero() -> Self {
		VarUint(StdBigInt::from(0))
	}

	/// Create a VarUint representing one
	pub fn one() -> Self {
		VarUint(StdBigInt::from(1))
	}

	/// Ensure the value is non-negative, converting negative values to zero
	fn ensure_non_negative(value: StdBigInt) -> StdBigInt {
		if value.is_negative() {
			StdBigInt::from(0)
		} else {
			value
		}
	}
}

impl Default for VarUint {
	fn default() -> Self {
		Self::zero()
	}
}

impl Deref for VarUint {
	type Target = StdBigInt;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for VarUint {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}

impl PartialOrd for VarUint {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for VarUint {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl From<u32> for VarUint {
	fn from(value: u32) -> Self {
		VarUint(StdBigInt::from(value))
	}
}

impl From<u64> for VarUint {
	fn from(value: u64) -> Self {
		VarUint(StdBigInt::from(value))
	}
}

impl From<u128> for VarUint {
	fn from(value: u128) -> Self {
		VarUint(StdBigInt::from(value))
	}
}

// Handle signed integer conversions by ensuring non-negative values
impl From<i32> for VarUint {
	fn from(value: i32) -> Self {
		VarUint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<i64> for VarUint {
	fn from(value: i64) -> Self {
		VarUint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<i128> for VarUint {
	fn from(value: i128) -> Self {
		VarUint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<StdBigInt> for VarUint {
	fn from(value: StdBigInt) -> Self {
		VarUint(Self::ensure_non_negative(value))
	}
}

impl From<VarUint> for StdBigInt {
	fn from(varuint: VarUint) -> Self {
		varuint.0
	}
}

impl Display for VarUint {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_varuint_create() {
		let varuint = VarUint::from_u64(42);
		assert_eq!(format!("{}", varuint), "42");
	}

	#[test]
	fn test_varuint_equality() {
		let a = VarUint::from_u64(100);
		let b = VarUint::from_u64(100);
		let c = VarUint::from_u64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_varuint_ordering() {
		let a = VarUint::from_u64(10);
		let b = VarUint::from_u64(20);
		let c = VarUint::from_u64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_varuint_large_values() {
		let large = VarUint::from_u128(u128::MAX);
		let larger = VarUint::from(StdBigInt::from(u128::MAX) + 1);

		assert!(large < larger);
	}

	#[test]
	fn test_varuint_display() {
		let varuint = VarUint::from_u64(12345);
		assert_eq!(format!("{}", varuint), "12345");
	}

	#[test]
	fn test_varuint_hash() {
		use std::collections::HashSet;

		let a = VarUint::from_u64(42);
		let b = VarUint::from_u64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}

	#[test]
	fn test_varuint_negative_input() {
		// Test that negative inputs are converted to zero
		let negative_i32 = VarUint::from(-42i32);
		let negative_i64 = VarUint::from(-999i64);
		let negative_i128 = VarUint::from(-12345i128);
		let negative_bigint = VarUint::from(StdBigInt::from(-777));

		assert_eq!(negative_i32, VarUint::zero());
		assert_eq!(negative_i64, VarUint::zero());
		assert_eq!(negative_i128, VarUint::zero());
		assert_eq!(negative_bigint, VarUint::zero());

		// Test that positive inputs remain unchanged
		let positive_i32 = VarUint::from(42i32);
		let positive_i64 = VarUint::from(999i64);
		assert_eq!(format!("{}", positive_i32), "42");
		assert_eq!(format!("{}", positive_i64), "999");
	}
}
