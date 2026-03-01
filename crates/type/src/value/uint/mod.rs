// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Ordering,
	fmt,
	fmt::{Display, Formatter},
	hash::{Hash, Hasher},
	ops::Deref,
};

use num_bigint::BigInt as StdBigInt;
use num_traits::Signed;
use serde::{Deserialize, Serialize};

pub mod parse;

/// A wrapper type for arbitrary-precision unsigned integers (Uint)
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uint(pub StdBigInt);

impl Uint {
	/// Create a new Uint from a u64
	pub fn from_u64(value: u64) -> Self {
		Uint(StdBigInt::from(value))
	}

	/// Create a new Uint from a u128
	pub fn from_u128(value: u128) -> Self {
		Uint(StdBigInt::from(value))
	}

	/// Create a Uint representing zero
	pub fn zero() -> Self {
		Uint(StdBigInt::from(0))
	}

	/// Create a Uint representing one
	pub fn one() -> Self {
		Uint(StdBigInt::from(1))
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

impl Default for Uint {
	fn default() -> Self {
		Self::zero()
	}
}

impl Deref for Uint {
	type Target = StdBigInt;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for Uint {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}

impl PartialOrd for Uint {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Uint {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl From<u8> for Uint {
	fn from(value: u8) -> Self {
		Uint(StdBigInt::from(value))
	}
}

impl From<u16> for Uint {
	fn from(value: u16) -> Self {
		Uint(StdBigInt::from(value))
	}
}

impl From<u32> for Uint {
	fn from(value: u32) -> Self {
		Uint(StdBigInt::from(value))
	}
}

impl From<u64> for Uint {
	fn from(value: u64) -> Self {
		Uint(StdBigInt::from(value))
	}
}

impl From<u128> for Uint {
	fn from(value: u128) -> Self {
		Uint(StdBigInt::from(value))
	}
}

// Handle signed integer conversions by ensuring non-negative values
impl From<i8> for Uint {
	fn from(value: i8) -> Self {
		Uint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<i16> for Uint {
	fn from(value: i16) -> Self {
		Uint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<i32> for Uint {
	fn from(value: i32) -> Self {
		Uint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<i64> for Uint {
	fn from(value: i64) -> Self {
		Uint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<i128> for Uint {
	fn from(value: i128) -> Self {
		Uint(Self::ensure_non_negative(StdBigInt::from(value)))
	}
}

impl From<StdBigInt> for Uint {
	fn from(value: StdBigInt) -> Self {
		Uint(Self::ensure_non_negative(value))
	}
}

impl From<Uint> for StdBigInt {
	fn from(uint: Uint) -> Self {
		uint.0
	}
}

impl Display for Uint {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashSet;

	use super::*;

	#[test]
	fn test_uint_create() {
		let uint = Uint::from_u64(42);
		assert_eq!(format!("{}", uint), "42");
	}

	#[test]
	fn test_uint_equality() {
		let a = Uint::from_u64(100);
		let b = Uint::from_u64(100);
		let c = Uint::from_u64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_uint_ordering() {
		let a = Uint::from_u64(10);
		let b = Uint::from_u64(20);
		let c = Uint::from_u64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_uint_large_values() {
		let large = Uint::from_u128(u128::MAX);
		let larger = Uint::from(StdBigInt::from(u128::MAX) + 1);

		assert!(large < larger);
	}

	#[test]
	fn test_uint_display() {
		let uint = Uint::from_u64(12345);
		assert_eq!(format!("{}", uint), "12345");
	}

	#[test]
	fn test_uint_hash() {
		let a = Uint::from_u64(42);
		let b = Uint::from_u64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}

	#[test]
	fn test_uint_negative_input() {
		// Test that negative inputs are converted to zero
		let negative_i32 = Uint::from(-42i32);
		let negative_i64 = Uint::from(-999i64);
		let negative_i128 = Uint::from(-12345i128);
		let negative_bigint = Uint::from(StdBigInt::from(-777));

		assert_eq!(negative_i32, Uint::zero());
		assert_eq!(negative_i64, Uint::zero());
		assert_eq!(negative_i128, Uint::zero());
		assert_eq!(negative_bigint, Uint::zero());

		// Test that positive inputs remain unchanged
		let positive_i32 = Uint::from(42i32);
		let positive_i64 = Uint::from(999i64);
		assert_eq!(format!("{}", positive_i32), "42");
		assert_eq!(format!("{}", positive_i64), "999");
	}
}
