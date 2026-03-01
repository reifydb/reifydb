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
use serde::{Deserialize, Serialize};

pub mod parse;

/// A wrapper type for arbitrary-precision signed integers (Int)
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Int(pub StdBigInt);

impl Int {
	/// Create a new Int from an i64
	pub fn from_i64(value: i64) -> Self {
		Int(StdBigInt::from(value))
	}

	/// Create a new Int from an i128
	pub fn from_i128(value: i128) -> Self {
		Int(StdBigInt::from(value))
	}

	/// Create a Int representing zero
	pub fn zero() -> Self {
		Int(StdBigInt::from(0))
	}

	/// Create a Int representing one
	pub fn one() -> Self {
		Int(StdBigInt::from(1))
	}
}

impl Default for Int {
	fn default() -> Self {
		Self::zero()
	}
}

impl Deref for Int {
	type Target = StdBigInt;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for Int {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}

impl PartialOrd for Int {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Int {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl From<i8> for Int {
	fn from(value: i8) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<i16> for Int {
	fn from(value: i16) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<i32> for Int {
	fn from(value: i32) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<i64> for Int {
	fn from(value: i64) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<i128> for Int {
	fn from(value: i128) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<u8> for Int {
	fn from(value: u8) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<u16> for Int {
	fn from(value: u16) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<u32> for Int {
	fn from(value: u32) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<u64> for Int {
	fn from(value: u64) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<u128> for Int {
	fn from(value: u128) -> Self {
		Int(StdBigInt::from(value))
	}
}

impl From<StdBigInt> for Int {
	fn from(value: StdBigInt) -> Self {
		Int(value)
	}
}

impl From<Int> for StdBigInt {
	fn from(int: Int) -> Self {
		int.0
	}
}

impl Display for Int {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashSet;

	use super::*;

	#[test]
	fn test_int_create() {
		let int = Int::from_i64(42);
		assert_eq!(format!("{}", int), "42");
	}

	#[test]
	fn test_int_equality() {
		let a = Int::from_i64(100);
		let b = Int::from_i64(100);
		let c = Int::from_i64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_int_ordering() {
		let a = Int::from_i64(10);
		let b = Int::from_i64(20);
		let c = Int::from_i64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_int_large_values() {
		let large = Int::from_i128(i128::MAX);
		let larger = Int::from(StdBigInt::from(i128::MAX) + 1);

		assert!(large < larger);
	}

	#[test]
	fn test_int_display() {
		let int = Int::from_i64(-12345);
		assert_eq!(format!("{}", int), "-12345");
	}

	#[test]
	fn test_int_hash() {
		let a = Int::from_i64(42);
		let b = Int::from_i64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}
}
