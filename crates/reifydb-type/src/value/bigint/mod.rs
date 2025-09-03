// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	hash::{Hash, Hasher},
	ops::Deref,
};

use num_bigint::BigInt as StdBigInt;
use serde::{Deserialize, Serialize};

/// A wrapper type for arbitrary-precision integers
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BigInt(pub StdBigInt);

impl BigInt {
	/// Create a new BigInt from an i64
	pub fn from_i64(value: i64) -> Self {
		BigInt(StdBigInt::from(value))
	}

	/// Create a new BigInt from an i128
	pub fn from_i128(value: i128) -> Self {
		BigInt(StdBigInt::from(value))
	}

	/// Create a BigInt representing zero
	pub fn zero() -> Self {
		BigInt(StdBigInt::from(0))
	}

	/// Create a BigInt representing one
	pub fn one() -> Self {
		BigInt(StdBigInt::from(1))
	}
}

impl Default for BigInt {
	fn default() -> Self {
		Self::zero()
	}
}

impl Deref for BigInt {
	type Target = StdBigInt;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for BigInt {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}

impl PartialOrd for BigInt {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for BigInt {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl From<i32> for BigInt {
	fn from(value: i32) -> Self {
		BigInt(StdBigInt::from(value))
	}
}

impl From<i64> for BigInt {
	fn from(value: i64) -> Self {
		BigInt(StdBigInt::from(value))
	}
}

impl From<i128> for BigInt {
	fn from(value: i128) -> Self {
		BigInt(StdBigInt::from(value))
	}
}

impl From<u32> for BigInt {
	fn from(value: u32) -> Self {
		BigInt(StdBigInt::from(value))
	}
}

impl From<u64> for BigInt {
	fn from(value: u64) -> Self {
		BigInt(StdBigInt::from(value))
	}
}

impl From<u128> for BigInt {
	fn from(value: u128) -> Self {
		BigInt(StdBigInt::from(value))
	}
}

impl From<StdBigInt> for BigInt {
	fn from(value: StdBigInt) -> Self {
		BigInt(value)
	}
}

impl From<BigInt> for StdBigInt {
	fn from(bigint: BigInt) -> Self {
		bigint.0
	}
}

impl Display for BigInt {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_bigint_create() {
		let bigint = BigInt::from_i64(42);
		assert_eq!(format!("{}", bigint), "42");
	}

	#[test]
	fn test_bigint_equality() {
		let a = BigInt::from_i64(100);
		let b = BigInt::from_i64(100);
		let c = BigInt::from_i64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_bigint_ordering() {
		let a = BigInt::from_i64(10);
		let b = BigInt::from_i64(20);
		let c = BigInt::from_i64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_bigint_large_values() {
		let large = BigInt::from_i128(i128::MAX);
		let larger = BigInt::from(StdBigInt::from(i128::MAX) + 1);

		assert!(large < larger);
	}

	#[test]
	fn test_bigint_display() {
		let bigint = BigInt::from_i64(-12345);
		assert_eq!(format!("{}", bigint), "-12345");
	}

	#[test]
	fn test_bigint_hash() {
		use std::collections::HashSet;

		let a = BigInt::from_i64(42);
		let b = BigInt::from_i64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}
}
