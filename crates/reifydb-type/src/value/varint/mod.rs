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

pub mod parse;
pub use parse::parse_varint;

/// A wrapper type for arbitrary-precision signed integers (VarInt)
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VarInt(pub StdBigInt);

impl VarInt {
	/// Create a new VarInt from an i64
	pub fn from_i64(value: i64) -> Self {
		VarInt(StdBigInt::from(value))
	}

	/// Create a new VarInt from an i128
	pub fn from_i128(value: i128) -> Self {
		VarInt(StdBigInt::from(value))
	}

	/// Create a VarInt representing zero
	pub fn zero() -> Self {
		VarInt(StdBigInt::from(0))
	}

	/// Create a VarInt representing one
	pub fn one() -> Self {
		VarInt(StdBigInt::from(1))
	}
}

impl Default for VarInt {
	fn default() -> Self {
		Self::zero()
	}
}

impl Deref for VarInt {
	type Target = StdBigInt;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Hash for VarInt {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}

impl PartialOrd for VarInt {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for VarInt {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl From<i32> for VarInt {
	fn from(value: i32) -> Self {
		VarInt(StdBigInt::from(value))
	}
}

impl From<i64> for VarInt {
	fn from(value: i64) -> Self {
		VarInt(StdBigInt::from(value))
	}
}

impl From<i128> for VarInt {
	fn from(value: i128) -> Self {
		VarInt(StdBigInt::from(value))
	}
}

impl From<u32> for VarInt {
	fn from(value: u32) -> Self {
		VarInt(StdBigInt::from(value))
	}
}

impl From<u64> for VarInt {
	fn from(value: u64) -> Self {
		VarInt(StdBigInt::from(value))
	}
}

impl From<u128> for VarInt {
	fn from(value: u128) -> Self {
		VarInt(StdBigInt::from(value))
	}
}

impl From<StdBigInt> for VarInt {
	fn from(value: StdBigInt) -> Self {
		VarInt(value)
	}
}

impl From<VarInt> for StdBigInt {
	fn from(varint: VarInt) -> Self {
		varint.0
	}
}

impl Display for VarInt {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_varint_create() {
		let varint = VarInt::from_i64(42);
		assert_eq!(format!("{}", varint), "42");
	}

	#[test]
	fn test_varint_equality() {
		let a = VarInt::from_i64(100);
		let b = VarInt::from_i64(100);
		let c = VarInt::from_i64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_varint_ordering() {
		let a = VarInt::from_i64(10);
		let b = VarInt::from_i64(20);
		let c = VarInt::from_i64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_varint_large_values() {
		let large = VarInt::from_i128(i128::MAX);
		let larger = VarInt::from(StdBigInt::from(i128::MAX) + 1);

		assert!(large < larger);
	}

	#[test]
	fn test_varint_display() {
		let varint = VarInt::from_i64(-12345);
		assert_eq!(format!("{}", varint), "-12345");
	}

	#[test]
	fn test_varint_hash() {
		use std::collections::HashSet;

		let a = VarInt::from_i64(42);
		let b = VarInt::from_i64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}
}
