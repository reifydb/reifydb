// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Ordering,
	fmt,
	fmt::{Display, Formatter},
	hash::{Hash, Hasher},
	ops::Deref,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::Visitor};

use crate::{
	error::{Error, TypeError},
	util::float_format::format_f64,
};

/// A wrapper around f64 that provides total ordering by rejecting NaN values.
/// This type is sortable and can be used in collections that require Ord,
/// such as BTreeMap and BTreeSet. It prevents NaN values from being stored,
/// ensuring that all values are comparable and can be sorted consistently.
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Default)]
pub struct OrderedF64(f64);

impl Serialize for OrderedF64 {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_f64(self.0)
	}
}

impl<'de> Deserialize<'de> for OrderedF64 {
	fn deserialize<D>(deserializer: D) -> Result<OrderedF64, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct F64Visitor;

		impl Visitor<'_> for F64Visitor {
			type Value = OrderedF64;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a 64-bit floating point number")
			}

			fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
				Ok(OrderedF64(value))
			}

			fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(OrderedF64(value as f64))
			}
		}

		deserializer.deserialize_f64(F64Visitor)
	}
}

impl OrderedF64 {
	pub fn value(&self) -> f64 {
		self.0
	}

	pub fn zero() -> OrderedF64 {
		OrderedF64(0.0f64)
	}
}

impl Deref for OrderedF64 {
	type Target = f64;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for OrderedF64 {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(&format_f64(self.0))
	}
}

impl PartialEq for OrderedF64 {
	fn eq(&self, other: &Self) -> bool {
		self.0.to_bits() == other.0.to_bits()
	}
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for OrderedF64 {
	fn cmp(&self, other: &Self) -> Ordering {
		let l = float_to_ordered_u64(self.0);
		let r = float_to_ordered_u64(other.0);
		l.cmp(&r)
	}
}

/// Convert f64 bits to a u64 that sorts in the same order as the float.
/// Positive floats: flip sign bit so they sort above negative.
/// Negative floats: flip all bits so magnitude order is reversed.
#[inline]
fn float_to_ordered_u64(f: f64) -> u64 {
	let bits = f.to_bits();
	if bits & 0x8000000000000000 == 0 {
		bits ^ 0x8000000000000000
	} else {
		!bits
	}
}

impl Hash for OrderedF64 {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.to_bits().hash(state);
	}
}

impl From<OrderedF64> for f64 {
	fn from(v: OrderedF64) -> Self {
		v.0
	}
}

impl TryFrom<f64> for OrderedF64 {
	type Error = Error;

	fn try_from(f: f64) -> Result<Self, Self::Error> {
		// normalize -0.0 and +0.0
		let normalized = if f == 0.0 {
			0.0
		} else {
			f
		};
		if f.is_nan() {
			Err(TypeError::NanNotAllowed.into())
		} else {
			Ok(OrderedF64(normalized))
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use std::{collections::HashSet, convert::TryFrom};

	use super::*;

	#[test]
	fn test_eq_and_ord() {
		let a = OrderedF64::try_from(3.14).unwrap();
		let b = OrderedF64::try_from(3.14).unwrap();
		let c = OrderedF64::try_from(2.71).unwrap();

		assert_eq!(a, b);
		assert!(a > c);
		assert!(c < a);
	}

	#[test]
	fn test_sorting() {
		let mut values = vec![
			OrderedF64::try_from(10.0).unwrap(),
			OrderedF64::try_from(2.0).unwrap(),
			OrderedF64::try_from(5.0).unwrap(),
		];
		values.sort();
		let sorted: Vec<f64> = values.into_iter().map(|v| v.0).collect();
		assert_eq!(sorted, vec![2.0, 5.0, 10.0]);
	}

	#[test]
	fn test_hash_eq() {
		let a = OrderedF64::try_from(1.0).unwrap();
		let b = OrderedF64::try_from(1.0).unwrap();

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}

	#[test]
	fn test_normalizes_zero() {
		let pos_zero = OrderedF64::try_from(0.0).unwrap();
		let neg_zero = OrderedF64::try_from(-0.0).unwrap();

		assert_eq!(pos_zero, neg_zero);

		let mut set = HashSet::new();
		set.insert(pos_zero);
		assert!(set.contains(&neg_zero));
	}

	#[test]
	fn test_nan_fails() {
		assert!(OrderedF64::try_from(f64::NAN).is_err());
	}

	#[test]
	fn test_negative_less_than_positive() {
		let neg = OrderedF64::try_from(-1.5).unwrap();
		let pos = OrderedF64::try_from(1.5).unwrap();
		assert!(neg < pos);
	}

	#[test]
	fn test_negative_less_than_zero() {
		let neg = OrderedF64::try_from(-0.0001).unwrap();
		let zero = OrderedF64::try_from(0.0).unwrap();
		assert!(neg < zero);
	}

	#[test]
	fn test_sorting_with_negatives() {
		let mut values = vec![
			OrderedF64::try_from(3.14).unwrap(),
			OrderedF64::try_from(-1.5).unwrap(),
			OrderedF64::try_from(0.0).unwrap(),
			OrderedF64::try_from(99999.0).unwrap(),
			OrderedF64::try_from(-100.0).unwrap(),
		];
		values.sort();
		let sorted: Vec<f64> = values.into_iter().map(|v| v.0).collect();
		assert_eq!(sorted, vec![-100.0, -1.5, 0.0, 3.14, 99999.0]);
	}
}
