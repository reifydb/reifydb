// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	fmt,
	fmt::{Display, Formatter},
	hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};

use crate::util::{cowvec::CowVec, float_format::format_f32};

#[repr(transparent)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorValue(CowVec<f32>);

impl VectorValue {
	pub fn new(data: Vec<f32>) -> Self {
		Self(CowVec::new(data))
	}

	pub fn from_slice(data: &[f32]) -> Self {
		Self(CowVec::new(data.to_vec()))
	}

	pub fn as_slice(&self) -> &[f32] {
		self.0.as_slice()
	}

	pub fn dims(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn from_le_bytes(bytes: &[u8]) -> Self {
		Self(CowVec::new(
			bytes.chunks_exact(4)
				.map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
				.collect(),
		))
	}

	pub fn to_le_bytes(&self) -> Vec<u8> {
		let mut out = Vec::with_capacity(self.0.len() * 4);
		for value in self.0.as_slice() {
			out.extend_from_slice(&value.to_le_bytes());
		}
		out
	}
}

impl PartialEq for VectorValue {
	fn eq(&self, other: &Self) -> bool {
		self.0.len() == other.0.len()
			&& self.0.as_slice().iter().zip(other.0.as_slice()).all(|(l, r)| l.to_bits() == r.to_bits())
	}
}

impl Eq for VectorValue {}

impl PartialOrd for VectorValue {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for VectorValue {
	fn cmp(&self, other: &Self) -> Ordering {
		for (l, r) in self.0.as_slice().iter().zip(other.0.as_slice()) {
			match l.total_cmp(r) {
				Ordering::Equal => continue,
				ordering => return ordering,
			}
		}
		self.0.len().cmp(&other.0.len())
	}
}

impl Hash for VectorValue {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.len().hash(state);
		for value in self.0.as_slice() {
			value.to_bits().hash(state);
		}
	}
}

impl Display for VectorValue {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str("[")?;
		for (i, value) in self.0.as_slice().iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			f.write_str(&format_f32(*value))?;
		}
		f.write_str("]")
	}
}

impl From<Vec<f32>> for VectorValue {
	fn from(data: Vec<f32>) -> Self {
		Self::new(data)
	}
}

impl From<&[f32]> for VectorValue {
	fn from(data: &[f32]) -> Self {
		Self::from_slice(data)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::hash_map::DefaultHasher,
		hash::{Hash, Hasher},
	};

	use super::*;

	fn hash_of(v: &VectorValue) -> u64 {
		let mut hasher = DefaultHasher::new();
		v.hash(&mut hasher);
		hasher.finish()
	}

	#[test]
	fn dims_counts_elements_not_bytes() {
		let v = VectorValue::new(vec![0.1, 0.2, 0.3, 0.4]);
		assert_eq!(v.dims(), 4);
		assert_eq!(v.to_le_bytes().len(), 16);
	}

	#[test]
	fn le_bytes_round_trip() {
		let v = VectorValue::new(vec![0.1, -2.5, 3.75, f32::MAX, f32::MIN_POSITIVE]);
		assert_eq!(VectorValue::from_le_bytes(&v.to_le_bytes()), v);
	}

	#[test]
	fn le_bytes_round_trip_empty() {
		let v = VectorValue::new(vec![]);
		assert_eq!(v.to_le_bytes(), Vec::<u8>::new());
		assert_eq!(VectorValue::from_le_bytes(&[]), v);
	}

	#[test]
	fn from_le_bytes_is_little_endian() {
		let bytes = 1.0f32.to_le_bytes();
		assert_eq!(VectorValue::from_le_bytes(&bytes).as_slice(), &[1.0f32]);
	}

	#[test]
	fn from_le_bytes_drops_trailing_partial_element() {
		let mut bytes = 1.0f32.to_le_bytes().to_vec();
		bytes.push(0xFF);
		assert_eq!(VectorValue::from_le_bytes(&bytes).dims(), 1);
	}

	#[test]
	fn ord_is_lexicographic_by_element() {
		let a = VectorValue::new(vec![1.0, 2.0]);
		let b = VectorValue::new(vec![1.0, 3.0]);
		assert!(a < b);
	}

	#[test]
	fn ord_uses_length_as_tiebreak_on_common_prefix() {
		let short = VectorValue::new(vec![1.0, 2.0]);
		let long = VectorValue::new(vec![1.0, 2.0, 0.0]);
		assert!(short < long);
	}

	#[test]
	fn ord_places_negative_below_positive() {
		let neg = VectorValue::new(vec![-1.5]);
		let pos = VectorValue::new(vec![1.5]);
		assert!(neg < pos);
	}

	#[test]
	fn ord_is_a_total_order_over_nan() {
		let mut values = vec![
			VectorValue::new(vec![f32::NAN]),
			VectorValue::new(vec![1.0]),
			VectorValue::new(vec![-1.0]),
		];
		values.sort();
		assert_eq!(values[0], VectorValue::new(vec![-1.0]));
		assert_eq!(values[1], VectorValue::new(vec![1.0]));
		assert_eq!(values[2], VectorValue::new(vec![f32::NAN]));
	}

	#[test]
	fn total_cmp_separates_signed_zero() {
		let neg = VectorValue::new(vec![-0.0]);
		let pos = VectorValue::new(vec![0.0]);
		assert!(neg < pos);
		assert_ne!(neg, pos);
	}

	#[test]
	fn hash_agrees_with_eq() {
		let a = VectorValue::new(vec![0.1, 0.2]);
		let b = VectorValue::new(vec![0.1, 0.2]);
		assert_eq!(a, b);
		assert_eq!(hash_of(&a), hash_of(&b));
	}

	#[test]
	fn hash_distinguishes_signed_zero_like_eq_does() {
		let neg = VectorValue::new(vec![-0.0]);
		let pos = VectorValue::new(vec![0.0]);
		assert_ne!(neg, pos);
		assert_ne!(hash_of(&neg), hash_of(&pos));
	}

	#[test]
	fn hash_distinguishes_by_length() {
		let short = VectorValue::new(vec![1.0]);
		let long = VectorValue::new(vec![1.0, 0.0]);
		assert_ne!(short, long);
		assert_ne!(hash_of(&short), hash_of(&long));
	}

	#[test]
	fn display_renders_bracketed_list() {
		assert_eq!(VectorValue::new(vec![0.5, -1.0, 0.0]).to_string(), "[0.5, -1, 0]");
		assert_eq!(VectorValue::new(vec![]).to_string(), "[]");
	}
}
