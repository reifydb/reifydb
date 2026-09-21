// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{reifydb_assertions, util::bitvec::BitVec};

use crate::value::column::mask::RowMask;

#[derive(Clone, Debug, PartialEq)]
pub struct NoneBitmap {
	defined: BitVec,
}

impl Eq for NoneBitmap {}

impl NoneBitmap {
	pub fn all_present(len: usize) -> Self {
		Self {
			defined: BitVec::repeat(len, true),
		}
	}

	pub fn all_none(len: usize) -> Self {
		Self {
			defined: BitVec::repeat(len, false),
		}
	}

	pub fn len(&self) -> usize {
		self.defined.len()
	}

	pub fn is_empty(&self) -> bool {
		self.defined.is_empty()
	}

	pub fn is_none(&self, row: usize) -> bool {
		reifydb_assertions! {
			assert!(row < self.len(), "row {} out of bounds for len {}", row, self.len());
		}
		!self.defined.get(row)
	}

	pub fn set_none(&mut self, row: usize) {
		reifydb_assertions! {
			assert!(row < self.len());
		}
		self.defined.set(row, false);
	}

	pub fn none_count(&self) -> usize {
		self.defined.count_zeros()
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		assert!(start <= end, "NoneBitmap::slice: start {start} > end {end}");
		assert!(end <= self.len(), "NoneBitmap::slice: end {end} > len {}", self.len());
		Self {
			defined: self.defined.slice(start, end),
		}
	}

	pub fn filter(&self, mask: &RowMask) -> Self {
		assert_eq!(self.len(), mask.len(), "NoneBitmap::filter length mismatch");
		let kept = mask.popcount();
		if kept == self.len() {
			return self.clone();
		}
		if self.defined.all_ones() {
			return Self::all_present(kept);
		}
		let mut defined = BitVec::with_capacity(kept);
		for (row, keep) in mask.as_bitvec().iter().enumerate() {
			if keep {
				defined.push(self.defined.get(row));
			}
		}
		Self {
			defined,
		}
	}

	pub fn gather(&self, indices: &[usize]) -> Self {
		Self {
			defined: BitVec::from_fn(indices.len(), |row| self.defined.get(indices[row])),
		}
	}

	pub fn from_defined_bitvec(bv: &BitVec) -> Self {
		Self {
			defined: bv.clone(),
		}
	}

	pub fn to_defined_bitvec(&self) -> BitVec {
		self.defined.clone()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn all_present_has_zero_none_count() {
		let b = NoneBitmap::all_present(100);
		assert_eq!(b.none_count(), 0);
		assert!(!b.is_none(0));
		assert!(!b.is_none(99));
	}

	#[test]
	fn all_none_counts_every_row() {
		let b = NoneBitmap::all_none(65);
		assert_eq!(b.none_count(), 65);
		assert!(b.is_none(0));
		assert!(b.is_none(64));
	}

	#[test]
	fn set_none_marks_exactly_those_rows() {
		let mut b = NoneBitmap::all_present(10);
		b.set_none(3);
		b.set_none(7);
		assert_eq!(b.none_count(), 2);
		assert!(b.is_none(3));
		assert!(b.is_none(7));
		assert!(!b.is_none(5));
	}
}
