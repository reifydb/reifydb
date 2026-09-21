// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{reifydb_assertions, util::bitvec::BitVec};

#[derive(Clone, Debug, PartialEq)]
pub struct RowMask {
	bits: BitVec,
}

impl Eq for RowMask {}

impl RowMask {
	pub fn all_set(len: usize) -> Self {
		Self {
			bits: BitVec::repeat(len, true),
		}
	}

	pub fn none_set(len: usize) -> Self {
		Self {
			bits: BitVec::repeat(len, false),
		}
	}

	pub fn len(&self) -> usize {
		self.bits.len()
	}

	pub fn is_empty(&self) -> bool {
		self.bits.is_empty()
	}

	pub fn get(&self, row: usize) -> bool {
		reifydb_assertions! {
			assert!(row < self.len());
		}
		self.bits.get(row)
	}

	pub fn set(&mut self, row: usize, value: bool) {
		reifydb_assertions! {
			assert!(row < self.len());
		}
		self.bits.set(row, value);
	}

	pub fn popcount(&self) -> usize {
		self.bits.count_ones()
	}

	pub fn and(&self, other: &Self) -> Self {
		assert_eq!(self.len(), other.len(), "RowMask::and length mismatch");
		Self {
			bits: self.bits.and(&other.bits),
		}
	}

	pub fn or(&self, other: &Self) -> Self {
		assert_eq!(self.len(), other.len(), "RowMask::or length mismatch");
		Self {
			bits: self.bits.or(&other.bits),
		}
	}

	pub fn not(&self) -> Self {
		Self {
			bits: self.bits.not(),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> Self {
		assert!(start <= end, "RowMask::slice: start {start} > end {end}");
		assert!(end <= self.len(), "RowMask::slice: end {end} > len {}", self.len());
		Self {
			bits: self.bits.slice(start, end),
		}
	}

	pub fn concat(parts: &[Self]) -> Self {
		let total: usize = parts.iter().map(|m| m.len()).sum();
		let mut bits = BitVec::with_capacity(total);
		for part in parts {
			bits.extend(&part.bits);
		}
		Self {
			bits,
		}
	}

	pub fn as_bitvec(&self) -> &BitVec {
		&self.bits
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn all_set_counts_every_row() {
		let m = RowMask::all_set(100);
		assert_eq!(m.popcount(), 100);
		assert!(m.get(0));
		assert!(m.get(99));
	}

	#[test]
	fn none_set_has_zero_popcount() {
		let m = RowMask::none_set(10);
		assert_eq!(m.popcount(), 0);
		assert!(!m.get(0));
	}

	#[test]
	fn not_inverts_within_length_only() {
		let mut m = RowMask::none_set(65);
		m.set(0, true);
		m.set(64, true);
		let inverted = m.not();
		assert_eq!(inverted.popcount(), 63);
		assert!(!inverted.get(0));
		assert!(inverted.get(1));
		assert!(!inverted.get(64));
	}

	#[test]
	fn and_or_combine_masks() {
		let mut a = RowMask::none_set(8);
		a.set(1, true);
		a.set(3, true);
		a.set(5, true);
		let mut b = RowMask::none_set(8);
		b.set(3, true);
		b.set(5, true);
		b.set(7, true);
		assert_eq!(a.and(&b).popcount(), 2);
		assert_eq!(a.or(&b).popcount(), 4);
	}

	#[test]
	fn concat_appends_each_part_at_its_offset() {
		let mut a = RowMask::none_set(3);
		a.set(0, true);
		a.set(2, true);
		let mut b = RowMask::none_set(2);
		b.set(1, true);
		let mut c = RowMask::none_set(4);
		c.set(0, true);
		c.set(3, true);
		let combined = RowMask::concat(&[a, b, c]);
		assert_eq!(combined.len(), 9);
		assert!(combined.get(0));
		assert!(!combined.get(1));
		assert!(combined.get(2));
		assert!(!combined.get(3));
		assert!(combined.get(4));
		assert!(combined.get(5));
		assert!(!combined.get(6));
		assert!(!combined.get(7));
		assert!(combined.get(8));
	}

	#[test]
	fn concat_handles_word_boundary_crossings() {
		let a = RowMask::all_set(70);
		let b = RowMask::all_set(70);
		let combined = RowMask::concat(&[a, b]);
		assert_eq!(combined.len(), 140);
		assert_eq!(combined.popcount(), 140);
	}

	#[test]
	fn concat_empty_parts_yield_empty_mask() {
		let combined = RowMask::concat(&[]);
		assert_eq!(combined.len(), 0);
		assert_eq!(combined.popcount(), 0);
	}

	#[test]
	fn slice_extracts_inner_window() {
		let mut m = RowMask::none_set(8);
		m.set(1, true);
		m.set(3, true);
		m.set(5, true);
		m.set(7, true);
		let s = m.slice(2, 6);
		assert_eq!(s.len(), 4);
		assert!(!s.get(0));
		assert!(s.get(1));
		assert!(!s.get(2));
		assert!(s.get(3));
	}

	#[test]
	fn slice_crosses_word_boundary() {
		let mut m = RowMask::none_set(140);
		m.set(60, true);
		m.set(64, true);
		m.set(70, true);
		let s = m.slice(50, 80);
		assert_eq!(s.len(), 30);
		assert_eq!(s.popcount(), 3);
		assert!(s.get(10));
		assert!(s.get(14));
		assert!(s.get(20));
	}

	#[test]
	fn slice_full_range_equals_self() {
		let mut m = RowMask::none_set(10);
		m.set(0, true);
		m.set(4, true);
		m.set(9, true);
		let s = m.slice(0, 10);
		assert_eq!(s, m);
	}

	#[test]
	fn slice_empty_range_yields_empty_mask() {
		let m = RowMask::all_set(10);
		let s = m.slice(5, 5);
		assert_eq!(s.len(), 0);
		assert_eq!(s.popcount(), 0);
	}
}
