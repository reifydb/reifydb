// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{storage::DataBitVec, util::bitvec::BitVec};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoneBitmap {
	words: Vec<u64>,
	len: usize,
}

impl NoneBitmap {
	pub fn all_present(len: usize) -> Self {
		Self {
			words: vec![0u64; words_for(len)],
			len,
		}
	}

	pub fn all_none(len: usize) -> Self {
		let word_count = words_for(len);
		let mut words = vec![u64::MAX; word_count];
		let trailing = len % 64;
		if trailing != 0 && word_count > 0 {
			words[word_count - 1] = (1u64 << trailing) - 1;
		}
		Self {
			words,
			len,
		}
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn is_none(&self, row: usize) -> bool {
		debug_assert!(row < self.len, "row {} out of bounds for len {}", row, self.len);
		(self.words[row / 64] >> (row % 64)) & 1 == 1
	}

	pub fn set_none(&mut self, row: usize) {
		debug_assert!(row < self.len);
		self.words[row / 64] |= 1u64 << (row % 64);
	}

	pub fn clear_none(&mut self, row: usize) {
		debug_assert!(row < self.len);
		self.words[row / 64] &= !(1u64 << (row % 64));
	}

	pub fn none_count(&self) -> usize {
		popcount_masked(&self.words, self.len)
	}

	pub fn and(&self, other: &Self) -> Self {
		assert_eq!(self.len, other.len, "NoneBitmap::and length mismatch");
		let words = self.words.iter().zip(&other.words).map(|(a, b)| a & b).collect();
		Self {
			words,
			len: self.len,
		}
	}

	pub fn or(&self, other: &Self) -> Self {
		assert_eq!(self.len, other.len, "NoneBitmap::or length mismatch");
		let words = self.words.iter().zip(&other.words).map(|(a, b)| a | b).collect();
		Self {
			words,
			len: self.len,
		}
	}

	pub fn from_defined_bitvec(bv: &BitVec) -> Self {
		let len = DataBitVec::len(bv);
		let mut out = Self::all_present(len);
		for row in 0..len {
			if !DataBitVec::get(bv, row) {
				out.set_none(row);
			}
		}
		out
	}

	pub fn to_defined_bitvec(&self) -> BitVec {
		let mut bits = Vec::with_capacity(self.len);
		for row in 0..self.len {
			bits.push(!self.is_none(row));
		}
		BitVec::from(bits)
	}
}

const fn words_for(bits: usize) -> usize {
	bits.div_ceil(64)
}

fn popcount_masked(words: &[u64], len: usize) -> usize {
	let word_count = words.len();
	if word_count == 0 {
		return 0;
	}
	let mut count = 0usize;
	for &w in &words[..word_count - 1] {
		count += w.count_ones() as usize;
	}
	let trailing = len - 64 * (word_count - 1);
	let mask = if trailing == 64 {
		u64::MAX
	} else {
		(1u64 << trailing) - 1
	};
	count += (words[word_count - 1] & mask).count_ones() as usize;
	count
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
	fn set_and_clear_round_trip() {
		let mut b = NoneBitmap::all_present(10);
		b.set_none(3);
		b.set_none(7);
		assert_eq!(b.none_count(), 2);
		assert!(b.is_none(3));
		assert!(b.is_none(7));
		assert!(!b.is_none(5));
		b.clear_none(3);
		assert_eq!(b.none_count(), 1);
		assert!(!b.is_none(3));
	}

	#[test]
	fn and_or_combine_bitmaps() {
		let mut a = NoneBitmap::all_present(8);
		a.set_none(1);
		a.set_none(3);
		let mut b = NoneBitmap::all_present(8);
		b.set_none(3);
		b.set_none(5);
		assert_eq!(a.and(&b).none_count(), 1);
		assert_eq!(a.or(&b).none_count(), 3);
	}
}
