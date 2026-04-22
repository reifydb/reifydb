// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Per-row selection bitmap. Set bit = row is selected/kept. This is the dual
// of `NoneBitmap` — the storage layout is identical but the semantics are
// opposite, so the two are kept as distinct types to prevent accidental mixing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMask {
	words: Vec<u64>,
	len: usize,
}

impl RowMask {
	pub fn all_set(len: usize) -> Self {
		let word_count = len.div_ceil(64);
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

	pub fn none_set(len: usize) -> Self {
		Self {
			words: vec![0u64; len.div_ceil(64)],
			len,
		}
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn get(&self, row: usize) -> bool {
		debug_assert!(row < self.len);
		(self.words[row / 64] >> (row % 64)) & 1 == 1
	}

	pub fn set(&mut self, row: usize, value: bool) {
		debug_assert!(row < self.len);
		let word = &mut self.words[row / 64];
		let bit = 1u64 << (row % 64);
		if value {
			*word |= bit;
		} else {
			*word &= !bit;
		}
	}

	pub fn popcount(&self) -> usize {
		let word_count = self.words.len();
		if word_count == 0 {
			return 0;
		}
		let mut count = 0usize;
		for &w in &self.words[..word_count - 1] {
			count += w.count_ones() as usize;
		}
		let trailing = self.len - 64 * (word_count - 1);
		let mask = if trailing == 64 {
			u64::MAX
		} else {
			(1u64 << trailing) - 1
		};
		count += (self.words[word_count - 1] & mask).count_ones() as usize;
		count
	}

	pub fn and(&self, other: &Self) -> Self {
		assert_eq!(self.len, other.len, "RowMask::and length mismatch");
		let words = self.words.iter().zip(&other.words).map(|(a, b)| a & b).collect();
		Self {
			words,
			len: self.len,
		}
	}

	pub fn or(&self, other: &Self) -> Self {
		assert_eq!(self.len, other.len, "RowMask::or length mismatch");
		let words = self.words.iter().zip(&other.words).map(|(a, b)| a | b).collect();
		Self {
			words,
			len: self.len,
		}
	}

	pub fn not(&self) -> Self {
		let word_count = self.words.len();
		let mut words: Vec<u64> = self.words.iter().map(|w| !w).collect();
		let trailing = self.len % 64;
		if trailing != 0 && word_count > 0 {
			words[word_count - 1] &= (1u64 << trailing) - 1;
		}
		Self {
			words,
			len: self.len,
		}
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
}
