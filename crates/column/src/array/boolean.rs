// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Bitset-packed boolean storage. `get(row)` returns the boolean value at
// that row. None tracking is out-of-band via the enclosing `CanonicalArray`'s
// `NoneBitmap`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoolArray {
	words: Vec<u64>,
	len: usize,
}

impl BoolArray {
	pub fn new(len: usize) -> Self {
		Self {
			words: vec![0u64; len.div_ceil(64)],
			len,
		}
	}

	pub fn from_bools(bits: impl IntoIterator<Item = bool>) -> Self {
		let values: Vec<bool> = bits.into_iter().collect();
		let mut a = Self::new(values.len());
		for (i, v) in values.iter().enumerate() {
			a.set(i, *v);
		}
		a
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
		let bit = 1u64 << (row % 64);
		let word = &mut self.words[row / 64];
		if value {
			*word |= bit;
		} else {
			*word &= !bit;
		}
	}

	pub fn true_count(&self) -> usize {
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

	pub fn false_count(&self) -> usize {
		self.len - self.true_count()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn from_bools_round_trips() {
		let a = BoolArray::from_bools([true, false, true, true, false]);
		assert_eq!(a.len(), 5);
		assert!(a.get(0));
		assert!(!a.get(1));
		assert!(a.get(2));
		assert!(a.get(3));
		assert!(!a.get(4));
		assert_eq!(a.true_count(), 3);
		assert_eq!(a.false_count(), 2);
	}
}
