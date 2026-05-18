// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::hash_map::DefaultHasher,
	f64::consts::LN_2,
	hash::{Hash, Hasher},
};

#[derive(Debug, Clone)]
pub struct BloomFilter {
	bits: Vec<u64>,
	size: usize,
	hash_count: usize,
}

impl BloomFilter {
	pub fn new(expected_items: usize) -> Self {
		let size = (expected_items as f64 * 10.0) as usize;
		let word_count = size.div_ceil(64);
		Self {
			bits: vec![0; word_count],
			size: word_count * 64,
			hash_count: 7,
		}
	}

	pub fn with_params(size_bits: usize, hash_count: usize) -> Self {
		let word_count = size_bits.div_ceil(64);
		Self {
			bits: vec![0; word_count],
			size: word_count * 64,
			hash_count,
		}
	}

	pub fn add<T: Hash>(&mut self, item: &T) {
		let hash = self.hash(item);
		for i in 0..self.hash_count {
			let bit_pos = self.get_bit_pos(hash, i);
			let word_idx = bit_pos / 64;
			let bit_idx = bit_pos % 64;
			self.bits[word_idx] |= 1u64 << bit_idx;
		}
	}

	pub fn might_contain<T: Hash>(&self, item: &T) -> bool {
		let hash = self.hash(item);
		for i in 0..self.hash_count {
			let bit_pos = self.get_bit_pos(hash, i);
			let word_idx = bit_pos / 64;
			let bit_idx = bit_pos % 64;
			if (self.bits[word_idx] & (1u64 << bit_idx)) == 0 {
				return false;
			}
		}
		true
	}

	pub fn clear(&mut self) {
		self.bits.fill(0);
	}

	pub fn is_empty(&self) -> bool {
		self.bits.iter().all(|&word| word == 0)
	}

	pub fn estimated_items(&self) -> usize {
		let set_bits = self.bits.iter().map(|&word| word.count_ones() as usize).sum::<usize>();

		let fill_ratio = set_bits as f64 / self.size as f64;
		if fill_ratio >= 1.0 {
			return usize::MAX;
		}

		let estimated = -(self.size as f64 / self.hash_count as f64) * (1.0 - fill_ratio).ln();
		estimated as usize
	}

	pub fn fill_ratio(&self) -> f64 {
		let set_bits = self.bits.iter().map(|&word| word.count_ones() as usize).sum::<usize>();
		set_bits as f64 / self.size as f64
	}

	#[inline]
	fn hash<T: Hash>(&self, item: &T) -> u64 {
		let mut hasher = DefaultHasher::new();
		item.hash(&mut hasher);
		hasher.finish()
	}

	#[inline]
	fn get_bit_pos(&self, hash: u64, i: usize) -> usize {
		let h1 = hash as usize;
		let h2 = (hash >> 32) as usize | 1;
		(h1.wrapping_add(i.wrapping_mul(h2))) % self.size
	}
}

pub struct BloomFilterBuilder {
	expected_items: usize,
	false_positive_rate: f64,
}

impl BloomFilterBuilder {
	pub fn new(expected_items: usize) -> Self {
		Self {
			expected_items,
			false_positive_rate: 0.01,
		}
	}

	pub fn false_positive_rate(mut self, rate: f64) -> Self {
		assert!(rate > 0.0 && rate < 1.0, "False positive rate must be between 0 and 1");
		self.false_positive_rate = rate;
		self
	}

	pub fn build(self) -> BloomFilter {
		let ln2_squared = LN_2.powi(2);
		let size_bits = (-(self.expected_items as f64) * self.false_positive_rate.ln() / ln2_squared) as usize;

		let hash_count = ((size_bits as f64 / self.expected_items as f64) * LN_2).round() as usize;

		BloomFilter::with_params(size_bits, hash_count.max(1))
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_bloom_filter_basic() {
		let mut bloom = BloomFilter::new(100);

		// Test empty filter
		assert!(bloom.is_empty());

		// Add some items
		bloom.add(&"hello");
		bloom.add(&"world");
		bloom.add(&42);

		// Check membership
		assert!(bloom.might_contain(&"hello"));
		assert!(bloom.might_contain(&"world"));
		assert!(bloom.might_contain(&42));

		// Should not contain items not added (with high probability)
		assert!(!bloom.might_contain(&"foo") || !bloom.might_contain(&"bar"));

		// Clear and test
		bloom.clear();
		assert!(bloom.is_empty());
		assert!(!bloom.might_contain(&"hello"));
	}

	#[test]
	fn test_bloom_filter_false_positive_rate() {
		let mut bloom = BloomFilterBuilder::new(1000)
			.false_positive_rate(0.001) // 0.1%
			.build();

		// Add 1000 items
		for i in 0..1000 {
			bloom.add(&i);
		}

		// Check all added items are found
		for i in 0..1000 {
			assert!(bloom.might_contain(&i));
		}

		// Count false positives in next 10000 items
		let mut false_positives = 0;
		for i in 1000..11000 {
			if bloom.might_contain(&i) {
				false_positives += 1;
			}
		}

		// Should be roughly around 0.1% (10 out of 10000)
		// Allow some variance
		assert!(false_positives < 30, "Too many false positives: {}", false_positives);
	}

	#[test]
	fn test_bloom_filter_fill_ratio() {
		let mut bloom = BloomFilter::new(10);

		assert_eq!(bloom.fill_ratio(), 0.0);

		for i in 0..5 {
			bloom.add(&i);
		}

		let ratio = bloom.fill_ratio();
		assert!(ratio > 0.0 && ratio < 1.0);

		// Add many more items to saturate
		for i in 5..100 {
			bloom.add(&i);
		}

		let saturated_ratio = bloom.fill_ratio();
		assert!(saturated_ratio > ratio);
	}
}
