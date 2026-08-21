// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	f64::consts::LN_2,
	hash::Hash,
	sync::atomic::{AtomicU64, Ordering},
};

use reifydb_core::util::bloom::hash_item;

#[derive(Debug)]
pub struct BloomFilter {
	bits: Vec<AtomicU64>,
	size: usize,
	hash_count: usize,
}

impl BloomFilter {
	pub fn new(expected_items: usize) -> Self {
		Self::with_params(expected_items * 10, 7)
	}

	pub fn with_params(size_bits: usize, hash_count: usize) -> Self {
		let word_count = size_bits.div_ceil(64).max(1);
		Self {
			bits: (0..word_count).map(|_| AtomicU64::new(0)).collect(),
			size: word_count * 64,
			hash_count,
		}
	}

	pub fn add<T: Hash>(&self, item: &T) {
		let hash = hash_item(item);
		for i in 0..self.hash_count {
			let bit_pos = self.get_bit_pos(hash, i);
			self.bits[bit_pos / 64].fetch_or(1u64 << (bit_pos % 64), Ordering::Release);
		}
	}

	pub fn might_contain<T: Hash>(&self, item: &T) -> bool {
		let hash = hash_item(item);
		for i in 0..self.hash_count {
			let bit_pos = self.get_bit_pos(hash, i);
			if self.bits[bit_pos / 64].load(Ordering::Acquire) & (1u64 << (bit_pos % 64)) == 0 {
				return false;
			}
		}
		true
	}

	pub fn clear(&self) {
		for word in &self.bits {
			word.store(0, Ordering::Release);
		}
	}

	pub fn is_empty(&self) -> bool {
		self.bits.iter().all(|word| word.load(Ordering::Acquire) == 0)
	}

	pub fn estimated_items(&self) -> usize {
		let fill_ratio = self.fill_ratio();
		if fill_ratio >= 1.0 {
			return usize::MAX;
		}
		let estimated = -(self.size as f64 / self.hash_count as f64) * (1.0 - fill_ratio).ln();
		estimated as usize
	}

	pub fn fill_ratio(&self) -> f64 {
		let set_bits: usize =
			self.bits.iter().map(|word| word.load(Ordering::Relaxed).count_ones() as usize).sum();
		set_bits as f64 / self.size as f64
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
		let bloom = BloomFilter::new(100);

		assert!(bloom.is_empty());

		bloom.add(&"hello");
		bloom.add(&"world");
		bloom.add(&42);

		assert!(bloom.might_contain(&"hello"));
		assert!(bloom.might_contain(&"world"));
		assert!(bloom.might_contain(&42));

		// An absent key may collide, so only one of the two is required to answer absent.
		assert!(!bloom.might_contain(&"foo") || !bloom.might_contain(&"bar"));

		bloom.clear();
		assert!(bloom.is_empty());
		assert!(!bloom.might_contain(&"hello"));
	}

	#[test]
	fn test_bloom_filter_false_positive_rate() {
		let bloom = BloomFilterBuilder::new(1000)
			.false_positive_rate(0.001) // 0.1%
			.build();

		for i in 0..1000 {
			bloom.add(&i);
		}

		for i in 0..1000 {
			assert!(bloom.might_contain(&i));
		}

		let mut false_positives = 0;
		for i in 1000..11000 {
			if bloom.might_contain(&i) {
				false_positives += 1;
			}
		}

		// The 0.1% target is ~10 of 10000; 30 is a loose bound that still fails on a systematic defect.
		assert!(false_positives < 30, "Too many false positives: {}", false_positives);
	}

	#[test]
	fn test_bloom_filter_fill_ratio() {
		let bloom = BloomFilter::new(10);

		assert_eq!(bloom.fill_ratio(), 0.0);

		for i in 0..5 {
			bloom.add(&i);
		}

		let ratio = bloom.fill_ratio();
		assert!(ratio > 0.0 && ratio < 1.0);

		for i in 5..100 {
			bloom.add(&i);
		}

		let saturated_ratio = bloom.fill_ratio();
		assert!(saturated_ratio > ratio);
	}
}
