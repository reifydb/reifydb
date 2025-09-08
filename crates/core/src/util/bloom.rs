// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::hash::{Hash, Hasher};

/// A simple bloom filter for fast negative membership checks
///
/// This implementation provides O(1) membership testing with a configurable
/// false positive rate. It's particularly useful for conflict detection
/// in transaction processing where we want to quickly rule out non-conflicts.
#[derive(Debug, Clone)]
pub struct BloomFilter {
	bits: Vec<u64>,
	size: usize,
	hash_count: usize,
}

impl BloomFilter {
	/// Create a new bloom filter optimized for the expected number of items
	/// with approximately 1% false positive rate
	pub fn new(expected_items: usize) -> Self {
		// Calculate optimal size for ~1% false positive rate
		// Formula: m = -n * ln(p) / (ln(2)^2) where p = 0.01
		let size = (expected_items as f64 * 10.0) as usize;
		let word_count = (size + 63) / 64;
		Self {
			bits: vec![0; word_count],
			size: word_count * 64,
			hash_count: 7, // Optimal k = m/n * ln(2) ≈ 7 for 1% FPR
		}
	}

	/// Create a bloom filter with custom parameters
	pub fn with_params(size_bits: usize, hash_count: usize) -> Self {
		let word_count = (size_bits + 63) / 64;
		Self {
			bits: vec![0; word_count],
			size: word_count * 64,
			hash_count,
		}
	}

	/// Add an item to the bloom filter
	pub fn add<T: Hash>(&mut self, item: &T) {
		let hash = self.hash(item);
		for i in 0..self.hash_count {
			let bit_pos = self.get_bit_pos(hash, i);
			let word_idx = bit_pos / 64;
			let bit_idx = bit_pos % 64;
			self.bits[word_idx] |= 1u64 << bit_idx;
		}
	}

	/// Check if an item might be in the set
	/// Returns false if definitely not in the set
	/// Returns true if possibly in the set (may be false positive)
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

	/// Clear all bits in the bloom filter
	pub fn clear(&mut self) {
		self.bits.fill(0);
	}

	/// Check if the bloom filter is empty
	pub fn is_empty(&self) -> bool {
		self.bits.iter().all(|&word| word == 0)
	}

	/// Estimate the number of items in the bloom filter
	/// This is based on the number of set bits
	pub fn estimated_items(&self) -> usize {
		let set_bits = self
			.bits
			.iter()
			.map(|&word| word.count_ones() as usize)
			.sum::<usize>();

		// Formula: n ≈ -m/k * ln(1 - X/m) where X is set bits
		let fill_ratio = set_bits as f64 / self.size as f64;
		if fill_ratio >= 1.0 {
			// Filter is saturated
			return usize::MAX;
		}

		let estimated = -(self.size as f64 / self.hash_count as f64)
			* (1.0 - fill_ratio).ln();
		estimated as usize
	}

	/// Get the fill ratio (proportion of bits set)
	pub fn fill_ratio(&self) -> f64 {
		let set_bits = self
			.bits
			.iter()
			.map(|&word| word.count_ones() as usize)
			.sum::<usize>();
		set_bits as f64 / self.size as f64
	}

	#[inline]
	fn hash<T: Hash>(&self, item: &T) -> u64 {
		use std::collections::hash_map::DefaultHasher;
		let mut hasher = DefaultHasher::new();
		item.hash(&mut hasher);
		hasher.finish()
	}

	#[inline]
	fn get_bit_pos(&self, hash: u64, i: usize) -> usize {
		// Use double hashing for multiple hash functions
		// h_i(x) = h1(x) + i * h2(x) mod m
		let h1 = hash as usize;
		let h2 = (hash >> 32) as usize | 1; // Ensure h2 is odd for better distribution
		(h1.wrapping_add(i.wrapping_mul(h2))) % self.size
	}
}

/// Builder for creating bloom filters with specific false positive rates
pub struct BloomFilterBuilder {
	expected_items: usize,
	false_positive_rate: f64,
}

impl BloomFilterBuilder {
	/// Create a new builder with the expected number of items
	pub fn new(expected_items: usize) -> Self {
		Self {
			expected_items,
			false_positive_rate: 0.01, // Default 1%
		}
	}

	/// Set the desired false positive rate (between 0 and 1)
	pub fn false_positive_rate(mut self, rate: f64) -> Self {
		assert!(
			rate > 0.0 && rate < 1.0,
			"False positive rate must be between 0 and 1"
		);
		self.false_positive_rate = rate;
		self
	}

	/// Build the bloom filter with optimal parameters
	pub fn build(self) -> BloomFilter {
		// Calculate optimal bit array size
		// m = -n * ln(p) / (ln(2)^2)
		let ln2_squared = 0.693147f64.powi(2);
		let size_bits = (-(self.expected_items as f64)
			* self.false_positive_rate.ln()
			/ ln2_squared) as usize;

		// Calculate optimal number of hash functions
		// k = m/n * ln(2)
		let hash_count = ((size_bits as f64
			/ self.expected_items as f64)
			* 0.693147)
			.round() as usize;

		BloomFilter::with_params(size_bits, hash_count.max(1))
	}
}

#[cfg(test)]
mod tests {
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
		assert!(!bloom.might_contain(&"foo")
			|| !bloom.might_contain(&"bar"));

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
		assert!(
			false_positives < 30,
			"Too many false positives: {}",
			false_positives
		);
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
