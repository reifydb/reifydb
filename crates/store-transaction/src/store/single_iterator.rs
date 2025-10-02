// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::{Equal, Greater, Less};

use reifydb_core::{EncodedKey, interface::SingleVersionValues};

use crate::backend::result::SingleVersionIterResult;

/// Single-version merging iterator that combines multiple iterators
/// Returns each unique key once (from the highest priority tier)
pub struct SingleVersionMergingIterator<'a> {
	iters: Vec<Box<dyn Iterator<Item = SingleVersionIterResult> + Send + 'a>>,
	buffers: Vec<Option<SingleVersionIterResult>>,
}

impl<'a> SingleVersionMergingIterator<'a> {
	pub fn new(mut iters: Vec<Box<dyn Iterator<Item = SingleVersionIterResult> + Send + 'a>>) -> Self {
		let mut buffers = Vec::with_capacity(iters.len());
		for iter in iters.iter_mut() {
			buffers.push(iter.next());
		}

		Self {
			iters,
			buffers,
		}
	}
}

impl<'a> Iterator for SingleVersionMergingIterator<'a> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		let mut min_key: Option<&EncodedKey> = None;
		let mut indices_with_min_key = Vec::new();

		for (idx, buffer) in self.buffers.iter().enumerate() {
			if let Some(item_result) = buffer {
				// Extract key from either Value or Tombstone
				let key = item_result.key();

				match min_key {
					None => {
						min_key = Some(key);
						indices_with_min_key.clear();
						indices_with_min_key.push(idx);
					}
					Some(current_min) => match key.cmp(current_min) {
						Less => {
							min_key = Some(key);
							indices_with_min_key.clear();
							indices_with_min_key.push(idx);
						}
						Equal => {
							indices_with_min_key.push(idx);
						}
						Greater => {}
					},
				}
			}
		}

		if indices_with_min_key.is_empty() {
			return None;
		}

		// Take the item from the first tier (highest priority)
		let result_item = self.buffers[indices_with_min_key[0]].take();

		// Refill buffers for all iterators that had the minimum key
		for &idx in &indices_with_min_key {
			if self.buffers[idx].is_none() {
				self.buffers[idx] = self.iters[idx].next();
			} else {
				// Discard the item if we didn't use it
				self.buffers[idx] = None;
				self.buffers[idx] = self.iters[idx].next();
			}
		}

		match result_item? {
			SingleVersionIterResult::Value(values) => Some(values),
			SingleVersionIterResult::Tombstone {
				..
			} => self.next(),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{CowVec, EncodedKey, value::encoded::EncodedValues};

	use super::*;

	struct MockSingleIter {
		items: Vec<SingleVersionIterResult>,
		index: usize,
	}

	impl MockSingleIter {
		fn new(items: Vec<SingleVersionIterResult>) -> Self {
			Self {
				items,
				index: 0,
			}
		}
	}

	impl Iterator for MockSingleIter {
		type Item = SingleVersionIterResult;

		fn next(&mut self) -> Option<Self::Item> {
			if self.index < self.items.len() {
				let item = self.items[self.index].clone();
				self.index += 1;
				Some(item)
			} else {
				None
			}
		}
	}

	fn create_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn create_values(data: &str) -> EncodedValues {
		EncodedValues(CowVec::new(data.as_bytes().to_vec()))
	}

	fn single_value_result(key: &str, data: &str) -> SingleVersionIterResult {
		SingleVersionIterResult::Value(SingleVersionValues {
			key: create_key(key),
			values: create_values(data),
		})
	}

	fn single_tombstone_result(key: &str) -> SingleVersionIterResult {
		SingleVersionIterResult::Tombstone {
			key: create_key(key),
		}
	}

	#[test]
	fn test_empty_iterators() {
		let iter = SingleVersionMergingIterator::new(vec![]);
		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_single_empty_iterator() {
		let mock = MockSingleIter::new(vec![]);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(mock)]);
		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_single_iterator_single_item() {
		let items = vec![single_value_result("A", "data_a")];
		let mock = MockSingleIter::new(items);
		let mut iter = SingleVersionMergingIterator::new(vec![Box::new(mock)]);

		let item = iter.next().unwrap();
		assert_eq!(item.key, create_key("A"));
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_single_iterator_multiple_items() {
		let items = vec![
			single_value_result("A", "data_a"),
			single_value_result("B", "data_b"),
			single_value_result("C", "data_c"),
		];
		let mock = MockSingleIter::new(items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(mock)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[2].key, create_key("C"));
	}

	#[test]
	fn test_two_tiers_no_overlap() {
		let hot_items = vec![single_value_result("A", "hot_a"), single_value_result("C", "hot_c")];

		let warm_items = vec![single_value_result("B", "warm_b"), single_value_result("D", "warm_d")];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 4);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[2].key, create_key("C"));
		assert_eq!(results[3].key, create_key("D"));
	}

	#[test]
	fn test_two_tiers_with_duplicates_first_tier_wins() {
		let hot_items = vec![single_value_result("A", "hot_a"), single_value_result("C", "hot_c")];

		let warm_items = vec![single_value_result("A", "warm_a"), single_value_result("B", "warm_b")];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].values, create_values("hot_a")); // First tier wins
		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[2].key, create_key("C"));
	}

	#[test]
	fn test_first_tier_priority() {
		let hot_items = vec![single_value_result("A", "hot_a")];
		let warm_items = vec![single_value_result("A", "warm_a")];
		let cold_items = vec![single_value_result("A", "cold_a")];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let cold = MockSingleIter::new(cold_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].values, create_values("hot_a"));
	}

	#[test]
	fn test_deletion_in_hot_tier() {
		let hot_items = vec![single_tombstone_result("A"), single_value_result("B", "hot_b")];

		let warm_items = vec![single_value_result("A", "warm_a"), single_value_result("B", "warm_b")];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		// Tombstone A is filtered out, B comes from hot tier
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("B"));
		assert_eq!(results[0].values, create_values("hot_b"));
	}

	#[test]
	fn test_three_tiers_complex_overlap() {
		let hot_items = vec![single_value_result("A", "hot_a"), single_value_result("D", "hot_d")];

		let warm_items = vec![single_value_result("B", "warm_b"), single_value_result("D", "warm_d")];

		let cold_items = vec![
			single_value_result("A", "cold_a"),
			single_value_result("C", "cold_c"),
			single_value_result("D", "cold_d"),
		];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let cold = MockSingleIter::new(cold_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 4);

		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].values, create_values("hot_a")); // Hot tier wins

		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[1].values, create_values("warm_b")); // Warm tier

		assert_eq!(results[2].key, create_key("C"));
		assert_eq!(results[2].values, create_values("cold_c")); // Cold tier

		assert_eq!(results[3].key, create_key("D"));
		assert_eq!(results[3].values, create_values("hot_d")); // Hot tier wins
	}

	#[test]
	fn test_interleaved_keys() {
		let hot_items = vec![single_value_result("B", "hot_b"), single_value_result("D", "hot_d")];

		let warm_items = vec![
			single_value_result("A", "warm_a"),
			single_value_result("C", "warm_c"),
			single_value_result("E", "warm_e"),
		];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 5);

		let keys: Vec<_> = results.iter().map(|r| r.key.clone()).collect();
		let expected =
			vec![create_key("A"), create_key("B"), create_key("C"), create_key("D"), create_key("E")];
		assert_eq!(keys, expected);
	}

	#[test]
	fn test_all_empty_iterators() {
		let hot = MockSingleIter::new(vec![]);
		let warm = MockSingleIter::new(vec![]);
		let cold = MockSingleIter::new(vec![]);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_some_empty_some_full() {
		let hot = MockSingleIter::new(vec![]);

		let warm_items = vec![single_value_result("A", "warm_a")];
		let warm = MockSingleIter::new(warm_items);

		let cold = MockSingleIter::new(vec![]);

		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("A"));
	}

	#[test]
	fn test_buffer_refill() {
		let hot_items = vec![single_value_result("A", "hot_a1"), single_value_result("B", "hot_b")];

		let warm_items = vec![single_value_result("A", "warm_a"), single_value_result("C", "warm_c")];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let mut iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let first = iter.next().unwrap();
		assert_eq!(first.key, create_key("A"));
		assert_eq!(first.values, create_values("hot_a1")); // Hot tier wins

		let second = iter.next().unwrap();
		assert_eq!(second.key, create_key("B"));

		let third = iter.next().unwrap();
		assert_eq!(third.key, create_key("C"));

		assert!(iter.next().is_none());
	}

	#[test]
	fn test_multiple_deletions() {
		let hot_items = vec![
			single_tombstone_result("A"),
			single_tombstone_result("B"),
			single_value_result("C", "hot_c"),
		];

		let warm_items = vec![
			single_tombstone_result("A"),
			single_value_result("B", "warm_b"),
			single_value_result("C", "warm_c"),
		];

		let cold_items = vec![single_value_result("A", "cold_a"), single_value_result("B", "cold_b")];

		let hot = MockSingleIter::new(hot_items);
		let warm = MockSingleIter::new(warm_items);
		let cold = MockSingleIter::new(cold_items);
		let iter = SingleVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		// A and B are tombstones and filtered out, only C remains
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("C"));
		assert_eq!(results[0].values, create_values("hot_c"));
	}
}
