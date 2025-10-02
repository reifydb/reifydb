// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::{Equal, Greater, Less};

use reifydb_core::{CommitVersion, EncodedKey, interface::MultiVersionValues};

use crate::backend::result::MultiVersionIterResult;

/// Multi-version merging iterator that combines multiple iterators
/// Returns each unique key once with the highest version
pub struct MultiVersionMergingIterator<'a> {
	iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + 'a>>,
	buffers: Vec<Option<MultiVersionIterResult>>,
}

impl<'a> MultiVersionMergingIterator<'a> {
	pub fn new(mut iters: Vec<Box<dyn Iterator<Item = MultiVersionIterResult> + Send + 'a>>) -> Self {
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

impl<'a> Iterator for MultiVersionMergingIterator<'a> {
	type Item = MultiVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		// Find the minimum key across all buffers
		let mut min_key: Option<&EncodedKey> = None;
		let mut indices_with_min_key = Vec::new();

		for (idx, buffer) in self.buffers.iter().enumerate() {
			if let Some(item_result) = buffer {
				// Extract key from either Value or Tombstone
				let key = match item_result {
					MultiVersionIterResult::Value(v) => &v.key,
					MultiVersionIterResult::Tombstone {
						key,
						..
					} => key,
				};

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

		// Find the highest version among all items with the minimum key
		// Both values and tombstones have versions
		let mut best_item: Option<MultiVersionIterResult> = None;
		let mut best_version = CommitVersion(0);

		for &idx in &indices_with_min_key {
			if let Some(item_result) = self.buffers[idx].take() {
				let version = match &item_result {
					MultiVersionIterResult::Value(v) => v.version,
					MultiVersionIterResult::Tombstone {
						version,
						..
					} => *version,
				};

				if best_item.is_none() || version > best_version {
					best_item = Some(item_result);
					best_version = version;
				}
			}
		}

		// Refill buffers for iterators that had the minimum key
		for &idx in &indices_with_min_key {
			self.buffers[idx] = self.iters[idx].next();
		}

		// Convert result to Option<MultiVersionValues>, filtering out tombstones
		match best_item? {
			MultiVersionIterResult::Value(values) => Some(values),
			MultiVersionIterResult::Tombstone {
				..
			} => {
				// Tombstone found - continue to next iteration
				self.next()
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{CommitVersion, CowVec, EncodedKey, value::encoded::EncodedValues};

	use super::*;

	struct MockMultiIter {
		items: Vec<MultiVersionIterResult>,
		index: usize,
	}

	impl MockMultiIter {
		fn new(items: Vec<MultiVersionIterResult>) -> Self {
			Self {
				items,
				index: 0,
			}
		}
	}

	impl Iterator for MockMultiIter {
		type Item = MultiVersionIterResult;

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

	fn value_result(key: &str, data: &str, version: u64) -> MultiVersionIterResult {
		MultiVersionIterResult::Value(MultiVersionValues {
			key: create_key(key),
			values: create_values(data),
			version: CommitVersion::from(version),
		})
	}

	fn tombstone_result(key: &str, version: u64) -> MultiVersionIterResult {
		MultiVersionIterResult::Tombstone {
			key: create_key(key),
			version: CommitVersion::from(version),
		}
	}

	#[test]
	fn test_empty_iterators() {
		let iter = MultiVersionMergingIterator::new(vec![]);
		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_single_empty_iterator() {
		let mock = MockMultiIter::new(vec![]);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(mock)]);
		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_single_iterator_single_item() {
		let items = vec![value_result("A", "data_a", 1)];
		let mock = MockMultiIter::new(items);
		let mut iter = MultiVersionMergingIterator::new(vec![Box::new(mock)]);

		let item = iter.next().unwrap();
		assert_eq!(item.key, create_key("A"));
		assert_eq!(item.version, CommitVersion::from(1));
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_single_iterator_multiple_items() {
		let items = vec![
			value_result("A", "data_a", 1),
			value_result("B", "data_b", 2),
			value_result("C", "data_c", 3),
		];
		let mock = MockMultiIter::new(items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(mock)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[2].key, create_key("C"));
	}

	#[test]
	fn test_two_tiers_no_overlap() {
		let hot_items = vec![value_result("A", "hot_a", 10), value_result("C", "hot_c", 12)];

		let warm_items = vec![value_result("B", "warm_b", 5), value_result("D", "warm_d", 6)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 4);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[2].key, create_key("C"));
		assert_eq!(results[3].key, create_key("D"));
	}

	#[test]
	fn test_two_tiers_with_duplicates_different_versions() {
		let hot_items = vec![value_result("A", "hot_a", 10), value_result("C", "hot_c", 12)];

		let warm_items = vec![value_result("A", "warm_a", 5), value_result("B", "warm_b", 6)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].version, CommitVersion::from(10));
		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[2].key, create_key("C"));
	}

	#[test]
	fn test_highest_version_wins() {
		let hot_items = vec![value_result("A", "hot_a", 10)];

		let warm_items = vec![value_result("A", "warm_a", 20)];

		let cold_items = vec![value_result("A", "cold_a", 5)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let cold = MockMultiIter::new(cold_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].version, CommitVersion::from(20));
	}

	#[test]
	fn test_deletion_in_hot_tier() {
		let hot_items = vec![tombstone_result("A", 10), value_result("B", "hot_b", 11)];

		let warm_items = vec![value_result("A", "warm_a", 5), value_result("B", "warm_b", 6)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();

		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("B"));
		assert_eq!(results[0].version, CommitVersion::from(11));
	}

	#[test]
	fn test_deletion_with_resurrection() {
		let hot_items = vec![value_result("A", "hot_a_new", 15)];

		let warm_items = vec![tombstone_result("A", 10)];

		let cold_items = vec![value_result("A", "cold_a", 5)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let cold = MockMultiIter::new(cold_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].version, CommitVersion::from(15));
		assert!(!results[0].values.as_ref().is_empty());
	}

	#[test]
	fn test_three_tiers_complex_overlap() {
		let hot_items = vec![value_result("A", "hot_a", 20), value_result("D", "hot_d", 21)];

		let warm_items = vec![value_result("B", "warm_b", 15), value_result("D", "warm_d", 16)];

		let cold_items = vec![
			value_result("A", "cold_a", 5),
			value_result("C", "cold_c", 6),
			value_result("D", "cold_d", 7),
		];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let cold = MockMultiIter::new(cold_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 4);

		assert_eq!(results[0].key, create_key("A"));
		assert_eq!(results[0].version, CommitVersion::from(20));

		assert_eq!(results[1].key, create_key("B"));
		assert_eq!(results[1].version, CommitVersion::from(15));

		assert_eq!(results[2].key, create_key("C"));
		assert_eq!(results[2].version, CommitVersion::from(6));

		assert_eq!(results[3].key, create_key("D"));
		assert_eq!(results[3].version, CommitVersion::from(21));
	}

	#[test]
	fn test_interleaved_keys() {
		let hot_items = vec![value_result("B", "hot_b", 20), value_result("D", "hot_d", 21)];

		let warm_items = vec![
			value_result("A", "warm_a", 15),
			value_result("C", "warm_c", 16),
			value_result("E", "warm_e", 17),
		];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 5);

		let keys: Vec<_> = results.iter().map(|r| r.key.clone()).collect();
		let expected =
			vec![create_key("A"), create_key("B"), create_key("C"), create_key("D"), create_key("E")];
		assert_eq!(keys, expected);
	}

	#[test]
	fn test_all_empty_iterators() {
		let hot = MockMultiIter::new(vec![]);
		let warm = MockMultiIter::new(vec![]);
		let cold = MockMultiIter::new(vec![]);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_some_empty_some_full() {
		let hot = MockMultiIter::new(vec![]);

		let warm_items = vec![value_result("A", "warm_a", 10)];
		let warm = MockMultiIter::new(warm_items);

		let cold = MockMultiIter::new(vec![]);

		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].key, create_key("A"));
	}

	#[test]
	fn test_buffer_refill() {
		let hot_items = vec![value_result("A", "hot_a1", 20), value_result("B", "hot_b", 21)];

		let warm_items = vec![value_result("A", "warm_a", 10), value_result("C", "warm_c", 11)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let mut iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let first = iter.next().unwrap();
		assert_eq!(first.key, create_key("A"));
		assert_eq!(first.version, CommitVersion::from(20));

		let second = iter.next().unwrap();
		assert_eq!(second.key, create_key("B"));

		let third = iter.next().unwrap();
		assert_eq!(third.key, create_key("C"));

		assert!(iter.next().is_none());
	}

	#[test]
	fn test_multiple_deletions() {
		let hot_items =
			vec![tombstone_result("A", 30), tombstone_result("B", 31), value_result("C", "hot_c", 32)];

		let warm_items = vec![
			tombstone_result("A", 20),
			value_result("B", "warm_b", 21),
			value_result("C", "warm_c", 22),
		];

		let cold_items = vec![value_result("A", "cold_a", 5), value_result("B", "cold_b", 6)];

		let hot = MockMultiIter::new(hot_items);
		let warm = MockMultiIter::new(warm_items);
		let cold = MockMultiIter::new(cold_items);
		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);

		assert_eq!(results[0].key, create_key("C"));
		assert_eq!(results[0].version, CommitVersion::from(32));
	}
}
