// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::{Equal, Greater, Less};

use reifydb_core::{EncodedKey, interface::MultiVersionValues};

use crate::MultiVersionIter;

/// Multi-version merging iterator that combines multiple iterators
/// Returns each unique key once with the highest version
pub struct MultiVersionMergingIterator<'a> {
	iters: Vec<Box<dyn MultiVersionIter + 'a>>,
	buffers: Vec<Option<MultiVersionValues>>,
}

impl<'a> MultiVersionMergingIterator<'a> {
	pub fn new(mut iters: Vec<Box<dyn MultiVersionIter + 'a>>) -> Self {
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
			if let Some(item) = buffer {
				match min_key {
					None => {
						min_key = Some(&item.key);
						indices_with_min_key.clear();
						indices_with_min_key.push(idx);
					}
					Some(current_min) => match item.key.cmp(current_min) {
						Less => {
							min_key = Some(&item.key);
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
		let mut best_item: Option<MultiVersionValues> = None;

		for &idx in &indices_with_min_key {
			if let Some(item) = self.buffers[idx].take() {
				match &best_item {
					None => best_item = Some(item),
					Some(current_best) => {
						if item.version > current_best.version {
							best_item = Some(item);
						}
					}
				}
			}
		}

		// Refill buffers for iterators that had the minimum key
		for &idx in &indices_with_min_key {
			self.buffers[idx] = self.iters[idx].next();
		}

		best_item
	}
}

// #[cfg(test)]
// mod tests {
// 	use super::*;
// 	use reifydb_core::{CommitVersion, EncodedKey, value::encoded::EncodedValues};
//
// 	struct MockMultiIter {
// 		items: Vec<MultiVersionValues>,
// 		index: usize,
// 	}
//
// 	impl MockMultiIter {
// 		fn new(items: Vec<MultiVersionValues>) -> Self {
// 			Self {
// 				items,
// 				index: 0,
// 			}
// 		}
// 	}
//
// 	impl Iterator for MockMultiIter {
// 		type Item = MultiVersionValues;
//
// 		fn next(&mut self) -> Option<Self::Item> {
// 			if self.index < self.items.len() {
// 				let item = self.items[self.index].clone();
// 				self.index += 1;
// 				Some(item)
// 			} else {
// 				None
// 			}
// 		}
// 	}
//
// 	fn create_key(s: &str) -> EncodedKey {
// 		let mut bytes = s.bytes().collect::<Vec<u8>>();
// 		for b in &mut bytes {
// 			*b = !*b;
// 		}
// 		EncodedKey::from(bytes)
// 	}
//
// 	fn create_values(data: &str) -> EncodedValues {
// 		EncodedValues::from(data.as_bytes().to_vec())
// 	}
//
// 	fn tombstone() -> EncodedValues {
// 		EncodedValues::from(vec![])
// 	}
//
// 	fn is_tombstone(values: &EncodedValues) -> bool {
// 		values.as_ref().is_empty()
// 	}
//
// 	#[test]
// 	fn test_empty_iterators() {
// 		let iter = MultiVersionMergingIterator::new(vec![]);
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 0);
// 	}
//
// 	#[test]
// 	fn test_single_empty_iterator() {
// 		let mock = MockMultiIter::new(vec![]);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(mock)]);
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 0);
// 	}
//
// 	#[test]
// 	fn test_single_iterator_single_item() {
// 		let items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("data_a"),
// 			version: CommitVersion::from(1),
// 		}];
// 		let mock = MockMultiIter::new(items);
// 		let mut iter = MultiVersionMergingIterator::new(vec![Box::new(mock)]);
//
// 		let item = iter.next().unwrap();
// 		assert_eq!(item.key, create_key("A"));
// 		assert_eq!(item.version, CommitVersion::from(1));
// 		assert!(iter.next().is_none());
// 	}
//
// 	#[test]
// 	fn test_single_iterator_multiple_items() {
// 		let items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("data_a"),
// 				version: CommitVersion::from(1),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("data_b"),
// 				version: CommitVersion::from(2),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("data_c"),
// 				version: CommitVersion::from(3),
// 			},
// 		];
// 		let mock = MockMultiIter::new(items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(mock)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 3);
// 		assert_eq!(results[0].key, create_key("A"));
// 		assert_eq!(results[1].key, create_key("B"));
// 		assert_eq!(results[2].key, create_key("C"));
// 	}
//
// 	#[test]
// 	fn test_two_tiers_no_overlap() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("hot_a"),
// 				version: CommitVersion::from(10),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("hot_c"),
// 				version: CommitVersion::from(12),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("warm_b"),
// 				version: CommitVersion::from(5),
// 			},
// 			MultiVersionValues {
// 				key: create_key("D"),
// 				values: create_values("warm_d"),
// 				version: CommitVersion::from(6),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 4);
// 		assert_eq!(results[0].key, create_key("A"));
// 		assert_eq!(results[1].key, create_key("B"));
// 		assert_eq!(results[2].key, create_key("C"));
// 		assert_eq!(results[3].key, create_key("D"));
// 	}
//
// 	#[test]
// 	fn test_two_tiers_with_duplicates_different_versions() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("hot_a"),
// 				version: CommitVersion::from(10),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("hot_c"),
// 				version: CommitVersion::from(12),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("warm_a"),
// 				version: CommitVersion::from(5),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("warm_b"),
// 				version: CommitVersion::from(6),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 3);
// 		assert_eq!(results[0].key, create_key("A"));
// 		assert_eq!(results[0].version, CommitVersion::from(10));
// 		assert_eq!(results[1].key, create_key("B"));
// 		assert_eq!(results[2].key, create_key("C"));
// 	}
//
// 	#[test]
// 	fn test_highest_version_wins() {
// 		let hot_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("hot_a"),
// 			version: CommitVersion::from(10),
// 		}];
//
// 		let warm_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("warm_a"),
// 			version: CommitVersion::from(20),
// 		}];
//
// 		let cold_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("cold_a"),
// 			version: CommitVersion::from(5),
// 		}];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let cold = MockMultiIter::new(cold_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 1);
// 		assert_eq!(results[0].key, create_key("A"));
// 		assert_eq!(results[0].version, CommitVersion::from(20));
// 	}
//
// 	#[test]
// 	fn test_deletion_in_hot_tier() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: tombstone(),
// 				version: CommitVersion::from(10),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("hot_b"),
// 				version: CommitVersion::from(11),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("warm_a"),
// 				version: CommitVersion::from(5),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("warm_b"),
// 				version: CommitVersion::from(6),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);
//
// 		let results: Vec<_> = iter.filter(|item| !is_tombstone(&item.values)).collect();
//
// 		assert_eq!(results.len(), 1);
// 		assert_eq!(results[0].key, create_key("B"));
// 		assert_eq!(results[0].version, CommitVersion::from(11));
// 	}
//
// 	#[test]
// 	fn test_deletion_with_resurrection() {
// 		let hot_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("hot_a_new"),
// 			version: CommitVersion::from(15),
// 		}];
//
// 		let warm_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: tombstone(),
// 			version: CommitVersion::from(10),
// 		}];
//
// 		let cold_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("cold_a"),
// 			version: CommitVersion::from(5),
// 		}];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let cold = MockMultiIter::new(cold_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 1);
// 		assert_eq!(results[0].key, create_key("A"));
// 		assert_eq!(results[0].version, CommitVersion::from(15));
// 		assert!(!is_tombstone(&results[0].values));
// 	}
//
// 	#[test]
// 	fn test_three_tiers_complex_overlap() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("hot_a"),
// 				version: CommitVersion::from(20),
// 			},
// 			MultiVersionValues {
// 				key: create_key("D"),
// 				values: create_values("hot_d"),
// 				version: CommitVersion::from(21),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("warm_b"),
// 				version: CommitVersion::from(15),
// 			},
// 			MultiVersionValues {
// 				key: create_key("D"),
// 				values: create_values("warm_d"),
// 				version: CommitVersion::from(16),
// 			},
// 		];
//
// 		let cold_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("cold_a"),
// 				version: CommitVersion::from(5),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("cold_c"),
// 				version: CommitVersion::from(6),
// 			},
// 			MultiVersionValues {
// 				key: create_key("D"),
// 				values: create_values("cold_d"),
// 				version: CommitVersion::from(7),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let cold = MockMultiIter::new(cold_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 4);
//
// 		assert_eq!(results[0].key, create_key("A"));
// 		assert_eq!(results[0].version, CommitVersion::from(20));
//
// 		assert_eq!(results[1].key, create_key("B"));
// 		assert_eq!(results[1].version, CommitVersion::from(15));
//
// 		assert_eq!(results[2].key, create_key("C"));
// 		assert_eq!(results[2].version, CommitVersion::from(6));
//
// 		assert_eq!(results[3].key, create_key("D"));
// 		assert_eq!(results[3].version, CommitVersion::from(21));
// 	}
//
// 	#[test]
// 	fn test_interleaved_keys() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("hot_b"),
// 				version: CommitVersion::from(20),
// 			},
// 			MultiVersionValues {
// 				key: create_key("D"),
// 				values: create_values("hot_d"),
// 				version: CommitVersion::from(21),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("warm_a"),
// 				version: CommitVersion::from(15),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("warm_c"),
// 				version: CommitVersion::from(16),
// 			},
// 			MultiVersionValues {
// 				key: create_key("E"),
// 				values: create_values("warm_e"),
// 				version: CommitVersion::from(17),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 5);
//
// 		let keys: Vec<_> = results.iter().map(|r| r.key.clone()).collect();
// 		let expected =
// 			vec![create_key("A"), create_key("B"), create_key("C"), create_key("D"), create_key("E")];
// 		assert_eq!(keys, expected);
// 	}
//
// 	#[test]
// 	fn test_all_empty_iterators() {
// 		let hot = MockMultiIter::new(vec![]);
// 		let warm = MockMultiIter::new(vec![]);
// 		let cold = MockMultiIter::new(vec![]);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 0);
// 	}
//
// 	#[test]
// 	fn test_some_empty_some_full() {
// 		let hot = MockMultiIter::new(vec![]);
//
// 		let warm_items = vec![MultiVersionValues {
// 			key: create_key("A"),
// 			values: create_values("warm_a"),
// 			version: CommitVersion::from(10),
// 		}];
// 		let warm = MockMultiIter::new(warm_items);
//
// 		let cold = MockMultiIter::new(vec![]);
//
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 1);
// 		assert_eq!(results[0].key, create_key("A"));
// 	}
//
// 	#[test]
// 	fn test_buffer_refill() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("hot_a1"),
// 				version: CommitVersion::from(20),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("hot_b"),
// 				version: CommitVersion::from(21),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("warm_a"),
// 				version: CommitVersion::from(10),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("warm_c"),
// 				version: CommitVersion::from(11),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let mut iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);
//
// 		let first = iter.next().unwrap();
// 		assert_eq!(first.key, create_key("A"));
// 		assert_eq!(first.version, CommitVersion::from(20));
//
// 		let second = iter.next().unwrap();
// 		assert_eq!(second.key, create_key("B"));
//
// 		let third = iter.next().unwrap();
// 		assert_eq!(third.key, create_key("C"));
//
// 		assert!(iter.next().is_none());
// 	}
//
// 	#[test]
// 	fn test_multiple_deletions() {
// 		let hot_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: tombstone(),
// 				version: CommitVersion::from(30),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: tombstone(),
// 				version: CommitVersion::from(31),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("hot_c"),
// 				version: CommitVersion::from(32),
// 			},
// 		];
//
// 		let warm_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: tombstone(),
// 				version: CommitVersion::from(20),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("warm_b"),
// 				version: CommitVersion::from(21),
// 			},
// 			MultiVersionValues {
// 				key: create_key("C"),
// 				values: create_values("warm_c"),
// 				version: CommitVersion::from(22),
// 			},
// 		];
//
// 		let cold_items = vec![
// 			MultiVersionValues {
// 				key: create_key("A"),
// 				values: create_values("cold_a"),
// 				version: CommitVersion::from(5),
// 			},
// 			MultiVersionValues {
// 				key: create_key("B"),
// 				values: create_values("cold_b"),
// 				version: CommitVersion::from(6),
// 			},
// 		];
//
// 		let hot = MockMultiIter::new(hot_items);
// 		let warm = MockMultiIter::new(warm_items);
// 		let cold = MockMultiIter::new(cold_items);
// 		let iter = MultiVersionMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);
//
// 		let results: Vec<_> = iter.collect();
// 		assert_eq!(results.len(), 3);
//
// 		assert!(is_tombstone(&results[0].values));
// 		assert_eq!(results[0].version, CommitVersion::from(30));
//
// 		assert!(is_tombstone(&results[1].values));
// 		assert_eq!(results[1].version, CommitVersion::from(31));
//
// 		assert!(!is_tombstone(&results[2].values));
// 		assert_eq!(results[2].key, create_key("C"));
// 		assert_eq!(results[2].version, CommitVersion::from(32));
// 	}
// }
