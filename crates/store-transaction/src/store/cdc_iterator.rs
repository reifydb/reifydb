// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::{Equal, Greater, Less};

use reifydb_core::{CommitVersion, interface::Cdc};

/// CDC merging iterator that combines multiple iterators
/// Returns CDC items in chronological order (by CommitVersion)
pub struct CdcMergingIterator<'a> {
	iters: Vec<Box<dyn Iterator<Item = Cdc> + 'a>>,
	buffers: Vec<Option<Cdc>>,
}

impl<'a> CdcMergingIterator<'a> {
	pub fn new(mut iters: Vec<Box<dyn Iterator<Item = Cdc> + 'a>>) -> Self {
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

impl<'a> Iterator for CdcMergingIterator<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		// Find the minimum version across all buffers
		let mut min_version: Option<CommitVersion> = None;
		let mut indices_with_min_version = Vec::new();

		for (idx, buffer) in self.buffers.iter().enumerate() {
			if let Some(cdc_item) = buffer {
				let version = cdc_item.version;

				match min_version {
					None => {
						min_version = Some(version);
						indices_with_min_version.clear();
						indices_with_min_version.push(idx);
					}
					Some(current_min) => match version.cmp(&current_min) {
						Less => {
							min_version = Some(version);
							indices_with_min_version.clear();
							indices_with_min_version.push(idx);
						}
						Equal => {
							indices_with_min_version.push(idx);
						}
						Greater => {}
					},
				}
			}
		}

		if indices_with_min_version.is_empty() {
			return None;
		}

		// Take the item from the first tier (highest priority) when versions are equal
		let chosen_idx = indices_with_min_version[0];
		let result_item = self.buffers[chosen_idx].take();

		// Refill buffers for all iterators that had the minimum version
		for &idx in &indices_with_min_version {
			if self.buffers[idx].is_none() {
				self.buffers[idx] = self.iters[idx].next();
			} else {
				// Discard the item if we didn't use it (happens when versions are equal)
				self.buffers[idx] = None;
				self.buffers[idx] = self.iters[idx].next();
			}
		}

		result_item
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		CowVec, EncodedKey,
		interface::{CdcChange, CdcSequencedChange},
		value::encoded::EncodedValues,
	};

	use super::*;

	struct MockCdcIter {
		items: Vec<Cdc>,
		index: usize,
	}

	impl MockCdcIter {
		fn new(items: Vec<Cdc>) -> Self {
			Self {
				items,
				index: 0,
			}
		}
	}

	impl Iterator for MockCdcIter {
		type Item = Cdc;

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

	fn create_cdc(version: u64, timestamp: u64, key: &str, value: &str) -> Cdc {
		Cdc::new(
			CommitVersion::from(version),
			timestamp,
			vec![CdcSequencedChange {
				sequence: 0,
				change: CdcChange::Insert {
					key: create_key(key),
					post: EncodedValues(CowVec::new(value.as_bytes().to_vec())), /* Create encoded value for the post data */
				},
			}],
		)
	}

	#[test]
	fn test_empty_iterators() {
		let iter = CdcMergingIterator::new(vec![]);
		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_single_empty_iterator() {
		let mock = MockCdcIter::new(vec![]);
		let iter = CdcMergingIterator::new(vec![Box::new(mock)]);
		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_single_iterator_single_item() {
		let items = vec![create_cdc(1, 1000, "key1", "value1")];
		let mock = MockCdcIter::new(items);
		let mut iter = CdcMergingIterator::new(vec![Box::new(mock)]);

		let item = iter.next().unwrap();
		assert_eq!(item.version, CommitVersion::from(1));
		assert_eq!(item.timestamp, 1000);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_single_iterator_multiple_items() {
		let items = vec![
			create_cdc(1, 1000, "key1", "value1"),
			create_cdc(2, 2000, "key2", "value2"),
			create_cdc(3, 3000, "key3", "value3"),
		];
		let mock = MockCdcIter::new(items);
		let iter = CdcMergingIterator::new(vec![Box::new(mock)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].version, CommitVersion::from(1));
		assert_eq!(results[1].version, CommitVersion::from(2));
		assert_eq!(results[2].version, CommitVersion::from(3));
	}

	#[test]
	fn test_two_tiers_no_overlap() {
		let hot_items =
			vec![create_cdc(1, 1000, "key1", "hot_value1"), create_cdc(3, 3000, "key3", "hot_value3")];

		let warm_items =
			vec![create_cdc(2, 2000, "key2", "warm_value2"), create_cdc(4, 4000, "key4", "warm_value4")];

		let hot = MockCdcIter::new(hot_items);
		let warm = MockCdcIter::new(warm_items);
		let iter = CdcMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 4);
		assert_eq!(results[0].version, CommitVersion::from(1));
		assert_eq!(results[1].version, CommitVersion::from(2));
		assert_eq!(results[2].version, CommitVersion::from(3));
		assert_eq!(results[3].version, CommitVersion::from(4));
	}

	#[test]
	fn test_two_tiers_with_same_versions_first_tier_wins() {
		let hot_items =
			vec![create_cdc(1, 1000, "key1", "hot_value1"), create_cdc(3, 3000, "key3", "hot_value3")];

		let warm_items =
			vec![create_cdc(1, 500, "key1", "warm_value1"), create_cdc(2, 2000, "key2", "warm_value2")];

		let hot = MockCdcIter::new(hot_items);
		let warm = MockCdcIter::new(warm_items);
		let iter = CdcMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].version, CommitVersion::from(1));
		assert_eq!(results[0].timestamp, 1000); // Hot tier wins
		assert_eq!(results[1].version, CommitVersion::from(2));
		assert_eq!(results[2].version, CommitVersion::from(3));
	}

	#[test]
	fn test_chronological_ordering() {
		let hot_items =
			vec![create_cdc(5, 5000, "key5", "hot_value5"), create_cdc(10, 10000, "key10", "hot_value10")];

		let warm_items =
			vec![create_cdc(3, 3000, "key3", "warm_value3"), create_cdc(7, 7000, "key7", "warm_value7")];

		let cold_items =
			vec![create_cdc(1, 1000, "key1", "cold_value1"), create_cdc(8, 8000, "key8", "cold_value8")];

		let hot = MockCdcIter::new(hot_items);
		let warm = MockCdcIter::new(warm_items);
		let cold = MockCdcIter::new(cold_items);
		let iter = CdcMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 6);

		// Verify chronological order
		let versions: Vec<_> = results.iter().map(|r| r.version).collect();
		let expected = vec![
			CommitVersion::from(1),
			CommitVersion::from(3),
			CommitVersion::from(5),
			CommitVersion::from(7),
			CommitVersion::from(8),
			CommitVersion::from(10),
		];
		assert_eq!(versions, expected);
	}

	#[test]
	fn test_all_empty_iterators() {
		let hot = MockCdcIter::new(vec![]);
		let warm = MockCdcIter::new(vec![]);
		let cold = MockCdcIter::new(vec![]);
		let iter = CdcMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_some_empty_some_full() {
		let hot = MockCdcIter::new(vec![]);

		let warm_items = vec![create_cdc(5, 5000, "key5", "warm_value5")];
		let warm = MockCdcIter::new(warm_items);

		let cold = MockCdcIter::new(vec![]);

		let iter = CdcMergingIterator::new(vec![Box::new(hot), Box::new(warm), Box::new(cold)]);

		let results: Vec<_> = iter.collect();
		assert_eq!(results.len(), 1);
		assert_eq!(results[0].version, CommitVersion::from(5));
	}

	#[test]
	fn test_buffer_refill() {
		let hot_items =
			vec![create_cdc(1, 1000, "key1", "hot_value1"), create_cdc(5, 5000, "key5", "hot_value5")];

		let warm_items =
			vec![create_cdc(2, 2000, "key2", "warm_value2"), create_cdc(6, 6000, "key6", "warm_value6")];

		let hot = MockCdcIter::new(hot_items);
		let warm = MockCdcIter::new(warm_items);
		let mut iter = CdcMergingIterator::new(vec![Box::new(hot), Box::new(warm)]);

		let first = iter.next().unwrap();
		assert_eq!(first.version, CommitVersion::from(1));

		let second = iter.next().unwrap();
		assert_eq!(second.version, CommitVersion::from(2));

		let third = iter.next().unwrap();
		assert_eq!(third.version, CommitVersion::from(5));

		let fourth = iter.next().unwrap();
		assert_eq!(fourth.version, CommitVersion::from(6));

		assert!(iter.next().is_none());
	}
}
