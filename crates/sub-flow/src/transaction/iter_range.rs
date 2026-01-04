// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// These types are now only used by tests (production code uses streaming),
// but we keep them for testing the merge algorithm.
#![allow(dead_code)]

use std::{cmp::Ordering, collections::btree_map::Range as BTreeMapRange};

use reifydb_core::{CommitVersion, EncodedKey, interface::MultiVersionValues};
use reifydb_store_transaction::MultiVersionBatch;

use super::Pending;

/// Iterator that merges pending writes with committed range query results
///
/// This iterator combines two sorted streams within a bounded range:
/// 1. Pending writes from the local BTreeMap range (sorted by key)
/// 2. Committed results from the underlying query transaction range
///
/// The merge algorithm:
/// - Compares keys at each step
/// - Prefers pending writes when keys are equal (overlay semantics)
/// - Filters out removed keys (Pending::Remove)
/// - Maintains sorted order
pub struct FlowRangeIter<'a> {
	/// Iterator over committed results from query transaction
	committed: Box<dyn Iterator<Item = MultiVersionValues> + Send + 'a>,
	/// Range iterator over pending writes in sorted order
	pending: BTreeMapRange<'a, EncodedKey, Pending>,
	/// Pre-fetched next pending item for lookahead comparison
	next_pending: Option<(&'a EncodedKey, &'a Pending)>,
	/// Pre-fetched next committed item for lookahead comparison
	next_committed: Option<MultiVersionValues>,
	/// Fixed version for pending writes (CDC version)
	version: CommitVersion,
}

impl<'a> FlowRangeIter<'a> {
	/// Create a new merge iterator for range queries
	pub fn new(
		pending: BTreeMapRange<'a, EncodedKey, Pending>,
		committed: Box<dyn Iterator<Item = MultiVersionValues> + Send + 'a>,
		version: CommitVersion,
	) -> Self {
		let mut iterator = Self {
			pending,
			committed,
			next_pending: None,
			next_committed: None,
			version,
		};

		iterator.advance_pending();
		iterator.advance_committed();

		iterator
	}

	/// Advance the pending iterator and cache the next item
	fn advance_pending(&mut self) {
		self.next_pending = self.pending.next();
	}

	/// Advance the committed iterator and cache the next item
	fn advance_committed(&mut self) {
		self.next_committed = self.committed.next();
	}
}

impl<'a> Iterator for FlowRangeIter<'a> {
	type Item = MultiVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			match (&self.next_pending, &self.next_committed) {
				// Both pending and committed have items
				(Some((pending_key, _pending_value)), Some(committed)) => {
					match pending_key.as_ref().cmp(committed.key.as_ref()) {
						// Pending has smaller key - yield pending if it's a write
						Ordering::Less => {
							let (key, value) = self.next_pending.take().unwrap();
							self.advance_pending();

							match value {
								Pending::Set(values) => {
									return Some(MultiVersionValues {
										key: key.clone(),
										values: values.clone(),
										version: self.version,
									});
								}
								Pending::Remove => continue, // Skip removed keys
							}
						}
						// Keys are equal - prefer pending, skip committed
						Ordering::Equal => {
							let (key, value) = self.next_pending.take().unwrap();
							self.advance_pending();
							self.advance_committed(); // Skip the duplicate committed entry

							match value {
								Pending::Set(values) => {
									return Some(MultiVersionValues {
										key: key.clone(),
										values: values.clone(),
										version: self.version,
									});
								}
								Pending::Remove => continue, // Skip removed keys
							}
						}
						// Committed has smaller key - yield committed
						Ordering::Greater => {
							let committed = self.next_committed.take().unwrap();
							self.advance_committed();
							return Some(committed);
						}
					}
				}
				// Only pending items left
				(Some(_), None) => {
					let (key, value) = self.next_pending.take().unwrap();
					self.advance_pending();

					match value {
						Pending::Set(values) => {
							return Some(MultiVersionValues {
								key: key.clone(),
								values: values.clone(),
								version: self.version,
							});
						}
						Pending::Remove => continue, // Skip removed keys
					}
				}
				// Only committed items left
				(None, Some(_)) => {
					let committed = self.next_committed.take().unwrap();
					self.advance_committed();
					return Some(committed);
				}
				// Both exhausted
				(None, None) => return None,
			}
		}
	}
}

/// Collect a merged batch of pending and committed values
///
/// This function uses the FlowRangeIter to merge pending writes with committed batch results,
/// materializing all items into a single batch. The `has_more` field is always false because
/// pending writes are finite and fully materialized.
pub fn collect_batch(
	pending: BTreeMapRange<'_, EncodedKey, Pending>,
	committed_batch: MultiVersionBatch,
	version: CommitVersion,
) -> MultiVersionBatch {
	// Create iterator with same merge logic
	let iter = FlowRangeIter::new(pending, Box::new(committed_batch.items.into_iter()), version);

	// Materialize all items
	let items: Vec<_> = iter.collect();

	MultiVersionBatch {
		items,
		has_more: false,
	}
}

#[cfg(test)]
mod tests {

	use std::collections::BTreeMap;

	use reifydb_core::{
		CommitVersion, CowVec, EncodedKey, interface::MultiVersionValues, value::encoded::EncodedValues,
	};

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_committed(key: &str, value: &str, version: u64) -> MultiVersionValues {
		MultiVersionValues {
			key: make_key(key),
			values: make_value(value),
			version: CommitVersion(version),
		}
	}

	#[tokio::test]
	async fn test_empty_range_both_iterators() {
		let pending: BTreeMap<EncodedKey, Pending> = BTreeMap::new();
		let committed: Vec<MultiVersionValues> = vec![];

		let mut iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(1));

		assert!(iter.next().is_none());
	}

	#[tokio::test]
	async fn test_range_only_pending() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("1")));
		pending.insert(make_key("b"), Pending::Set(make_value("2")));
		pending.insert(make_key("c"), Pending::Set(make_value("3")));
		pending.insert(make_key("d"), Pending::Set(make_value("4")));

		let committed: Vec<MultiVersionValues> = vec![];

		// Range from "b" to "d" (exclusive)
		let iter = FlowRangeIter::new(
			pending.range(make_key("b")..make_key("d")),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_only_committed() {
		let pending: BTreeMap<EncodedKey, Pending> = BTreeMap::new();
		let committed =
			vec![make_committed("a", "1", 5), make_committed("b", "2", 6), make_committed("c", "3", 7)];

		let iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_filters_removes() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("1")));
		pending.insert(make_key("b"), Pending::Remove);
		pending.insert(make_key("c"), Pending::Set(make_value("3")));

		let committed: Vec<MultiVersionValues> = vec![];

		let iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_pending_shadows_committed() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("b"), Pending::Set(make_value("new")));

		let committed =
			vec![make_committed("a", "1", 5), make_committed("b", "old", 6), make_committed("c", "3", 7)];

		let iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 3);
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[1].values, make_value("new"));
		assert_eq!(items[1].version, CommitVersion(10));
	}

	#[tokio::test]
	async fn test_range_remove_hides_committed() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("b"), Pending::Remove);

		let committed =
			vec![make_committed("a", "1", 5), make_committed("b", "2", 6), make_committed("c", "3", 7)];

		let iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_bounded_query() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("a")));
		pending.insert(make_key("b"), Pending::Set(make_value("b")));
		pending.insert(make_key("c"), Pending::Set(make_value("c")));
		pending.insert(make_key("d"), Pending::Set(make_value("d")));
		pending.insert(make_key("e"), Pending::Set(make_value("e")));
		pending.insert(make_key("f"), Pending::Set(make_value("f")));

		let committed = vec![make_committed("b", "old_b", 5), make_committed("f", "f_old", 6)];

		// Range from "b" (inclusive) to "e" (exclusive)
		let iter = FlowRangeIter::new(
			pending.range(make_key("b")..make_key("e")),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();

		assert_eq!(items.len(), 4);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[0].values, make_value("b")); // Pending value

		assert_eq!(items[1].key, make_key("c"));
		assert_eq!(items[2].key, make_key("d"));
		assert_eq!(items[3].key, make_key("f"));
		assert_eq!(items[3].values, make_value("f_old"));
	}

	#[tokio::test]
	async fn test_range_interleaved_merge() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("b"), Pending::Set(make_value("pending_b")));
		pending.insert(make_key("d"), Pending::Set(make_value("pending_d")));

		let committed = vec![
			make_committed("a", "committed_a", 5),
			make_committed("c", "committed_c", 6),
			make_committed("e", "committed_e", 7),
		];

		let iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 5);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
		assert_eq!(items[3].key, make_key("d"));
		assert_eq!(items[4].key, make_key("e"));
	}

	#[tokio::test]
	async fn test_range_sorted_order() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("m"), Pending::Set(make_value("m")));
		pending.insert(make_key("a"), Pending::Set(make_value("a")));
		pending.insert(make_key("z"), Pending::Set(make_value("z")));

		let committed =
			vec![make_committed("d", "d", 5), make_committed("k", "k", 6), make_committed("p", "p", 7)];

		let iter = FlowRangeIter::new(pending.range(..), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 6);

		let keys: Vec<_> = items.iter().map(|i| i.key.clone()).collect();
		assert_eq!(
			keys,
			vec![make_key("a"), make_key("d"), make_key("k"), make_key("m"), make_key("p"), make_key("z")]
		);
	}

	#[tokio::test]
	async fn test_range_with_start_bound() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("a")));
		pending.insert(make_key("b"), Pending::Set(make_value("b")));
		pending.insert(make_key("c"), Pending::Set(make_value("c")));
		pending.insert(make_key("d"), Pending::Set(make_value("d")));

		let committed: Vec<MultiVersionValues> = vec![];

		// Start from "b" onwards
		let iter = FlowRangeIter::new(
			pending.range(make_key("b")..),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
		assert_eq!(items[2].key, make_key("d"));
	}

	#[tokio::test]
	async fn test_range_with_end_bound() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("a")));
		pending.insert(make_key("b"), Pending::Set(make_value("b")));
		pending.insert(make_key("c"), Pending::Set(make_value("c")));
		pending.insert(make_key("d"), Pending::Set(make_value("d")));

		let committed: Vec<MultiVersionValues> = vec![];

		// Up to "c" (exclusive)
		let iter = FlowRangeIter::new(
			pending.range(..make_key("c")),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
	}

	#[tokio::test]
	async fn test_range_inclusive_bounds() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("a")));
		pending.insert(make_key("b"), Pending::Set(make_value("b")));
		pending.insert(make_key("c"), Pending::Set(make_value("c")));
		pending.insert(make_key("d"), Pending::Set(make_value("d")));

		let committed: Vec<MultiVersionValues> = vec![];

		// From "b" to "c" inclusive
		let iter = FlowRangeIter::new(
			pending.range(make_key("b")..=make_key("c")),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[tokio::test]
	async fn test_range_complex_scenario() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("new_a")));
		pending.insert(make_key("b"), Pending::Remove);
		pending.insert(make_key("c"), Pending::Set(make_value("new_c")));
		pending.insert(make_key("f"), Pending::Remove);
		pending.insert(make_key("g"), Pending::Set(make_value("new_g")));

		let committed = vec![
			make_committed("a", "old_a", 5),
			make_committed("b", "old_b", 6),
			make_committed("d", "old_d", 7),
			make_committed("e", "old_e", 8),
			make_committed("f", "old_f", 9),
		];

		// Range from "a" to "g" (exclusive)
		let iter = FlowRangeIter::new(
			pending.range(make_key("a")..make_key("g")),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();

		// Expected: a(new), c(new), d(old), e(old)
		// Removed: b, f
		assert_eq!(items.len(), 4);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[0].values, make_value("new_a"));
		assert_eq!(items[1].key, make_key("c"));
		assert_eq!(items[1].values, make_value("new_c"));
		assert_eq!(items[2].key, make_key("d"));
		assert_eq!(items[2].values, make_value("old_d"));
		assert_eq!(items[3].key, make_key("e"));
		assert_eq!(items[3].values, make_value("old_e"));
	}

	#[tokio::test]
	async fn test_range_empty_result() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Set(make_value("a")));
		pending.insert(make_key("z"), Pending::Set(make_value("z")));

		let committed: Vec<MultiVersionValues> = vec![];

		// Range with no matching keys
		let iter = FlowRangeIter::new(
			pending.range(make_key("m")..make_key("n")),
			Box::new(committed.into_iter()),
			CommitVersion(10),
		);

		let items: Vec<_> = iter.collect();
		assert!(items.is_empty());
	}
}
