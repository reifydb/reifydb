// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::btree_map::Iter as BTreeMapIter;

use reifydb_core::{CommitVersion, EncodedKey, interface::MultiVersionValues};

use super::Pending;

/// Iterator that merges pending writes with committed scan results
///
/// This iterator combines two sorted streams:
/// 1. Pending writes from the local BTreeMap (sorted by key)
/// 2. Committed results from the underlying query transaction
///
/// The merge algorithm:
/// - Compares keys at each step
/// - Prefers pending writes when keys are equal (overlay semantics)
/// - Filters out removed keys (Pending::Remove)
/// - Maintains sorted order
pub struct FlowScanIter<'a> {
	/// Iterator over committed results from query transaction
	committed: Box<dyn Iterator<Item = MultiVersionValues> + Send + 'a>,
	/// Iterator over pending writes in sorted order
	pending: BTreeMapIter<'a, EncodedKey, Pending>,
	/// Pre-fetched next pending item for lookahead comparison
	next_pending: Option<(&'a EncodedKey, &'a Pending)>,
	/// Pre-fetched next committed item for lookahead comparison
	next_committed: Option<MultiVersionValues>,
	/// Fixed version for pending writes (CDC version)
	version: CommitVersion,
}

impl<'a> FlowScanIter<'a> {
	/// Create a new merge iterator
	pub fn new(
		pending: BTreeMapIter<'a, EncodedKey, Pending>,
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

		// Pre-fetch first items from both iterators
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

impl<'a> Iterator for FlowScanIter<'a> {
	type Item = MultiVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			match (&self.next_pending, &self.next_committed) {
				// Both pending and committed have items
				(Some((pending_key, _pending_value)), Some(committed)) => {
					match pending_key.as_ref().cmp(committed.key.as_ref()) {
						// Pending has smaller key - yield pending if it's a write
						std::cmp::Ordering::Less => {
							let (key, value) = self.next_pending.take().unwrap();
							self.advance_pending();

							match value {
								Pending::Write(values) => {
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
						std::cmp::Ordering::Equal => {
							let (key, value) = self.next_pending.take().unwrap();
							self.advance_pending();
							self.advance_committed(); // Skip the duplicate committed entry

							match value {
								Pending::Write(values) => {
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
						std::cmp::Ordering::Greater => {
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
						Pending::Write(values) => {
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

	#[test]
	fn test_empty_both_iterators() {
		let pending: BTreeMap<EncodedKey, Pending> = BTreeMap::new();
		let committed: Vec<MultiVersionValues> = vec![];

		let mut iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(1));

		assert!(iter.next().is_none());
	}

	#[test]
	fn test_only_pending_writes() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Write(make_value("1")));
		pending.insert(make_key("b"), Pending::Write(make_value("2")));
		pending.insert(make_key("c"), Pending::Write(make_value("3")));

		let committed: Vec<MultiVersionValues> = vec![];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[0].values, make_value("1"));
		assert_eq!(items[0].version, CommitVersion(10));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_only_committed() {
		let pending: BTreeMap<EncodedKey, Pending> = BTreeMap::new();
		let committed =
			vec![make_committed("a", "1", 5), make_committed("b", "2", 6), make_committed("c", "3", 7)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[0].version, CommitVersion(5)); // Committed keeps its version
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_pending_filters_removes() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Write(make_value("1")));
		pending.insert(make_key("b"), Pending::Remove);
		pending.insert(make_key("c"), Pending::Write(make_value("3")));

		let committed: Vec<MultiVersionValues> = vec![];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_pending_shadows_committed() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("b"), Pending::Write(make_value("new")));

		let committed =
			vec![make_committed("a", "1", 5), make_committed("b", "old", 6), make_committed("c", "3", 7)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[0].values, make_value("1"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[1].values, make_value("new")); // Pending value, not "old"
		assert_eq!(items[1].version, CommitVersion(10)); // Pending version
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_pending_remove_hides_committed() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("b"), Pending::Remove);

		let committed =
			vec![make_committed("a", "1", 5), make_committed("b", "2", 6), make_committed("c", "3", 7)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
		// "b" should be hidden
	}

	#[test]
	fn test_merge_interleaved_keys() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("b"), Pending::Write(make_value("pending_b")));
		pending.insert(make_key("d"), Pending::Write(make_value("pending_d")));

		let committed = vec![
			make_committed("a", "committed_a", 5),
			make_committed("c", "committed_c", 6),
			make_committed("e", "committed_e", 7),
		];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 5);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[0].values, make_value("committed_a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[1].values, make_value("pending_b"));
		assert_eq!(items[2].key, make_key("c"));
		assert_eq!(items[2].values, make_value("committed_c"));
		assert_eq!(items[3].key, make_key("d"));
		assert_eq!(items[3].values, make_value("pending_d"));
		assert_eq!(items[4].key, make_key("e"));
		assert_eq!(items[4].values, make_value("committed_e"));
	}

	#[test]
	fn test_sorted_order_maintained() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("m"), Pending::Write(make_value("m")));
		pending.insert(make_key("a"), Pending::Write(make_value("a")));
		pending.insert(make_key("z"), Pending::Write(make_value("z")));

		let committed =
			vec![make_committed("d", "d", 5), make_committed("k", "k", 6), make_committed("p", "p", 7)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 6);

		// Check sorted order
		let keys: Vec<_> = items.iter().map(|i| i.key.clone()).collect();
		assert_eq!(
			keys,
			vec![make_key("a"), make_key("d"), make_key("k"), make_key("m"), make_key("p"), make_key("z")]
		);
	}

	#[test]
	fn test_multiple_removes_in_pending() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Remove);
		pending.insert(make_key("b"), Pending::Remove);
		pending.insert(make_key("c"), Pending::Write(make_value("c")));

		let committed = vec![make_committed("a", "1", 5), make_committed("b", "2", 6)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 1);
		assert_eq!(items[0].key, make_key("c"));
	}

	#[test]
	fn test_all_pending_after_committed() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("x"), Pending::Write(make_value("x")));
		pending.insert(make_key("y"), Pending::Write(make_value("y")));
		pending.insert(make_key("z"), Pending::Write(make_value("z")));

		let committed = vec![make_committed("a", "1", 5), make_committed("b", "2", 6)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 5);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("x"));
		assert_eq!(items[3].key, make_key("y"));
		assert_eq!(items[4].key, make_key("z"));
	}

	#[test]
	fn test_all_committed_after_pending() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Write(make_value("a")));
		pending.insert(make_key("b"), Pending::Write(make_value("b")));

		let committed =
			vec![make_committed("x", "1", 5), make_committed("y", "2", 6), make_committed("z", "3", 7)];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();
		assert_eq!(items.len(), 5);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("x"));
		assert_eq!(items[3].key, make_key("y"));
		assert_eq!(items[4].key, make_key("z"));
	}

	#[test]
	fn test_complex_merge_scenario() {
		let mut pending = BTreeMap::new();
		pending.insert(make_key("a"), Pending::Write(make_value("new_a")));
		pending.insert(make_key("b"), Pending::Remove);
		pending.insert(make_key("d"), Pending::Write(make_value("new_d")));
		pending.insert(make_key("f"), Pending::Remove);
		pending.insert(make_key("g"), Pending::Write(make_value("new_g")));

		let committed = vec![
			make_committed("a", "old_a", 5),
			make_committed("b", "old_b", 6),
			make_committed("c", "old_c", 7),
			make_committed("e", "old_e", 8),
			make_committed("f", "old_f", 9),
		];

		let iter = FlowScanIter::new(pending.iter(), Box::new(committed.into_iter()), CommitVersion(10));

		let items: Vec<_> = iter.collect();

		// Expected: a(new), c(old), d(new), e(old), g(new)
		// Removed: b, f
		assert_eq!(items.len(), 5);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[0].values, make_value("new_a"));
		assert_eq!(items[1].key, make_key("c"));
		assert_eq!(items[1].values, make_value("old_c"));
		assert_eq!(items[2].key, make_key("d"));
		assert_eq!(items[2].values, make_value("new_d"));
		assert_eq!(items[3].key, make_key("e"));
		assert_eq!(items[3].values, make_value("old_e"));
		assert_eq!(items[4].key, make_key("g"));
		assert_eq!(items[4].values, make_value("new_g"));
	}
}
