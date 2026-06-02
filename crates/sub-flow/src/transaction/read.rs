// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::BTreeMap,
	ops::{
		Bound::{Excluded, Included, Unbounded},
		RangeBounds,
	},
};

use reifydb_core::{
	actors::pending::PendingWrite,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{MultiVersionBatch, MultiVersionRow},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;

use super::FlowTransaction;

mod merge;
mod source;

use merge::{flow_merge_pending_iterator, flow_merge_pending_iterator_rev};
pub(crate) use source::{ReadFrom, read_from};

impl FlowTransaction {
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let inner = self.inner();
		if inner.pending.is_removed(key) {
			return Ok(None);
		}
		if let Some(value) = inner.pending.get(key) {
			return Ok(Some(value.clone()));
		}

		if let Self::Transactional {
			base_pending,
			..
		} = self
		{
			if base_pending.is_removed(key) {
				return Ok(None);
			}
			if let Some(value) = base_pending.get(key) {
				return Ok(Some(value.clone()));
			}
		}

		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			return match read_from(key) {
				ReadFrom::StateQuery => Ok(state.get(key).cloned()),
				ReadFrom::Query => match inner.query.get(key)? {
					Some(multi) => Ok(Some(multi.row().clone())),
					None => Ok(None),
				},
			};
		}

		if let Some(cached) = self.inner().prefetch.get(key) {
			return Ok(cached.clone());
		}

		let inner = self.inner_mut();
		let query = match read_from(key) {
			ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
			ReadFrom::Query => &inner.query,
		};
		match query.get(key)? {
			Some(multi) => Ok(Some(multi.row().clone())),
			None => Ok(None),
		}
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		let inner = self.inner();
		if inner.pending.is_removed(key) {
			return Ok(false);
		}
		if inner.pending.get(key).is_some() {
			return Ok(true);
		}

		if let Self::Transactional {
			base_pending,
			..
		} = self
		{
			if base_pending.is_removed(key) {
				return Ok(false);
			}
			if base_pending.get(key).is_some() {
				return Ok(true);
			}
		}

		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			return match read_from(key) {
				ReadFrom::StateQuery => Ok(state.contains_key(key)),
				ReadFrom::Query => inner.query.contains_key(key),
			};
		}

		let inner = self.inner_mut();
		let query = match read_from(key) {
			ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
			ReadFrom::Query => &inner.query,
		};
		query.contains_key(key)
	}

	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let range = EncodedKeyRange::prefix(prefix);
		let items = self.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		self.range_directed(range, scope, batch_size, true)
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		self.range_directed(range, scope, batch_size, false)
	}

	fn range_directed(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
		forward: bool,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred {
				inner,
				..
			}
			| Self::Committing {
				inner,
				..
			} => {
				let merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				let pending_vec = ordered_pending(merged, forward);

				let query = match range.start.as_ref() {
					Included(start) | Excluded(start) => match read_from(start) {
						ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
						ReadFrom::Query => &inner.query,
					},
					Unbounded => &inner.query,
				};

				let v = inner.version;
				if forward {
					let storage_iter = query.range(range, scope, batch_size);
					Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
				} else {
					let storage_iter = query.range_rev(range, scope, batch_size);
					Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
				}
			}
			Self::Transactional {
				inner,
				base_pending,
				..
			} => {
				let mut merged: BTreeMap<EncodedKey, PendingWrite> = base_pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				for (k, v) in inner.pending.range((range.start.as_ref(), range.end.as_ref())) {
					merged.insert(k.clone(), v.clone());
				}
				let pending_vec = ordered_pending(merged, forward);

				let query = match range.start.as_ref() {
					Included(start) | Excluded(start) => match read_from(start) {
						ReadFrom::StateQuery => inner.state_query.as_ref().unwrap(),
						ReadFrom::Query => &inner.query,
					},
					Unbounded => &inner.query,
				};

				let v = inner.version;
				if forward {
					let storage_iter = query.range(range, scope, batch_size);
					Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
				} else {
					let storage_iter = query.range_rev(range, scope, batch_size);
					Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
				}
			}
			Self::Ephemeral {
				inner,
				state,
			} => {
				let is_state_range = match range.start.as_ref() {
					Included(start) | Excluded(start) => {
						matches!(read_from(start), ReadFrom::StateQuery)
					}
					Unbounded => false,
				};

				let merged: BTreeMap<EncodedKey, PendingWrite> = inner
					.pending
					.range((range.start.as_ref(), range.end.as_ref()))
					.map(|(k, v)| (k.clone(), v.clone()))
					.collect();
				let pending_vec = ordered_pending(merged, forward);

				let v = inner.version;
				if is_state_range {
					let mut state_items: Vec<Result<MultiVersionRow>> = state
						.iter()
						.filter(|(k, _)| range.contains(k))
						.map(|(k, v)| {
							Ok(MultiVersionRow {
								key: k.clone(),
								row: v.clone(),
								version: inner.version,
							})
						})
						.collect();

					if forward {
						state_items.sort_by(|a, b| match (a, b) {
							(Ok(a), Ok(b)) => a.key.cmp(&b.key),
							_ => Ordering::Equal,
						});
						Box::new(flow_merge_pending_iterator(
							pending_vec,
							state_items.into_iter(),
							v,
						))
					} else {
						state_items.sort_by(|a, b| match (a, b) {
							(Ok(a), Ok(b)) => b.key.cmp(&a.key),
							_ => Ordering::Equal,
						});
						Box::new(flow_merge_pending_iterator_rev(
							pending_vec,
							state_items.into_iter(),
							v,
						))
					}
				} else if forward {
					let storage_iter = inner.query.range(range, scope, batch_size);
					Box::new(flow_merge_pending_iterator(pending_vec, storage_iter, v))
				} else {
					let storage_iter = inner.query.range_rev(range, scope, batch_size);
					Box::new(flow_merge_pending_iterator_rev(pending_vec, storage_iter, v))
				}
			}
		}
	}
}

fn ordered_pending(merged: BTreeMap<EncodedKey, PendingWrite>, forward: bool) -> Vec<(EncodedKey, PendingWrite)> {
	if forward {
		merged.into_iter().collect()
	} else {
		merged.into_iter().rev().collect()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		common::CommitVersion,
		encoded::{
			key::{EncodedKey, EncodedKeyRange},
			row::EncodedRow,
		},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{util::cowvec::CowVec, value::identity::IdentityId};

	use super::*;
	use crate::operator::stateful::test_utils::test::create_test_transaction;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_get_from_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Should get value from pending buffer
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_from_committed() {
		let t = TestEngine::new();

		let key = make_key("key1");
		let value = make_value("value1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
			cmd_txn.set(&key, value.clone()).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction to read committed data
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();

		// Create FlowTransaction - should see committed value
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		// Should get value from query transaction
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_get_pending_shadows_committed() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("old")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		// Override with new value in pending
		let new_value = make_value("new");
		txn.set(&key, new_value.clone()).unwrap();

		// Should get new value from pending, not old value from committed
		let result = txn.get(&key).unwrap();
		assert_eq!(result, Some(new_value));
	}

	#[test]
	fn test_get_removed_returns_none() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		// Remove in pending
		txn.remove(&key).unwrap();

		// Should return None even though it exists in committed
		let result = txn.get(&key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_get_nonexistent_key() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let result = txn.get(&make_key("missing")).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_contains_key_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_committed() {
		let t = TestEngine::new();

		let key = make_key("key1");

		// Set value in first transaction and commit
		{
			let mut cmd_txn = t.begin_admin(IdentityId::system()).unwrap();
			cmd_txn.set(&key, make_value("value1")).unwrap();
			cmd_txn.commit().unwrap();
		}

		// Create new command transaction
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert!(txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_removed_returns_false() {
		let mut parent = create_test_transaction();

		let key = make_key("key1");
		parent.set(&key, make_value("value1")).unwrap();
		let version = parent.version();

		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		txn.remove(&key).unwrap();

		assert!(!txn.contains_key(&key).unwrap());
	}

	#[test]
	fn test_contains_key_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert!(!txn.contains_key(&make_key("missing")).unwrap());
	}

	#[test]
	fn test_scan_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let mut iter = txn.range(EncodedKeyRange::all(), RangeScope::All, 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_scan_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> =
			txn.range(EncodedKeyRange::all(), RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

		// Should be in sorted order
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("b"));
		assert_eq!(items[2].key, make_key("c"));
	}

	#[test]
	fn test_scan_filters_removes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let items: Vec<_> =
			txn.range(EncodedKeyRange::all(), RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

		// Should only have 2 items (remove filtered out)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("a"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_range_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let mut iter = txn.range(range, RangeScope::All, 1024);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
		let items: Vec<_> = txn.range(range, RangeScope::All, 1024).collect::<Result<Vec<_>>>().unwrap();

		// Should only include b and c (not d, exclusive end)
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("b"));
		assert_eq!(items[1].key, make_key("c"));
	}

	#[test]
	fn test_prefix_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[test]
	fn test_prefix_only_pending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("test_a"), make_value("1")).unwrap();
		txn.set(&make_key("test_b"), make_value("2")).unwrap();
		txn.set(&make_key("other_c"), make_value("3")).unwrap();

		let prefix = make_key("test_");
		let iter = txn.prefix(&prefix).unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only include keys with prefix "test_"
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].key, make_key("test_a"));
		assert_eq!(items[1].key, make_key("test_b"));
	}

	fn fwd(txn: &mut FlowTransaction, range: EncodedKeyRange) -> Vec<(EncodedKey, EncodedRow)> {
		txn.range(range, RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()
			.unwrap()
			.into_iter()
			.map(|m| (m.key, m.row))
			.collect()
	}

	fn rev(txn: &mut FlowTransaction, range: EncodedKeyRange) -> Vec<(EncodedKey, EncodedRow)> {
		txn.range_rev(range, RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()
			.unwrap()
			.into_iter()
			.map(|m| (m.key, m.row))
			.collect()
	}

	fn engine_with_committed(pairs: &[(&str, &str)]) -> TestEngine {
		let t = TestEngine::new();
		{
			let mut cmd = t.begin_admin(IdentityId::system()).unwrap();
			for (k, v) in pairs {
				cmd.set(&make_key(k), make_value(v)).unwrap();
			}
			cmd.commit().unwrap();
		}
		t
	}

	// range_rev returns pending-only rows in descending key order.
	#[test]
	fn test_range_rev_only_pending_descending() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let keys: Vec<_> = rev(&mut txn, EncodedKeyRange::all()).into_iter().map(|(k, _)| k).collect();
		assert_eq!(keys, vec![make_key("c"), make_key("b"), make_key("a")]);
	}

	// range_rev excludes keys removed in pending.
	#[test]
	fn test_range_rev_filters_removes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.remove(&make_key("b")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();

		let keys: Vec<_> = rev(&mut txn, EncodedKeyRange::all()).into_iter().map(|(k, _)| k).collect();
		assert_eq!(keys, vec![make_key("c"), make_key("a")]);
	}

	// range_rev honors [Included(start), Excluded(end)) bounds, descending.
	#[test]
	fn test_range_rev_bounds() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("1")).unwrap();
		txn.set(&make_key("b"), make_value("2")).unwrap();
		txn.set(&make_key("c"), make_value("3")).unwrap();
		txn.set(&make_key("d"), make_value("4")).unwrap();

		let range = EncodedKeyRange::new(Included(make_key("b")), Excluded(make_key("d")));
		let keys: Vec<_> = rev(&mut txn, range).into_iter().map(|(k, _)| k).collect();
		assert_eq!(keys, vec![make_key("c"), make_key("b")]);
	}

	// Forward range merges committed storage with pending, in ascending order with values.
	#[test]
	fn test_range_merges_committed_and_pending_forward() {
		let t = engine_with_committed(&[("a", "A"), ("c", "C")]);
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("B")).unwrap();

		assert_eq!(
			fwd(&mut txn, EncodedKeyRange::all()),
			vec![
				(make_key("a"), make_value("A")),
				(make_key("b"), make_value("B")),
				(make_key("c"), make_value("C")),
			]
		);
	}

	// Reverse range merges committed storage with pending, in descending order with values.
	#[test]
	fn test_range_merges_committed_and_pending_reverse() {
		let t = engine_with_committed(&[("a", "A"), ("c", "C"), ("e", "E")]);
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("B")).unwrap();
		txn.set(&make_key("d"), make_value("D")).unwrap();

		assert_eq!(
			rev(&mut txn, EncodedKeyRange::all()),
			vec![
				(make_key("e"), make_value("E")),
				(make_key("d"), make_value("D")),
				(make_key("c"), make_value("C")),
				(make_key("b"), make_value("B")),
				(make_key("a"), make_value("A")),
			]
		);
	}

	// A pending Set shadows the committed value for the same key in a forward range.
	#[test]
	fn test_range_pending_shadows_committed_forward() {
		let t = engine_with_committed(&[("a", "OLD"), ("b", "B")]);
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("a"), make_value("NEW")).unwrap();

		assert_eq!(
			fwd(&mut txn, EncodedKeyRange::all()),
			vec![(make_key("a"), make_value("NEW")), (make_key("b"), make_value("B"))]
		);
	}

	// A pending Set shadows the committed value for the same key in a reverse range.
	#[test]
	fn test_range_pending_shadows_committed_reverse() {
		let t = engine_with_committed(&[("a", "A"), ("b", "OLD")]);
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.set(&make_key("b"), make_value("NEW")).unwrap();

		assert_eq!(
			rev(&mut txn, EncodedKeyRange::all()),
			vec![(make_key("b"), make_value("NEW")), (make_key("a"), make_value("A"))]
		);
	}

	// A pending Remove of a committed key excludes it from a forward range.
	#[test]
	fn test_range_remove_of_committed_excluded_forward() {
		let t = engine_with_committed(&[("a", "A"), ("b", "B"), ("c", "C")]);
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.remove(&make_key("b")).unwrap();

		let keys: Vec<_> = fwd(&mut txn, EncodedKeyRange::all()).into_iter().map(|(k, _)| k).collect();
		assert_eq!(keys, vec![make_key("a"), make_key("c")]);
	}

	// A pending Remove of a committed key excludes it from a reverse range.
	#[test]
	fn test_range_remove_of_committed_excluded_reverse() {
		let t = engine_with_committed(&[("a", "A"), ("b", "B"), ("c", "C")]);
		let parent = t.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		txn.remove(&make_key("b")).unwrap();

		let keys: Vec<_> = rev(&mut txn, EncodedKeyRange::all()).into_iter().map(|(k, _)| k).collect();
		assert_eq!(keys, vec![make_key("c"), make_key("a")]);
	}
}
