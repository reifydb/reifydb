// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;
use tracing::{Span, field, instrument};

use super::FlowTransaction;

#[derive(Clone, Copy)]
enum StateScope {
	Public,
	Internal,
}

impl StateScope {
	fn encode(self, id: FlowNodeId, key: &EncodedKey) -> EncodedKey {
		match self {
			StateScope::Public => FlowNodeStateKey::new(id, key.as_ref().to_vec()).encode(),
			StateScope::Internal => FlowNodeInternalStateKey::new(id, key.as_ref().to_vec()).encode(),
		}
	}
}

impl FlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found = field::Empty
	))]
	pub fn state_get(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let result = self.scoped_get(StateScope::Public, id, key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::state::get_many", level = "debug", skip(self, keys), fields(
		node_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn state_get_many(&mut self, id: FlowNodeId, keys: &[EncodedKey]) -> Result<MultiVersionBatch> {
		let batch = self.scoped_get_many(StateScope::Public, id, keys)?;
		Span::current().record("found_count", batch.items.len());
		Ok(batch)
	}

	#[instrument(name = "flow::state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.len()
	))]
	pub fn state_set(&mut self, id: FlowNodeId, key: &EncodedKey, value: EncodedRow) -> Result<()> {
		self.scoped_set(StateScope::Public, id, key, value)
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn state_remove(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		self.scoped_remove(StateScope::Public, id, key)
	}

	#[instrument(name = "flow::state::drop", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn state_drop(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		self.scoped_drop(StateScope::Public, id, key)
	}

	#[instrument(name = "flow::internal_state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found = field::Empty
	))]
	pub fn internal_state_get(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let result = self.scoped_get(StateScope::Internal, id, key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::internal_state::get_many", level = "debug", skip(self, keys), fields(
		node_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn internal_state_get_many(&mut self, id: FlowNodeId, keys: &[EncodedKey]) -> Result<MultiVersionBatch> {
		let batch = self.scoped_get_many(StateScope::Internal, id, keys)?;
		Span::current().record("found_count", batch.items.len());
		Ok(batch)
	}

	#[instrument(name = "flow::internal_state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.len()
	))]
	pub fn internal_state_set(&mut self, id: FlowNodeId, key: &EncodedKey, value: EncodedRow) -> Result<()> {
		self.scoped_set(StateScope::Internal, id, key, value)
	}

	#[instrument(name = "flow::internal_state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn internal_state_remove(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		self.scoped_remove(StateScope::Internal, id, key)
	}

	#[instrument(name = "flow::internal_state::drop", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn internal_state_drop(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		self.scoped_drop(StateScope::Internal, id, key)
	}

	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		node_id = id.0,
		result_count = field::Empty
	))]
	pub fn state_scan_all(&mut self, id: FlowNodeId) -> Result<MultiVersionBatch> {
		let range = FlowNodeStateKey::node_range(id);
		let iter = self.range(range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
		Span::current().record("result_count", items.len());
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::range", level = "debug", skip(self, range), fields(
		node_id = id.0
	))]
	pub fn state_range_all(&mut self, id: FlowNodeId, range: EncodedKeyRange) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(id, vec![]));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			items.push(result?);
		}
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::internal_state::range", level = "debug", skip(self, range), fields(
		node_id = id.0
	))]
	pub fn internal_state_range(
		&mut self,
		id: FlowNodeId,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<MultiVersionBatch> {
		let prefixed_range = range.with_prefix(FlowNodeInternalStateKey::encoded(id, vec![]));
		let iter = self.range(prefixed_range, RangeScope::All, 1024);
		let mut items = Vec::new();
		for result in iter {
			if limit.is_some_and(|l| items.len() == l) {
				return Ok(MultiVersionBatch {
					items,
					has_more: true,
				});
			}
			items.push(result?);
		}
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::clear", level = "trace", skip(self), fields(
		node_id = id.0,
		keys_removed = field::Empty
	))]
	pub fn state_clear(&mut self, id: FlowNodeId) -> Result<()> {
		let keys_to_remove = self.scan_keys_for_clear(id)?;

		let count = keys_to_remove.len();
		self.remove_keys(keys_to_remove)?;

		Span::current().record("keys_removed", count);
		Ok(())
	}

	#[inline]
	#[instrument(name = "flow::state::clear::scan", level = "trace", skip(self), fields(node_id = id.0))]
	fn scan_keys_for_clear(&mut self, id: FlowNodeId) -> Result<Vec<EncodedKey>> {
		let range = FlowNodeStateKey::node_range(id);
		let iter = self.range(range, RangeScope::All, 1024);
		let mut keys = Vec::new();
		for result in iter {
			let multi = result?;
			keys.push(multi.key);
		}
		Ok(keys)
	}

	#[inline]
	#[instrument(name = "flow::state::clear::remove", level = "trace", skip(self, keys), fields(count = keys.len()))]
	fn remove_keys(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		for key in keys {
			self.remove(&key)?;
		}
		Ok(())
	}

	#[instrument(name = "flow::state::load_or_create", level = "debug", skip(self, shape), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		created
	))]
	pub fn load_or_create_row(&mut self, id: FlowNodeId, key: &EncodedKey, shape: &RowShape) -> Result<EncodedRow> {
		match self.state_get(id, key)? {
			Some(row) => {
				Span::current().record("created", false);
				Ok(row)
			}
			None => {
				Span::current().record("created", true);
				Ok(shape.allocate())
			}
		}
	}

	#[instrument(name = "flow::state::save", level = "trace", skip(self, row), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn save_row(&mut self, id: FlowNodeId, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.state_set(id, key, row)
	}

	fn scoped_get(&mut self, scope: StateScope, id: FlowNodeId, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let encoded_key = scope.encode(id, key);
		self.get(&encoded_key)
	}

	fn scoped_get_many(
		&mut self,
		scope: StateScope,
		id: FlowNodeId,
		keys: &[EncodedKey],
	) -> Result<MultiVersionBatch> {
		let version = self.version();
		let encoded: Vec<EncodedKey> = keys.iter().map(|key| scope.encode(id, key)).collect();

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for encoded_key in &encoded {
			match self.lookup_overlays(encoded_key) {
				Some(None) => continue,
				Some(Some(row)) => items.push(MultiVersionRow {
					key: encoded_key.clone(),
					row,
					version,
				}),
				None => to_batch.push(encoded_key.clone()),
			}
		}

		self.fetch_external(&to_batch, &mut items)?;

		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[inline]
	fn lookup_overlays(&self, encoded_key: &EncodedKey) -> Option<Option<EncodedRow>> {
		let inner = self.inner();
		let pending = if inner.pending.is_removed(encoded_key) {
			Some(None)
		} else {
			inner.pending.get(encoded_key).map(|row| Some(row.clone()))
		};
		if pending.is_some() {
			return pending;
		}

		if inner.base_pending.is_removed(encoded_key) {
			return Some(None);
		}
		if let Some(row) = inner.base_pending.get(encoded_key) {
			return Some(Some(row.clone()));
		}

		inner.prefetch.get(encoded_key).cloned()
	}

	#[inline]
	fn fetch_external(&mut self, to_batch: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()> {
		if to_batch.is_empty() {
			return Ok(());
		}

		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			let version = inner.version;
			for encoded_key in to_batch {
				if let Some(row) = state.get(encoded_key) {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						row: row.clone(),
						version,
					});
				}
			}
		} else {
			let inner = self.inner_mut();
			inner.store_reads += to_batch.len() as u64;
			let found = inner.state_query.as_ref().unwrap().get_many(to_batch)?;
			for encoded_key in to_batch {
				match found.get(encoded_key) {
					Some(multi) => {
						inner.prefetch.insert(encoded_key.clone(), Some(multi.row.clone()));
						items.push(multi.clone());
					}
					None => {
						inner.prefetch.insert(encoded_key.clone(), None);
					}
				}
			}
		}

		Ok(())
	}

	fn scoped_set(&mut self, scope: StateScope, id: FlowNodeId, key: &EncodedKey, value: EncodedRow) -> Result<()> {
		self.set(&scope.encode(id, key), value)
	}

	fn scoped_remove(&mut self, scope: StateScope, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		let encoded_key = scope.encode(id, key);
		self.remove(&encoded_key)
	}

	fn scoped_drop(&mut self, scope: StateScope, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		let encoded_key = scope.encode(id, key);
		self.drop_key(&encoded_key)
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		collections::{Bound, HashMap},
		sync::Arc,
	};

	use reifydb_catalog::catalog::Catalog;
	use reifydb_codec::{
		encoded::{
			row::{EncodedRow, SHAPE_HEADER_SIZE},
			shape::RowShape,
		},
		key::encoded::{EncodedKey, EncodedKeyRange},
	};
	use reifydb_core::{
		actors::pending::Pending,
		common::CommitVersion,
		interface::catalog::{flow::FlowNodeId, id::ViewId, shape::ShapeId},
		key::row::RowKey,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{identity::IdentityId, row_number::RowNumber, value_type::ValueType},
	};

	use super::*;
	use crate::{
		operator::stateful::test_utils::test::create_test_transaction,
		transaction::{CommittingParams, DeferredParams, TransactionalParams, allocators::FlowAllocators},
	};

	fn commit_state_row(engine: &TestEngine, node: FlowNodeId, key: &EncodedKey, row: EncodedRow) -> CommitVersion {
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&FlowNodeStateKey::new(node, key.as_ref().to_vec()).encode(), row).unwrap();
		cmd.commit_unchecked().unwrap()
	}

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	fn anchored_row(payload: &[u8], created_at: u64, updated_at: u64) -> EncodedRow {
		let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
		buf[8..16].copy_from_slice(&created_at.to_le_bytes());
		buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
		buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
		EncodedRow(CowVec::new(buf))
	}

	#[test]
	fn test_state_get_set() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		let key = make_key("state_key");
		let value = make_value("state_value");

		// Set state
		txn.state_set(node_id, &key, value.clone()).unwrap();

		// Get state back
		let result = txn.state_get(node_id, &key).unwrap();
		assert_eq!(result, Some(value));
	}

	#[test]
	fn test_internal_state_get_many() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		txn.internal_state_set(node_id, &make_key("a"), make_value("1")).unwrap();
		txn.internal_state_set(node_id, &make_key("b"), make_value("2")).unwrap();

		// A data-state key sharing the name must not leak into the internal batch read:
		// the two namespaces use different envelopes.
		txn.state_set(node_id, &make_key("a"), make_value("data")).unwrap();

		let batch = txn
			.internal_state_get_many(node_id, &[make_key("a"), make_key("b"), make_key("missing")])
			.unwrap();

		// Missing key is omitted; present keys come back under the internal envelope.
		assert_eq!(batch.items.len(), 2);
		let mut decoded: Vec<(Vec<u8>, EncodedRow)> = batch
			.items
			.iter()
			.map(|item| (FlowNodeInternalStateKey::decode(&item.key).unwrap().key, item.row.clone()))
			.collect();
		decoded.sort_by(|a, b| a.0.cmp(&b.0));
		assert_eq!(decoded[0], (b"a".to_vec(), make_value("1")));
		assert_eq!(decoded[1], (b"b".to_vec(), make_value("2")));
	}

	#[test]
	fn test_state_get_nonexistent() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		let key = make_key("missing");

		let result = txn.state_get(node_id, &key).unwrap();
		assert_eq!(result, None);
	}

	#[test]
	fn test_state_remove() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		let key = make_key("state_key");
		let value = make_value("state_value");

		// Set then remove
		txn.state_set(node_id, &key, value.clone()).unwrap();
		assert_eq!(txn.state_get(node_id, &key).unwrap(), Some(value));

		txn.state_remove(node_id, &key).unwrap();
		assert_eq!(txn.state_get(node_id, &key).unwrap(), None);
	}

	#[test]
	fn test_state_isolation_between_nodes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let key = make_key("same_key");

		txn.state_set(node1, &key, make_value("node1_value")).unwrap();
		txn.state_set(node2, &key, make_value("node2_value")).unwrap();

		// Each node should have its own value
		assert_eq!(txn.state_get(node1, &key).unwrap(), Some(make_value("node1_value")));
		assert_eq!(txn.state_get(node2, &key).unwrap(), Some(make_value("node2_value")));
	}

	#[test]
	fn test_state_scan_all() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);

		txn.state_set(node_id, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node_id, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node_id, &make_key("key3"), make_value("value3")).unwrap();

		let iter = txn.state_scan_all(node_id).unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		assert_eq!(items.len(), 3);
	}

	#[test]
	fn test_state_scan_only_own_node() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);

		txn.state_set(node1, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node1, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node2, &make_key("key3"), make_value("value3")).unwrap();

		// Scan node1 should only return node1's state
		let items: Vec<_> = txn.state_scan_all(node1).unwrap().items.into_iter().collect();
		assert_eq!(items.len(), 2);

		// Scan node2 should only return node2's state
		let items: Vec<_> = txn.state_scan_all(node2).unwrap().items.into_iter().collect();
		assert_eq!(items.len(), 1);
	}

	#[test]
	fn test_state_scan_empty() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);

		let iter = txn.state_scan_all(node_id).unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[test]
	fn test_state_range_all() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);

		txn.state_set(node_id, &make_key("a"), make_value("1")).unwrap();
		txn.state_set(node_id, &make_key("b"), make_value("2")).unwrap();
		txn.state_set(node_id, &make_key("c"), make_value("3")).unwrap();
		txn.state_set(node_id, &make_key("d"), make_value("4")).unwrap();

		// Range query from "b" to "d" (exclusive)
		let range = EncodedKeyRange::new(Bound::Included(make_key("b")), Bound::Excluded(make_key("d")));
		let iter = txn.state_range_all(node_id, range).unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only include "b" and "c"
		assert_eq!(items.len(), 2);
	}

	#[test]
	fn test_state_clear() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);

		txn.state_set(node_id, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node_id, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node_id, &make_key("key3"), make_value("value3")).unwrap();

		// Verify state exists
		assert_eq!(txn.state_scan_all(node_id).unwrap().items.into_iter().count(), 3);

		// Clear all state
		txn.state_clear(node_id).unwrap();

		// Verify state is empty
		assert_eq!(txn.state_scan_all(node_id).unwrap().items.into_iter().count(), 0);
	}

	#[test]
	fn test_state_clear_only_own_node() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);

		txn.state_set(node1, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node1, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node2, &make_key("key3"), make_value("value3")).unwrap();

		// Clear node1
		txn.state_clear(node1).unwrap();

		// Node1 should be empty
		assert_eq!(txn.state_scan_all(node1).unwrap().items.into_iter().count(), 0);

		// Node2 should still have state
		assert_eq!(txn.state_scan_all(node2).unwrap().items.into_iter().count(), 1);
	}

	#[test]
	fn test_state_clear_empty_node() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);

		// Clear on empty node should not error
		txn.state_clear(node_id).unwrap();
	}

	#[test]
	fn test_load_or_create_existing() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let value = make_value("existing");
		let shape = RowShape::testing(&[ValueType::Int8, ValueType::Float8]);

		// Set existing state
		txn.state_set(node_id, &key, value.clone()).unwrap();

		// load_or_create should return existing value
		let result = txn.load_or_create_row(node_id, &key, &shape).unwrap();
		assert_eq!(result, value);
	}

	#[test]
	fn test_load_or_create_new() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let shape = RowShape::testing(&[ValueType::Int8, ValueType::Float8]);

		// load_or_create should allocate new row
		let result = txn.load_or_create_row(node_id, &key, &shape).unwrap();

		// Result should be a newly allocated row (shape.allocate())
		assert!(!result.is_empty());
	}

	#[test]
	fn test_save_row() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let row = make_value("row_data");

		txn.save_row(node_id, &key, row.clone()).unwrap();

		// Verify saved
		let result = txn.state_get(node_id, &key).unwrap();
		assert_eq!(result, Some(row));
	}

	#[test]
	fn test_state_multiple_nodes() {
		let parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let node3 = FlowNodeId(3);

		txn.state_set(node1, &make_key("a"), make_value("n1_a")).unwrap();
		txn.state_set(node1, &make_key("b"), make_value("n1_b")).unwrap();
		txn.state_set(node2, &make_key("a"), make_value("n2_a")).unwrap();
		txn.state_set(node3, &make_key("c"), make_value("n3_c")).unwrap();

		// Verify each node has correct state
		assert_eq!(txn.state_get(node1, &make_key("a")).unwrap(), Some(make_value("n1_a")));
		assert_eq!(txn.state_get(node1, &make_key("b")).unwrap(), Some(make_value("n1_b")));
		assert_eq!(txn.state_get(node2, &make_key("a")).unwrap(), Some(make_value("n2_a")));
		assert_eq!(txn.state_get(node3, &make_key("c")).unwrap(), Some(make_value("n3_c")));

		// Cross-node keys should not exist
		assert_eq!(txn.state_get(node2, &make_key("b")).unwrap(), None);
		assert_eq!(txn.state_get(node3, &make_key("a")).unwrap(), None);
	}

	#[test]
	fn store_reads_counts_store_reaching_reads_only() {
		// The store_reads counter feeds the flow::engine::apply gets= extra: per-operator
		// attribution of store traffic. It must count exactly the reads that leave the
		// transaction (point gets and batched external keys, hits and misses alike) and
		// never the ones served by the pending overlay, otherwise the profiler dump would
		// misattribute the read amplification it exists to measure.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let committed_key = make_key("committed");
		commit_state_row(&engine, node_id, &committed_key, make_value("v"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		assert_eq!(txn.store_reads(), 0);

		// A write is a pure overlay operation: neither it nor its overlay-served read-back
		// may count.
		let pending_key = make_key("pending");
		txn.state_set(node_id, &pending_key, make_value("p")).unwrap();
		assert_eq!(txn.state_get(node_id, &pending_key).unwrap(), Some(make_value("p")));
		assert_eq!(txn.store_reads(), 0, "overlay-served reads must not count as store reads");

		// A committed row and a miss both reach the store: one point get each.
		assert!(txn.state_get(node_id, &committed_key).unwrap().is_some());
		assert_eq!(txn.store_reads(), 1);
		assert!(txn.state_get(node_id, &make_key("absent")).unwrap().is_none());
		assert_eq!(txn.store_reads(), 2, "a store-reaching miss is still a store read");

		// Batched reads count per external key, not per call.
		let batch = txn.state_get_many(node_id, &[make_key("absent_a"), make_key("absent_b")]).unwrap();
		assert!(batch.items.is_empty());
		assert_eq!(txn.store_reads(), 4);

		// A header-sized value to a never-read key must not reach the store either: state
		// writes carry their own anchors, so no write ever reads the prior row back.
		let wide_key = make_key("wide");
		txn.state_set(node_id, &wide_key, EncodedRow(CowVec::new(vec![0u8; 32]))).unwrap();
		assert_eq!(txn.store_reads(), 4, "a state write must never reach the store");
	}

	#[test]
	fn state_reads_are_cached_within_a_transaction() {
		// Operator state is read-mostly and the snapshot is immutable within a txn, so a
		// second read of the same key (hit or miss, point or batch) must be served from
		// the read-through cache instead of reaching the store again. This is the fix for
		// the per-version re-read amplification: without it every operator re-pays a
		// store roundtrip for state it already loaded in the same slice.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let committed_key = make_key("committed");
		commit_state_row(&engine, node_id, &committed_key, make_value("v"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert_eq!(txn.state_get(node_id, &committed_key).unwrap(), Some(make_value("v")));
		assert_eq!(txn.store_reads(), 1);
		assert_eq!(txn.state_get(node_id, &committed_key).unwrap(), Some(make_value("v")));
		assert_eq!(txn.store_reads(), 1, "a repeated state read must be served from the cache");

		assert_eq!(txn.state_get(node_id, &make_key("absent")).unwrap(), None);
		assert_eq!(txn.store_reads(), 2);
		assert_eq!(txn.state_get(node_id, &make_key("absent")).unwrap(), None);
		assert_eq!(txn.store_reads(), 2, "a miss must be cached too, or absent-key probes re-scan forever");

		// The batch path shares the cache in both directions: prior point reads are not
		// re-fetched, and batch-fetched keys serve later point reads.
		let batch = txn
			.state_get_many(node_id, &[committed_key.clone(), make_key("absent"), make_key("batch_only")])
			.unwrap();
		assert_eq!(batch.items.len(), 1);
		assert_eq!(batch.items[0].row, make_value("v"));
		assert_eq!(txn.store_reads(), 3, "only the never-read key may reach the store");
		assert_eq!(txn.state_get(node_id, &make_key("batch_only")).unwrap(), None);
		assert_eq!(txn.store_reads(), 3);
	}

	#[test]
	fn cached_state_reads_never_mask_writes_or_removes() {
		// The cache sits BELOW the pending overlays: a write or remove issued after a
		// cached read must win on every later read. If the cache were consulted first,
		// an operator would read back its own stale pre-write state and fold updates
		// into a dead accumulator.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let key = make_key("k");
		commit_state_row(&engine, node_id, &key, make_value("old"));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert_eq!(txn.state_get(node_id, &key).unwrap(), Some(make_value("old")));
		txn.state_set(node_id, &key, make_value("new")).unwrap();
		assert_eq!(txn.state_get(node_id, &key).unwrap(), Some(make_value("new")));

		txn.state_remove(node_id, &key).unwrap();
		assert_eq!(txn.state_get(node_id, &key).unwrap(), None);
		let batch = txn.state_get_many(node_id, &[key.clone()]).unwrap();
		assert!(batch.items.is_empty(), "a removed key must not resurface through the batch path");

		// A key first seen as a cached miss must surface a later write.
		let fresh = make_key("fresh");
		assert_eq!(txn.state_get(node_id, &fresh).unwrap(), None);
		txn.state_set(node_id, &fresh, make_value("live")).unwrap();
		assert_eq!(txn.state_get(node_id, &fresh).unwrap(), Some(make_value("live")));
	}

	#[test]
	fn a_state_write_keeps_the_callers_anchors_without_reading_the_prior_row() {
		// The dominant operator shape is load-mutate-save, and the save must cost nothing:
		// operator state rows carry the anchors their writer stamped, so the write is a
		// pure overlay op. A save that read the prior row back to carry its created_at
		// forward cost one store read per written key on every flush, and it defeats the
		// operator-resident caches above it: once a cache serves the load, the prior row is
		// no longer in the transaction and the save alone would reach the store.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let key = make_key("acc");
		commit_state_row(&engine, node_id, &key, anchored_row(b"v0", 1_000, 1_000));

		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		let mut txn = FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);

		assert!(txn.state_get(node_id, &key).unwrap().is_some());
		assert_eq!(txn.store_reads(), 1);
		txn.state_set(node_id, &key, anchored_row(b"v1", 5_000, 5_000)).unwrap();
		assert_eq!(txn.store_reads(), 1, "a save must not read the prior row back");

		let stored = txn.state_get(node_id, &key).unwrap().unwrap();
		assert_eq!(
			stored.created_at_nanos(),
			5_000,
			"the write's own created_at stands: nothing is carried over from the prior row"
		);
		assert_eq!(stored.updated_at_nanos(), 5_000);
	}

	#[test]
	fn deferred_read_sees_state_committed_above_primitive_version() {
		// A deferred consume's operator-state reads must observe the latest committed
		// snapshot, not be bounded to the consume's own input (primitive) version. A
		// prior consume's accumulated join state is committed at that consume's COMMIT
		// version, which is strictly greater than any input data version. If a later
		// consume read operator state bounded to its own lower primitive_version, the
		// other side of a join written by the prior consume would be invisible and the
		// row would wrongly emit an unmatched (null) result. This pins that invariant
		// (it is the root cause of the deferred left-join null-match flake).
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let inner_key = make_key("late_right_side");
		let value = make_value("matched_row");

		// The primitive (input) version we will read at. Two further commits then push
		// the operator-state write strictly more than one version above it, so the read
		// bound (which resolves to primitive_version + 1) cannot reach it on its own.
		let primitive_version = commit_state_row(&engine, node_id, &make_key("warmup_a"), make_value("a"));
		commit_state_row(&engine, node_id, &make_key("warmup_b"), make_value("b"));
		let committed_at = commit_state_row(&engine, node_id, &inner_key, value.clone());
		assert!(
			committed_at.0 >= primitive_version.0 + 2,
			"operator state must commit at least two versions above the primitive version: committed_at={committed_at:?} primitive_version={primitive_version:?}"
		);

		let (state_version, lease) = engine.acquire_current_snapshot_lease().unwrap();
		assert!(state_version >= committed_at);

		let query = engine.multi().begin_query_at_version(&lease).unwrap();
		let state_query = engine.multi().begin_query_at_version(&lease).unwrap();
		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: primitive_version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query,
			state_query,
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			allocators: FlowAllocators::new(),
		});

		let batch = txn.state_get_many(node_id, &[inner_key]).unwrap();
		assert_eq!(
			batch.items.len(),
			1,
			"operator state committed at {committed_at:?} (above primitive_version {primitive_version:?}) must be visible to a deferred read"
		);
		assert_eq!(batch.items[0].row, value);
	}

	#[test]
	fn committing_persists_state_writes_and_keeps_prior_state() {
		// The committing variant wraps the command being committed: its state writes route
		// to that command (state_set -> cmd, not the in-memory pending) and become durable
		// when the flow commits, alongside any state committed by prior transactions. This
		// guards the committing write+commit path the transactional tick relies on; a
		// regression that dropped these writes or failed to persist them would be caught.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let prior_key = make_key("prior");
		let prior_value = make_value("prior_value");
		commit_state_row(&engine, node_id, &prior_key, prior_value.clone());

		let written_key = make_key("written_by_tick");
		let written_value = make_value("tick_value");
		{
			let cmd = engine.begin_command(IdentityId::system()).unwrap();
			let mut txn = FlowTransaction::committing(CommittingParams {
				cmd,
				catalog: Catalog::testing(),
				interceptors: engine.create_interceptors(),
				clock: engine.clock().clone(),
				allocators: FlowAllocators::new(),
			})
			.unwrap();
			txn.state_set(node_id, &written_key, written_value.clone()).unwrap();
			txn.commit().unwrap();
		}

		// After the committing flow commits, both the prior state and the state it wrote
		// are durable and observable at the latest snapshot.
		let (_version, lease) = engine.acquire_current_snapshot_lease().unwrap();
		let query = engine.multi().begin_query_at_version(&lease).unwrap();
		let prior_encoded = FlowNodeStateKey::new(node_id, prior_key.as_ref().to_vec()).encode();
		let written_encoded = FlowNodeStateKey::new(node_id, written_key.as_ref().to_vec()).encode();
		let found = query.get_many(&[prior_encoded.clone(), written_encoded.clone()]).unwrap();
		assert_eq!(
			found.len(),
			2,
			"the committing flow's write and the prior committed state must both be durable after commit"
		);
		assert_eq!(found.get(&prior_encoded).unwrap().row, prior_value);
		assert_eq!(found.get(&written_encoded).unwrap().row, written_value);
	}

	#[test]
	fn transactional_read_sees_committed_state_below_version_and_base_pending() {
		// The transactional variant reads committed operator state via state_query (opened
		// at the latest snapshot by the interceptor) plus a base_pending overlay for the
		// current transaction's own writes. Its state read must NOT be bounded to the txn
		// `version`: here `version` is set below the committed state, which must still be
		// visible. This is the exact situation that broke the deferred path; this guards
		// the transactional path against the same version-bounding regression.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let committed_key = make_key("committed");
		let committed_value = make_value("committed_value");

		let low_version = commit_state_row(&engine, node_id, &make_key("warmup"), make_value("w"));
		commit_state_row(&engine, node_id, &make_key("bump"), make_value("bump"));
		let committed_at = commit_state_row(&engine, node_id, &committed_key, committed_value.clone());
		assert!(
			committed_at.0 >= low_version.0 + 2,
			"committed state must land at least two versions above the txn version so a wrongful bound (which resolves to version + 1) would hide it: committed_at={committed_at:?} low_version={low_version:?}"
		);

		let base_key = make_key("in_flight");
		let base_value = make_value("in_flight_value");
		let mut base_pending = Pending::new();
		base_pending.insert(
			FlowNodeStateKey::new(node_id, base_key.as_ref().to_vec()).encode(),
			base_value.clone(),
		);

		let mut txn = FlowTransaction::transactional(TransactionalParams {
			version: low_version,
			pending: Pending::new(),
			base_pending,
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			view_overlay: Arc::new(Vec::new()),
			allocators: FlowAllocators::new(),
		});

		// Committed state above the txn version is visible (state_query is at the snapshot).
		let committed = txn.state_get_many(node_id, &[committed_key]).unwrap();
		assert_eq!(
			committed.items.len(),
			1,
			"committed state at {committed_at:?} must be visible even though the txn version is {low_version:?}"
		);
		assert_eq!(committed.items[0].row, committed_value);

		// base_pending (the current transaction's writes) is visible via the overlay.
		let base = txn.state_get_many(node_id, &[base_key]).unwrap();
		assert_eq!(base.items.len(), 1);
		assert_eq!(base.items[0].row, base_value);
	}

	#[test]
	fn deferred_read_sees_base_pending_overlay() {
		// A deferred slice reads its own prior writes through the base_pending overlay:
		// the pinned query snapshot cannot see the flow's last commit (it landed above
		// chunk_end), so a Set must resolve from the overlay, a Remove must shadow a
		// committed row, and the slice's own pending must shadow the overlay. This is
		// the deferred mirror of the transactional guard above.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);

		let committed_key = make_key("committed");
		let committed_value = make_value("committed_value");
		let low_version = commit_state_row(&engine, node_id, &make_key("warmup"), make_value("w"));
		commit_state_row(&engine, node_id, &committed_key, committed_value.clone());

		let overlaid_key = make_key("overlaid");
		let overlaid_value = make_value("overlaid_value");
		let mut base_pending = Pending::new();
		base_pending.insert(
			FlowNodeStateKey::new(node_id, overlaid_key.as_ref().to_vec()).encode(),
			overlaid_value.clone(),
		);
		base_pending.remove(FlowNodeStateKey::new(node_id, committed_key.as_ref().to_vec()).encode());

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: low_version,
			pending: Pending::new(),
			base_pending: Arc::new(base_pending),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			allocators: FlowAllocators::new(),
		});

		// Point reads: Set resolves from the overlay, Remove shadows committed state.
		assert_eq!(
			txn.state_get(node_id, &overlaid_key).unwrap(),
			Some(overlaid_value.clone()),
			"a Set in base_pending must resolve through the overlay"
		);
		assert_eq!(
			txn.state_get(node_id, &committed_key).unwrap(),
			None,
			"a Remove in base_pending must shadow the committed row"
		);

		// Batch read path (lookup_overlays).
		let batch = txn.state_get_many(node_id, &[overlaid_key.clone(), committed_key.clone()]).unwrap();
		assert_eq!(batch.items.len(), 1);
		assert_eq!(batch.items[0].row, overlaid_value);

		// Range/scan path: the overlay entry appears, the removed row does not.
		let scan = txn.state_scan_all(node_id).unwrap();
		let scanned: Vec<_> = scan.items.iter().map(|item| item.row.clone()).collect();
		assert!(scanned.contains(&overlaid_value), "range merge must surface base_pending Sets");
		assert!(!scanned.contains(&committed_value), "range merge must shadow base_pending Removes");

		// The slice's own pending shadows the overlay.
		let shadow_value = make_value("shadow");
		txn.state_set(node_id, &overlaid_key, shadow_value.clone()).unwrap();
		assert_eq!(txn.state_get(node_id, &overlaid_key).unwrap(), Some(shadow_value));
	}

	#[test]
	fn deferred_reads_owned_rows_at_state_version() {
		// The restart scenario: a flow's own materialized rows commit above the
		// version its next slice pins `query` to, and the in-memory overlay is
		// empty after a restart. Owned-row keys (Row kind) must therefore route
		// through state_query, which reads at the lease. An Ephemeral txn at the
		// same pinned version must keep the pinned behavior (subscription
		// hydration reads views deliberately as-of a version).
		let engine = TestEngine::new();
		let row_key = RowKey::encoded(ShapeId::view(ViewId(7)), RowNumber(1));
		let row_value = make_value("own_row");

		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&make_key("warmup"), make_value("w")).unwrap();
		let low_version = cmd.commit_unchecked().unwrap();

		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		cmd.set(&row_key, row_value.clone()).unwrap();
		let committed_at = cmd.commit_unchecked().unwrap();
		assert!(low_version < committed_at);

		let mut txn = FlowTransaction::deferred_from_parts(DeferredParams {
			version: low_version,
			pending: Pending::new(),
			base_pending: Arc::new(Pending::new()),
			query: engine.multi().begin_query().unwrap(),
			state_query: engine.multi().begin_query().unwrap(),
			single: engine.single().clone(),
			catalog: Catalog::testing(),
			interceptors: engine.create_interceptors(),
			clock: engine.clock().clone(),
			allocators: FlowAllocators::new(),
		});
		assert_eq!(
			txn.get(&row_key).unwrap(),
			Some(row_value.clone()),
			"a deferred txn pinned below the flow's own commit must read its rows at the state version"
		);
		assert!(txn.contains_key(&row_key).unwrap());

		let mut ephemeral = FlowTransaction::ephemeral(
			low_version,
			engine.multi().begin_query().unwrap(),
			engine.single().clone(),
			Catalog::testing(),
			HashMap::new(),
			engine.clock().clone(),
		);
		assert_eq!(
			ephemeral.get(&row_key).unwrap(),
			None,
			"ephemeral (subscription) row reads must stay pinned to the requested version"
		);
	}

	#[test]
	fn ephemeral_read_sees_state_map_and_pending() {
		// The ephemeral variant has no state_query; it serves operator-state reads from an
		// in-memory state map (its seeded prior state) with the pending overlay on top.
		// Guards that both the seeded map and live writes are read back.
		let engine = TestEngine::new();
		let node_id = FlowNodeId(1);
		let seeded_key = make_key("seeded");
		let seeded_value = make_value("seeded_value");

		let mut state = HashMap::new();
		state.insert(
			FlowNodeStateKey::new(node_id, seeded_key.as_ref().to_vec()).encode(),
			seeded_value.clone(),
		);

		let mut txn = FlowTransaction::ephemeral(
			CommitVersion(1),
			engine.multi().begin_query().unwrap(),
			engine.single().clone(),
			Catalog::testing(),
			state,
			engine.clock().clone(),
		);

		let seeded = txn.state_get_many(node_id, &[seeded_key]).unwrap();
		assert_eq!(seeded.items.len(), 1, "seeded ephemeral state must be readable");
		assert_eq!(seeded.items[0].row, seeded_value);

		// A live write is visible via the pending overlay.
		let live_key = make_key("live");
		let live_value = make_value("live_value");
		txn.state_set(node_id, &live_key, live_value.clone()).unwrap();
		let live = txn.state_get_many(node_id, &[live_key]).unwrap();
		assert_eq!(live.items.len(), 1);
		assert_eq!(live.items[0].row, live_value);
	}
}
