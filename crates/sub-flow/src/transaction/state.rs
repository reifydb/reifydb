// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::{EncodedRow, SHAPE_HEADER_SIZE},
		shape::RowShape,
	},
	interface::{
		catalog::flow::FlowNodeId,
		store::{MultiVersionBatch, MultiVersionRow},
	},
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
};
use reifydb_type::Result;
use tracing::{Span, field, instrument};

use super::FlowTransaction;

impl FlowTransaction {
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found = field::Empty
	))]
	pub fn state_get(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		let result = self.get(&encoded_key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::state::get_many", level = "debug", skip(self, keys), fields(
		node_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn state_get_many(&mut self, id: FlowNodeId, keys: &[EncodedKey]) -> Result<MultiVersionBatch> {
		let version = self.version();
		let encoded: Vec<EncodedKey> =
			keys.iter().map(|key| FlowNodeStateKey::new(id, key.as_ref().to_vec()).encode()).collect();

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for encoded_key in &encoded {
			let pending = {
				let inner = self.inner();
				if inner.pending.is_removed(encoded_key) {
					Some(None)
				} else {
					inner.pending.get(encoded_key).map(|row| Some(row.clone()))
				}
			};
			match pending {
				Some(None) => continue,
				Some(Some(row)) => {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						row,
						version,
					});
					continue;
				}
				None => {}
			}

			let base = if let Self::Transactional {
				base_pending,
				..
			} = &*self
			{
				if base_pending.is_removed(encoded_key) {
					Some(None)
				} else {
					base_pending.get(encoded_key).map(|row| Some(row.clone()))
				}
			} else {
				None
			};
			match base {
				Some(None) => continue,
				Some(Some(row)) => {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						row,
						version,
					});
					continue;
				}
				None => {}
			}

			to_batch.push(encoded_key.clone());
		}

		if !to_batch.is_empty() {
			if let Self::Ephemeral {
				inner,
				state,
			} = self
			{
				let version = inner.version;
				for encoded_key in &to_batch {
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
				let found = inner.state_query.as_ref().unwrap().get_many(&to_batch)?;
				for encoded_key in &to_batch {
					if let Some(multi) = found.get(encoded_key) {
						items.push(multi.clone());
					}
				}
			}
		}

		Span::current().record("found_count", items.len());
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::state::prefetch", level = "debug", skip(self, keys), fields(node_id = id.0, key_count = keys.len()))]
	pub fn prefetch_state(&mut self, id: FlowNodeId, keys: &[EncodedKey]) -> Result<()> {
		if keys.is_empty() {
			return Ok(());
		}

		let batch = self.state_get_many(id, keys)?;
		let mut found: HashMap<EncodedKey, EncodedRow> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			found.insert(item.key, item.row);
		}

		let inner = self.inner_mut();
		for key in keys {
			let encoded_key = FlowNodeStateKey::new(id, key.as_ref().to_vec()).encode();
			let value = found.get(&encoded_key).cloned();
			inner.prefetch.insert(encoded_key, value);
		}
		Ok(())
	}

	#[instrument(name = "flow::state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.len()
	))]
	pub fn state_set(&mut self, id: FlowNodeId, key: &EncodedKey, mut value: EncodedRow) -> Result<()> {
		let state_key = FlowNodeStateKey::new(id, key.to_vec());
		let encoded_key = state_key.encode();

		if value.len() >= SHAPE_HEADER_SIZE
			&& let Some(prior) = self.get(&encoded_key)?
			&& prior.len() >= SHAPE_HEADER_SIZE
		{
			let prior_created = prior.created_at_nanos();
			if prior_created != 0 {
				let updated = value.updated_at_nanos();
				value.set_timestamps(prior_created, updated);
			}
		}

		self.set(&encoded_key, value)
	}

	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn state_remove(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		self.remove(&encoded_key)
	}

	#[instrument(name = "flow::internal_state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found = field::Empty
	))]
	pub fn internal_state_get(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<Option<EncodedRow>> {
		let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		let result = self.get(&encoded_key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::internal_state::get_many", level = "debug", skip(self, keys), fields(
		node_id = id.0,
		key_count = keys.len(),
		found_count = field::Empty
	))]
	pub fn internal_state_get_many(&mut self, id: FlowNodeId, keys: &[EncodedKey]) -> Result<MultiVersionBatch> {
		let version = self.version();
		let encoded: Vec<EncodedKey> = keys
			.iter()
			.map(|key| FlowNodeInternalStateKey::new(id, key.as_ref().to_vec()).encode())
			.collect();

		let mut items: Vec<MultiVersionRow> = Vec::new();
		let mut to_batch: Vec<EncodedKey> = Vec::new();

		for encoded_key in &encoded {
			let pending = {
				let inner = self.inner();
				if inner.pending.is_removed(encoded_key) {
					Some(None)
				} else {
					inner.pending.get(encoded_key).map(|row| Some(row.clone()))
				}
			};
			match pending {
				Some(None) => continue,
				Some(Some(row)) => {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						row,
						version,
					});
					continue;
				}
				None => {}
			}

			let base = if let Self::Transactional {
				base_pending,
				..
			} = &*self
			{
				if base_pending.is_removed(encoded_key) {
					Some(None)
				} else {
					base_pending.get(encoded_key).map(|row| Some(row.clone()))
				}
			} else {
				None
			};
			match base {
				Some(None) => continue,
				Some(Some(row)) => {
					items.push(MultiVersionRow {
						key: encoded_key.clone(),
						row,
						version,
					});
					continue;
				}
				None => {}
			}

			to_batch.push(encoded_key.clone());
		}

		if !to_batch.is_empty() {
			if let Self::Ephemeral {
				inner,
				state,
			} = self
			{
				let version = inner.version;
				for encoded_key in &to_batch {
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
				let found = inner.state_query.as_ref().unwrap().get_many(&to_batch)?;
				for encoded_key in &to_batch {
					if let Some(multi) = found.get(encoded_key) {
						items.push(multi.clone());
					}
				}
			}
		}

		Span::current().record("found_count", items.len());
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	#[instrument(name = "flow::internal_state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.len()
	))]
	pub fn internal_state_set(&mut self, id: FlowNodeId, key: &EncodedKey, mut value: EncodedRow) -> Result<()> {
		let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();

		if value.len() >= SHAPE_HEADER_SIZE
			&& let Some(prior) = self.get(&encoded_key)?
			&& prior.len() >= SHAPE_HEADER_SIZE
		{
			let prior_created = prior.created_at_nanos();
			if prior_created != 0 {
				let updated = value.updated_at_nanos();
				value.set_timestamps(prior_created, updated);
			}
		}

		self.set(&encoded_key, value)
	}

	#[instrument(name = "flow::internal_state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn internal_state_remove(&mut self, id: FlowNodeId, key: &EncodedKey) -> Result<()> {
		let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		self.remove(&encoded_key)
	}

	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		node_id = id.0,
		result_count = field::Empty
	))]
	pub fn state_scan_all(&mut self, id: FlowNodeId) -> Result<MultiVersionBatch> {
		let range = FlowNodeStateKey::node_range(id);
		let iter = self.range(range, 1024);
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
		let iter = self.range(prefixed_range, 1024);
		let mut items = Vec::new();
		for result in iter {
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
		let iter = self.range(range, 1024);
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
}

#[cfg(test)]
pub mod tests {
	use std::collections::Bound;

	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{
		common::CommitVersion,
		encoded::{
			key::{EncodedKey, EncodedKeyRange},
			row::EncodedRow,
			shape::RowShape,
		},
		interface::catalog::flow::FlowNodeId,
	};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::{util::cowvec::CowVec, value::r#type::Type};

	use super::*;
	use crate::operator::stateful::test_utils::test::create_test_transaction;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
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
		let shape = RowShape::testing(&[Type::Int8, Type::Float8]);

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
		let shape = RowShape::testing(&[Type::Int8, Type::Float8]);

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
}
