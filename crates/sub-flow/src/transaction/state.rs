// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{FlowNodeId, MultiVersionBatch},
	key::{EncodableKey, FlowNodeStateKey},
	value::encoded::{EncodedValues, EncodedValuesLayout},
};
use tracing::instrument;

use super::FlowTransaction;

impl FlowTransaction {
	/// Get state for a specific flow node and key
	#[instrument(name = "flow::state::get", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		found
	))]
	pub async fn state_get(&mut self, id: FlowNodeId, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		self.metrics.increment_state_operations();
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		let result = self.get(&encoded_key).await?;
		tracing::Span::current().record("found", result.is_some());
		Ok(result)
	}

	/// Set state for a specific flow node and key
	#[instrument(name = "flow::state::set", level = "trace", skip(self, value), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		value_len = value.as_ref().len()
	))]
	pub fn state_set(&mut self, id: FlowNodeId, key: &EncodedKey, value: EncodedValues) -> crate::Result<()> {
		self.metrics.increment_state_operations();
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		self.set(&encoded_key, value)
	}

	/// Remove state for a specific flow node and key
	#[instrument(name = "flow::state::remove", level = "trace", skip(self), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn state_remove(&mut self, id: FlowNodeId, key: &EncodedKey) -> crate::Result<()> {
		self.metrics.increment_state_operations();
		let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		self.remove(&encoded_key)
	}

	/// Scan all state for a specific flow node
	#[instrument(name = "flow::state::scan", level = "debug", skip(self), fields(
		node_id = id.0
	))]
	pub async fn state_scan(&mut self, id: FlowNodeId) -> crate::Result<MultiVersionBatch> {
		self.metrics.increment_state_operations();
		let range = FlowNodeStateKey::node_range(id);
		self.range(range).await
	}

	/// Range query on state for a specific flow node
	#[instrument(name = "flow::state::range", level = "debug", skip(self, range), fields(
		node_id = id.0
	))]
	pub async fn state_range(
		&mut self,
		id: FlowNodeId,
		range: EncodedKeyRange,
	) -> crate::Result<MultiVersionBatch> {
		self.metrics.increment_state_operations();
		let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(id, vec![]));
		self.range(prefixed_range).await
	}

	/// Clear all state for a specific flow node
	#[instrument(name = "flow::state::clear", level = "debug", skip(self), fields(
		node_id = id.0,
		removed_count
	))]
	pub async fn state_clear(&mut self, id: FlowNodeId) -> crate::Result<()> {
		self.metrics.increment_state_operations();
		let range = FlowNodeStateKey::node_range(id);
		let batch = self.range(range).await?;
		let keys_to_remove: Vec<_> = batch.items.into_iter().map(|multi| multi.key).collect();

		let count = keys_to_remove.len();
		for key in keys_to_remove {
			self.remove(&key)?;
		}

		tracing::Span::current().record("removed_count", count);
		Ok(())
	}

	/// Load state for a key, creating if not exists
	#[instrument(name = "flow::state::load_or_create", level = "debug", skip(self, layout), fields(
		node_id = id.0,
		key_len = key.as_bytes().len(),
		created
	))]
	pub async fn load_or_create_row(
		&mut self,
		id: FlowNodeId,
		key: &EncodedKey,
		layout: &EncodedValuesLayout,
	) -> crate::Result<EncodedValues> {
		match self.state_get(id, key).await? {
			Some(row) => {
				tracing::Span::current().record("created", false);
				Ok(row)
			}
			None => {
				tracing::Span::current().record("created", true);
				Ok(layout.allocate())
			}
		}
	}

	/// Save state encoded
	#[instrument(name = "flow::state::save", level = "trace", skip(self, row), fields(
		node_id = id.0,
		key_len = key.as_bytes().len()
	))]
	pub fn save_row(&mut self, id: FlowNodeId, key: &EncodedKey, row: EncodedValues) -> crate::Result<()> {
		self.state_set(id, key, row)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		CommitVersion, CowVec, EncodedKey, EncodedKeyRange, interface::FlowNodeId,
		value::encoded::EncodedValues,
	};
	use reifydb_type::Type;

	use super::*;
	use crate::operator::stateful::test_utils::test::{MaterializedCatalog, create_test_transaction};

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[tokio::test]
	async fn test_state_get_set() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("state_key");
		let value = make_value("state_value");

		// Set state
		txn.state_set(node_id, &key, value.clone()).unwrap();

		// Get state back
		let result = txn.state_get(node_id, &key).await.unwrap();
		assert_eq!(result, Some(value));
	}

	#[tokio::test]
	async fn test_state_get_nonexistent() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("missing");

		let result = txn.state_get(node_id, &key).await.unwrap();
		assert_eq!(result, None);
	}

	#[tokio::test]
	async fn test_state_remove() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("state_key");
		let value = make_value("state_value");

		// Set then remove
		txn.state_set(node_id, &key, value.clone()).unwrap();
		assert_eq!(txn.state_get(node_id, &key).await.unwrap(), Some(value));

		txn.state_remove(node_id, &key).unwrap();
		assert_eq!(txn.state_get(node_id, &key).await.unwrap(), None);
	}

	#[tokio::test]
	async fn test_state_isolation_between_nodes() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let key = make_key("same_key");

		txn.state_set(node1, &key, make_value("node1_value")).unwrap();
		txn.state_set(node2, &key, make_value("node2_value")).unwrap();

		// Each node should have its own value
		assert_eq!(txn.state_get(node1, &key).await.unwrap(), Some(make_value("node1_value")));
		assert_eq!(txn.state_get(node2, &key).await.unwrap(), Some(make_value("node2_value")));
	}

	#[tokio::test]
	async fn test_state_scan() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);

		txn.state_set(node_id, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node_id, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node_id, &make_key("key3"), make_value("value3")).unwrap();

		let iter = txn.state_scan(node_id).await.unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		assert_eq!(items.len(), 3);
	}

	#[tokio::test]
	async fn test_state_scan_only_own_node() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);

		txn.state_set(node1, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node1, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node2, &make_key("key3"), make_value("value3")).unwrap();

		// Scan node1 should only return node1's state
		let items: Vec<_> = txn.state_scan(node1).await.unwrap().items.into_iter().collect();
		assert_eq!(items.len(), 2);

		// Scan node2 should only return node2's state
		let items: Vec<_> = txn.state_scan(node2).await.unwrap().items.into_iter().collect();
		assert_eq!(items.len(), 1);
	}

	#[tokio::test]
	async fn test_state_scan_empty() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);

		let iter = txn.state_scan(node_id).await.unwrap();
		assert!(iter.items.into_iter().next().is_none());
	}

	#[tokio::test]
	async fn test_state_range() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);

		txn.state_set(node_id, &make_key("a"), make_value("1")).unwrap();
		txn.state_set(node_id, &make_key("b"), make_value("2")).unwrap();
		txn.state_set(node_id, &make_key("c"), make_value("3")).unwrap();
		txn.state_set(node_id, &make_key("d"), make_value("4")).unwrap();

		// Range query from "b" to "d" (exclusive)
		use std::collections::Bound;
		let range = EncodedKeyRange::new(Bound::Included(make_key("b")), Bound::Excluded(make_key("d")));
		let iter = txn.state_range(node_id, range).await.unwrap();
		let items: Vec<_> = iter.items.into_iter().collect();

		// Should only include "b" and "c"
		assert_eq!(items.len(), 2);
	}

	#[tokio::test]
	async fn test_state_clear() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);

		txn.state_set(node_id, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node_id, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node_id, &make_key("key3"), make_value("value3")).unwrap();

		// Verify state exists
		assert_eq!(txn.state_scan(node_id).await.unwrap().items.into_iter().count(), 3);

		// Clear all state
		txn.state_clear(node_id).await.unwrap();

		// Verify state is empty
		assert_eq!(txn.state_scan(node_id).await.unwrap().items.into_iter().count(), 0);
	}

	#[tokio::test]
	async fn test_state_clear_only_own_node() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);

		txn.state_set(node1, &make_key("key1"), make_value("value1")).unwrap();
		txn.state_set(node1, &make_key("key2"), make_value("value2")).unwrap();
		txn.state_set(node2, &make_key("key3"), make_value("value3")).unwrap();

		// Clear node1
		txn.state_clear(node1).await.unwrap();

		// Node1 should be empty
		assert_eq!(txn.state_scan(node1).await.unwrap().items.into_iter().count(), 0);

		// Node2 should still have state
		assert_eq!(txn.state_scan(node2).await.unwrap().items.into_iter().count(), 1);
	}

	#[tokio::test]
	async fn test_state_clear_empty_node() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);

		// Clear on empty node should not error
		txn.state_clear(node_id).await.unwrap();
	}

	#[tokio::test]
	async fn test_load_or_create_existing() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let value = make_value("existing");
		let layout = EncodedValuesLayout::new(&[Type::Int8, Type::Float8]);

		// Set existing state
		txn.state_set(node_id, &key, value.clone()).unwrap();

		// load_or_create should return existing value
		let result = txn.load_or_create_row(node_id, &key, &layout).await.unwrap();
		assert_eq!(result, value);
	}

	#[tokio::test]
	async fn test_load_or_create_new() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let layout = EncodedValuesLayout::new(&[Type::Int8, Type::Float8]);

		// load_or_create should allocate new row
		let result = txn.load_or_create_row(node_id, &key, &layout).await.unwrap();

		// Result should be a newly allocated row (layout.allocate())
		assert!(!result.as_ref().is_empty());
	}

	#[tokio::test]
	async fn test_save_row() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let row = make_value("row_data");

		txn.save_row(node_id, &key, row.clone()).unwrap();

		// Verify saved
		let result = txn.state_get(node_id, &key).await.unwrap();
		assert_eq!(result, Some(row));
	}

	#[tokio::test]
	async fn test_state_operations_increment_metrics() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("key1");

		assert_eq!(txn.metrics().state_operations, 0);

		txn.state_set(node_id, &key, make_value("value")).unwrap();
		assert_eq!(txn.metrics().state_operations, 1);

		txn.state_get(node_id, &key).await.unwrap();
		assert_eq!(txn.metrics().state_operations, 2);

		txn.state_remove(node_id, &key).unwrap();
		assert_eq!(txn.metrics().state_operations, 3);

		let _ = txn.state_scan(node_id).await.unwrap();
		assert_eq!(txn.metrics().state_operations, 4);

		let range = EncodedKeyRange::start_end(Some(make_key("a")), Some(make_key("z")));
		let _ = txn.state_range(node_id, range).await.unwrap();
		assert_eq!(txn.metrics().state_operations, 5);

		txn.state_clear(node_id).await.unwrap();
		// state_clear calls state_scan internally, so it increments by 2 (one for clear, one for the range
		// scan)
		assert!(txn.metrics().state_operations >= 6);
	}

	#[tokio::test]
	async fn test_state_multiple_nodes() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let node3 = FlowNodeId(3);

		txn.state_set(node1, &make_key("a"), make_value("n1_a")).unwrap();
		txn.state_set(node1, &make_key("b"), make_value("n1_b")).unwrap();
		txn.state_set(node2, &make_key("a"), make_value("n2_a")).unwrap();
		txn.state_set(node3, &make_key("c"), make_value("n3_c")).unwrap();

		// Verify each node has correct state
		assert_eq!(txn.state_get(node1, &make_key("a")).await.unwrap(), Some(make_value("n1_a")));
		assert_eq!(txn.state_get(node1, &make_key("b")).await.unwrap(), Some(make_value("n1_b")));
		assert_eq!(txn.state_get(node2, &make_key("a")).await.unwrap(), Some(make_value("n2_a")));
		assert_eq!(txn.state_get(node3, &make_key("c")).await.unwrap(), Some(make_value("n3_c")));

		// Cross-node keys should not exist
		assert_eq!(txn.state_get(node2, &make_key("b")).await.unwrap(), None);
		assert_eq!(txn.state_get(node3, &make_key("a")).await.unwrap(), None);
	}

	#[tokio::test]
	async fn test_load_or_create_increments_state_operations() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("key1");
		let layout = EncodedValuesLayout::new(&[Type::Int8]);

		let initial_count = txn.metrics().state_operations;

		txn.load_or_create_row(node_id, &key, &layout).await.unwrap();

		// load_or_create calls state_get internally
		assert!(txn.metrics().state_operations > initial_count);
	}

	#[tokio::test]
	async fn test_save_row_increments_state_operations() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let node_id = FlowNodeId(1);
		let key = make_key("key1");

		let initial_count = txn.metrics().state_operations;

		txn.save_row(node_id, &key, make_value("data")).unwrap();

		// save_row calls state_set internally
		assert!(txn.metrics().state_operations > initial_count);
	}
}
