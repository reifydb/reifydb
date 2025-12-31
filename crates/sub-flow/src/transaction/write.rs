// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, value::encoded::EncodedValues};

use super::FlowTransaction;

impl FlowTransaction {
	/// Set a value, buffering it in pending writes
	pub fn set(&mut self, key: &EncodedKey, value: EncodedValues) -> crate::Result<()> {
		self.metrics.increment_writes();
		self.pending.insert(key.clone(), value);
		Ok(())
	}

	/// Remove a key, buffering the deletion in pending operations
	pub fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.metrics.increment_removes();
		self.pending.remove(key.clone());
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		CommitVersion, CowVec, EncodedKey,
		interface::{CommandTransaction, QueryTransaction},
		value::encoded::EncodedValues,
	};
	use reifydb_engine::StandardCommandTransaction;

	use super::*;
	use crate::operator::stateful::test_utils::test::{MaterializedCatalog, create_test_transaction};

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn make_value(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	async fn get_values(parent: &mut StandardCommandTransaction, key: &EncodedKey) -> Option<EncodedValues> {
		parent.get(key).await.unwrap().map(|m| m.values)
	}

	#[tokio::test]
	async fn test_set_buffers_to_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let key = make_key("key1");
		let value = make_value("value1");

		txn.set(&key, value.clone()).unwrap();

		// Value should be in pending buffer
		assert_eq!(txn.pending.get(&key), Some(&value));
		assert_eq!(txn.pending.len(), 1);
	}

	#[tokio::test]
	async fn test_set_increments_writes_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		assert_eq!(txn.metrics().writes, 0);

		txn.set(&make_key("key1"), make_value("value1")).unwrap();
		assert_eq!(txn.metrics().writes, 1);

		txn.set(&make_key("key2"), make_value("value2")).unwrap();
		assert_eq!(txn.metrics().writes, 2);
	}

	#[tokio::test]
	async fn test_set_multiple_keys() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		txn.set(&make_key("key1"), make_value("value1")).unwrap();
		txn.set(&make_key("key2"), make_value("value2")).unwrap();
		txn.set(&make_key("key3"), make_value("value3")).unwrap();

		assert_eq!(txn.pending.len(), 3);
		assert_eq!(txn.metrics().writes, 3);
		assert_eq!(txn.pending.get(&make_key("key1")), Some(&make_value("value1")));
		assert_eq!(txn.pending.get(&make_key("key2")), Some(&make_value("value2")));
		assert_eq!(txn.pending.get(&make_key("key3")), Some(&make_value("value3")));
	}

	#[tokio::test]
	async fn test_set_overwrites_same_key() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();
		txn.set(&key, make_value("value2")).unwrap();

		// Should have only one entry with latest value
		assert_eq!(txn.pending.len(), 1);
		assert_eq!(txn.pending.get(&key), Some(&make_value("value2")));
		// Both writes should be counted in metrics
		assert_eq!(txn.metrics().writes, 2);
	}

	#[tokio::test]
	async fn test_remove_buffers_to_pending() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let key = make_key("key1");
		txn.remove(&key).unwrap();

		// Key should be marked for removal in pending buffer
		assert!(txn.pending.is_removed(&key));
		assert_eq!(txn.pending.len(), 1);
	}

	#[tokio::test]
	async fn test_remove_increments_removes_metric() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		assert_eq!(txn.metrics().removes, 0);

		txn.remove(&make_key("key1")).unwrap();
		assert_eq!(txn.metrics().removes, 1);

		txn.remove(&make_key("key2")).unwrap();
		assert_eq!(txn.metrics().removes, 2);
	}

	#[tokio::test]
	async fn test_remove_multiple_keys() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		txn.remove(&make_key("key1")).unwrap();
		txn.remove(&make_key("key2")).unwrap();
		txn.remove(&make_key("key3")).unwrap();

		assert_eq!(txn.pending.len(), 3);
		assert_eq!(txn.metrics().removes, 3);
		assert!(txn.pending.is_removed(&make_key("key1")));
		assert!(txn.pending.is_removed(&make_key("key2")));
		assert!(txn.pending.is_removed(&make_key("key3")));
	}

	#[tokio::test]
	async fn test_set_then_remove() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let key = make_key("key1");
		txn.set(&key, make_value("value1")).unwrap();
		assert_eq!(txn.pending.get(&key), Some(&make_value("value1")));

		txn.remove(&key).unwrap();
		assert!(txn.pending.is_removed(&key));
		assert_eq!(txn.pending.get(&key), None);

		// Metrics should count both operations
		assert_eq!(txn.metrics().writes, 1);
		assert_eq!(txn.metrics().removes, 1);
	}

	#[tokio::test]
	async fn test_remove_then_set() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let key = make_key("key1");
		txn.remove(&key).unwrap();
		assert!(txn.pending.is_removed(&key));

		txn.set(&key, make_value("value1")).unwrap();
		assert!(!txn.pending.is_removed(&key));
		assert_eq!(txn.pending.get(&key), Some(&make_value("value1")));

		// Metrics should count both operations
		assert_eq!(txn.metrics().removes, 1);
		assert_eq!(txn.metrics().writes, 1);
	}

	#[tokio::test]
	async fn test_writes_not_visible_to_parent() {
		let mut parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		let key = make_key("key1");
		let value = make_value("value1");

		// Set in FlowTransaction
		txn.set(&key, value.clone()).unwrap();

		// Parent should not see the write
		assert_eq!(get_values(&mut parent, &key).await, None);
	}

	#[tokio::test]
	async fn test_removes_not_visible_to_parent() {
		let mut parent = create_test_transaction().await;

		// Set a value in parent
		let key = make_key("key1");
		let value = make_value("value1");
		parent.set(&key, value.clone()).await.unwrap();
		assert_eq!(get_values(&mut parent, &key).await, Some(value.clone()));

		// Create FlowTransaction and remove the key
		let parent_version = parent.version();
		let mut txn = FlowTransaction::new(&parent, parent_version, &MaterializedCatalog::new()).await;
		txn.remove(&key).unwrap();

		// Parent should still see the value
		assert_eq!(get_values(&mut parent, &key).await, Some(value));
	}

	#[tokio::test]
	async fn test_mixed_writes_and_removes() {
		let parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1), &MaterializedCatalog::new()).await;

		txn.set(&make_key("write1"), make_value("v1")).unwrap();
		txn.remove(&make_key("remove1")).unwrap();
		txn.set(&make_key("write2"), make_value("v2")).unwrap();
		txn.remove(&make_key("remove2")).unwrap();

		assert_eq!(txn.pending.len(), 4);
		assert_eq!(txn.metrics().writes, 2);
		assert_eq!(txn.metrics().removes, 2);

		assert_eq!(txn.pending.get(&make_key("write1")), Some(&make_value("v1")));
		assert_eq!(txn.pending.get(&make_key("write2")), Some(&make_value("v2")));
		assert!(txn.pending.is_removed(&make_key("remove1")));
		assert!(txn.pending.is_removed(&make_key("remove2")));
	}
}
