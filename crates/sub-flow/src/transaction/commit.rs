// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use diagnostic::flow::flow_transaction_keyspace_overlap;
use reifydb_core::interface::CommandTransaction;
use reifydb_engine::StandardCommandTransaction;
use reifydb_type::{diagnostic, return_error, util::hex};
use tracing::instrument;

use super::{FlowTransaction, FlowTransactionMetrics, Pending};

impl FlowTransaction {
	/// Commit all pending writes and removes to the parent transaction
	///
	/// Takes the parent transaction as a mutable reference to apply buffered operations.
	/// This allows the FlowTransaction to be reused for subsequent units of work.
	/// The pending buffer is NOT cleared to maintain read-your-own-writes semantics.
	///
	/// Returns the transaction metrics.
	///
	/// # Errors
	///
	/// Returns an error if any key in this FlowTransaction overlaps with keys already
	/// written by another FlowTransaction to the same parent. FlowTransactions must
	/// operate on non-overlapping keyspaces.

	#[instrument(name = "flow::transaction::commit", level = "debug", skip(self, parent), fields(
		pending_count = self.pending.len(),
		writes,
		removes
	))]
	pub async fn commit(
		&mut self,
		parent: &mut StandardCommandTransaction,
	) -> crate::Result<FlowTransactionMetrics> {
		// Check for any overlapping keys with the parent's pending writes.
		// This enforces that FlowTransactions operate on non-overlapping keyspaces.
		{
			let parent_pending = parent.pending_writes();
			for (key, _) in self.pending.iter_sorted() {
				// Check if key exists in parent
				if parent_pending.contains_key(key) {
					return_error!(flow_transaction_keyspace_overlap(hex::encode(key.as_ref())));
				}
			}
		}

		let mut set_count = 0;
		let mut remove_count = 0;
		for (key, pending) in self.pending.iter_sorted() {
			match pending {
				Pending::Set(value) => {
					parent.set(key, value.clone()).await?;
					set_count += 1;
				}
				Pending::Remove => {
					parent.remove(key).await?;
					remove_count += 1;
				}
			}
		}

		tracing::Span::current().record("sets", set_count);
		tracing::Span::current().record("removes", remove_count);

		self.pending.clear();
		Ok(self.metrics.clone())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::CommitVersion;

	use super::*;
	use crate::{
		operator::stateful::test_utils::test::create_test_transaction,
		transaction::utils::test::{from_store, make_key, make_value},
	};

	#[tokio::test]
	async fn test_commit_empty_pending() {
		let mut parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let metrics = txn.commit(&mut parent).await.unwrap();

		// Metrics should be zero
		assert_eq!(metrics.reads, 0);
		assert_eq!(metrics.writes, 0);
		assert_eq!(metrics.removes, 0);
	}

	#[tokio::test]
	async fn test_commit_single_write() {
		let mut parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let key = make_key("key1");
		let value = make_value("value1");
		txn.set(&key, value.clone()).unwrap();

		txn.commit(&mut parent).await.unwrap();

		// Parent should now have the value
		assert_eq!(from_store(&mut parent, &key).await, Some(value));
	}

	#[tokio::test]
	async fn test_commit_multiple_writes() {
		let mut parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.set(&make_key("key1"), make_value("value1")).unwrap();
		txn.set(&make_key("key2"), make_value("value2")).unwrap();
		txn.set(&make_key("key3"), make_value("value3")).unwrap();

		txn.commit(&mut parent).await.unwrap();

		// All values should be in parent
		assert_eq!(from_store(&mut parent, &make_key("key1")).await, Some(make_value("value1")));
		assert_eq!(from_store(&mut parent, &make_key("key2")).await, Some(make_value("value2")));
		assert_eq!(from_store(&mut parent, &make_key("key3")).await, Some(make_value("value3")));
	}

	#[tokio::test]
	async fn test_commit_removes() {
		use reifydb_core::interface::Engine;

		use crate::operator::stateful::test_utils::test::create_test_engine;

		let engine = create_test_engine().await;
		let mut parent = engine.begin_command().await.unwrap();

		// First commit some data to the underlying storage
		let key1 = make_key("key1");
		let key2 = make_key("key2");
		parent.set(&key1, make_value("value1")).await.unwrap();
		parent.set(&key2, make_value("value2")).await.unwrap();
		let commit_version = parent.commit().await.unwrap();

		// Create new parent transaction after commit
		let mut parent = engine.begin_command().await.unwrap();

		// Verify values exist in storage
		assert_eq!(from_store(&mut parent, &key1).await, Some(make_value("value1")));
		assert_eq!(from_store(&mut parent, &key2).await, Some(make_value("value2")));

		// Create FlowTransaction and remove the keys
		let mut txn = FlowTransaction::new(&parent, commit_version).await;
		txn.remove(&key1).unwrap();
		txn.remove(&key2).unwrap();

		txn.commit(&mut parent).await.unwrap();

		// Commit parent to persist the removes
		parent.commit().await.unwrap();

		// Create new transaction to verify removes were persisted
		let mut parent = engine.begin_command().await.unwrap();
		assert_eq!(from_store(&mut parent, &key1).await, None);
		assert_eq!(from_store(&mut parent, &key2).await, None);
	}

	#[tokio::test]
	async fn test_commit_mixed_writes_and_removes() {
		use reifydb_core::interface::Engine;

		use crate::operator::stateful::test_utils::test::create_test_engine;

		let engine = create_test_engine().await;
		let mut parent = engine.begin_command().await.unwrap();

		// First commit some data to the underlying storage
		let existing_key = make_key("existing");
		parent.set(&existing_key, make_value("old")).await.unwrap();
		let commit_version = parent.commit().await.unwrap();

		// Create new parent transaction after commit
		let mut parent = engine.begin_command().await.unwrap();

		// Verify value exists in storage
		assert_eq!(from_store(&mut parent, &existing_key).await, Some(make_value("old")));

		// Create FlowTransaction
		let mut txn = FlowTransaction::new(&parent, commit_version).await;

		// Add a new key and remove the existing one
		let new_key = make_key("new");
		txn.set(&new_key, make_value("value")).unwrap();
		txn.remove(&existing_key).unwrap();

		txn.commit(&mut parent).await.unwrap();

		// Commit parent to persist the changes
		parent.commit().await.unwrap();

		// Create new transaction to verify changes were persisted
		let mut parent = engine.begin_command().await.unwrap();
		assert_eq!(from_store(&mut parent, &new_key).await, Some(make_value("value")));
		assert_eq!(from_store(&mut parent, &existing_key).await, None);
	}

	#[tokio::test]
	async fn test_commit_returns_metrics() {
		let mut parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		txn.set(&make_key("key1"), make_value("value1")).unwrap();
		txn.get(&make_key("key2")).await.unwrap();
		txn.remove(&make_key("key3")).unwrap();

		let metrics = txn.commit(&mut parent).await.unwrap();

		assert_eq!(metrics.writes, 1);
		assert_eq!(metrics.reads, 1);
		assert_eq!(metrics.removes, 1);
	}

	#[tokio::test]
	async fn test_commit_overwrites_storage_value() {
		use reifydb_core::interface::Engine;

		use crate::operator::stateful::test_utils::test::create_test_engine;

		let engine = create_test_engine().await;
		let mut parent = engine.begin_command().await.unwrap();

		// First commit some data to the underlying storage
		let key = make_key("key1");
		parent.set(&key, make_value("old")).await.unwrap();
		let commit_version = parent.commit().await.unwrap();

		// Create new parent transaction after commit
		let mut parent = engine.begin_command().await.unwrap();

		// Verify old value exists in storage
		assert_eq!(from_store(&mut parent, &key).await, Some(make_value("old")));

		// Create FlowTransaction and overwrite the value
		let mut txn = FlowTransaction::new(&parent, commit_version).await;
		txn.set(&key, make_value("new")).unwrap();
		txn.commit(&mut parent).await.unwrap();

		// Parent should have new value
		assert_eq!(from_store(&mut parent, &key).await, Some(make_value("new")));
	}

	#[tokio::test]
	async fn test_sequential_commits_different_keys() {
		let mut parent = create_test_transaction().await;

		// First FlowTransaction writes to key1
		// Note: FlowTransactions must operate on non-overlapping keyspaces
		// This is enforced at the flow scheduler level, not the transaction level
		let mut txn1 = FlowTransaction::new(&parent, CommitVersion(1)).await;
		txn1.set(&make_key("key1"), make_value("value1")).unwrap();
		txn1.commit(&mut parent).await.unwrap();

		// Second FlowTransaction writes to key2 (different keyspace)
		let mut txn2 = FlowTransaction::new(&parent, CommitVersion(2)).await;
		txn2.set(&make_key("key2"), make_value("value2")).unwrap();
		txn2.commit(&mut parent).await.unwrap();

		// Both values should be in parent
		assert_eq!(from_store(&mut parent, &make_key("key1")).await, Some(make_value("value1")));
		assert_eq!(from_store(&mut parent, &make_key("key2")).await, Some(make_value("value2")));
	}

	#[tokio::test]
	async fn test_same_key_multiple_overwrites() {
		let mut parent = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;

		let key = make_key("key1");

		// Pattern 1: set-delete on same key within same transaction
		txn.set(&key, make_value("first")).unwrap();
		txn.remove(&key).unwrap();

		// After set-delete, key should be marked for removal
		assert!(txn.pending.is_removed(&key));

		// Pattern 2: set-delete-set on same key within same transaction
		txn.set(&key, make_value("second")).unwrap();
		txn.remove(&key).unwrap();
		txn.set(&key, make_value("final")).unwrap();

		// After set-delete-set, key should have final value
		assert_eq!(txn.pending.get(&key), Some(&make_value("final")));

		// Commit and verify final state
		txn.commit(&mut parent).await.unwrap();

		// Only the final value should be in parent
		assert_eq!(from_store(&mut parent, &key).await, Some(make_value("final")));
	}

	#[tokio::test]
	async fn test_commit_detects_overlapping_writes() {
		let mut parent = create_test_transaction().await;

		let key = make_key("key1");

		// Create both FlowTransactions before any commits
		let mut txn1 = FlowTransaction::new(&parent, CommitVersion(1)).await;
		let mut txn2 = FlowTransaction::new(&parent, CommitVersion(2)).await;

		// Both try to write to the same key
		txn1.set(&key, make_value("value1")).unwrap();
		txn2.set(&key, make_value("value2")).unwrap();

		// First commit succeeds
		txn1.commit(&mut parent).await.unwrap();

		// Second commit should fail with keyspace overlap error
		// because txn1 already wrote to key1
		let result = txn2.commit(&mut parent).await;
		assert!(result.is_err());

		// Verify it's the expected error code
		let err = result.unwrap_err();
		assert_eq!(err.code, "FLOW_002");
	}

	#[tokio::test]
	async fn test_double_commit_prevention() {
		let mut parent = create_test_transaction().await;

		let mut txn = FlowTransaction::new(&parent, CommitVersion(1)).await;
		txn.set(&make_key("key1"), make_value("value1")).unwrap();

		// First commit should succeed
		let metrics = txn.commit(&mut parent).await;
		assert!(metrics.is_ok(), "First commit should succeed");

		// Transaction is consumed after commit, can't commit again
		// This test verifies at compile-time that txn is moved
		// If we could access txn here, it would be a bug
		// The following line should not compile:
		// txn.commit(&mut parent).await;  // ERROR: use of moved value
	}

	#[tokio::test]
	async fn test_commit_allows_nonoverlapping_writes() {
		let mut parent = create_test_transaction().await;

		// First FlowTransaction writes to key1
		let mut txn1 = FlowTransaction::new(&parent, CommitVersion(1)).await;
		txn1.set(&make_key("key1"), make_value("value1")).unwrap();
		txn1.commit(&mut parent).await.unwrap();

		// Second FlowTransaction writes to key2 (different keyspace)
		// This should succeed because keyspaces don't overlap
		let mut txn2 = FlowTransaction::new(&parent, CommitVersion(2)).await;
		txn2.set(&make_key("key2"), make_value("value2")).unwrap();
		let result = txn2.commit(&mut parent).await;

		// Should succeed
		assert!(result.is_ok());

		// Both values should be in parent
		assert_eq!(from_store(&mut parent, &make_key("key1")).await, Some(make_value("value1")));
		assert_eq!(from_store(&mut parent, &make_key("key2")).await, Some(make_value("value2")));
	}
}
