// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, EncodedKeyRange, value::encoded::EncodedValues};

use super::utils;
use crate::{operator::transform::TransformOperator, transaction::FlowTransaction};

/// Raw Stateful operations - provides raw key-value access
/// This is the foundation for operators that need state management
pub trait RawStatefulOperator: TransformOperator {
	/// Get raw bytes for a key
	fn state_get(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> crate::Result<Option<EncodedValues>> {
		utils::state_get(self.id(), txn, key)
	}

	/// Set raw bytes for a key
	fn state_set(&self, txn: &mut FlowTransaction, key: &EncodedKey, value: EncodedValues) -> crate::Result<()> {
		utils::state_set(self.id(), txn, key, value)
	}

	/// Remove a key
	fn state_remove(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> crate::Result<()> {
		utils::state_remove(self.id(), txn, key)
	}

	/// Scan all keys for this operator
	async fn state_scan(&self, txn: &mut FlowTransaction) -> crate::Result<super::StateIterator> {
		utils::state_scan(self.id(), txn).await
	}

	/// Range query between keys
	async fn state_range(
		&self,
		txn: &mut FlowTransaction,
		range: EncodedKeyRange,
	) -> crate::Result<super::StateIterator> {
		utils::state_range(self.id(), txn, range).await
	}

	/// Clear all state for this operator
	async fn state_clear(&self, txn: &mut FlowTransaction) -> crate::Result<()> {
		utils::state_clear(self.id(), txn).await
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Included};

	use reifydb_core::{
		CommitVersion,
		interface::{Engine, FlowNodeId},
		key::FlowNodeStateKey,
		util::CowVec,
	};

	use super::*;
	use crate::{Operator, operator::stateful::test_utils::test::*, transaction::FlowTransaction};

	// Test implementation of SimpleStatefulOperator
	impl RawStatefulOperator for TestOperator {}

	#[tokio::test]
	async fn test_simple_state_get_set() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(1));
		let key = test_key("simple_test");
		let value = test_row();

		// Initially should be None
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());

		// Set and verify
		operator.state_set(&mut txn, &key, value.clone()).unwrap();
		let result = operator.state_get(&mut txn, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &value);
	}

	#[tokio::test]
	async fn test_simple_state_remove() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(1));
		let key = test_key("remove_test");
		let value = test_row();

		// Set, verify, remove, verify
		operator.state_set(&mut txn, &key, value).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_some());

		operator.state_remove(&mut txn, &key).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[tokio::test]
	async fn test_simple_state_scan() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Add multiple entries
		let entries = vec![("key_a", vec![1, 2]), ("key_b", vec![3, 4]), ("key_c", vec![5, 6])];
		for (key_suffix, data) in &entries {
			let key = test_key(key_suffix);
			let value = EncodedValues(CowVec::new(data.clone()));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Scan and verify count
		let scanned: Vec<_> = operator.state_scan(&mut txn).await.unwrap().collect();
		assert_eq!(scanned.len(), 3);
	}

	#[tokio::test]
	async fn test_simple_state_range() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(2));

		// Add ordered entries
		for i in 0..10 {
			let key = test_key(&format!("{:02}", i)); // Ensures lexical ordering
			let value = EncodedValues(CowVec::new(vec![i as u8]));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		let range = EncodedKeyRange::new(Included(test_key("02")), Excluded(test_key("05")));
		let range_result: Vec<_> = operator.state_range(&mut txn, range).await.unwrap().collect();

		// Should get keys 02, 03, 04 (not 05 as end is exclusive)
		assert_eq!(range_result.len(), 3);
		assert_eq!(range_result[0].1.as_ref()[0], 2);
		assert_eq!(range_result[1].1.as_ref()[0], 3);
		assert_eq!(range_result[2].1.as_ref()[0], 4);
	}

	#[tokio::test]
	async fn test_simple_state_clear() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(3));

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("clear_{}", i));
			let value = EncodedValues(CowVec::new(vec![i as u8]));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Verify entries exist
		let count = {
			let range = FlowNodeStateKey::node_range(operator.id());
			txn.range(range).await.unwrap().items.into_iter().count()
		};
		assert_eq!(count, 5);

		// Clear all
		operator.state_clear(&mut txn).await.unwrap();

		// Verify all cleared
		let count = {
			let range = FlowNodeStateKey::node_range(operator.id());
			txn.range(range).await.unwrap().items.into_iter().count()
		};
		assert_eq!(count, 0);
	}

	#[tokio::test]
	async fn test_operator_isolation() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator1 = TestOperator::simple(FlowNodeId(10));
		let operator2 = TestOperator::simple(FlowNodeId(20));
		let shared_key = test_key("shared");

		let value1 = EncodedValues(CowVec::new(vec![1]));
		let value2 = EncodedValues(CowVec::new(vec![2]));

		// Set different values for same key in different operators
		operator1.state_set(&mut txn, &shared_key, value1.clone()).unwrap();
		operator2.state_set(&mut txn, &shared_key, value2.clone()).unwrap();

		// Each operator should have its own value
		let result1 = operator1.state_get(&mut txn, &shared_key).unwrap().unwrap();
		let result2 = operator2.state_get(&mut txn, &shared_key).unwrap().unwrap();

		assert_row_eq(&result1, &value1);
		assert_row_eq(&result2, &value2);
	}

	#[tokio::test]
	async fn test_empty_range() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(4));

		// Add some entries
		for i in 0..5 {
			let key = test_key(&format!("item_{}", i));
			let value = test_row();
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Query range that doesn't exist (after all "item_*" entries)
		let range = EncodedKeyRange::new(Included(test_key("z_aaa")), Excluded(test_key("z_zzz")));
		let range_result: Vec<_> = operator.state_range(&mut txn, range).await.unwrap().collect();

		assert_eq!(range_result.len(), 0);
	}

	#[tokio::test]
	async fn test_overwrite_existing_key() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(5));
		let key = test_key("overwrite");

		let value1 = EncodedValues(CowVec::new(vec![1, 1, 1]));
		let value2 = EncodedValues(CowVec::new(vec![2, 2, 2]));

		// Set initial value
		operator.state_set(&mut txn, &key, value1).unwrap();

		// Overwrite with new value
		operator.state_set(&mut txn, &key, value2.clone()).unwrap();

		// Should have the new value
		let result = operator.state_get(&mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &value2);
	}

	#[tokio::test]
	async fn test_remove_non_existent_key() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(6));
		let key = test_key("non_existent");

		// Remove non-existent key should not error
		operator.state_remove(&mut txn, &key).unwrap();

		// Should still be None
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[tokio::test]
	async fn test_scan_after_partial_removal() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(7));

		// Add 5 entries
		for i in 0..5 {
			let key = test_key(&format!("partial_{}", i));
			let value = EncodedValues(CowVec::new(vec![i as u8]));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Remove some entries
		operator.state_remove(&mut txn, &test_key("partial_1")).unwrap();
		operator.state_remove(&mut txn, &test_key("partial_3")).unwrap();

		// Should have 3 entries left (0, 2, 4)
		let remaining: Vec<_> = operator.state_scan(&mut txn).await.unwrap().collect();
		assert_eq!(remaining.len(), 3);
	}

	#[tokio::test]
	async fn test_transaction_isolation() {
		let engine = create_test_engine().await;
		let operator = TestOperator::simple(FlowNodeId(8));
		let key = test_key("isolation");

		// Transaction 1: Write a value
		let mut parent_txn1 = engine.begin_command().unwrap();
		let mut flow_txn1 = FlowTransaction::new(&parent_txn1, CommitVersion(1)).await;
		let value1 = EncodedValues(CowVec::new(vec![1]));
		operator.state_set(&mut flow_txn1, &key, value1.clone()).unwrap();

		// Transaction 2: Should not see uncommitted value
		let parent_txn2 = engine.begin_command().unwrap();
		let mut flow_txn2 = FlowTransaction::new(&parent_txn2, CommitVersion(2)).await;
		assert!(operator.state_get(&mut flow_txn2, &key).unwrap().is_none());

		// Commit transaction 1
		flow_txn1.commit(&mut parent_txn1).await.unwrap();
		parent_txn1.commit().unwrap();

		// Transaction 3: Should now see the value
		let parent_txn3 = engine.begin_command().unwrap();
		let mut flow_txn3 = FlowTransaction::new(&parent_txn3, CommitVersion(3)).await;
		let result = operator.state_get(&mut flow_txn3, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &value1);
	}
}
