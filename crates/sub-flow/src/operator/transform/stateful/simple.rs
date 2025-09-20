// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, EncodedKeyRange, interface::Transaction, row::EncodedRow};
use reifydb_engine::StandardCommandTransaction;

use super::{super::TransformOperator, utils};

/// Simple Stateful operations - provides raw key-value access
/// This is the foundation for operators that need state management
pub trait SimpleStatefulOperator<T: Transaction>: TransformOperator<T> {
	/// Get raw bytes for a key
	fn state_get(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		key: &EncodedKey,
	) -> crate::Result<Option<EncodedRow>> {
		utils::state_get(self.id(), txn, key)
	}

	/// Set raw bytes for a key
	fn state_set(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		key: &EncodedKey,
		value: EncodedRow,
	) -> crate::Result<()> {
		utils::state_set(self.id(), txn, key, value)
	}

	/// Remove a key
	fn state_remove(&self, txn: &mut StandardCommandTransaction<T>, key: &EncodedKey) -> crate::Result<()> {
		utils::state_remove(self.id(), txn, key)
	}

	/// Scan all keys for this operator
	fn state_scan<'a>(
		&self,
		txn: &'a mut StandardCommandTransaction<T>,
	) -> crate::Result<super::StateIterator<'a>> {
		utils::state_scan(self.id(), txn)
	}

	/// Range query between keys
	fn state_range<'a>(
		&self,
		txn: &'a mut StandardCommandTransaction<T>,
		range: EncodedKeyRange,
	) -> crate::Result<super::StateIterator<'a>> {
		utils::state_range(self.id(), txn, range)
	}

	/// Clear all state for this operator
	fn state_clear(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<()> {
		utils::state_clear(self.id(), txn)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Included};

	use reifydb_core::{
		interface::{Engine, FlowNodeId},
		util::CowVec,
	};

	use super::*;
	use crate::operator::transform::stateful::utils_test::test::*;

	// Test implementation of SimpleStatefulOperator
	impl SimpleStatefulOperator<TestTransaction> for TestOperator {}

	#[test]
	fn test_simple_state_get_set() {
		let mut txn = create_test_transaction();
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

	#[test]
	fn test_simple_state_remove() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));
		let key = test_key("remove_test");
		let value = test_row();

		// Set, verify, remove, verify
		operator.state_set(&mut txn, &key, value).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_some());

		operator.state_remove(&mut txn, &key).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_simple_state_scan() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Add multiple entries
		let entries = vec![("key_a", vec![1, 2]), ("key_b", vec![3, 4]), ("key_c", vec![5, 6])];
		for (key_suffix, data) in &entries {
			let key = test_key(key_suffix);
			let value = EncodedRow(CowVec::new(data.clone()));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Scan and verify count
		let scanned: Vec<_> = operator.state_scan(&mut txn).unwrap().collect();
		assert_eq!(scanned.len(), 3);
	}

	#[test]
	fn test_simple_state_range() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(2));

		// Add ordered entries
		for i in 0..10 {
			let key = test_key(&format!("{:02}", i)); // Ensures lexical ordering
			let value = EncodedRow(CowVec::new(vec![i as u8]));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		let range = EncodedKeyRange::new(Included(test_key("02")), Excluded(test_key("05")));
		let range_result: Vec<_> = operator.state_range(&mut txn, range).unwrap().collect();

		// Should get keys 02, 03, 04 (not 05 as end is exclusive)
		assert_eq!(range_result.len(), 3);
		assert_eq!(range_result[0].1.as_ref()[0], 2);
		assert_eq!(range_result[1].1.as_ref()[0], 3);
		assert_eq!(range_result[2].1.as_ref()[0], 4);
	}

	#[test]
	fn test_simple_state_clear() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(3));

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("clear_{}", i));
			let value = EncodedRow(CowVec::new(vec![i as u8]));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Verify entries exist
		assert_eq!(operator.state_scan(&mut txn).unwrap().count(), 5);

		// Clear all
		operator.state_clear(&mut txn).unwrap();

		// Verify all cleared
		assert_eq!(operator.state_scan(&mut txn).unwrap().count(), 0);
	}

	#[test]
	fn test_operator_isolation() {
		let mut txn = create_test_transaction();
		let operator1 = TestOperator::simple(FlowNodeId(10));
		let operator2 = TestOperator::simple(FlowNodeId(20));
		let shared_key = test_key("shared");

		let value1 = EncodedRow(CowVec::new(vec![1]));
		let value2 = EncodedRow(CowVec::new(vec![2]));

		// Set different values for same key in different operators
		operator1.state_set(&mut txn, &shared_key, value1.clone()).unwrap();
		operator2.state_set(&mut txn, &shared_key, value2.clone()).unwrap();

		// Each operator should have its own value
		let result1 = operator1.state_get(&mut txn, &shared_key).unwrap().unwrap();
		let result2 = operator2.state_get(&mut txn, &shared_key).unwrap().unwrap();

		assert_row_eq(&result1, &value1);
		assert_row_eq(&result2, &value2);
	}

	#[test]
	fn test_empty_range() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(4));

		// Add some entries
		for i in 0..5 {
			let key = test_key(&format!("item_{}", i));
			let value = test_row();
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Query range that doesn't exist (after all "item_*" entries)
		let range = EncodedKeyRange::new(Included(test_key("z_aaa")), Excluded(test_key("z_zzz")));
		let range_result: Vec<_> = operator.state_range(&mut txn, range).unwrap().collect();

		assert_eq!(range_result.len(), 0);
	}

	#[test]
	fn test_overwrite_existing_key() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(5));
		let key = test_key("overwrite");

		let value1 = EncodedRow(CowVec::new(vec![1, 1, 1]));
		let value2 = EncodedRow(CowVec::new(vec![2, 2, 2]));

		// Set initial value
		operator.state_set(&mut txn, &key, value1).unwrap();

		// Overwrite with new value
		operator.state_set(&mut txn, &key, value2.clone()).unwrap();

		// Should have the new value
		let result = operator.state_get(&mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &value2);
	}

	#[test]
	fn test_remove_non_existent_key() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(6));
		let key = test_key("non_existent");

		// Remove non-existent key should not error
		operator.state_remove(&mut txn, &key).unwrap();

		// Should still be None
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_scan_after_partial_removal() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(7));

		// Add 5 entries
		for i in 0..5 {
			let key = test_key(&format!("partial_{}", i));
			let value = EncodedRow(CowVec::new(vec![i as u8]));
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Remove some entries
		operator.state_remove(&mut txn, &test_key("partial_1")).unwrap();
		operator.state_remove(&mut txn, &test_key("partial_3")).unwrap();

		// Should have 3 entries left (0, 2, 4)
		let remaining: Vec<_> = operator.state_scan(&mut txn).unwrap().collect();
		assert_eq!(remaining.len(), 3);
	}

	#[test]
	fn test_transaction_isolation() {
		let engine = create_test_engine();
		let operator = TestOperator::simple(FlowNodeId(8));
		let key = test_key("isolation");

		// Transaction 1: Write a value
		let mut txn1 = engine.begin_command().unwrap();
		let value1 = EncodedRow(CowVec::new(vec![1]));
		operator.state_set(&mut txn1, &key, value1.clone()).unwrap();

		// Transaction 2: Should not see uncommitted value
		let mut txn2 = engine.begin_command().unwrap();
		assert!(operator.state_get(&mut txn2, &key).unwrap().is_none());

		// Commit transaction 1
		txn1.commit().unwrap();

		// Transaction 3: Should now see the value
		let mut txn3 = engine.begin_command().unwrap();
		let result = operator.state_get(&mut txn3, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &value1);
	}
}
