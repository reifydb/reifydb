// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	row::operator::EncodedOperatorRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_value::Result;

use super::{StateIterator, utils};
use crate::flow::{Operator, transaction::FlowTransaction};

pub trait RawStatefulOperator: Operator {
	fn state_get(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		utils::state_get(self.id(), txn, key)
	}

	fn state_set(&self, txn: &mut FlowTransaction, key: &EncodedKey, value: EncodedOperatorRow) -> Result<()> {
		utils::state_set(self.id(), txn, key, value)
	}

	fn state_remove(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
		utils::state_remove(self.id(), txn, key)
	}

	fn state_drop(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
		utils::state_drop(self.id(), txn, key)
	}

	fn state_scan_all(&self, txn: &mut FlowTransaction) -> Result<Vec<(EncodedKey, EncodedOperatorRow)>> {
		utils::state_scan_all(self.id(), txn)
	}

	fn state_range<'a>(&self, txn: &'a mut FlowTransaction, range: EncodedKeyRange) -> StateIterator<'a> {
		utils::state_range(self.id(), txn, range)
	}

	fn state_clear(&self, txn: &mut FlowTransaction) -> Result<()> {
		utils::state_clear(self.id(), txn)
	}

	fn internal_state_get(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
		utils::internal_state_get(self.id(), txn, key)
	}

	fn internal_state_set(&self, txn: &mut FlowTransaction, key: &EncodedKey, value: EncodedOperatorRow) -> Result<()> {
		utils::internal_state_set(self.id(), txn, key, value)
	}

	fn internal_state_remove(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
		utils::internal_state_remove(self.id(), txn, key)
	}

	fn internal_state_drop(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
		utils::internal_state_drop(self.id(), txn, key)
	}
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Included};

	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_transaction::transaction::Transaction;
	

	use super::*;
	use crate::flow::operator::stateful::test_utils::test::*;

	impl RawStatefulOperator for TestOperator {}

	#[test]
	fn test_simple_state_get_set() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(1));
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
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(1));
		let key = test_key("remove_test");
		let value = test_row();

		// Set, verify, remove, verify
		operator.state_set(&mut txn, &key, value).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_some());

		operator.state_remove(&mut txn, &key).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_simple_state_scan_all() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(1));

		// Add multiple entries
		let entries = vec![("key_a", vec![1, 2]), ("key_b", vec![3, 4]), ("key_c", vec![5, 6])];
		for (key_suffix, data) in &entries {
			let key = test_key(key_suffix);
			let value = EncodedOperatorRow::timeless(&data);
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Scan and verify count
		let scanned: Vec<_> = operator.state_scan_all(&mut txn).unwrap();
		assert_eq!(scanned.len(), 3);
	}

	#[test]
	fn test_simple_state_range() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(2));

		// Add ordered entries
		for i in 0..10 {
			let key = test_key(&format!("{:02}", i)); // Ensures lexical ordering
			let value = EncodedOperatorRow::timeless(&[i as u8]);
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		let range = EncodedKeyRange::new(Included(test_key("02")), Excluded(test_key("05")));
		let range_result: Vec<_> = operator.state_range(&mut txn, range).collect::<Result<Vec<_>>>().unwrap();

		// Should get keys 02, 03, 04 (not 05 as end is exclusive)
		assert_eq!(range_result.len(), 3);
		assert_eq!(range_result[0].1.body()[0], 2);
		assert_eq!(range_result[1].1.body()[0], 3);
		assert_eq!(range_result[2].1.body()[0], 4);
	}

	#[test]
	fn test_simple_state_clear() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(3));

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("clear_{}", i));
			let value = EncodedOperatorRow::timeless(&[i as u8]);
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Verify entries exist
		let count = operator.state_scan_all(&mut txn).unwrap().len();
		assert_eq!(count, 5);

		// Clear all
		operator.state_clear(&mut txn).unwrap();

		// Verify all cleared
		let count = operator.state_scan_all(&mut txn).unwrap().len();
		assert_eq!(count, 0);
	}

	#[test]
	fn test_operator_isolation() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator1 = TestOperator::simple(OperatorId(10));
		let operator2 = TestOperator::simple(OperatorId(20));
		let shared_key = test_key("shared");

		let value1 = EncodedOperatorRow::timeless(&[1]);
		let value2 = EncodedOperatorRow::timeless(&[2]);

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
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(4));

		// Add some entries
		for i in 0..5 {
			let key = test_key(&format!("item_{}", i));
			let value = test_row();
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Query range that doesn't exist (after all "item_*" entries)
		let range = EncodedKeyRange::new(Included(test_key("z_aaa")), Excluded(test_key("z_zzz")));
		let range_result: Vec<_> = operator.state_range(&mut txn, range).collect::<Result<Vec<_>>>().unwrap();

		assert_eq!(range_result.len(), 0);
	}

	#[test]
	fn test_overwrite_existing_key() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(5));
		let key = test_key("overwrite");

		let value1 = EncodedOperatorRow::timeless(&[1, 1, 1]);
		let value2 = EncodedOperatorRow::timeless(&[2, 2, 2]);

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
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(6));
		let key = test_key("non_existent");

		// Remove non-existent key should not error
		operator.state_remove(&mut txn, &key).unwrap();

		// Should still be None
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_scan_after_partial_removal() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(7));

		// Add 5 entries
		for i in 0..5 {
			let key = test_key(&format!("partial_{}", i));
			let value = EncodedOperatorRow::timeless(&[i as u8]);
			operator.state_set(&mut txn, &key, value).unwrap();
		}

		// Remove some entries
		operator.state_remove(&mut txn, &test_key("partial_1")).unwrap();
		operator.state_remove(&mut txn, &test_key("partial_3")).unwrap();

		// Should have 3 entries left (0, 2, 4)
		let remaining: Vec<_> = operator.state_scan_all(&mut txn).unwrap();
		assert_eq!(remaining.len(), 3);
	}
}
