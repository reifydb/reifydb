// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::key::operator_group_state::GroupStateKey;
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_value::Result;

use super::{StateIterator, utils};

pub trait RawStatefulOperator: Operator {
	fn state_get(&self, txn: &mut FlowTransaction, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
		utils::state_get(self.id(), txn, key)
	}

	fn state_set(&self, txn: &mut FlowTransaction, key: &GroupStateKey, row: EncodedOperatorRow) -> Result<()> {
		utils::state_set(self.id(), txn, key, row)
	}

	fn state_remove(&self, txn: &mut FlowTransaction, key: &GroupStateKey) -> Result<()> {
		utils::state_remove(self.id(), txn, key)
	}

	fn state_scan_all(&self, txn: &mut FlowTransaction) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
		utils::state_scan_all(self.id(), txn)
	}

	fn state_range<'a>(&self, txn: &'a mut FlowTransaction, range: EncodedKeyRange) -> StateIterator<'a> {
		utils::state_range(self.id(), txn, range)
	}

	fn state_clear(&self, txn: &mut FlowTransaction) -> Result<()> {
		utils::state_clear(self.id(), txn)
	}
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Included};

	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	impl RawStatefulOperator for TestOperator {}

	#[test]
	fn test_simple_state_get_set() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));
		let key = test_key("simple_test");
		let row = test_row();

		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());

		operator.state_set(&mut txn, &key, row.clone()).unwrap();
		let result = operator.state_get(&mut txn, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &row);
	}

	#[test]
	fn test_simple_state_remove() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));
		let key = test_key("remove_test");
		operator.state_set(&mut txn, &key, test_row()).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_some());

		operator.state_remove(&mut txn, &key).unwrap();
		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_simple_state_scan_all() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let entries = vec![("key_a", vec![1, 2]), ("key_b", vec![3, 4]), ("key_c", vec![5, 6])];
		for (key_suffix, data) in &entries {
			let key = test_key(key_suffix);
			operator.state_set(&mut txn, &key, EncodedOperatorRow::timeless(data)).unwrap();
		}

		let scanned: Vec<_> = operator.state_scan_all(&mut txn).unwrap();
		assert_eq!(scanned.len(), 3);
	}

	#[test]
	fn test_simple_state_range() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(2));

		for i in 0..10 {
			let key = test_key(&format!("{:02}", i)); // padded so the keys sort numerically
			operator.state_set(&mut txn, &key, EncodedOperatorRow::timeless(&[i as u8])).unwrap();
		}

		let range = EncodedKeyRange::new(
			Included(test_key("02").into_encoded()),
			Excluded(test_key("05").into_encoded()),
		);
		let range_result: Vec<_> = operator.state_range(&mut txn, range).collect::<Result<Vec<_>>>().unwrap();

		// 02, 03, 04 - the end bound is exclusive.
		assert_eq!(range_result.len(), 3);
		// The range path is untyped, so the payload only surfaces once the row header is stripped.
		for (offset, expected) in [(0usize, 2u8), (1, 3), (2, 4)] {
			let row = EncodedOperatorRow::try_from(range_result[offset].1.clone()).unwrap();
			assert_eq!(row.body()[0], expected);
		}
	}

	#[test]
	fn test_simple_state_clear() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(3));

		for i in 0..5 {
			let key = test_key(&format!("clear_{}", i));
			operator.state_set(&mut txn, &key, EncodedOperatorRow::timeless(&[i as u8])).unwrap();
		}

		let count = operator.state_scan_all(&mut txn).unwrap().len();
		assert_eq!(count, 5);

		operator.state_clear(&mut txn).unwrap();

		let count = operator.state_scan_all(&mut txn).unwrap().len();
		assert_eq!(count, 0);
	}

	#[test]
	fn test_operator_isolation() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator1 = TestOperator::simple(OperatorId(10));
		let operator2 = TestOperator::simple(OperatorId(20));
		let shared_key = test_key("shared");

		let row1 = EncodedOperatorRow::timeless(&[1]);
		let row2 = EncodedOperatorRow::timeless(&[2]);

		operator1.state_set(&mut txn, &shared_key, row1.clone()).unwrap();
		operator2.state_set(&mut txn, &shared_key, row2.clone()).unwrap();

		let result1 = operator1.state_get(&mut txn, &shared_key).unwrap().unwrap();
		let result2 = operator2.state_get(&mut txn, &shared_key).unwrap().unwrap();

		assert_row_eq(&result1, &row1);
		assert_row_eq(&result2, &row2);
	}

	#[test]
	fn test_empty_range() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(4));

		for i in 0..5 {
			let key = test_key(&format!("item_{}", i));
			operator.state_set(&mut txn, &key, test_row()).unwrap();
		}

		// A range that sorts entirely after the stored keys.
		let range = EncodedKeyRange::new(
			Included(test_key("z_aaa").into_encoded()),
			Excluded(test_key("z_zzz").into_encoded()),
		);
		let range_result: Vec<_> = operator.state_range(&mut txn, range).collect::<Result<Vec<_>>>().unwrap();

		assert_eq!(range_result.len(), 0);
	}

	#[test]
	fn test_overwrite_existing_key() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(5));
		let key = test_key("overwrite");

		let row2 = EncodedOperatorRow::timeless(&[2, 2, 2]);

		operator.state_set(&mut txn, &key, EncodedOperatorRow::timeless(&[1, 1, 1])).unwrap();
		operator.state_set(&mut txn, &key, row2.clone()).unwrap();

		let result = operator.state_get(&mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &row2);
	}

	#[test]
	fn test_remove_non_existent_key() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(6));
		let key = test_key("non_existent");

		operator.state_remove(&mut txn, &key).unwrap();

		assert!(operator.state_get(&mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_scan_after_partial_removal() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(7));

		for i in 0..5 {
			let key = test_key(&format!("partial_{}", i));
			operator.state_set(&mut txn, &key, EncodedOperatorRow::timeless(&[i as u8])).unwrap();
		}

		operator.state_remove(&mut txn, &test_key("partial_1")).unwrap();
		operator.state_remove(&mut txn, &test_key("partial_3")).unwrap();

		// 0, 2 and 4 survive.
		let remaining: Vec<_> = operator.state_scan_all(&mut txn).unwrap();
		assert_eq!(remaining.len(), 3);
	}
}
