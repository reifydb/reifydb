// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::key::operator_state::GroupStateKey;
use reifydb_value::Result;

use super::iter::StateIterator;
use crate::operator::host::HostContext;

pub fn state_get(host: &mut dyn HostContext, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
	host.state_get(key)
}

pub fn state_set(host: &mut dyn HostContext, key: &GroupStateKey, row: EncodedOperatorRow) -> Result<()> {
	host.state_set(key, row)
}

pub fn state_remove(host: &mut dyn HostContext, key: &GroupStateKey) -> Result<()> {
	host.state_remove(key)
}

pub fn state_scan_all(host: &mut dyn HostContext) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	host.state_scan_all()
}

pub fn state_range<'a>(host: &'a mut dyn HostContext, range: EncodedKeyRange) -> StateIterator<'a> {
	host.state_range_iter(range)
}

pub fn state_clear(host: &mut dyn HostContext) -> Result<()> {
	host.state_clear()
}

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{OperatorStateKey, node_prefix},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::multi::RangeScope;

	use super::*;
	use crate::{
		operator::{host::TxnHostContext, state::test_utils::test::*},
		transaction::{FlowTransaction, deferred::DeferredTransaction, mock::FlowTxn},
	};

	fn host(txn: &mut DeferredTransaction, operator: OperatorId) -> TxnHostContext<'_, DeferredTransaction> {
		TxnHostContext::new(txn, operator)
	}

	#[test]
	fn test_state_get_existing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("get");
		let row = test_row();

		state_set(&mut host(&mut txn, operator_id), &key, row.clone()).unwrap();

		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &row);
	}

	#[test]
	fn test_state_get_non_existing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("nonexistent");

		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_state_set_and_update() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("set");
		let row1 = EncodedOperatorRow::timeless(&[1, 2, 3]);
		let row2 = EncodedOperatorRow::timeless(&[4, 5, 6]);

		state_set(&mut host(&mut txn, operator_id), &key, row1.clone()).unwrap();
		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap().unwrap();
		assert_row_eq(&result, &row1);

		state_set(&mut host(&mut txn, operator_id), &key, row2.clone()).unwrap();
		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap().unwrap();
		assert_row_eq(&result, &row2);
	}

	#[test]
	fn test_state_remove() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("remove");
		state_set(&mut host(&mut txn, operator_id), &key, test_row()).unwrap();
		assert!(state_get(&mut host(&mut txn, operator_id), &key).unwrap().is_some());

		state_remove(&mut host(&mut txn, operator_id), &key).unwrap();
		assert!(state_get(&mut host(&mut txn, operator_id), &key).unwrap().is_none());
	}

	#[test]
	fn test_state_scan_all() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);

		for i in 0..5 {
			let key = test_key(&format!("scan_{:02}", i)); // padded so the keys sort numerically
			state_set(&mut host(&mut txn, operator_id), &key, EncodedOperatorRow::timeless(&[i as u8]))
				.unwrap();
		}

		let entries: Vec<_> = state_scan_all(&mut host(&mut txn, operator_id)).unwrap();
		assert_eq!(entries.len(), 5);

		// The scan path is untyped, so the payload only surfaces once the row header is stripped.
		for i in 0..5 {
			let row = EncodedOperatorRow::try_from(entries[i].1.clone()).unwrap();
			assert_eq!(row.body()[0], i as u8);
		}
	}

	#[test]
	fn test_state_range() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);

		let keys = vec!["a", "b", "c", "d", "e"];
		for key_suffix in &keys {
			let key = test_key(key_suffix);
			state_set(&mut host(&mut txn, operator_id), &key, test_row()).unwrap();
		}

		let range = EncodedKeyRange::new(
			Included(test_key("b").into_encoded()),
			Excluded(test_key("d").into_encoded()),
		);
		let entries: Vec<_> =
			state_range(&mut host(&mut txn, operator_id), range).collect::<Result<Vec<_>>>().unwrap();

		// b and c, but not the excluded end d.
		assert_eq!(entries.len(), 2);
	}

	#[test]
	fn test_state_range_open_ended() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);

		for i in 0..5 {
			let key = test_key(&format!("range_{}", i));
			state_set(&mut host(&mut txn, operator_id), &key, test_row()).unwrap();
		}

		let entries = {
			let range = EncodedKeyRange::new(Unbounded, Excluded(test_key("range_3").into_encoded()));
			let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(operator_id)));
			let mut stream = txn.range(prefixed_range, RangeScope::All, 1024);
			let mut entries = Vec::new();
			while let Some(result) = stream.next() {
				entries.push(result.unwrap());
			}
			entries
		};
		assert_eq!(entries.len(), 3); // range_0, range_1, range_2

		let entries = {
			let range = EncodedKeyRange::new(Included(test_key("range_3").into_encoded()), Unbounded);
			let prefixed_range = range.with_prefix(EncodedKey::new(node_prefix(operator_id)));
			let mut stream = txn.range(prefixed_range, RangeScope::All, 1024);
			let mut entries = Vec::new();
			while let Some(result) = stream.next() {
				entries.push(result.unwrap());
			}
			entries
		};
		assert_eq!(entries.len(), 2); // range_3, range_4
	}

	#[test]
	fn test_state_clear() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);

		for i in 0..3 {
			let key = test_key(&format!("clear_{}", i));
			state_set(&mut host(&mut txn, operator_id), &key, test_row()).unwrap();
		}

		let count = {
			let range = OperatorStateKey::node_range(operator_id);
			let mut stream = txn.range(range, RangeScope::All, 1024);
			let mut count = 0;
			while let Some(result) = stream.next() {
				let _ = result.unwrap();
				count += 1;
			}
			count
		};
		assert_eq!(count, 3);

		state_clear(&mut host(&mut txn, operator_id)).unwrap();

		let count = {
			let range = OperatorStateKey::node_range(operator_id);
			let mut stream = txn.range(range, RangeScope::All, 1024);
			let mut count = 0;
			while let Some(result) = stream.next() {
				let _ = result.unwrap();
				count += 1;
			}
			count
		};
		assert_eq!(count, 0);
	}

	#[test]
	fn test_empty_key() {
		let key = empty_key();
		assert_eq!(key.len(), 0);
		assert!(key.as_ref().is_empty());
	}

	#[test]
	fn test_multiple_nodes_isolation() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node1 = OperatorId(1);
		let node2 = OperatorId(2);
		let key = test_key("shared");
		let row1 = EncodedOperatorRow::timeless(&[1]);
		let row2 = EncodedOperatorRow::timeless(&[2]);

		state_set(&mut host(&mut txn, node1), &key, row1.clone()).unwrap();
		state_set(&mut host(&mut txn, node2), &key, row2.clone()).unwrap();

		let result1 = state_get(&mut host(&mut txn, node1), &key).unwrap().unwrap();
		let result2 = state_get(&mut host(&mut txn, node2), &key).unwrap().unwrap();

		assert_row_eq(&result1, &row1);
		assert_row_eq(&result2, &row2);

		state_clear(&mut host(&mut txn, node1)).unwrap();
		assert!(state_get(&mut host(&mut txn, node1), &key).unwrap().is_none());
		assert!(state_get(&mut host(&mut txn, node2), &key).unwrap().is_some());
	}

	#[test]
	fn test_large_values() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("large");

		let large_row = EncodedOperatorRow::timeless(&[0xAB; 10240]);

		state_set(&mut host(&mut txn, operator_id), &key, large_row.clone()).unwrap();
		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap().unwrap();

		assert_row_eq(&result, &large_row);
	}

	#[test]
	fn test_simple_state_get_set() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("simple_test");
		let row = test_row();

		assert!(state_get(&mut host(&mut txn, operator_id), &key).unwrap().is_none());

		state_set(&mut host(&mut txn, operator_id), &key, row.clone()).unwrap();
		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &row);
	}

	#[test]
	fn test_simple_state_remove() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("remove_test");
		state_set(&mut host(&mut txn, operator_id), &key, test_row()).unwrap();
		assert!(state_get(&mut host(&mut txn, operator_id), &key).unwrap().is_some());

		state_remove(&mut host(&mut txn, operator_id), &key).unwrap();
		assert!(state_get(&mut host(&mut txn, operator_id), &key).unwrap().is_none());
	}

	#[test]
	fn test_simple_state_scan_all() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);

		let entries = vec![("key_a", vec![1, 2]), ("key_b", vec![3, 4]), ("key_c", vec![5, 6])];
		for (key_suffix, data) in &entries {
			let key = test_key(key_suffix);
			state_set(&mut host(&mut txn, operator_id), &key, EncodedOperatorRow::timeless(data)).unwrap();
		}

		let scanned: Vec<_> = state_scan_all(&mut host(&mut txn, operator_id)).unwrap();
		assert_eq!(scanned.len(), 3);
	}

	#[test]
	fn test_simple_state_range() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(2);

		for i in 0..10 {
			let key = test_key(&format!("{:02}", i)); // padded so the keys sort numerically
			state_set(&mut host(&mut txn, operator_id), &key, EncodedOperatorRow::timeless(&[i as u8]))
				.unwrap();
		}

		let range = EncodedKeyRange::new(
			Included(test_key("02").into_encoded()),
			Excluded(test_key("05").into_encoded()),
		);
		let range_result: Vec<_> =
			state_range(&mut host(&mut txn, operator_id), range).collect::<Result<Vec<_>>>().unwrap();

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
		let operator_id = OperatorId(3);

		for i in 0..5 {
			let key = test_key(&format!("clear_{}", i));
			state_set(&mut host(&mut txn, operator_id), &key, EncodedOperatorRow::timeless(&[i as u8]))
				.unwrap();
		}

		let count = state_scan_all(&mut host(&mut txn, operator_id)).unwrap().len();
		assert_eq!(count, 5);

		state_clear(&mut host(&mut txn, operator_id)).unwrap();

		let count = state_scan_all(&mut host(&mut txn, operator_id)).unwrap().len();
		assert_eq!(count, 0);
	}

	#[test]
	fn test_operator_isolation() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator1 = OperatorId(10);
		let operator2 = OperatorId(20);
		let shared_key = test_key("shared");

		let row1 = EncodedOperatorRow::timeless(&[1]);
		let row2 = EncodedOperatorRow::timeless(&[2]);

		state_set(&mut host(&mut txn, operator1), &shared_key, row1.clone()).unwrap();
		state_set(&mut host(&mut txn, operator2), &shared_key, row2.clone()).unwrap();

		let result1 = state_get(&mut host(&mut txn, operator1), &shared_key).unwrap().unwrap();
		let result2 = state_get(&mut host(&mut txn, operator2), &shared_key).unwrap().unwrap();

		assert_row_eq(&result1, &row1);
		assert_row_eq(&result2, &row2);
	}

	#[test]
	fn test_empty_range() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(4);

		for i in 0..5 {
			let key = test_key(&format!("item_{}", i));
			state_set(&mut host(&mut txn, operator_id), &key, test_row()).unwrap();
		}

		// A range that sorts entirely after the stored keys.
		let range = EncodedKeyRange::new(
			Included(test_key("z_aaa").into_encoded()),
			Excluded(test_key("z_zzz").into_encoded()),
		);
		let range_result: Vec<_> =
			state_range(&mut host(&mut txn, operator_id), range).collect::<Result<Vec<_>>>().unwrap();

		assert_eq!(range_result.len(), 0);
	}

	#[test]
	fn test_overwrite_existing_key() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(5);
		let key = test_key("overwrite");

		let row2 = EncodedOperatorRow::timeless(&[2, 2, 2]);

		state_set(&mut host(&mut txn, operator_id), &key, EncodedOperatorRow::timeless(&[1, 1, 1])).unwrap();
		state_set(&mut host(&mut txn, operator_id), &key, row2.clone()).unwrap();

		let result = state_get(&mut host(&mut txn, operator_id), &key).unwrap().unwrap();
		assert_row_eq(&result, &row2);
	}

	#[test]
	fn test_remove_non_existent_key() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(6);
		let key = test_key("non_existent");

		state_remove(&mut host(&mut txn, operator_id), &key).unwrap();

		assert!(state_get(&mut host(&mut txn, operator_id), &key).unwrap().is_none());
	}

	#[test]
	fn test_scan_after_partial_removal() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(7);

		for i in 0..5 {
			let key = test_key(&format!("partial_{}", i));
			state_set(&mut host(&mut txn, operator_id), &key, EncodedOperatorRow::timeless(&[i as u8]))
				.unwrap();
		}

		state_remove(&mut host(&mut txn, operator_id), &test_key("partial_1")).unwrap();
		state_remove(&mut host(&mut txn, operator_id), &test_key("partial_3")).unwrap();

		// 0, 2 and 4 survive.
		let remaining: Vec<_> = state_scan_all(&mut host(&mut txn, operator_id)).unwrap();
		assert_eq!(remaining.len(), 3);
	}
}
