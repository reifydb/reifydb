// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::{bytes::EncodedBytes, shape::RowShape},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{EncodableKey, operator_group_state::GroupStateKey, operator_state::OperatorStateKey},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;

use super::StateIterator;

pub fn state_get(id: OperatorId, txn: &mut FlowTransaction, key: &GroupStateKey) -> Result<Option<EncodedBytes>> {
	let encoded_key = OperatorStateKey::encoded(id, key.as_slice());

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

pub fn state_set(id: OperatorId, txn: &mut FlowTransaction, key: &GroupStateKey, value: EncodedBytes) -> Result<()> {
	let encoded_key = OperatorStateKey::encoded(id, key.as_slice());
	txn.set(&encoded_key, value)?;
	Ok(())
}

pub fn state_remove(id: OperatorId, txn: &mut FlowTransaction, key: &GroupStateKey) -> Result<()> {
	let encoded_key = OperatorStateKey::encoded(id, key.as_slice());
	txn.remove_silent(&encoded_key)?;
	Ok(())
}

pub fn state_scan_all(id: OperatorId, txn: &mut FlowTransaction) -> Result<Vec<(EncodedKey, EncodedBytes)>> {
	let range = OperatorStateKey::node_range(id);
	let stream = txn.range(range, RangeScope::All, 1024);
	let mut items = Vec::new();
	for result in stream {
		let multi = result?;
		if let Some(state_key) = OperatorStateKey::decode(&multi.key) {
			items.push((EncodedKey::new(state_key.key), multi.bytes));
		} else {
			items.push((multi.key, multi.bytes));
		}
	}
	Ok(items)
}

pub fn state_range<'a>(id: OperatorId, txn: &'a mut FlowTransaction, range: EncodedKeyRange) -> StateIterator<'a> {
	let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, vec![]));
	StateIterator::new(txn.range(prefixed_range, RangeScope::All, 1024))
}

pub fn state_clear(id: OperatorId, txn: &mut FlowTransaction) -> Result<()> {
	let range = OperatorStateKey::node_range(id);
	let keys_to_remove = {
		let stream = txn.range(range, RangeScope::All, 1024);
		let mut keys = Vec::new();
		for result in stream {
			let multi = result?;
			keys.push(multi.key);
		}
		keys
	};

	for key in keys_to_remove {
		txn.remove(&key)?;
	}
	Ok(())
}

pub fn load_or_create_row(
	id: OperatorId,
	txn: &mut FlowTransaction,
	key: &GroupStateKey,
	shape: &RowShape,
) -> Result<EncodedBytes> {
	match state_get(id, txn, key)? {
		Some(row) => Ok(row),
		None => Ok(shape.allocate().freeze()),
	}
}

pub fn save_row(id: OperatorId, txn: &mut FlowTransaction, key: &GroupStateKey, row: EncodedBytes) -> Result<()> {
	state_set(id, txn, key, row)
}

pub fn empty_state_key() -> GroupStateKey {
	GroupStateKey::from_framed(empty_key()).expect("the empty key is framing-valid")
}

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::{util::cowvec::CowVec, value::value_type::ValueType};

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	#[test]
	fn test_state_get_existing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("get");
		let value = test_bytes();

		state_set(operator_id, &mut txn, &key, value.clone()).unwrap();

		let result = state_get(operator_id, &mut txn, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &value);
	}

	#[test]
	fn test_state_get_non_existing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("nonexistent");

		let result = state_get(operator_id, &mut txn, &key).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_state_set_and_update() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("set");
		let value1 = EncodedBytes(CowVec::new(vec![1, 2, 3]));
		let value2 = EncodedBytes(CowVec::new(vec![4, 5, 6]));

		state_set(operator_id, &mut txn, &key, value1.clone()).unwrap();
		let result = state_get(operator_id, &mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &value1);

		state_set(operator_id, &mut txn, &key, value2.clone()).unwrap();
		let result = state_get(operator_id, &mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &value2);
	}

	#[test]
	fn test_state_remove() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("remove");
		let value = test_bytes();

		state_set(operator_id, &mut txn, &key, value.clone()).unwrap();
		assert!(state_get(operator_id, &mut txn, &key).unwrap().is_some());

		state_remove(operator_id, &mut txn, &key).unwrap();
		assert!(state_get(operator_id, &mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_state_scan_all() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);

		for i in 0..5 {
			let key = test_key(&format!("scan_{:02}", i)); // padded so the keys sort numerically
			let value = EncodedBytes(CowVec::new(vec![i as u8]));
			state_set(operator_id, &mut txn, &key, value).unwrap();
		}

		let entries: Vec<_> = state_scan_all(operator_id, &mut txn).unwrap();
		assert_eq!(entries.len(), 5);

		for i in 0..5 {
			assert_eq!(entries[i].1.as_slice()[0], i as u8);
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
			let value = test_bytes();
			state_set(operator_id, &mut txn, &key, value).unwrap();
		}

		let range = EncodedKeyRange::new(
			Included(test_key("b").into_encoded()),
			Excluded(test_key("d").into_encoded()),
		);
		let entries: Vec<_> = state_range(operator_id, &mut txn, range).collect::<Result<Vec<_>>>().unwrap();

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
			let value = test_bytes();
			state_set(operator_id, &mut txn, &key, value).unwrap();
		}

		let entries = {
			let range = EncodedKeyRange::new(Unbounded, Excluded(test_key("range_3").into_encoded()));
			let prefixed_range = range.with_prefix(OperatorStateKey::encoded(operator_id, vec![]));
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
			let prefixed_range = range.with_prefix(OperatorStateKey::encoded(operator_id, vec![]));
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
			let value = test_bytes();
			state_set(operator_id, &mut txn, &key, value).unwrap();
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

		state_clear(operator_id, &mut txn).unwrap();

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
	fn test_load_or_create_row_existing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("load_existing");
		let value = test_bytes();
		let layout = TestOperator::simple(operator_id).layout;

		state_set(operator_id, &mut txn, &key, value.clone()).unwrap();

		let result = load_or_create_row(operator_id, &mut txn, &key, &layout).unwrap();
		assert_row_eq(&result, &value);
	}

	#[test]
	fn test_load_or_create_row_new() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("load_new");
		let shape = RowShape::testing(&[ValueType::Int4]);

		let result = load_or_create_row(operator_id, &mut txn, &key, &shape).unwrap();
		assert!(result.len() > 0);
	}

	#[test]
	fn test_save_row() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("save");
		let value = test_bytes();

		save_row(operator_id, &mut txn, &key, value.clone()).unwrap();

		let result = state_get(operator_id, &mut txn, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &value);
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
		let value1 = EncodedBytes(CowVec::new(vec![1]));
		let value2 = EncodedBytes(CowVec::new(vec![2]));

		state_set(node1, &mut txn, &key, value1.clone()).unwrap();
		state_set(node2, &mut txn, &key, value2.clone()).unwrap();

		let result1 = state_get(node1, &mut txn, &key).unwrap().unwrap();
		let result2 = state_get(node2, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result1, &value1);
		assert_row_eq(&result2, &value2);

		state_clear(node1, &mut txn).unwrap();
		assert!(state_get(node1, &mut txn, &key).unwrap().is_none());
		assert!(state_get(node2, &mut txn, &key).unwrap().is_some());
	}

	#[test]
	fn test_large_values() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator_id = OperatorId(1);
		let key = test_key("large");

		let large_value = EncodedBytes(CowVec::new(vec![0xAB; 10240]));

		state_set(operator_id, &mut txn, &key, large_value.clone()).unwrap();
		let result = state_get(operator_id, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result, &large_value);
	}
}
