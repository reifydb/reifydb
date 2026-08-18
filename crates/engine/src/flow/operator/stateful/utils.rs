// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	row::{operator::EncodedOperatorRow, shape::{RowFamily, RowShape, RowShapeField}},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{EncodableKey, operator_state::{GroupId, Keyspace, OperatorStateKey}},
};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{Result, error::Error as ValueError};

use super::{StateIterator, StateIteratorVersioned};
use crate::flow::transaction::FlowTransaction;

pub fn state_get(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::CUSTOM, key.as_ref());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

pub fn state_set(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey, value: EncodedOperatorRow) -> Result<()> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::CUSTOM, key.as_ref());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

pub fn state_remove(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::CUSTOM, key.as_ref());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

pub fn state_drop(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::CUSTOM, key.as_ref());
	let encoded_key = state_key.encode();
	txn.remove_silent(&encoded_key)?;
	Ok(())
}

pub fn internal_state_get(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<EncodedOperatorRow>> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::ENGINE_META, key.as_ref());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

pub fn internal_state_set(
	id: OperatorId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	value: EncodedOperatorRow,
) -> Result<()> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::ENGINE_META, key.as_ref());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

pub fn internal_state_remove(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::ENGINE_META, key.as_ref());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

pub fn internal_state_drop(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = OperatorStateKey::new(id, GroupId::ROOT, Keyspace::ENGINE_META, key.as_ref());
	let encoded_key = state_key.encode();
	txn.remove_silent(&encoded_key)?;
	Ok(())
}

pub fn state_scan_all(id: OperatorId, txn: &mut FlowTransaction) -> Result<Vec<(EncodedKey, EncodedOperatorRow)>> {
	let range = OperatorStateKey::node_range(id);
	let stream = txn.range(range, RangeScope::All, 1024);
	let mut items = Vec::new();
	for result in stream {
		let multi = result?;
		let key = match OperatorStateKey::decode(&multi.key) {
			Some(state_key) => EncodedKey::new(state_key.suffix),
			None => multi.key,
		};
		items.push((key, EncodedOperatorRow::try_from(multi.bytes).map_err(ValueError::from)?));
	}
	Ok(items)
}

pub fn state_range<'a>(id: OperatorId, txn: &'a mut FlowTransaction, range: EncodedKeyRange) -> StateIterator<'a> {
	let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::CUSTOM, []));
	StateIterator::new(txn.range(prefixed_range, RangeScope::All, 1024))
}

pub fn internal_state_range<'a>(
	id: OperatorId,
	txn: &'a mut FlowTransaction,
	range: EncodedKeyRange,
) -> StateIterator<'a> {
	let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::ENGINE_META, []));
	StateIterator::new(txn.range(prefixed_range, RangeScope::All, 1024))
}

pub fn state_range_versioned<'a>(
	id: OperatorId,
	txn: &'a mut FlowTransaction,
	range: EncodedKeyRange,
) -> StateIteratorVersioned<'a> {
	let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::CUSTOM, []));
	StateIteratorVersioned::new(txn.range(prefixed_range, RangeScope::All, 1024))
}

pub fn internal_state_range_versioned<'a>(
	id: OperatorId,
	txn: &'a mut FlowTransaction,
	range: EncodedKeyRange,
) -> StateIteratorVersioned<'a> {
	let prefixed_range = range.with_prefix(OperatorStateKey::encoded(id, GroupId::ROOT, Keyspace::ENGINE_META, []));
	StateIteratorVersioned::new(txn.range(prefixed_range, RangeScope::All, 1024))
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
	key: &EncodedKey,
	shape: &RowShape,
) -> Result<EncodedOperatorRow> {
	match state_get(id, txn, key)? {
		Some(row) => Ok(row),
		None => Ok(shape.allocate_operator().freeze()),
	}
}

pub fn save_row(id: OperatorId, txn: &mut FlowTransaction, key: &EncodedKey, row: EncodedOperatorRow) -> Result<()> {
	state_set(id, txn, key, row)
}

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::common::CommitVersion;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{util::cowvec::CowVec, value::value_type::ValueType};

	use super::*;
	use crate::flow::{operator::stateful::test_utils::test::*, transaction::FlowTransaction};

	#[test]
	fn test_state_get_existing() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("get");
		let value = test_row();

		// Set a value first
		state_set(node_id, &mut txn, &key, value.clone()).unwrap();

		// Get should return the value
		let result = state_get(node_id, &mut txn, &key).unwrap();
		assert!(result.is_some());
		assert_row_eq(&result.unwrap(), &value);
	}

	#[test]
	fn test_state_get_non_existing() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("nonexistent");

		let result = state_get(node_id, &mut txn, &key).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_state_set_and_update() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("set");
		let value1 = EncodedOperatorRow(CowVec::new(vec![1, 2, 3]));
		let value2 = EncodedOperatorRow(CowVec::new(vec![4, 5, 6]));

		// Set initial value
		state_set(node_id, &mut txn, &key, value1.clone()).unwrap();
		let result = state_get(node_id, &mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &value1);

		// Update value
		state_set(node_id, &mut txn, &key, value2.clone()).unwrap();
		let result = state_get(node_id, &mut txn, &key).unwrap().unwrap();
		assert_row_eq(&result, &value2);
	}

	#[test]
	fn test_state_remove() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("remove");
		let value = test_row();

		// Set and verify
		state_set(node_id, &mut txn, &key, value.clone()).unwrap();
		assert!(state_get(node_id, &mut txn, &key).unwrap().is_some());

		// Remove and verify
		state_remove(node_id, &mut txn, &key).unwrap();
		assert!(state_get(node_id, &mut txn, &key).unwrap().is_none());
	}

	#[test]
	fn test_state_scan_all() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("scan_{:02}", i)); // Use padding for proper ordering
			let value = EncodedOperatorRow(CowVec::new(vec![i as u8]));
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Scan all entries
		let entries: Vec<_> = state_scan_all(node_id, &mut txn).unwrap();
		assert_eq!(entries.len(), 5);

		// Verify we got all the expected values
		for i in 0..5 {
			assert_eq!(entries[i].1.as_slice()[0], i as u8);
		}
	}

	#[test]
	fn test_state_range() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);

		// Add entries with different keys
		let keys = vec!["a", "b", "c", "d", "e"];
		for key_suffix in &keys {
			let key = test_key(key_suffix);
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Test range query from b to d (exclusive end)
		let range = EncodedKeyRange::new(Included(test_key("b")), Excluded(test_key("d")));
		let entries: Vec<_> = state_range(node_id, &mut txn, range).collect::<Result<Vec<_>>>().unwrap();

		// Should include b and c, but not d (exclusive end)
		assert_eq!(entries.len(), 2);
	}

	#[test]
	fn test_state_range_open_ended() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);

		// Add some entries
		for i in 0..5 {
			let key = test_key(&format!("range_{}", i));
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		let entries = {
			let range = EncodedKeyRange::new(Unbounded, Excluded(test_key("range_3")));
			let prefixed_range = range.with_prefix(OperatorStateKey::encoded(node_id, GroupId::ROOT, Keyspace::CUSTOM, []));
			let mut stream = txn.range(prefixed_range, RangeScope::All, 1024);
			let mut entries = Vec::new();
			while let Some(result) = stream.next() {
				entries.push(result.unwrap());
			}
			entries
		};
		assert_eq!(entries.len(), 3); // range_0, range_1, range_2

		// Test with no end (to end)
		let entries = {
			let range = EncodedKeyRange::new(Included(test_key("range_3")), Unbounded);
			let prefixed_range = range.with_prefix(OperatorStateKey::encoded(node_id, GroupId::ROOT, Keyspace::CUSTOM, []));
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
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);

		// Add multiple entries
		for i in 0..3 {
			let key = test_key(&format!("clear_{}", i));
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Verify entries exist
		let count = {
			let range = OperatorStateKey::node_range(node_id);
			let mut stream = txn.range(range, RangeScope::All, 1024);
			let mut count = 0;
			while let Some(result) = stream.next() {
				let _ = result.unwrap();
				count += 1;
			}
			count
		};
		assert_eq!(count, 3);

		// Clear all state
		state_clear(node_id, &mut txn).unwrap();

		// Verify all entries are removed
		let count = {
			let range = OperatorStateKey::node_range(node_id);
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
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("load_existing");
		let value = test_row();
		let layout = TestOperator::simple(node_id).layout;

		// Set existing value
		state_set(node_id, &mut txn, &key, value.clone()).unwrap();

		// Load should return existing
		let result = load_or_create_row(node_id, &mut txn, &key, &layout).unwrap();
		assert_row_eq(&result, &value);
	}

	#[test]
	fn test_load_or_create_row_new() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("load_new");
		let shape = RowShape::testing(&[ValueType::Int4]);

		// Load non-existing should create new
		let result = load_or_create_row(node_id, &mut txn, &key, &shape).unwrap();
		// Should create a encoded with the expected layout
		assert!(result.len() > 0);
	}

	#[test]
	fn test_save_row() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("save");
		let value = test_row();

		// Save encoded
		save_row(node_id, &mut txn, &key, value.clone()).unwrap();

		// Verify saved
		let result = state_get(node_id, &mut txn, &key).unwrap();
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
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node1 = OperatorId(1);
		let node2 = OperatorId(2);
		let key = test_key("shared");
		let value1 = EncodedOperatorRow::timeless(&[1]);
		let value2 = EncodedOperatorRow(CowVec::new(vec![2]));

		// Set different values for same key in different nodes
		state_set(node1, &mut txn, &key, value1.clone()).unwrap();
		state_set(node2, &mut txn, &key, value2.clone()).unwrap();

		// Each operator should have its own value
		let result1 = state_get(node1, &mut txn, &key).unwrap().unwrap();
		let result2 = state_get(node2, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result1, &value1);
		assert_row_eq(&result2, &value2);

		// Clearing one operator shouldn't affect the other
		state_clear(node1, &mut txn).unwrap();
		assert!(state_get(node1, &mut txn, &key).unwrap().is_none());
		assert!(state_get(node2, &mut txn, &key).unwrap().is_some());
	}

	#[test]
	fn test_large_values() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = OperatorId(1);
		let key = test_key("large");

		// Create a large value (10KB)
		let large_value = EncodedOperatorRow(CowVec::new(vec![0xAB; 10240]));

		// Store and retrieve
		state_set(node_id, &mut txn, &key, large_value.clone()).unwrap();
		let result = state_get(node_id, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result, &large_value);
	}
}
