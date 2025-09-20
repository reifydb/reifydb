// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{
		FlowNodeId, Transaction, VersionedCommandTransaction, VersionedQueryTransaction,
		key::{EncodableKey, FlowNodeStateKey},
	},
	row::{EncodedRow, EncodedRowLayout},
};
use reifydb_engine::StandardCommandTransaction;

/// Helper functions for state operations that can be used by any stateful trait

/// Get raw bytes for a key
pub fn state_get<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
	key: &EncodedKey,
) -> crate::Result<Option<EncodedRow>> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(versioned) => Ok(Some(versioned.row)),
		None => Ok(None),
	}
}

/// Set raw bytes for a key
pub fn state_set<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
	key: &EncodedKey,
	value: EncodedRow,
) -> crate::Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

/// Remove a key
pub fn state_remove<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
	key: &EncodedKey,
) -> crate::Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

/// Scan all keys for this operator
pub fn state_scan<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
) -> crate::Result<super::StateIterator> {
	let range = FlowNodeStateKey::node_range(id);
	Ok(super::StateIterator {
		inner: txn.range(range)?,
	})
}

/// Range query between keys  
pub fn state_range<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
	range: EncodedKeyRange,
) -> crate::Result<super::StateIterator> {
	Ok(super::StateIterator {
		inner: txn.range(range.with_prefix(FlowNodeStateKey::new(id, vec![]).encode()))?,
	})
}

/// Clear all state for this operator
pub fn state_clear<T: Transaction>(id: FlowNodeId, txn: &mut StandardCommandTransaction<T>) -> crate::Result<()> {
	let range = FlowNodeStateKey::node_range(id);
	let keys_to_remove: Vec<_> = txn.range(range)?.map(|versioned| versioned.key).collect();

	for key in keys_to_remove {
		txn.remove(&key)?;
	}
	Ok(())
}

/// Load state for a key, creating if not exists
pub fn load_or_create_row<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
	key: &EncodedKey,
	layout: &EncodedRowLayout,
) -> crate::Result<EncodedRow> {
	match state_get(id, txn, key)? {
		Some(row) => Ok(row),
		None => Ok(layout.allocate_row()),
	}
}

/// Save state row
pub fn save_row<T: Transaction>(
	id: FlowNodeId,
	txn: &mut StandardCommandTransaction<T>,
	key: &EncodedKey,
	row: EncodedRow,
) -> crate::Result<()> {
	state_set(id, txn, key, row)
}

/// Create an empty key for single-state operators
pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
mod tests {
	use std::ops::{Bound, Bound::Unbounded};

	use Bound::{Excluded, Included};
	use reifydb_core::{interface::Engine, util::CowVec};
	use reifydb_type::Type;

	use super::*;
	use crate::operator::transform::stateful::utils_test::test::*;

	#[test]
	fn test_state_get_existing() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);
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
		let node_id = FlowNodeId(1);
		let key = test_key("nonexistent");

		let result = state_get(node_id, &mut txn, &key).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_state_set_and_update() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);
		let key = test_key("set");
		let value1 = EncodedRow(CowVec::new(vec![1, 2, 3]));
		let value2 = EncodedRow(CowVec::new(vec![4, 5, 6]));

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
		let node_id = FlowNodeId(1);
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
	fn test_state_scan() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("scan_{:02}", i)); // Use padding for proper ordering
			let value = EncodedRow(CowVec::new(vec![i as u8]));
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Scan all entries
		let entries: Vec<_> = state_scan(node_id, &mut txn).unwrap().collect();
		assert_eq!(entries.len(), 5);

		// Verify we got all the expected values
		for i in 0..5 {
			assert_eq!(entries[i].1.as_ref()[0], i as u8);
		}
	}

	#[test]
	fn test_state_range() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);

		// Add entries with different keys
		let keys = vec!["a", "b", "c", "d", "e"];
		for key_suffix in &keys {
			let key = test_key(key_suffix);
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Test range query from b to d (exclusive end)
		let range = EncodedKeyRange::new(Included(test_key("b")), Excluded(test_key("d")));
		let entries: Vec<_> = state_range(node_id, &mut txn, range).unwrap().collect();

		// Should include b and c, but not d (exclusive end)
		assert_eq!(entries.len(), 2);
	}

	#[test]
	fn test_state_range_open_ended() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);

		// Add some entries
		for i in 0..5 {
			let key = test_key(&format!("range_{}", i));
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		let range = EncodedKeyRange::new(Unbounded, Excluded(test_key("range_3")));
		let entries: Vec<_> = state_range(node_id, &mut txn, range).unwrap().collect();
		assert_eq!(entries.len(), 3); // range_0, range_1, range_2

		// Test with no end (to end)
		let range = EncodedKeyRange::new(Included(test_key("range_3")), Unbounded);
		let entries: Vec<_> = state_range(node_id, &mut txn, range).unwrap().collect();
		assert_eq!(entries.len(), 2); // range_3, range_4
	}

	#[test]
	fn test_state_clear() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);

		// Add multiple entries
		for i in 0..3 {
			let key = test_key(&format!("clear_{}", i));
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Verify entries exist
		let count = state_scan(node_id, &mut txn).unwrap().count();
		assert_eq!(count, 3);

		// Clear all state
		state_clear(node_id, &mut txn).unwrap();

		// Verify all entries are removed
		let count = state_scan(node_id, &mut txn).unwrap().count();
		assert_eq!(count, 0);
	}

	#[test]
	fn test_load_or_create_row_existing() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);
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
		let node_id = FlowNodeId(1);
		let key = test_key("load_new");
		let layout = EncodedRowLayout::new(&[Type::Int4]);

		// Load non-existing should create new
		let result = load_or_create_row(node_id, &mut txn, &key, &layout).unwrap();
		// Should create a row with the expected layout
		assert!(result.len() > 0);
	}

	#[test]
	fn test_save_row() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);
		let key = test_key("save");
		let value = test_row();

		// Save row
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
		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let key = test_key("shared");
		let value1 = EncodedRow(CowVec::new(vec![1]));
		let value2 = EncodedRow(CowVec::new(vec![2]));

		// Set different values for same key in different nodes
		state_set(node1, &mut txn, &key, value1.clone()).unwrap();
		state_set(node2, &mut txn, &key, value2.clone()).unwrap();

		// Each node should have its own value
		let result1 = state_get(node1, &mut txn, &key).unwrap().unwrap();
		let result2 = state_get(node2, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result1, &value1);
		assert_row_eq(&result2, &value2);

		// Clearing one node shouldn't affect the other
		state_clear(node1, &mut txn).unwrap();
		assert!(state_get(node1, &mut txn, &key).unwrap().is_none());
		assert!(state_get(node2, &mut txn, &key).unwrap().is_some());
	}

	#[test]
	fn test_large_values() {
		let mut txn = create_test_transaction();
		let node_id = FlowNodeId(1);
		let key = test_key("large");

		// Create a large value (10KB)
		let large_value = EncodedRow(CowVec::new(vec![0xAB; 10240]));

		// Store and retrieve
		state_set(node_id, &mut txn, &key, large_value.clone()).unwrap();
		let result = state_get(node_id, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result, &large_value);
	}

	#[test]
	fn test_concurrent_modifications() {
		let engine = create_test_engine();
		let node_id = FlowNodeId(1);
		let key = test_key("concurrent");

		// Transaction 1: Set initial value
		let mut txn1 = engine.begin_command().unwrap();
		let value1 = EncodedRow(CowVec::new(vec![1]));
		state_set(node_id, &mut txn1, &key, value1.clone()).unwrap();
		txn1.commit().unwrap();

		// Transaction 2: Update value
		let mut txn2 = engine.begin_command().unwrap();
		let value2 = EncodedRow(CowVec::new(vec![2]));
		state_set(node_id, &mut txn2, &key, value2.clone()).unwrap();
		txn2.commit().unwrap();

		// Transaction 3: Verify final value
		let mut txn3 = engine.begin_command().unwrap();
		let result = state_get(node_id, &mut txn3, &key).unwrap().unwrap();
		assert_row_eq(&result, &value2);
	}
}
