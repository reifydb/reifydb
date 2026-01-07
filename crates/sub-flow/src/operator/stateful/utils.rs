// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::FlowNodeId,
	key::{EncodableKey, FlowNodeInternalStateKey, FlowNodeStateKey},
	value::encoded::{EncodedValues, EncodedValuesLayout},
};

use crate::transaction::FlowTransaction;

/// Helper functions for state operations that can be used by any stateful trait

/// Get raw bytes for a key
pub fn state_get(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
) -> reifydb_type::Result<Option<EncodedValues>> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

/// Set raw bytes for a key
pub fn state_set(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	value: EncodedValues,
) -> reifydb_type::Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

/// Remove a key
pub fn state_remove(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> reifydb_type::Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

/// Get raw bytes for a key from internal state (not subject to retention policies)
pub fn internal_state_get(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
) -> reifydb_type::Result<Option<EncodedValues>> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

/// Set raw bytes for a key in internal state (not subject to retention policies)
pub fn internal_state_set(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	value: EncodedValues,
) -> reifydb_type::Result<()> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

/// Remove a key from internal state
pub fn internal_state_remove(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> reifydb_type::Result<()> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

/// Scan all keys for this operator
pub fn state_scan(id: FlowNodeId, txn: &mut FlowTransaction) -> reifydb_type::Result<super::StateIterator> {
	let range = FlowNodeStateKey::node_range(id);
	let mut stream = txn.range(range, 1024);
	let mut items = Vec::new();
	while let Some(result) = stream.next() {
		let multi = result?;
		if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
			items.push((EncodedKey::new(state_key.key), multi.values));
		} else {
			items.push((multi.key, multi.values));
		}
	}
	Ok(super::StateIterator::from_items(items))
}

/// Range query between keys
pub fn state_range(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	range: EncodedKeyRange,
) -> reifydb_type::Result<super::StateIterator> {
	let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(id, vec![]));
	let mut stream = txn.range(prefixed_range, 1024);
	let mut items = Vec::new();
	while let Some(result) = stream.next() {
		let multi = result?;
		if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
			items.push((EncodedKey::new(state_key.key), multi.values));
		} else {
			items.push((multi.key, multi.values));
		}
	}
	Ok(super::StateIterator::from_items(items))
}

/// Clear all state for this operator
pub fn state_clear(id: FlowNodeId, txn: &mut FlowTransaction) -> reifydb_type::Result<()> {
	let range = FlowNodeStateKey::node_range(id);
	let keys_to_remove = {
		let mut stream = txn.range(range, 1024);
		let mut keys = Vec::new();
		while let Some(result) = stream.next() {
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

/// Load state for a key, creating if not exists
pub fn load_or_create_row(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	layout: &EncodedValuesLayout,
) -> reifydb_type::Result<EncodedValues> {
	match state_get(id, txn, key)? {
		Some(row) => Ok(row),
		None => Ok(layout.allocate()),
	}
}

/// Save state encoded
pub fn save_row(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	row: EncodedValues,
) -> reifydb_type::Result<()> {
	state_set(id, txn, key, row)
}

/// Create an empty key for single-state operators
pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_catalog::Catalog;
	use reifydb_core::{CommitVersion, util::CowVec};
	use reifydb_type::Type;

	use super::*;
	use crate::{operator::stateful::test_utils::test::*, transaction::FlowTransaction};

	#[test]
	fn test_state_get_existing() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);
		let key = test_key("nonexistent");

		let result = state_get(node_id, &mut txn, &key).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_state_set_and_update() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);
		let key = test_key("set");
		let value1 = EncodedValues(CowVec::new(vec![1, 2, 3]));
		let value2 = EncodedValues(CowVec::new(vec![4, 5, 6]));

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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("scan_{:02}", i)); // Use padding for proper ordering
			let value = EncodedValues(CowVec::new(vec![i as u8]));
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);

		// Add some entries
		for i in 0..5 {
			let key = test_key(&format!("range_{}", i));
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		let entries = {
			let range = EncodedKeyRange::new(Unbounded, Excluded(test_key("range_3")));
			let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(node_id, vec![]));
			let mut stream = txn.range(prefixed_range, 1024);
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
			let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(node_id, vec![]));
			let mut stream = txn.range(prefixed_range, 1024);
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);

		// Add multiple entries
		for i in 0..3 {
			let key = test_key(&format!("clear_{}", i));
			let value = test_row();
			state_set(node_id, &mut txn, &key, value).unwrap();
		}

		// Verify entries exist
		let count = {
			let range = FlowNodeStateKey::node_range(node_id);
			let mut stream = txn.range(range, 1024);
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
			let range = FlowNodeStateKey::node_range(node_id);
			let mut stream = txn.range(range, 1024);
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);
		let key = test_key("load_new");
		let layout = EncodedValuesLayout::new(&[Type::Int4]);

		// Load non-existing should create new
		let result = load_or_create_row(node_id, &mut txn, &key, &layout).unwrap();
		// Should create a encoded with the expected layout
		assert!(result.len() > 0);
	}

	#[test]
	fn test_save_row() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);
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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let key = test_key("shared");
		let value1 = EncodedValues(CowVec::new(vec![1]));
		let value2 = EncodedValues(CowVec::new(vec![2]));

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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let node_id = FlowNodeId(1);
		let key = test_key("large");

		// Create a large value (10KB)
		let large_value = EncodedValues(CowVec::new(vec![0xAB; 10240]));

		// Store and retrieve
		state_set(node_id, &mut txn, &key, large_value.clone()).unwrap();
		let result = state_get(node_id, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result, &large_value);
	}
}
