// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
		shape::RowShape,
	},
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey, flow_node_state::FlowNodeStateKey},
	row::TtlAnchor,
};
use reifydb_type::Result;

use super::StateIterator;
use crate::transaction::FlowTransaction;

pub fn state_get(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<EncodedRow>> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

pub fn state_set(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey, value: EncodedRow) -> Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

pub fn state_remove(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

pub fn state_drop(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = FlowNodeStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.drop_key(&encoded_key)?;
	Ok(())
}

pub fn internal_state_get(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<EncodedRow>> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();

	match txn.get(&encoded_key)? {
		Some(multi) => Ok(Some(multi)),
		None => Ok(None),
	}
}

pub fn internal_state_set(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	value: EncodedRow,
) -> Result<()> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.set(&encoded_key, value)?;
	Ok(())
}

pub fn internal_state_remove(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.remove(&encoded_key)?;
	Ok(())
}

pub fn internal_state_drop(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<()> {
	let state_key = FlowNodeInternalStateKey::new(id, key.as_ref().to_vec());
	let encoded_key = state_key.encode();
	txn.drop_key(&encoded_key)?;
	Ok(())
}

pub fn evict_state_by_ttl(
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	ttl_nanos: u64,
	ttl_anchor: TtlAnchor,
	now_nanos: u64,
	cursor: &mut Option<EncodedKey>,
) -> Result<usize> {
	const EVICT_BATCH: usize = 4096;
	let base = EncodedKeyRange::all();
	let start = match cursor.as_ref() {
		Some(c) => Bound::Excluded(c.clone()),
		None => base.start.clone(),
	};
	let range = EncodedKeyRange::new(start, base.end.clone());
	let cutoff = now_nanos.saturating_sub(ttl_nanos);

	let batch: Vec<(EncodedKey, EncodedRow)> =
		state_range(id, txn, range).take(EVICT_BATCH).collect::<Result<_>>()?;
	let reached_end = batch.len() < EVICT_BATCH;
	let last_key = batch.last().map(|(key, _)| key.clone());

	let mut evicted = 0;
	for (key, row) in batch {
		let anchor = match ttl_anchor {
			TtlAnchor::Created => row.created_at_nanos(),
			TtlAnchor::Updated => row.updated_at_nanos(),
		};
		if anchor < cutoff {
			state_drop(id, txn, &key)?;
			evicted += 1;
		}
	}

	*cursor = if reached_end {
		None
	} else {
		last_key
	};
	Ok(evicted)
}

pub fn state_scan_all(id: FlowNodeId, txn: &mut FlowTransaction) -> Result<Vec<(EncodedKey, EncodedRow)>> {
	let range = FlowNodeStateKey::node_range(id);
	let stream = txn.range(range, 1024);
	let mut items = Vec::new();
	for result in stream {
		let multi = result?;
		if let Some(state_key) = FlowNodeStateKey::decode(&multi.key) {
			items.push((EncodedKey::new(state_key.key), multi.row));
		} else {
			items.push((multi.key, multi.row));
		}
	}
	Ok(items)
}

pub fn state_range<'a>(id: FlowNodeId, txn: &'a mut FlowTransaction, range: EncodedKeyRange) -> StateIterator<'a> {
	let prefixed_range = range.with_prefix(FlowNodeStateKey::encoded(id, vec![]));
	StateIterator::new(txn.range(prefixed_range, 1024))
}

pub fn state_clear(id: FlowNodeId, txn: &mut FlowTransaction) -> Result<()> {
	let range = FlowNodeStateKey::node_range(id);
	let keys_to_remove = {
		let stream = txn.range(range, 1024);
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
	id: FlowNodeId,
	txn: &mut FlowTransaction,
	key: &EncodedKey,
	shape: &RowShape,
) -> Result<EncodedRow> {
	match state_get(id, txn, key)? {
		Some(row) => Ok(row),
		None => Ok(shape.allocate()),
	}
}

pub fn save_row(id: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey, row: EncodedRow) -> Result<()> {
	state_set(id, txn, key, row)
}

pub fn empty_key() -> EncodedKey {
	EncodedKey::new(Vec::new())
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{common::CommitVersion, encoded::row::SHAPE_HEADER_SIZE};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::{util::cowvec::CowVec, value::r#type::Type};

	use super::*;
	use crate::{operator::stateful::test_utils::test::*, transaction::FlowTransaction};

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
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = FlowNodeId(1);
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
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
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
	fn test_state_scan_all() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = FlowNodeId(1);

		// Add multiple entries
		for i in 0..5 {
			let key = test_key(&format!("scan_{:02}", i)); // Use padding for proper ordering
			let value = EncodedRow(CowVec::new(vec![i as u8]));
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
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
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
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
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
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node_id = FlowNodeId(1);
		let key = test_key("load_new");
		let shape = RowShape::testing(&[Type::Int4]);

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
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node1 = FlowNodeId(1);
		let node2 = FlowNodeId(2);
		let key = test_key("shared");
		let value1 = EncodedRow(CowVec::new(vec![1]));
		let value2 = EncodedRow(CowVec::new(vec![2]));

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
		let node_id = FlowNodeId(1);
		let key = test_key("large");

		// Create a large value (10KB)
		let large_value = EncodedRow(CowVec::new(vec![0xAB; 10240]));

		// Store and retrieve
		state_set(node_id, &mut txn, &key, large_value.clone()).unwrap();
		let result = state_get(node_id, &mut txn, &key).unwrap().unwrap();

		assert_row_eq(&result, &large_value);
	}

	fn aged_row(payload: &[u8], created_at: u64) -> EncodedRow {
		let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
		buf[8..16].copy_from_slice(&created_at.to_le_bytes());
		buf[16..24].copy_from_slice(&created_at.to_le_bytes());
		buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
		EncodedRow(CowVec::new(buf))
	}

	#[test]
	fn evict_state_by_ttl_never_touches_internal_state() {
		// Eviction must scan/drop ONLY FlowNodeStateKey, never
		// FlowNodeInternalStateKey, or the row-number counter (stored under
		// internal state) could be deleted and row numbers reused.
		let mut parent = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut parent,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let node = FlowNodeId(1);

		let aged = aged_row(b"x", 100);
		let state_key = test_key("expired_state");
		state_set(node, &mut txn, &state_key, aged.clone()).unwrap();

		let counter_key = EncodedKey::new(vec![b'C']);
		internal_state_set(node, &mut txn, &counter_key, aged).unwrap();

		let mut cursor = None;
		let evicted =
			evict_state_by_ttl(node, &mut txn, 10, TtlAnchor::Created, 1_000_000_000, &mut cursor).unwrap();

		assert_eq!(evicted, 1, "the expired FlowNodeState row must be dropped");
		assert!(
			state_get(node, &mut txn, &state_key).unwrap().is_none(),
			"expired FlowNodeState row must be dropped"
		);
		assert!(
			internal_state_get(node, &mut txn, &counter_key).unwrap().is_some(),
			"FlowNodeInternalState (the row-number counter) must be immune from TTL eviction"
		);
	}
}
