// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	},
	interface::catalog::flow::FlowNodeId,
	internal,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{
	Result,
	error::Error,
	value::{blob::Blob, row_number::RowNumber},
};

use super::state::JoinSide;
use crate::{
	operator::stateful::utils::{state_get, state_range, state_remove, state_set},
	transaction::FlowTransaction,
};

const HASH_BYTES: usize = 16;
const ROW_NUMBER_BYTES: usize = 8;

pub(crate) struct Store {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
	schema_key: EncodedKey,
}

impl Store {
	pub(crate) fn new(node_id: FlowNodeId, side: JoinSide) -> Self {
		let (prefix, schema_byte) = match side {
			JoinSide::Left => (vec![0x01], 0x03u8),
			JoinSide::Right => (vec![0x02], 0x04u8),
		};
		Self {
			node_id,
			prefix,
			schema_key: EncodedKey::new(vec![schema_byte]),
		}
	}

	fn hash_prefix(&self, hash: &Hash128) -> Vec<u8> {
		let mut bytes = Vec::with_capacity(self.prefix.len() + HASH_BYTES);
		bytes.extend_from_slice(&self.prefix);
		bytes.extend_from_slice(&hash.0.to_le_bytes());
		bytes
	}

	fn row_key(&self, hash: &Hash128, row_number: RowNumber) -> EncodedKey {
		let mut bytes = Vec::with_capacity(self.prefix.len() + HASH_BYTES + ROW_NUMBER_BYTES);
		bytes.extend_from_slice(&self.prefix);
		bytes.extend_from_slice(&hash.0.to_le_bytes());
		bytes.extend_from_slice(&row_number.0.to_be_bytes());
		EncodedKey::new(bytes)
	}

	pub(crate) fn put_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedRow,
	) -> Result<()> {
		let key = self.row_key(hash, row_number);
		let mut row = encoded.clone();
		let now_nanos = txn.clock().now_nanos();
		row.set_timestamps(now_nanos, now_nanos);
		state_set(self.node_id, txn, &key, row)
	}

	pub(crate) fn update_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedRow,
	) -> Result<bool> {
		let key = self.row_key(hash, row_number);
		if state_get(self.node_id, txn, &key)?.is_none() {
			return Ok(false);
		}
		let mut row = encoded.clone();
		let now_nanos = txn.clock().now_nanos();
		row.set_timestamps(now_nanos, now_nanos);
		state_set(self.node_id, txn, &key, row)?;
		Ok(true)
	}

	pub(crate) fn remove_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<bool> {
		let key = self.row_key(hash, row_number);
		let existed = state_get(self.node_id, txn, &key)?.is_some();
		if existed {
			state_remove(self.node_id, txn, &key)?;
		}
		Ok(existed)
	}

	pub(crate) fn remove_all_for_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<usize> {
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		let entries: Vec<(EncodedKey, EncodedRow)> = state_range(self.node_id, txn, range)?.collect();
		let count = entries.len();
		for (key, _) in entries {
			state_remove(self.node_id, txn, &key)?;
		}
		Ok(count)
	}

	pub(crate) fn rows_for_key(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		let entries: Vec<(EncodedKey, EncodedRow)> = state_range(self.node_id, txn, range)?.collect();
		let mut out = Vec::with_capacity(entries.len());
		for (full_key, row) in entries {
			if let Some(rn) = row_number_from_key(full_key.as_slice()) {
				out.push((rn, row));
			}
		}
		Ok(out)
	}

	pub(crate) fn contains_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<bool> {
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		Ok(state_range(self.node_id, txn, range)?.next().is_some())
	}

	pub(crate) fn get_row_shape(&self, txn: &mut FlowTransaction) -> Result<Option<RowShape>> {
		match state_get(self.node_id, txn, &self.schema_key)? {
			Some(row) => {
				let op = RowShape::operator_state();
				let blob = op.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let fields: Vec<RowShapeField> = from_bytes(blob.as_ref()).map_err(|e| {
					Error(Box::new(internal!("Failed to deserialize row shape: {}", e)))
				})?;
				Ok(Some(RowShape::new(fields)))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set_row_shape(&self, txn: &mut FlowTransaction, shape: &RowShape) -> Result<()> {
		let serialized = to_stdvec(&shape.fields().to_vec())
			.map_err(|e| Error(Box::new(internal!("Failed to serialize row shape: {}", e))))?;
		let op = RowShape::operator_state();
		let now_nanos = txn.clock().now_nanos();
		let (mut row, created_at) = match state_get(self.node_id, txn, &self.schema_key)? {
			Some(existing) => {
				let c = existing.created_at_nanos();
				(
					existing,
					if c == 0 {
						now_nanos
					} else {
						c
					},
				)
			}
			None => (op.allocate(), now_nanos),
		};
		op.set_blob(&mut row, 0, &Blob::from(serialized));
		row.set_timestamps(created_at, now_nanos);
		state_set(self.node_id, txn, &self.schema_key, row)?;
		Ok(())
	}

	pub(crate) fn tick_evict(&self, txn: &mut FlowTransaction, cutoff_nanos: u64) -> Result<usize> {
		let prefix_range = EncodedKeyRange::prefix(&self.prefix);
		let entries: Vec<(EncodedKey, EncodedRow)> = state_range(self.node_id, txn, prefix_range)?.collect();
		let mut evicted = 0;
		for (key, row) in entries {
			if row.updated_at_nanos() < cutoff_nanos {
				state_remove(self.node_id, txn, &key)?;
				evicted += 1;
			}
		}
		Ok(evicted)
	}
}

fn row_number_from_key(bytes: &[u8]) -> Option<RowNumber> {
	if bytes.len() < ROW_NUMBER_BYTES {
		return None;
	}
	let suffix: [u8; ROW_NUMBER_BYTES] = bytes[bytes.len() - ROW_NUMBER_BYTES..].try_into().ok()?;
	Some(RowNumber(u64::from_be_bytes(suffix)))
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{common::CommitVersion, encoded::row::EncodedRow};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::value::identity::IdentityId;

	use super::*;

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	fn rn(v: u64) -> RowNumber {
		RowNumber(v)
	}

	fn row(payload: u8) -> EncodedRow {
		let shape = RowShape::operator_state();
		let mut r = shape.allocate();
		shape.set_blob(&mut r, 0, &Blob::from(vec![payload]));
		r
	}

	#[test]
	fn put_row_then_rows_for_key_returns_inserted() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(1), JoinSide::Left);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20)).unwrap();
		store.put_row(&mut txn, &h(0xBBB), rn(3), &row(0x30)).unwrap();

		let rows_a = store.rows_for_key(&mut txn, &h(0xAAA)).unwrap();
		assert_eq!(rows_a.len(), 2);
		assert_eq!(rows_a[0].0, rn(1));
		assert_eq!(rows_a[1].0, rn(2));

		let rows_b = store.rows_for_key(&mut txn, &h(0xBBB)).unwrap();
		assert_eq!(rows_b.len(), 1);
		assert_eq!(rows_b[0].0, rn(3));
	}

	#[test]
	fn update_row_overwrites_existing_returns_true() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(2), JoinSide::Right);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		assert!(store.update_row(&mut txn, &h(0xAAA), rn(1), &row(0x99)).unwrap());

		let rows = store.rows_for_key(&mut txn, &h(0xAAA)).unwrap();
		assert_eq!(rows.len(), 1);
		let shape = RowShape::operator_state();
		let blob = shape.get_blob(&rows[0].1, 0);
		assert_eq!(blob.as_bytes(), &[0x99u8][..]);
	}

	#[test]
	fn update_row_returns_false_when_missing() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(3), JoinSide::Left);

		assert!(!store.update_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap());
		assert!(store.rows_for_key(&mut txn, &h(0xAAA)).unwrap().is_empty());
	}

	#[test]
	fn remove_row_returns_existence_and_contains_key_reports_empty() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(4), JoinSide::Left);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20)).unwrap();
		assert!(store.contains_key(&mut txn, &h(0xAAA)).unwrap());

		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(1)).unwrap());
		assert!(store.contains_key(&mut txn, &h(0xAAA)).unwrap());

		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(2)).unwrap());
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());

		assert!(!store.remove_row(&mut txn, &h(0xAAA), rn(99)).unwrap());
	}

	#[test]
	fn remove_all_for_key_clears_only_that_hash() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(5), JoinSide::Left);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20)).unwrap();
		store.put_row(&mut txn, &h(0xBBB), rn(3), &row(0x30)).unwrap();

		let removed = store.remove_all_for_key(&mut txn, &h(0xAAA)).unwrap();
		assert_eq!(removed, 2);
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(store.contains_key(&mut txn, &h(0xBBB)).unwrap());
	}

	#[test]
	fn tick_evict_drops_stale_rows_only_per_row() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(6), JoinSide::Left);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		mock_clock.advance_millis(50);
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20)).unwrap();

		let cutoff = mock_clock.now_nanos() - 30_000_000;
		let evicted = store.tick_evict(&mut txn, cutoff).unwrap();
		assert_eq!(evicted, 1, "only the older row should be evicted");

		let remaining = store.rows_for_key(&mut txn, &h(0xAAA)).unwrap();
		assert_eq!(remaining.len(), 1);
		assert_eq!(remaining[0].0, rn(2));
	}

	#[test]
	fn tick_evict_is_noop_when_nothing_stale() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(7), JoinSide::Left);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		assert_eq!(store.tick_evict(&mut txn, 0).unwrap(), 0);
		assert!(store.contains_key(&mut txn, &h(0xAAA)).unwrap());
	}

	#[test]
	fn tick_evict_only_touches_own_side() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(8);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		left.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		right.put_row(&mut txn, &h(0xBBB), rn(2), &row(0x20)).unwrap();

		let evicted = left.tick_evict(&mut txn, u64::MAX).unwrap();
		assert_eq!(evicted, 1);

		assert!(!left.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(right.contains_key(&mut txn, &h(0xBBB)).unwrap());
	}
}
