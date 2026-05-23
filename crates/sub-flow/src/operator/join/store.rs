// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{cell::Cell, ops::Bound};

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
		shape::{RowShape, RowShapeField, cache::RowShapeCacheCell, fingerprint::RowShapeFingerprint},
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
	operator::stateful::utils::{state_get, state_purge, state_range, state_remove, state_set},
	transaction::FlowTransaction,
};

const HASH_BYTES: usize = 16;
const ROW_NUMBER_BYTES: usize = 8;
const SHAPE_CACHE_CAPACITY: usize = 8;

pub(crate) struct Store {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
	schema_key: EncodedKey,
	shape_written: Cell<bool>,
	shape_cache: RowShapeCacheCell,
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
			shape_written: Cell::new(false),
			shape_cache: RowShapeCacheCell::new(SHAPE_CACHE_CAPACITY),
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

	pub(crate) fn rows_for_key(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		let mut out = Vec::new();
		for entry in state_range(self.node_id, txn, range) {
			let (full_key, row) = entry?;
			if let Some(rn) = row_number_from_key(full_key.as_slice()) {
				out.push((rn, row));
			}
		}
		Ok(out)
	}

	pub(crate) fn contains_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<bool> {
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		Ok(state_range(self.node_id, txn, range).next().transpose()?.is_some())
	}

	pub(crate) fn get_row_shape(
		&self,
		txn: &mut FlowTransaction,
		fingerprint: RowShapeFingerprint,
	) -> Result<Option<RowShape>> {
		if let Some(shape) = self.shape_cache.get(&fingerprint) {
			return Ok(Some(shape));
		}
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
				let shape = RowShape::new(fields);
				self.shape_cache.insert(shape.clone());
				Ok(Some(shape))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set_row_shape(&self, txn: &mut FlowTransaction, shape: &RowShape) -> Result<()> {
		if self.shape_written.get() {
			return Ok(());
		}
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
		self.shape_written.set(true);
		self.shape_cache.insert(shape.clone());
		Ok(())
	}

	pub(crate) fn tick_evict(
		&self,
		txn: &mut FlowTransaction,
		cutoff_nanos: u64,
		cursor: &mut Option<EncodedKey>,
	) -> Result<usize> {
		const EVICT_BATCH: usize = 4096;
		let base = EncodedKeyRange::prefix(&self.prefix);
		let start = match cursor.as_ref() {
			Some(c) => Bound::Excluded(c.clone()),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch = state_range(self.node_id, txn, range).take(EVICT_BATCH).collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < EVICT_BATCH;
		let last_key = batch.last().map(|(key, _)| key.clone());

		let mut evicted = 0;
		for (key, row) in batch {
			if row.updated_at_nanos() < cutoff_nanos {
				state_purge(self.node_id, txn, &key)?;
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
	use reifydb_type::value::{identity::IdentityId, r#type::Type};

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
		let evicted = store.tick_evict(&mut txn, cutoff, &mut None).unwrap();
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
		assert_eq!(store.tick_evict(&mut txn, 0, &mut None).unwrap(), 0);
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

		let evicted = left.tick_evict(&mut txn, u64::MAX, &mut None).unwrap();
		assert_eq!(evicted, 1);

		assert!(!left.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(right.contains_key(&mut txn, &h(0xBBB)).unwrap());
	}

	#[test]
	fn get_row_shape_round_trips_written_shape() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(20), JoinSide::Left);

		let shape = RowShape::testing(&[Type::Int4, Type::Utf8]);
		store.set_row_shape(&mut txn, &shape).unwrap();

		let got = store.get_row_shape(&mut txn, shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape));
	}

	#[test]
	fn get_row_shape_loads_from_state_when_cache_is_cold() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(21);
		let shape = RowShape::testing(&[Type::Int4]);

		let writer = Store::new(node, JoinSide::Left);
		writer.set_row_shape(&mut txn, &shape).unwrap();

		let reader = Store::new(node, JoinSide::Left);
		let got = reader.get_row_shape(&mut txn, shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape), "a cold in-memory cache must fall back to the persisted shape");
	}

	#[test]
	fn get_row_shape_returns_none_when_shape_absent() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(22), JoinSide::Right);

		let fp = RowShape::testing(&[Type::Int4]).fingerprint();
		assert_eq!(store.get_row_shape(&mut txn, fp).unwrap(), None);
	}
}
