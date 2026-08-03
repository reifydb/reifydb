// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, RowShapeField, cache::RowShapeCacheCell, fingerprint::RowShapeFingerprint},
	},
	key::{
		decode_u64_asc, encode_u64_asc, encode_u128_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
};
#[cfg(test)]
use reifydb_core::interface::catalog::config::{ConfigKey, GetConfig};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey, keyspace_inner_range},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{blob::Blob, row_number::RowNumber},
};
use tracing::instrument;

use super::state::JoinSide;
use crate::{
	error::FlowStateError,
	operator::stateful::utils::{state_get, state_range, state_remove, state_set},
};

const ROW_NUMBER_BYTES: usize = 8;
const SHAPE_CACHE_CAPACITY: usize = 8;

pub(crate) fn group_bytes(hash: &Hash128) -> EncodedKey {
	EncodedKey::new(encode_u128_asc(hash.0))
}

pub(crate) struct Store {
	operator_id: OperatorId,
	side: JoinSide,
	shape_cache: RowShapeCacheCell,
	co_stamped: Vec<Keyspace>,
}

impl Store {
	pub(crate) fn new(operator_id: OperatorId, side: JoinSide) -> Self {
		Self {
			operator_id,
			side,
			shape_cache: RowShapeCacheCell::new(SHAPE_CACHE_CAPACITY),
			co_stamped: Vec::new(),
		}
	}

	pub(crate) fn also_stamping(mut self, keyspaces: Vec<Keyspace>) -> Self {
		self.co_stamped = keyspaces;
		self
	}

	fn stamp(&self, txn: &mut FlowTransaction, group: GroupId) -> Result<()> {
		txn.stamp_side(self.operator_id, group, self.side.keyspace())?;
		for keyspace in &self.co_stamped {
			txn.stamp_side(self.operator_id, group, *keyspace)?;
		}
		Ok(())
	}

	fn resolve(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<Option<GroupId>> {
		txn.lookup_group(self.operator_id, &group_bytes(hash))
	}

	fn intern(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<GroupId> {
		let (group, _) = txn.intern_group(self.operator_id, &group_bytes(hash))?;
		Ok(group)
	}

	fn schema_key(&self, fingerprint: RowShapeFingerprint) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(1 + 8);
		suffix.push(self.side.tag());
		suffix.extend_from_slice(&fingerprint.to_le_bytes());
		OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::JOIN_SCHEMA, suffix)
	}

	fn row_key(&self, group: GroupId, row_number: RowNumber) -> GroupStateKey {
		OperatorGroupStateKey::inner_encoded(group, self.side.keyspace(), encode_u64_asc(row_number.0))
	}

	fn rows_range(&self, group: GroupId) -> EncodedKeyRange {
		keyspace_inner_range(group, self.side.keyspace())
	}

	#[instrument(name = "flow::operator::join::store::put_row", level = "trace", skip_all)]
	pub(crate) fn put_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedRow,
	) -> Result<()> {
		let group = self.intern(txn, hash)?;
		self.stamp(txn, group)?;
		let key = self.row_key(group, row_number);
		state_set(self.operator_id, txn, &key, encoded.clone())
	}

	pub(crate) fn group_of(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<Option<GroupId>> {
		self.resolve(txn, hash)
	}

	pub(crate) fn group_for(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<GroupId> {
		self.intern(txn, hash)
	}

	#[instrument(name = "flow::operator::join::store::get_row", level = "trace", skip_all)]
	pub(crate) fn get_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<Option<EncodedRow>> {
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(None);
		};
		let key = self.row_key(group, row_number);
		state_get(self.operator_id, txn, &key)
	}

	pub(crate) fn update_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedRow,
	) -> Result<bool> {
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(false);
		};
		let key = self.row_key(group, row_number);
		if state_get(self.operator_id, txn, &key)?.is_none() {
			return Ok(false);
		}
		self.stamp(txn, group)?;
		state_set(self.operator_id, txn, &key, encoded.clone())?;
		Ok(true)
	}

	pub(crate) fn remove_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<bool> {
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(false);
		};
		let key = self.row_key(group, row_number);
		let existed = state_get(self.operator_id, txn, &key)?.is_some();
		if existed {
			state_remove(self.operator_id, txn, &key)?;
		}
		Ok(existed)
	}

	#[instrument(name = "flow::operator::join::rows_for_key_block", level = "trace", skip_all, fields(limit = limit))]
	pub(crate) fn rows_for_key_block(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		after: Option<&RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(Vec::new());
		};
		let mut range = self.rows_range(group);
		if let Some(after) = after {
			range.start = Bound::Excluded(self.row_key(group, *after).into_encoded());
		}
		let mut out = Vec::new();
		for entry in state_range(self.operator_id, txn, range) {
			let (full_key, row) = entry?;
			if let Some(rn) = row_number_from_key(full_key.as_slice()) {
				out.push((rn, row));
				if out.len() >= limit {
					break;
				}
			}
		}
		Ok(out)
	}

	#[cfg(test)]
	pub(crate) fn rows_for_key(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		let limit = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize) as usize;
		let mut out = Vec::new();
		let mut after: Option<RowNumber> = None;
		loop {
			let block = self.rows_for_key_block(txn, hash, after.as_ref(), limit)?;
			if block.is_empty() {
				break;
			}
			let last = block.last().unwrap().0;
			let exhausted = block.len() < limit;
			out.extend(block);
			if exhausted {
				break;
			}
			after = Some(last);
		}
		Ok(out)
	}

	pub(crate) fn contains_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<bool> {
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(false);
		};
		let range = self.rows_range(group);
		Ok(state_range(self.operator_id, txn, range).next().transpose()?.is_some())
	}

	pub(crate) fn get_row_shape(
		&self,
		txn: &mut FlowTransaction,
		fingerprint: RowShapeFingerprint,
	) -> Result<Option<RowShape>> {
		if let Some(shape) = self.shape_cache.get(&fingerprint) {
			return Ok(Some(shape));
		}
		let key = self.schema_key(fingerprint);
		match state_get(self.operator_id, txn, &key)? {
			Some(row) => {
				let op = RowShape::operator_state();
				let blob = op.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let fields: Vec<RowShapeField> = from_bytes(blob.as_ref()).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "row shape",
						cause: e.to_string(),
					})
				})?;
				let shape = RowShape::new(fields);
				self.shape_cache.insert(shape.clone());
				Ok(Some(shape))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set_row_shape(&self, txn: &mut FlowTransaction, shape: &RowShape) -> Result<()> {
		let fingerprint = shape.fingerprint();
		if self.shape_cache.contains_key(&fingerprint) {
			return Ok(());
		}
		let key = self.schema_key(fingerprint);
		if state_get(self.operator_id, txn, &key)?.is_some() {
			self.shape_cache.insert(shape.clone());
			return Ok(());
		}
		let serialized = to_stdvec(&shape.fields().to_vec()).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "row shape",
				cause: e.to_string(),
			})
		})?;
		let op = RowShape::operator_state();
		let mut row = op.allocate();
		op.set_blob(&mut row, 0, &Blob::from(serialized));
		state_set(self.operator_id, txn, &key, row)?;
		self.shape_cache.insert(shape.clone());
		Ok(())
	}
}

fn row_number_from_key(bytes: &[u8]) -> Option<RowNumber> {
	if bytes.len() < ROW_NUMBER_BYTES {
		return None;
	}
	let suffix: [u8; ROW_NUMBER_BYTES] = bytes[bytes.len() - ROW_NUMBER_BYTES..].try_into().ok()?;
	Some(RowNumber(decode_u64_asc(suffix)))
}

#[cfg(test)]
mod tests {
	use reifydb_codec::encoded::row::EncodedRow;
	use reifydb_core::state::horizon::Cutoff;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::value::{datetime::DateTime, value_type::ValueType};

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
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(1), JoinSide::Left);

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
	fn writing_a_row_stamps_its_own_side_and_only_its_own_side() {
		// Both sides of a key share one group, so a stamp that landed on the wrong side would let
		// a busy left side hold the right side's rows past the right ttl - the exact conflation
		// the side index exists to break.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(51);
		let left = Store::new(operator, JoinSide::Left);

		left.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		let group =
			txn.lookup_group(operator, &group_bytes(&h(0xAAA))).unwrap().expect("the write interned it");

		let far_future = Cutoff(DateTime::MAX);
		assert_eq!(
			txn.due_side_groups(operator, Keyspace::JOIN_LEFT, far_future, 10).unwrap(),
			vec![group],
			"the written side must be enrolled in its own sweep"
		);
		assert!(
			txn.due_side_groups(operator, Keyspace::JOIN_RIGHT, far_future, 10).unwrap().is_empty(),
			"a left write must not enrol the right side, which holds no rows to retire"
		);
	}

	#[test]
	fn updating_a_row_renews_its_side() {
		// An update is activity, so the ttl clock restarts: otherwise a key kept alive purely by
		// updates is reclaimed on the strength of its first insert while the join still probes it.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(52);
		let store = Store::new(operator, JoinSide::Left);
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		let group =
			txn.lookup_group(operator, &group_bytes(&h(0xAAA))).unwrap().expect("the write interned it");
		txn.forget_side(operator, group, Keyspace::JOIN_LEFT).unwrap();

		assert!(store.update_row(&mut txn, &h(0xAAA), rn(1), &row(0x11)).unwrap());

		assert_eq!(
			txn.due_side_groups(operator, Keyspace::JOIN_LEFT, Cutoff(DateTime::MAX), 10).unwrap(),
			vec![group],
			"the update must have re-stamped the side"
		);
	}

	#[test]
	fn a_rejected_update_stamps_nothing() {
		// Stamping before the row-exists check would enrol a side that holds no rows, and every
		// later sweep would pay to reclaim an empty keyspace for a key never stored.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(53);
		let store = Store::new(operator, JoinSide::Left);
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		let group =
			txn.lookup_group(operator, &group_bytes(&h(0xAAA))).unwrap().expect("the write interned it");
		txn.forget_side(operator, group, Keyspace::JOIN_LEFT).unwrap();

		assert!(!store.update_row(&mut txn, &h(0xAAA), rn(99), &row(0x11)).unwrap(), "no such row number");

		assert!(
			txn.due_side_groups(operator, Keyspace::JOIN_LEFT, Cutoff(DateTime::MAX), 10)
				.unwrap()
				.is_empty(),
			"an update that stored nothing must leave the side index alone"
		);
	}

	#[test]
	fn both_sides_of_one_join_key_share_a_group_without_sharing_rows() {
		// The group IS the join key hash, with both sides inside it; only the keyspace byte keeps
		// a left and a right row at the same hash and row number from overwriting each other.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(50);
		let left = Store::new(operator, JoinSide::Left);
		let right = Store::new(operator, JoinSide::Right);

		left.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		right.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x20)).unwrap();

		let shape = RowShape::operator_state();
		let left_row = left.get_row(&mut txn, &h(0xAAA), rn(1)).unwrap().expect("left row present");
		let right_row = right.get_row(&mut txn, &h(0xAAA), rn(1)).unwrap().expect("right row present");
		assert_eq!(shape.get_blob(&left_row, 0).as_bytes(), &[0x10u8][..]);
		assert_eq!(shape.get_blob(&right_row, 0).as_bytes(), &[0x20u8][..]);

		assert_eq!(
			txn.lookup_group(operator, &group_bytes(&h(0xAAA))).unwrap(),
			txn.lookup_group(operator, &group_bytes(&h(0xAAA))).unwrap(),
			"both sides must intern the same key to one group id"
		);
		assert!(
			txn.lookup_group(operator, &group_bytes(&h(0xAAA))).unwrap().is_some(),
			"storing a row must intern its join key"
		);
	}

	#[test]
	fn reads_never_intern_a_key() {
		// A resolution that interned instead of looking up would mint a dictionary entry, an
		// activity-index row and a reclaim obligation for every absent key probed, turning a
		// read into unbounded group growth.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(51);
		let store = Store::new(operator, JoinSide::Left);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();

		assert!(store.get_row(&mut txn, &h(0xCCC), rn(1)).unwrap().is_none());
		assert!(store.rows_for_key_block(&mut txn, &h(0xCCC), None, 8).unwrap().is_empty());
		assert!(!store.contains_key(&mut txn, &h(0xCCC)).unwrap());
		assert!(!store.remove_row(&mut txn, &h(0xCCC), rn(1)).unwrap());
		assert!(!store.update_row(&mut txn, &h(0xCCC), rn(1), &row(0x20)).unwrap());

		assert!(
			txn.lookup_group(operator, &group_bytes(&h(0xCCC))).unwrap().is_none(),
			"a read probe must resolve the key, never intern it"
		);
	}

	#[test]
	fn get_row_point_reads_exact_row_number_for_hash() {
		// The latest-join probe reads its single right slot by exact (hash, RowNumber::MAX), so a
		// point read must never fall back to a sibling row under the same hash.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(5), JoinSide::Right);

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut txn, &h(0xAAA), RowNumber::MAX, &row(0x20)).unwrap();

		let slot = store.get_row(&mut txn, &h(0xAAA), RowNumber::MAX).unwrap();
		let shape = RowShape::operator_state();
		assert_eq!(shape.get_blob(&slot.expect("slot present"), 0).as_bytes(), &[0x20u8][..]);

		assert!(
			store.get_row(&mut txn, &h(0xAAA), rn(99)).unwrap().is_none(),
			"a row number that was never written must not resolve to any sibling row"
		);
		assert!(
			store.get_row(&mut txn, &h(0xBBB), RowNumber::MAX).unwrap().is_none(),
			"a different hash must not share the slot stored under another hash"
		);
	}

	#[test]
	fn update_row_overwrites_existing_returns_true() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(2), JoinSide::Right);

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
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(3), JoinSide::Left);

		assert!(!store.update_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap());
		assert!(store.rows_for_key(&mut txn, &h(0xAAA)).unwrap().is_empty());
	}

	#[test]
	fn remove_row_returns_existence_and_contains_key_reports_empty() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(4), JoinSide::Left);

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
	fn get_row_shape_round_trips_written_shape() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(20), JoinSide::Left);

		let shape = RowShape::testing(&[ValueType::Int4, ValueType::Utf8]);
		store.set_row_shape(&mut txn, &shape).unwrap();

		let got = store.get_row_shape(&mut txn, shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape));
	}

	#[test]
	fn get_row_shape_loads_from_state_when_cache_is_cold() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(21);
		let shape = RowShape::testing(&[ValueType::Int4]);

		let writer = Store::new(operator, JoinSide::Left);
		writer.set_row_shape(&mut txn, &shape).unwrap();

		let reader = Store::new(operator, JoinSide::Left);
		let got = reader.get_row_shape(&mut txn, shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape), "a cold in-memory cache must fall back to the persisted shape");
	}

	#[test]
	fn each_side_keeps_its_own_shape_under_one_node_scoped_keyspace() {
		// Both sides share the operator-scoped JOIN_SCHEMA keyspace, separated only by the side tag:
		// without it they collide on identical fingerprints and a side decodes the other's shape.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(52);
		let left = Store::new(operator, JoinSide::Left);
		let right = Store::new(operator, JoinSide::Right);

		let shape = RowShape::testing(&[ValueType::Int4]);
		left.set_row_shape(&mut txn, &shape).unwrap();

		let cold_right = Store::new(operator, JoinSide::Right);
		assert_eq!(
			cold_right.get_row_shape(&mut txn, shape.fingerprint()).unwrap(),
			None,
			"one side writing a shape must not publish it to the other side"
		);

		right.set_row_shape(&mut txn, &shape).unwrap();
		let cold_right = Store::new(operator, JoinSide::Right);
		assert_eq!(cold_right.get_row_shape(&mut txn, shape.fingerprint()).unwrap(), Some(shape));
	}

	#[test]
	fn rows_for_key_block_pages_with_resume_cursor() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(30), JoinSide::Left);

		for i in 1..=4u64 {
			store.put_row(&mut txn, &h(0xAAA), rn(i), &row(i as u8)).unwrap();
		}
		// A different hash must not leak into the scanned key's blocks.
		store.put_row(&mut txn, &h(0xBBB), rn(99), &row(0xFF)).unwrap();

		let page1 = store.rows_for_key_block(&mut txn, &h(0xAAA), None, 2).unwrap();
		assert_eq!(page1.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(), vec![rn(1), rn(2)]);

		let after = page1.last().unwrap().0;
		let page2 = store.rows_for_key_block(&mut txn, &h(0xAAA), Some(&after), 2).unwrap();
		assert_eq!(page2.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(), vec![rn(3), rn(4)]);

		// Resuming past the last row of an exact-multiple key must terminate, not wrap.
		let after = page2.last().unwrap().0;
		let page3 = store.rows_for_key_block(&mut txn, &h(0xAAA), Some(&after), 2).unwrap();
		assert!(page3.is_empty(), "scan must end exactly at the key's last row");
	}

	#[test]
	fn rows_for_key_stitches_full_and_partial_blocks_without_loss() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(31), JoinSide::Right);

		// One full block plus a partial one: a blocked probe risks dropping or duplicating rows
		// at the boundary.
		let block_size = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize);
		let total = block_size + 3;
		for i in 1..=total {
			store.put_row(&mut txn, &h(0xCCC), rn(i), &row(0x01)).unwrap();
		}

		let rows = store.rows_for_key(&mut txn, &h(0xCCC)).unwrap();
		let got: Vec<u64> = rows.iter().map(|(rn, _)| rn.0).collect();
		let expected: Vec<u64> = (1..=total).collect();
		assert_eq!(got, expected, "every match exactly once, in row-number order, across the block boundary");
	}

	#[test]
	fn get_row_shape_returns_none_when_shape_absent() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(22), JoinSide::Right);

		let fp = RowShape::testing(&[ValueType::Int4]).fingerprint();
		assert_eq!(store.get_row_shape(&mut txn, fp).unwrap(), None);
	}

	#[test]
	fn set_row_shape_persists_a_second_distinct_shape_on_the_same_instance() {
		// A side's row shape is not uniform for its whole lifetime: a column that is all none in
		// one batch and typed in the next yields a different fingerprint, so every distinct shape
		// has to be retained rather than only the first.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(23), JoinSide::Right);

		let narrow = RowShape::testing(&[ValueType::Int4]);
		let wide = RowShape::testing(&[ValueType::Int4, ValueType::Utf8]);

		store.set_row_shape(&mut txn, &narrow).unwrap();
		store.set_row_shape(&mut txn, &wide).unwrap();

		assert_eq!(
			store.get_row_shape(&mut txn, narrow.fingerprint()).unwrap(),
			Some(narrow),
			"the first shape this instance ever wrote must still resolve"
		);
		assert_eq!(
			store.get_row_shape(&mut txn, wide.fingerprint()).unwrap(),
			Some(wide),
			"a second, differently-shaped write on the same instance must not be dropped"
		);
	}

	#[test]
	fn set_row_shape_second_distinct_shape_survives_a_cold_instance() {
		// A cold cache after a restart must resolve the second distinct shape a side ever wrote,
		// not only the first.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(24);
		let narrow = RowShape::testing(&[ValueType::Int4]);
		let wide = RowShape::testing(&[ValueType::Int4, ValueType::Utf8]);

		let writer = Store::new(operator, JoinSide::Right);
		writer.set_row_shape(&mut txn, &narrow).unwrap();
		writer.set_row_shape(&mut txn, &wide).unwrap();

		let reader = Store::new(operator, JoinSide::Right);
		assert_eq!(
			reader.get_row_shape(&mut txn, wide.fingerprint()).unwrap(),
			Some(wide),
			"a cold in-memory cache must fall back to the persisted second shape, not just the first"
		);
	}

	#[test]
	fn an_absent_key_reports_absent_on_every_read_path() {
		// A key with no rows on this side must report absent identically through all five read
		// paths; a path that disagreed would emit or drop join output for that key.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(40), JoinSide::Right);
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();

		assert!(!store.contains_key(&mut txn, &h(0xBBB)).unwrap());
		assert!(store.rows_for_key_block(&mut txn, &h(0xBBB), None, 8).unwrap().is_empty());
		assert!(store.get_row(&mut txn, &h(0xBBB), RowNumber::MAX).unwrap().is_none());
		assert!(!store.remove_row(&mut txn, &h(0xBBB), rn(1)).unwrap());
		assert!(!store.update_row(&mut txn, &h(0xBBB), rn(1), &row(0x20)).unwrap());
	}

	#[test]
	fn a_key_stays_present_until_its_last_row_is_removed() {
		// Removal is per row, so a key holding two rows must still read present after the first
		// removal; reporting absent early would strand the surviving row.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(OperatorId(41), JoinSide::Left);
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20)).unwrap();

		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(1)).unwrap());
		assert!(store.contains_key(&mut txn, &h(0xAAA)).unwrap(), "one row remains");

		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(2)).unwrap());
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());
	}

	#[test]
	fn a_cold_store_reports_presence_from_persisted_state() {
		// A Store instance carries no cross-run state, so a freshly constructed one must answer
		// presence purely from what was persisted; anything else would lose rows on restart.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(45);
		let writer = Store::new(operator, JoinSide::Right);
		writer.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap();

		let restarted = Store::new(operator, JoinSide::Right);
		assert!(
			restarted.contains_key(&mut txn, &h(0xAAA)).unwrap(),
			"a persisted key must survive a cold store as present"
		);
		assert!(!restarted.contains_key(&mut txn, &h(0xBBB)).unwrap());
	}
}
