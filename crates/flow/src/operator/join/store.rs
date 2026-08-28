// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::{
		decode_u64_asc, encode_u64_asc, encode_u128_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
	row::{
		bytes::EncodedBytes,
		operator::state::{decode_body, encode},
		pod::EncodedPodRow,
		shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
};
#[cfg(test)]
use reifydb_core::interface::catalog::{
	config::{ConfigKey, GetConfig},
	flow::OperatorId,
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	error::Error,
	util::{cowvec::CowVec, hash::Hash128},
	value::row_number::RowNumber,
};
use tracing::instrument;

use super::state::JoinSide;
use crate::{
	error::FlowStateError,
	operator::{
		host::HostContext,
		join::strategy::hash::columns_from_block,
		state::store::{state_get, state_range, state_remove, state_set},
	},
};

const ROW_NUMBER_BYTES: usize = 8;
const SLOT: RowNumber = RowNumber::MAX;

pub(crate) fn group_bytes(hash: &Hash128) -> EncodedKey {
	EncodedKey::new(encode_u128_asc(hash.0))
}

pub(crate) fn body_bytes(row: &EncodedPodRow) -> EncodedBytes {
	EncodedBytes(CowVec::new(row.body().to_vec()))
}

pub(crate) struct Store {
	side: JoinSide,
}

impl Store {
	pub(crate) fn new(side: JoinSide) -> Self {
		Self {
			side,
		}
	}

	pub(crate) fn slot(
		&self,
		host: &mut dyn HostContext,
		group: GroupId,
	) -> Result<Option<(EncodedBytes, Columns)>> {
		match self.get_row_in(host, group, SLOT)? {
			Some(row) => {
				let columns = columns_from_block(host, self, vec![(SLOT, row.clone())])?;
				Ok(Some((row, columns)))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn group_of(&self, hash: &Hash128) -> GroupId {
		GroupId::of(&group_bytes(hash))
	}

	fn schema_key(&self, fingerprint: RowShapeFingerprint) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(1 + 8);
		suffix.push(self.side.tag());
		suffix.extend_from_slice(&fingerprint.to_le_bytes());
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::JOIN_SCHEMA, suffix)
	}

	fn row_key(&self, group: GroupId, row_number: RowNumber) -> GroupStateKey {
		OperatorStateKey::inner_encoded(group, self.side.keyspace(), encode_u64_asc(row_number.0))
	}

	fn rows_range(&self, group: GroupId) -> EncodedKeyRange {
		keyspace_inner_range(group, self.side.keyspace())
	}

	#[instrument(name = "flow::operator::join::store::put_row", level = "trace", skip_all)]
	pub(crate) fn put_row(
		&self,
		host: &mut dyn HostContext,
		hash: &Hash128,
		row_number: RowNumber,
		row: &EncodedPodRow,
	) -> Result<()> {
		self.write_row(host, self.group_of(hash), row_number, row)
	}

	pub(crate) fn write_row(
		&self,
		host: &mut dyn HostContext,
		group: GroupId,
		row_number: RowNumber,
		row: &EncodedPodRow,
	) -> Result<()> {
		let key = self.row_key(group, row_number);
		state_set(host, &key, row.clone())
	}

	pub(crate) fn get_row_in(
		&self,
		host: &mut dyn HostContext,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<Option<EncodedBytes>> {
		let key = self.row_key(group, row_number);
		Ok(state_get(host, &key)?.as_ref().map(body_bytes))
	}

	pub(crate) fn update_row(
		&self,
		host: &mut dyn HostContext,
		hash: &Hash128,
		row_number: RowNumber,
		row: &EncodedPodRow,
	) -> Result<bool> {
		self.update_row_in(host, self.group_of(hash), row_number, row)
	}

	pub(crate) fn update_row_in(
		&self,
		host: &mut dyn HostContext,
		group: GroupId,
		row_number: RowNumber,
		row: &EncodedPodRow,
	) -> Result<bool> {
		let key = self.row_key(group, row_number);
		if state_get(host, &key)?.is_none() {
			return Ok(false);
		}
		state_set(host, &key, row.clone())?;
		Ok(true)
	}

	pub(crate) fn remove_row(
		&self,
		host: &mut dyn HostContext,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<bool> {
		let group = self.group_of(hash);
		if self.get_row_in(host, group, row_number)?.is_none() {
			return Ok(false);
		}
		self.remove_row_in(host, group, row_number)?;
		Ok(true)
	}

	pub(crate) fn remove_row_in(
		&self,
		host: &mut dyn HostContext,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		let key = self.row_key(group, row_number);
		state_remove(host, &key)
	}

	#[instrument(name = "flow::operator::join::rows_for_key", level = "trace", skip_all, fields(limit = limit))]
	pub(crate) fn rows_for_key(
		&self,
		host: &mut dyn HostContext,
		hash: &Hash128,
		after: Option<&RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedBytes)>> {
		let group = self.group_of(hash);
		let mut range = self.rows_range(group);
		if let Some(after) = after {
			range.start = Bound::Excluded(self.row_key(group, *after).into_encoded());
		}
		let mut out = Vec::new();
		for entry in state_range(host, range) {
			let (full_key, bytes) = entry?;
			if let Some(rn) = row_number_from_key(full_key.as_slice()) {
				out.push((rn, body_bytes(&EncodedPodRow::from(bytes))));
				if out.len() >= limit {
					break;
				}
			}
		}

		Ok(out)
	}

	pub(crate) fn holds_rows(&self, host: &mut dyn HostContext, group: GroupId) -> Result<bool> {
		Ok(state_range(host, self.rows_range(group)).next().transpose()?.is_some())
	}

	pub(crate) fn row_numbers_in(&self, host: &mut dyn HostContext, group: GroupId) -> Result<Vec<RowNumber>> {
		let mut out = Vec::new();
		for entry in state_range(host, self.rows_range(group)) {
			let (full_key, _) = entry?;
			if let Some(row_number) = row_number_from_key(full_key.as_slice()) {
				out.push(row_number);
			}
		}
		Ok(out)
	}

	pub(crate) fn contains_key(&self, host: &mut dyn HostContext, hash: &Hash128) -> Result<bool> {
		let range = self.rows_range(self.group_of(hash));
		Ok(state_range(host, range).next().transpose()?.is_some())
	}

	pub(crate) fn get_row_shape(
		&self,
		host: &mut dyn HostContext,
		fingerprint: RowShapeFingerprint,
	) -> Result<Option<RowShape>> {
		let key = self.schema_key(fingerprint);
		match state_get(host, &key)? {
			Some(row) => {
				if row.is_empty() {
					return Ok(None);
				}
				let fields: Vec<RowShapeField> =
					decode_body::<Vec<RowShapeField>>(&row).map_err(|e| {
						Error::from(FlowStateError::Decode {
							state: "row shape",
							cause: e.to_string(),
						})
					})?;
				Ok(Some(RowShape::new(RowFamily::Pod, fields)))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set_row_shape(&self, host: &mut dyn HostContext, shape: &RowShape) -> Result<()> {
		let key = self.schema_key(shape.fingerprint());
		if state_get(host, &key)?.is_some() {
			return Ok(());
		}
		let row = encode(&shape.fields().to_vec()).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "row shape",
				cause: e.to_string(),
			})
		})?;
		state_set(host, &key, row)
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
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::key::operator_state::node_range;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{
			FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
	};

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	fn rn(v: u64) -> RowNumber {
		RowNumber(v)
	}

	fn row(payload: u8) -> EncodedPodRow {
		EncodedPodRow::new(&[payload])
	}

	fn b<'a>(txn: &'a mut DeferredTransaction, operator: OperatorId) -> TxnHostContext<'a, DeferredTransaction> {
		TxnHostContext::new(txn, operator)
	}

	/// Resolve-then-read, the composition production used before callers began holding the group
	/// across a batch. Kept here so the read-path assertions still exercise both halves together.
	fn get_row(
		store: &Store,
		operator: OperatorId,
		txn: &mut DeferredTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<Option<EncodedBytes>> {
		let group = store.group_of(hash);
		store.get_row_in(&mut b(txn, operator), group, row_number)
	}

	#[test]
	fn put_row_then_rows_for_key_returns_inserted() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(1);
		let store = Store::new(JoinSide::Left);

		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(2), &row(0x20)).unwrap();
		store.put_row(&mut b(&mut txn, operator), &h(0xBBB), rn(3), &row(0x30)).unwrap();

		let rows_a = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), None, 64).unwrap();
		assert_eq!(rows_a.len(), 2);
		assert_eq!(rows_a[0].0, rn(1));
		assert_eq!(rows_a[1].0, rn(2));

		let rows_b = store.rows_for_key(&mut b(&mut txn, operator), &h(0xBBB), None, 64).unwrap();
		assert_eq!(rows_b.len(), 1);
		assert_eq!(rows_b[0].0, rn(3));
	}

	#[test]
	fn both_sides_of_one_join_key_share_a_group_without_sharing_rows() {
		// The group IS the join key hash, with both sides inside it; only the keyspace byte keeps
		// a left and a right row at the same hash and row number from overwriting each other.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(50);
		let left = Store::new(JoinSide::Left);
		let right = Store::new(JoinSide::Right);

		left.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		right.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x20)).unwrap();

		let left_row = get_row(&left, operator, &mut txn, &h(0xAAA), rn(1)).unwrap().expect("left row present");
		let right_row =
			get_row(&right, operator, &mut txn, &h(0xAAA), rn(1)).unwrap().expect("right row present");
		assert_eq!(left_row.as_slice(), &[0x10u8][..]);
		assert_eq!(right_row.as_slice(), &[0x20u8][..]);

		assert_eq!(
			left.group_of(&h(0xAAA)),
			right.group_of(&h(0xAAA)),
			"both sides must resolve the same key to one group id"
		);
	}

	#[test]
	fn a_read_probe_writes_nothing() {
		// a probe that wrote would mint an activity-index row and a reclaim obligation per absent key
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(51);
		let store = Store::new(JoinSide::Left);

		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		let before = txn.state_range(operator, StateRange::forward(node_range(operator), "test")).unwrap();

		assert!(get_row(&store, operator, &mut txn, &h(0xCCC), rn(1)).unwrap().is_none());
		assert!(store.rows_for_key(&mut b(&mut txn, operator), &h(0xCCC), None, 8).unwrap().is_empty());
		assert!(!store.contains_key(&mut b(&mut txn, operator), &h(0xCCC)).unwrap());
		assert!(!store.remove_row(&mut b(&mut txn, operator), &h(0xCCC), rn(1)).unwrap());
		assert!(!store.update_row(&mut b(&mut txn, operator), &h(0xCCC), rn(1), &row(0x20)).unwrap());

		let after = txn.state_range(operator, StateRange::forward(node_range(operator), "test")).unwrap();
		assert_eq!(after.items.len(), before.items.len(), "no probe may leave a row behind");
	}

	#[test]
	fn get_row_point_reads_exact_row_number_for_hash() {
		// The latest-join probe reads its single right slot by exact (hash, RowNumber::MAX), so a
		// point read must never fall back to a sibling row under the same hash.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(5);
		let store = Store::new(JoinSide::Right);

		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), RowNumber::MAX, &row(0x20)).unwrap();

		let slot = get_row(&store, operator, &mut txn, &h(0xAAA), RowNumber::MAX).unwrap();
		assert_eq!(slot.expect("slot present").as_slice(), &[0x20u8][..]);

		assert!(
			get_row(&store, operator, &mut txn, &h(0xAAA), rn(99)).unwrap().is_none(),
			"a row number that was never written must not resolve to any sibling row"
		);
		assert!(
			get_row(&store, operator, &mut txn, &h(0xBBB), RowNumber::MAX).unwrap().is_none(),
			"a different hash must not share the slot stored under another hash"
		);
	}

	#[test]
	fn update_row_overwrites_existing_returns_true() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(2);
		let store = Store::new(JoinSide::Right);

		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		assert!(store.update_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x99)).unwrap());

		let rows = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), None, 64).unwrap();
		assert_eq!(rows.len(), 1);
		assert_eq!(rows[0].1.as_slice(), &[0x99u8][..]);
	}

	#[test]
	fn update_row_returns_false_when_missing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(3);
		let store = Store::new(JoinSide::Left);

		assert!(!store.update_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap());
		assert!(store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), None, 64).unwrap().is_empty());
	}

	#[test]
	fn remove_row_returns_existence_and_contains_key_reports_empty() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(4);
		let store = Store::new(JoinSide::Left);

		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(2), &row(0x20)).unwrap();
		assert!(store.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap());

		assert!(store.remove_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1)).unwrap());
		assert!(store.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap());

		assert!(store.remove_row(&mut b(&mut txn, operator), &h(0xAAA), rn(2)).unwrap());
		assert!(!store.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap());

		assert!(!store.remove_row(&mut b(&mut txn, operator), &h(0xAAA), rn(99)).unwrap());
	}

	#[test]
	fn removing_an_absent_row_is_invisible_to_every_reader() {
		// remove_row_in no longer reads before deleting, so it can be handed a row number that the
		// retention sweep already reclaimed, or one that was never stored under this group at all.
		// The tombstone that produces must stay invisible: siblings under the key must still scan in
		// order, the key must still report present, and the absent row must still read as absent.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(46);
		let store = Store::new(JoinSide::Left);
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(3), &row(0x30)).unwrap();
		let group = store.group_of(&h(0xAAA));

		store.remove_row_in(&mut b(&mut txn, operator), group, rn(2)).unwrap();
		store.remove_row_in(&mut b(&mut txn, operator), group, rn(2)).unwrap();

		assert!(store.get_row_in(&mut b(&mut txn, operator), group, rn(2)).unwrap().is_none());
		assert!(
			store.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap(),
			"the key still holds two rows"
		);
		let rows = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), None, 64).unwrap();
		assert_eq!(
			rows.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(),
			vec![rn(1), rn(3)],
			"a tombstone for a row that was never stored must not surface in the key's scan"
		);

		store.remove_row_in(&mut b(&mut txn, operator), group, rn(1)).unwrap();
		let rows = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), None, 64).unwrap();
		assert_eq!(
			rows.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(),
			vec![rn(3)],
			"a blind remove of a row that is there must still delete exactly that row"
		);
	}

	#[test]
	fn get_row_shape_round_trips_written_shape() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(20);
		let store = Store::new(JoinSide::Left);

		let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int4, ValueType::Utf8]);
		store.set_row_shape(&mut b(&mut txn, operator), &shape).unwrap();

		let got = store.get_row_shape(&mut b(&mut txn, operator), shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape));
	}

	#[test]
	fn get_row_shape_loads_from_state_when_cache_is_cold() {
		// Only fields are persisted and the shape is rebuilt as a pod, so any other family loses its round
		// trip.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(21);
		let shape = RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("f0", ValueType::Int4)]);

		let writer = Store::new(JoinSide::Left);
		writer.set_row_shape(&mut b(&mut txn, operator), &shape).unwrap();

		let reader = Store::new(JoinSide::Left);
		let got = reader.get_row_shape(&mut b(&mut txn, operator), shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape), "a cold in-memory cache must fall back to the persisted shape");
	}

	#[test]
	fn each_side_keeps_its_own_shape_under_one_root_group_keyspace() {
		// Both sides share the operator-scoped JOIN_SCHEMA keyspace, separated only by the side tag:
		// without it they collide on identical fingerprints and a side decodes the other's shape.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(52);
		let left = Store::new(JoinSide::Left);
		let right = Store::new(JoinSide::Right);

		let shape = RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("f0", ValueType::Int4)]);
		left.set_row_shape(&mut b(&mut txn, operator), &shape).unwrap();

		let cold_right = Store::new(JoinSide::Right);
		assert_eq!(
			cold_right.get_row_shape(&mut b(&mut txn, operator), shape.fingerprint()).unwrap(),
			None,
			"one side writing a shape must not publish it to the other side"
		);

		right.set_row_shape(&mut b(&mut txn, operator), &shape).unwrap();
		let cold_right = Store::new(JoinSide::Right);
		assert_eq!(
			cold_right.get_row_shape(&mut b(&mut txn, operator), shape.fingerprint()).unwrap(),
			Some(shape)
		);
	}

	#[test]
	fn rows_for_key_pages_with_resume_cursor() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(30);
		let store = Store::new(JoinSide::Left);

		for i in 1..=4u64 {
			store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(i), &row(i as u8)).unwrap();
		}
		// A different hash must not leak into the scanned key's blocks.
		store.put_row(&mut b(&mut txn, operator), &h(0xBBB), rn(99), &row(0xFF)).unwrap();

		let page1 = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), None, 2).unwrap();
		assert_eq!(page1.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(), vec![rn(1), rn(2)]);

		let after = page1.last().unwrap().0;
		let page2 = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), Some(&after), 2).unwrap();
		assert_eq!(page2.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(), vec![rn(3), rn(4)]);

		// Resuming past the last row of an exact-multiple key must terminate, not wrap.
		let after = page2.last().unwrap().0;
		let page3 = store.rows_for_key(&mut b(&mut txn, operator), &h(0xAAA), Some(&after), 2).unwrap();
		assert!(page3.is_empty(), "scan must end exactly at the key's last row");
	}

	#[test]
	fn rows_for_key_stitches_full_and_partial_blocks_without_loss() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(31);
		let store = Store::new(JoinSide::Right);

		// Paging is now the only way to read a key, so the boundary risk moved into the caller's
		// loop: a wrong resume cursor drops or repeats the rows either side of a full block.
		let block_size = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize);
		let total = block_size + 3;
		for i in 1..=total {
			store.put_row(&mut b(&mut txn, operator), &h(0xCCC), rn(i), &row(0x01)).unwrap();
		}

		let mut got: Vec<u64> = Vec::new();
		let mut after: Option<RowNumber> = None;
		while got.len() <= total as usize {
			let block = store
				.rows_for_key(
					&mut b(&mut txn, operator),
					&h(0xCCC),
					after.as_ref(),
					block_size as usize,
				)
				.unwrap();
			if block.is_empty() {
				break;
			}
			after = Some(block.last().unwrap().0);
			got.extend(block.iter().map(|(rn, _)| rn.0));
		}
		let expected: Vec<u64> = (1..=total).collect();
		assert_eq!(got, expected, "every match exactly once, in row-number order, across the block boundary");
	}

	#[test]
	fn get_row_shape_returns_none_when_shape_absent() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(22);
		let store = Store::new(JoinSide::Right);

		let fp = RowShape::testing(RowFamily::Pod, &[ValueType::Int4]).fingerprint();
		assert_eq!(store.get_row_shape(&mut b(&mut txn, operator), fp).unwrap(), None);
	}

	#[test]
	fn set_row_shape_persists_a_second_distinct_shape_on_the_same_instance() {
		// A side's row shape is not uniform for its whole lifetime: a column that is all none in
		// one batch and typed in the next yields a different fingerprint, so every distinct shape
		// has to be retained rather than only the first.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(23);
		let store = Store::new(JoinSide::Right);

		let narrow = RowShape::testing(RowFamily::Pod, &[ValueType::Int4]);
		let wide = RowShape::testing(RowFamily::Pod, &[ValueType::Int4, ValueType::Utf8]);

		store.set_row_shape(&mut b(&mut txn, operator), &narrow).unwrap();
		store.set_row_shape(&mut b(&mut txn, operator), &wide).unwrap();

		assert_eq!(
			store.get_row_shape(&mut b(&mut txn, operator), narrow.fingerprint()).unwrap(),
			Some(narrow),
			"the first shape this instance ever wrote must still resolve"
		);
		assert_eq!(
			store.get_row_shape(&mut b(&mut txn, operator), wide.fingerprint()).unwrap(),
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
		let narrow = RowShape::new(RowFamily::Pod, vec![RowShapeField::unconstrained("f0", ValueType::Int4)]);
		let wide = RowShape::new(
			RowFamily::Pod,
			vec![
				RowShapeField::unconstrained("f0", ValueType::Int4),
				RowShapeField::unconstrained("f1", ValueType::Utf8),
			],
		);

		let writer = Store::new(JoinSide::Right);
		writer.set_row_shape(&mut b(&mut txn, operator), &narrow).unwrap();
		writer.set_row_shape(&mut b(&mut txn, operator), &wide).unwrap();

		let reader = Store::new(JoinSide::Right);
		assert_eq!(
			reader.get_row_shape(&mut b(&mut txn, operator), wide.fingerprint()).unwrap(),
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
		let operator = OperatorId(40);
		let store = Store::new(JoinSide::Right);
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();

		assert!(!store.contains_key(&mut b(&mut txn, operator), &h(0xBBB)).unwrap());
		assert!(store.rows_for_key(&mut b(&mut txn, operator), &h(0xBBB), None, 8).unwrap().is_empty());
		assert!(get_row(&store, operator, &mut txn, &h(0xBBB), RowNumber::MAX).unwrap().is_none());
		assert!(!store.remove_row(&mut b(&mut txn, operator), &h(0xBBB), rn(1)).unwrap());
		assert!(!store.update_row(&mut b(&mut txn, operator), &h(0xBBB), rn(1), &row(0x20)).unwrap());
	}

	#[test]
	fn a_key_stays_present_until_its_last_row_is_removed() {
		// Removal is per row, so a key holding two rows must still read present after the first
		// removal; reporting absent early would strand the surviving row.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(41);
		let store = Store::new(JoinSide::Left);
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();
		store.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(2), &row(0x20)).unwrap();

		assert!(store.remove_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1)).unwrap());
		assert!(store.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap(), "one row remains");

		assert!(store.remove_row(&mut b(&mut txn, operator), &h(0xAAA), rn(2)).unwrap());
		assert!(!store.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap());
	}

	#[test]
	fn a_cold_store_reports_presence_from_persisted_state() {
		// A Store instance carries no cross-run state, so a freshly constructed one must answer
		// presence purely from what was persisted; anything else would lose rows on restart.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = OperatorId(45);
		let writer = Store::new(JoinSide::Right);
		writer.put_row(&mut b(&mut txn, operator), &h(0xAAA), rn(1), &row(0x10)).unwrap();

		let restarted = Store::new(JoinSide::Right);
		assert!(
			restarted.contains_key(&mut b(&mut txn, operator), &h(0xAAA)).unwrap(),
			"a persisted key must survive a cold store as present"
		);
		assert!(!restarted.contains_key(&mut b(&mut txn, operator), &h(0xBBB)).unwrap());
	}
}
