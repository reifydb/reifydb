// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, RowShapeField, cache::RowShapeCacheCell, fingerprint::RowShapeFingerprint},
	},
	key::{
		decode_u64_asc, decode_u128_asc, encode_u64_asc, encode_u128_asc,
		encoded::{EncodedKey, EncodedKeyRange},
	},
};
#[cfg(test)]
use reifydb_core::interface::catalog::config::{ConfigKey, GetConfig};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey, keyspace_inner_range},
	state::{keyspace::fold_hash128, membership::MembershipAnswer},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{blob::Blob, row_number::RowNumber},
};

use super::state::{JoinMembership, JoinSide};
use crate::{
	error::FlowStateError,
	operator::stateful::utils::{state_get, state_range, state_remove, state_set},
};

const HASH_BYTES: usize = 16;
const ROW_NUMBER_BYTES: usize = 8;
const SHAPE_CACHE_CAPACITY: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RowPresence {
	Live,
	New,
	Unknown,
}

pub(crate) fn group_bytes(hash: &Hash128) -> EncodedKey {
	EncodedKey::new(encode_u128_asc(hash.0))
}

pub(crate) fn hash_from_group_bytes(bytes: &EncodedKey) -> Option<Hash128> {
	let raw: [u8; HASH_BYTES] = bytes.as_ref().try_into().ok()?;
	Some(Hash128(decode_u128_asc(raw)))
}

pub(crate) struct Store {
	node_id: FlowNodeId,
	side: JoinSide,
	shape_cache: RowShapeCacheCell,
	membership: Arc<JoinMembership>,
}

impl Store {
	pub(crate) fn new(node_id: FlowNodeId, side: JoinSide, membership: Arc<JoinMembership>) -> Self {
		Self {
			node_id,
			side,
			shape_cache: RowShapeCacheCell::new(SHAPE_CACHE_CAPACITY),
			membership,
		}
	}

	fn ensure_membership_hydrated(&self, txn: &mut FlowTransaction) -> Result<()> {
		self.membership.hydrate(self.node_id, txn)
	}

	fn probe(&self, hash: &Hash128) -> MembershipAnswer {
		self.membership.side(self.side).probe(fold_hash128(hash))
	}

	fn resolve(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<Option<GroupId>> {
		txn.lookup_group(self.node_id, &group_bytes(hash))
	}

	fn intern(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<GroupId> {
		let (group, _) = txn.intern_group(self.node_id, &group_bytes(hash))?;
		Ok(group)
	}

	fn schema_key(&self, fingerprint: RowShapeFingerprint) -> StateKey {
		let mut suffix = Vec::with_capacity(1 + 8);
		suffix.push(self.side.tag());
		suffix.extend_from_slice(&fingerprint.to_le_bytes());
		OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::JOIN_SCHEMA, suffix)
	}

	fn row_key(&self, group: GroupId, row_number: RowNumber) -> StateKey {
		OperatorStateKey::inner_encoded(group, self.side.keyspace(), encode_u64_asc(row_number.0))
	}

	fn rows_range(&self, group: GroupId) -> EncodedKeyRange {
		keyspace_inner_range(group, self.side.keyspace())
	}

	pub(crate) fn put_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedRow,
		presence: RowPresence,
	) -> Result<()> {
		self.ensure_membership_hydrated(txn)?;
		match presence {
			RowPresence::Live => {}
			RowPresence::New | RowPresence::Unknown => {
				self.membership.side(self.side).insert(fold_hash128(hash))
			}
		}
		let group = self.intern(txn, hash)?;
		txn.stamp_side(self.node_id, group, self.side.keyspace())?;
		let key = self.row_key(group, row_number);
		state_set(self.node_id, txn, &key, encoded.clone())
	}

	pub(crate) fn get_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<Option<EncodedRow>> {
		self.ensure_membership_hydrated(txn)?;
		if self.probe(hash) == MembershipAnswer::DefinitelyAbsent {
			return Ok(None);
		}
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(None);
		};
		let key = self.row_key(group, row_number);
		state_get(self.node_id, txn, &key)
	}

	pub(crate) fn update_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedRow,
	) -> Result<bool> {
		self.ensure_membership_hydrated(txn)?;
		if self.probe(hash) == MembershipAnswer::DefinitelyAbsent {
			return Ok(false);
		}
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(false);
		};
		let key = self.row_key(group, row_number);
		if state_get(self.node_id, txn, &key)?.is_none() {
			return Ok(false);
		}
		txn.stamp_side(self.node_id, group, self.side.keyspace())?;
		state_set(self.node_id, txn, &key, encoded.clone())?;
		Ok(true)
	}

	pub(crate) fn remove_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<bool> {
		self.ensure_membership_hydrated(txn)?;
		if self.probe(hash) == MembershipAnswer::DefinitelyAbsent {
			return Ok(false);
		}
		let Some(group) = self.resolve(txn, hash)? else {
			return Ok(false);
		};
		let key = self.row_key(group, row_number);
		let existed = state_get(self.node_id, txn, &key)?.is_some();
		if existed {
			state_remove(self.node_id, txn, &key)?;
			self.membership.side(self.side).remove(fold_hash128(hash));
		}
		Ok(existed)
	}

	pub(crate) fn rows_for_key_block(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		after: Option<&RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		self.ensure_membership_hydrated(txn)?;
		let answer = self.probe(hash);
		if after.is_none() && answer == MembershipAnswer::DefinitelyAbsent {
			return Ok(Vec::new());
		}
		let Some(group) = self.resolve(txn, hash)? else {
			if after.is_none() && answer == MembershipAnswer::MaybePresent {
				self.membership.side(self.side).record_store_miss();
			}
			return Ok(Vec::new());
		};
		let mut range = self.rows_range(group);
		if let Some(after) = after {
			range.start = Bound::Excluded(self.row_key(group, *after).into_encoded());
		}
		let mut out = Vec::new();
		for entry in state_range(self.node_id, txn, range) {
			let (full_key, row) = entry?;
			if let Some(rn) = row_number_from_key(full_key.as_slice()) {
				out.push((rn, row));
				if out.len() >= limit {
					break;
				}
			}
		}
		if out.is_empty() && after.is_none() && answer == MembershipAnswer::MaybePresent {
			self.membership.side(self.side).record_store_miss();
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
		self.ensure_membership_hydrated(txn)?;
		let answer = self.probe(hash);
		if answer == MembershipAnswer::DefinitelyAbsent {
			return Ok(false);
		}
		let Some(group) = self.resolve(txn, hash)? else {
			if answer == MembershipAnswer::MaybePresent {
				self.membership.side(self.side).record_store_miss();
			}
			return Ok(false);
		};
		let range = self.rows_range(group);
		let found = state_range(self.node_id, txn, range).next().transpose()?.is_some();
		if !found && answer == MembershipAnswer::MaybePresent {
			self.membership.side(self.side).record_store_miss();
		}
		Ok(found)
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
		match state_get(self.node_id, txn, &key)? {
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
		if state_get(self.node_id, txn, &key)?.is_some() {
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
		state_set(self.node_id, txn, &key, row)?;
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

	fn test_membership() -> Arc<JoinMembership> {
		Arc::new(JoinMembership::new())
	}

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
		let store = Store::new(FlowNodeId(1), JoinSide::Left, test_membership());

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xBBB), rn(3), &row(0x30), RowPresence::Unknown).unwrap();

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
		// The per-side sweep can only retire a side it was told about. Because both sides of a key
		// share one group, a stamp that landed on the wrong side - or on both - would let a busy
		// left side hold the right side's rows past the right ttl, which is the exact conflation
		// the side index exists to break. Reading through due_side_groups at a cutoff far past
		// everything asserts what the sweep will actually see.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let node = FlowNodeId(51);
		let left = Store::new(node, JoinSide::Left, membership.clone());

		left.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		let group = txn.lookup_group(node, &group_bytes(&h(0xAAA))).unwrap().expect("the write interned it");

		let far_future = Cutoff(DateTime::MAX);
		assert_eq!(
			txn.due_side_groups(node, Keyspace::JOIN_LEFT, far_future, 10).unwrap(),
			vec![group],
			"the written side must be enrolled in its own sweep"
		);
		assert!(
			txn.due_side_groups(node, Keyspace::JOIN_RIGHT, far_future, 10).unwrap().is_empty(),
			"a left write must not enrol the right side, which holds no rows to retire"
		);
	}

	#[test]
	fn updating_a_row_renews_its_side() {
		// An update is activity: the row is current again, so the ttl clock restarts. If only
		// put_row stamped, a key kept alive purely by updates would be reclaimed on the strength of
		// its first insert while the join is still probing it.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(52);
		let store = Store::new(node, JoinSide::Left, test_membership());
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		let group = txn.lookup_group(node, &group_bytes(&h(0xAAA))).unwrap().expect("the write interned it");
		txn.forget_side(node, group, Keyspace::JOIN_LEFT).unwrap();

		assert!(store.update_row(&mut txn, &h(0xAAA), rn(1), &row(0x11)).unwrap());

		assert_eq!(
			txn.due_side_groups(node, Keyspace::JOIN_LEFT, Cutoff(DateTime::MAX), 10).unwrap(),
			vec![group],
			"the update must have re-stamped the side"
		);
	}

	#[test]
	fn a_rejected_update_stamps_nothing() {
		// update_row returns false when the row is not there. Stamping before that check would
		// enrol a side that holds no rows, and every later sweep would pay to reclaim an empty
		// keyspace for a key the join has never stored.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(53);
		let store = Store::new(node, JoinSide::Left, test_membership());
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		let group = txn.lookup_group(node, &group_bytes(&h(0xAAA))).unwrap().expect("the write interned it");
		txn.forget_side(node, group, Keyspace::JOIN_LEFT).unwrap();

		assert!(!store.update_row(&mut txn, &h(0xAAA), rn(99), &row(0x11)).unwrap(), "no such row number");

		assert!(
			txn.due_side_groups(node, Keyspace::JOIN_LEFT, Cutoff(DateTime::MAX), 10).unwrap().is_empty(),
			"an update that stored nothing must leave the side index alone"
		);
	}

	#[test]
	fn both_sides_of_one_join_key_share_a_group_without_sharing_rows() {
		// Decision 1 of the group migration: the group IS the join key hash, with both
		// sides inside it. That is only safe because the keyspace byte separates them -
		// if it did not, a left row and a right row stored under the same key hash and
		// the same row number would collide on one key and one side would silently
		// overwrite the other.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let node = FlowNodeId(50);
		let left = Store::new(node, JoinSide::Left, membership.clone());
		let right = Store::new(node, JoinSide::Right, membership);

		left.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		right.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x20), RowPresence::Unknown).unwrap();

		let shape = RowShape::operator_state();
		let left_row = left.get_row(&mut txn, &h(0xAAA), rn(1)).unwrap().expect("left row present");
		let right_row = right.get_row(&mut txn, &h(0xAAA), rn(1)).unwrap().expect("right row present");
		assert_eq!(shape.get_blob(&left_row, 0).as_bytes(), &[0x10u8][..]);
		assert_eq!(shape.get_blob(&right_row, 0).as_bytes(), &[0x20u8][..]);

		assert_eq!(
			txn.lookup_group(node, &group_bytes(&h(0xAAA))).unwrap(),
			txn.lookup_group(node, &group_bytes(&h(0xAAA))).unwrap(),
			"both sides must intern the same key to one group id"
		);
		assert!(
			txn.lookup_group(node, &group_bytes(&h(0xAAA))).unwrap().is_some(),
			"storing a row must intern its join key"
		);
	}

	#[test]
	fn reads_never_intern_a_key_even_once_the_filter_has_been_revoked() {
		// While the filter is complete an absent key is answered from RAM and never
		// reaches group resolution at all. The path that matters is the one after a
		// revocation (byte cap exceeded, as in the 2026-07-21 profile): every probe then
		// reads through, so if resolution interned instead of looking up, every absent
		// probe in a hash join would mint a dictionary entry, an activity-index row and
		// a reclaim obligation for a key that holds nothing - turning a degraded filter
		// into unbounded group growth.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(51);
		let membership = Arc::new(JoinMembership::with_byte_cap(64));
		let store = Store::new(node, JoinSide::Left, membership.clone());

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		for hash in 0..100_000u64 {
			membership.side(JoinSide::Left).insert(hash);
		}
		assert!(
			!membership.side(JoinSide::Left).completeness().membership_complete,
			"the cap must have revoked the filter, so probes read through to the store"
		);

		assert!(store.get_row(&mut txn, &h(0xCCC), rn(1)).unwrap().is_none());
		assert!(store.rows_for_key_block(&mut txn, &h(0xCCC), None, 8).unwrap().is_empty());
		assert!(!store.contains_key(&mut txn, &h(0xCCC)).unwrap());
		assert!(!store.remove_row(&mut txn, &h(0xCCC), rn(1)).unwrap());
		assert!(!store.update_row(&mut txn, &h(0xCCC), rn(1), &row(0x20)).unwrap());

		assert!(
			txn.lookup_group(node, &group_bytes(&h(0xCCC))).unwrap().is_none(),
			"a read-through probe must resolve the key, never intern it"
		);
	}

	#[test]
	fn get_row_point_reads_exact_row_number_for_hash() {
		// The latest-join probe reads its single right slot by exact (hash, RowNumber::MAX) rather than
		// a prefix scan. get_row must return the row at that exact key, None for an absent row number,
		// and must not return a sibling row stored under the same hash but a different number.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(FlowNodeId(5), JoinSide::Right, test_membership());

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xAAA), RowNumber::MAX, &row(0x20), RowPresence::Unknown).unwrap();

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
		let store = Store::new(FlowNodeId(2), JoinSide::Right, test_membership());

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
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
		let store = Store::new(FlowNodeId(3), JoinSide::Left, test_membership());

		assert!(!store.update_row(&mut txn, &h(0xAAA), rn(1), &row(0x10)).unwrap());
		assert!(store.rows_for_key(&mut txn, &h(0xAAA)).unwrap().is_empty());
	}

	#[test]
	fn remove_row_returns_existence_and_contains_key_reports_empty() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(FlowNodeId(4), JoinSide::Left, test_membership());

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20), RowPresence::Unknown).unwrap();
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
		let store = Store::new(FlowNodeId(20), JoinSide::Left, test_membership());

		let shape = RowShape::testing(&[ValueType::Int4, ValueType::Utf8]);
		store.set_row_shape(&mut txn, &shape).unwrap();

		let got = store.get_row_shape(&mut txn, shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape));
	}

	#[test]
	fn get_row_shape_loads_from_state_when_cache_is_cold() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(21);
		let shape = RowShape::testing(&[ValueType::Int4]);

		let writer = Store::new(node, JoinSide::Left, test_membership());
		writer.set_row_shape(&mut txn, &shape).unwrap();

		let reader = Store::new(node, JoinSide::Left, test_membership());
		let got = reader.get_row_shape(&mut txn, shape.fingerprint()).unwrap();
		assert_eq!(got, Some(shape), "a cold in-memory cache must fall back to the persisted shape");
	}

	#[test]
	fn each_side_keeps_its_own_shape_under_one_node_scoped_keyspace() {
		// Schemas are per side, not per join key, so both sides share the node-scoped
		// JOIN_SCHEMA keyspace and are separated only by the side tag in the suffix.
		// Without that tag the two sides would collide on identical fingerprints and a
		// side would decode the other side's shape - the exact mis-shaped read the
		// per-side schema prefix used to prevent.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(52);
		let membership = test_membership();
		let left = Store::new(node, JoinSide::Left, membership.clone());
		let right = Store::new(node, JoinSide::Right, membership);

		let shape = RowShape::testing(&[ValueType::Int4]);
		left.set_row_shape(&mut txn, &shape).unwrap();

		let cold_right = Store::new(node, JoinSide::Right, test_membership());
		assert_eq!(
			cold_right.get_row_shape(&mut txn, shape.fingerprint()).unwrap(),
			None,
			"one side writing a shape must not publish it to the other side"
		);

		right.set_row_shape(&mut txn, &shape).unwrap();
		let cold_right = Store::new(node, JoinSide::Right, test_membership());
		assert_eq!(cold_right.get_row_shape(&mut txn, shape.fingerprint()).unwrap(), Some(shape));
	}

	#[test]
	fn rows_for_key_block_pages_with_resume_cursor() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(FlowNodeId(30), JoinSide::Left, test_membership());

		for i in 1..=4u64 {
			store.put_row(&mut txn, &h(0xAAA), rn(i), &row(i as u8), RowPresence::Unknown).unwrap();
		}
		// A different hash must not leak into the scanned key's blocks.
		store.put_row(&mut txn, &h(0xBBB), rn(99), &row(0xFF), RowPresence::Unknown).unwrap();

		let page1 = store.rows_for_key_block(&mut txn, &h(0xAAA), None, 2).unwrap();
		assert_eq!(page1.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(), vec![rn(1), rn(2)]);

		let after = page1.last().unwrap().0;
		let page2 = store.rows_for_key_block(&mut txn, &h(0xAAA), Some(&after), 2).unwrap();
		assert_eq!(page2.iter().map(|(rn, _)| *rn).collect::<Vec<_>>(), vec![rn(3), rn(4)]);

		// Resuming past the last row of an exact-multiple key must terminate, not wrap or
		// pull a neighbouring key's rows.
		let after = page2.last().unwrap().0;
		let page3 = store.rows_for_key_block(&mut txn, &h(0xAAA), Some(&after), 2).unwrap();
		assert!(page3.is_empty(), "scan must end exactly at the key's last row");
	}

	#[test]
	fn rows_for_key_stitches_full_and_partial_blocks_without_loss() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(FlowNodeId(31), JoinSide::Right, test_membership());

		// One full block plus a partial block: the wrapper must walk both, in order, with
		// no dropped or duplicated rows - the exact failure mode a blocked probe risks.
		let block_size = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize);
		let total = block_size + 3;
		for i in 1..=total {
			store.put_row(&mut txn, &h(0xCCC), rn(i), &row(0x01), RowPresence::Unknown).unwrap();
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
		let store = Store::new(FlowNodeId(22), JoinSide::Right, test_membership());

		let fp = RowShape::testing(&[ValueType::Int4]).fingerprint();
		assert_eq!(store.get_row_shape(&mut txn, fp).unwrap(), None);
	}

	#[test]
	fn set_row_shape_persists_a_second_distinct_shape_on_the_same_instance() {
		// A join side is not guaranteed to see a uniform row shape for its whole
		// lifetime (e.g. a value that is entirely undefined in one batch and
		// resolved to a concrete type in a later one yields a different
		// fingerprint). The store must retain every distinct shape it is asked
		// to persist, not silently drop every shape after the first.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let store = Store::new(FlowNodeId(23), JoinSide::Right, test_membership());

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
		// Reproduces the production crash directly: a fresh Store (e.g. after an
		// actor restart, cold in-memory cache) must still resolve a shape that
		// was persisted as the *second* distinct shape ever written on that
		// side, not only the very first one.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(24);
		let narrow = RowShape::testing(&[ValueType::Int4]);
		let wide = RowShape::testing(&[ValueType::Int4, ValueType::Utf8]);

		let writer = Store::new(node, JoinSide::Right, test_membership());
		writer.set_row_shape(&mut txn, &narrow).unwrap();
		writer.set_row_shape(&mut txn, &wide).unwrap();

		let reader = Store::new(node, JoinSide::Right, test_membership());
		assert_eq!(
			reader.get_row_shape(&mut txn, wide.fingerprint()).unwrap(),
			Some(wide),
			"a cold in-memory cache must fall back to the persisted second shape, not just the first"
		);
	}

	#[test]
	fn a_hydrated_side_answers_key_absence_from_membership() {
		// The absent-key probe is the join hot path this filter exists for: a left
		// row whose key has no right-side rows must not pay a store range scan on
		// every block. DefinitelyAbsent short-circuits before any store access -
		// before the group is even resolved - so the absences_served counter plus
		// the zero-read point probes pin that the answer came from RAM.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let store = Store::new(FlowNodeId(40), JoinSide::Right, membership.clone());
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();

		assert!(!store.contains_key(&mut txn, &h(0xBBB)).unwrap());
		assert!(store.rows_for_key_block(&mut txn, &h(0xBBB), None, 8).unwrap().is_empty());

		let reads_before = txn.store_reads();
		assert!(store.get_row(&mut txn, &h(0xBBB), RowNumber::MAX).unwrap().is_none());
		assert!(!store.remove_row(&mut txn, &h(0xBBB), rn(1)).unwrap());
		assert!(!store.update_row(&mut txn, &h(0xBBB), rn(1), &row(0x20)).unwrap());
		assert_eq!(txn.store_reads() - reads_before, 0, "a definite absence must never reach the store");

		let completeness = membership.side(JoinSide::Right).completeness();
		assert!(completeness.membership_complete);
		assert_eq!(completeness.absences_served.as_u64(), 5);
		assert_eq!(completeness.false_positives.as_u64(), 0);
	}

	#[test]
	fn hundreds_of_rows_under_one_join_key_keep_the_side_its_absence_proofs() {
		// Production regression pin (2026-07-21 profile): a hot join key inserts
		// one filter instance per stored row, which chained the cuckoo filter to
		// its byte cap and discarded the whole side - revocations=1 on every
		// hash-join node, and absent-key probes paid store reads for the rest of
		// the run. A key with hundreds of rows must leave the side's membership
		// intact so unrelated absent keys keep their RAM answer.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let store = Store::new(FlowNodeId(42), JoinSide::Right, membership.clone());
		for i in 0..200u64 {
			store.put_row(&mut txn, &h(0xAAA), rn(i + 1), &row(0x10), RowPresence::Unknown).unwrap();
		}

		let reads_before = txn.store_reads();
		assert!(store.get_row(&mut txn, &h(0xBBB), RowNumber::MAX).unwrap().is_none());
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"the hot key must not cost unrelated absent keys their RAM answer"
		);

		let completeness = membership.side(JoinSide::Right).completeness();
		assert!(completeness.membership_complete, "the side's filter must survive a hot key");
		assert_eq!(completeness.revocations.as_u64(), 0);
	}

	#[test]
	fn removing_the_last_row_turns_the_key_into_a_ram_absence() {
		// Multiset accounting: two rows under one hash are two filter instances.
		// The emptiness re-check after each removal (remove_from_state_entry) is a
		// range scan today; once the last instance is gone it must become a RAM
		// answer, and after the FIRST removal the key must still read as present.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let store = Store::new(FlowNodeId(41), JoinSide::Left, membership.clone());
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20), RowPresence::Unknown).unwrap();

		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(1)).unwrap());
		assert!(store.contains_key(&mut txn, &h(0xAAA)).unwrap(), "one row remains");

		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(2)).unwrap());
		let absences_before = membership.side(JoinSide::Left).completeness().absences_served.as_u64();
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert_eq!(
			membership.side(JoinSide::Left).completeness().absences_served.as_u64(),
			absences_before + 1,
			"the post-removal emptiness check must be served by membership, not a range scan"
		);
		assert_eq!(membership.side(JoinSide::Left).completeness().false_positives.as_u64(), 0);
	}

	#[test]
	fn latest_slot_overwrites_with_known_presence_do_not_inflate_membership() {
		// A latest join overwrites its (hash, MAX) slot on every right-side tick.
		// Blind inserts there would grow the filter one instance per tick until the
		// byte cap discards it weeks into a run. The overwrite path knows the slot
		// was occupied (read_right_slot precedes it), passes Live, and the instance
		// count stays exact: one remove after N overwrites must flip to absent.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let store = Store::new(FlowNodeId(42), JoinSide::Right, membership.clone());

		store.put_row(&mut txn, &h(0xAAA), RowNumber::MAX, &row(0x01), RowPresence::New).unwrap();
		for tick in 2..=4u8 {
			store.put_row(&mut txn, &h(0xAAA), RowNumber::MAX, &row(tick), RowPresence::Live).unwrap();
		}
		assert!(store.remove_row(&mut txn, &h(0xAAA), RowNumber::MAX).unwrap());

		let reads_before = txn.store_reads();
		assert!(store.get_row(&mut txn, &h(0xAAA), RowNumber::MAX).unwrap().is_none());
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"exact overwrite accounting must leave zero stale instances behind"
		);
		assert_eq!(membership.side(JoinSide::Right).completeness().false_positives.as_u64(), 0);
	}

	#[test]
	fn a_blind_overcount_degrades_to_a_false_positive_never_a_false_absence() {
		// put_row with Unknown presence inserts blindly; overwriting an existing
		// row leaves a stale instance. The failure direction must be a wasted
		// verify scan (counted as a false positive), NEVER an absent answer for a
		// key that still has rows - that would emit wrong join output silently.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let membership = test_membership();
		let store = Store::new(FlowNodeId(43), JoinSide::Left, membership.clone());

		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x20), RowPresence::Unknown).unwrap();
		assert!(store.remove_row(&mut txn, &h(0xAAA), rn(1)).unwrap());

		assert!(
			!store.contains_key(&mut txn, &h(0xAAA)).unwrap(),
			"the stale instance must cost a verify scan, not change the answer"
		);
		assert_eq!(membership.side(JoinSide::Left).completeness().false_positives.as_u64(), 1);
	}

	#[test]
	fn a_restarted_store_hydrates_membership_from_the_persisted_side() {
		// After a restart the filter is rebuilt by scanning persisted state. A key
		// persisted before the restart must read maybe-present (no false absence),
		// and an unknown key must be a RAM absence again. The hash is no longer in
		// the row key, so hydration has to resolve each group id back to its bytes -
		// if that resolution were dropped the side would install nothing and every
		// persisted key would read as a false absence.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(45);
		let writer = Store::new(node, JoinSide::Right, test_membership());
		writer.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();

		let restarted_membership = test_membership();
		let restarted = Store::new(node, JoinSide::Right, restarted_membership.clone());
		assert!(
			restarted.contains_key(&mut txn, &h(0xAAA)).unwrap(),
			"a persisted key must survive rehydration as present"
		);
		assert!(!restarted.contains_key(&mut txn, &h(0xBBB)).unwrap());
		let completeness = restarted_membership.side(JoinSide::Right).completeness();
		assert!(completeness.membership_complete);
		assert_eq!(completeness.absences_served.as_u64(), 1);
		assert_eq!(completeness.false_positives.as_u64(), 0);
	}

	#[test]
	fn hydration_rebuilds_one_instance_per_persisted_row_on_both_sides() {
		// Hydration is one scan feeding both sides, and the filter is a multiset: a key
		// with two persisted rows must come back with two instances, or the first
		// removal after a restart would flip a key that still has a row to absent and
		// the join would drop live matches.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = FlowNodeId(54);
		let writer = test_membership();
		let left = Store::new(node, JoinSide::Left, writer.clone());
		let right = Store::new(node, JoinSide::Right, writer);
		left.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		left.put_row(&mut txn, &h(0xAAA), rn(2), &row(0x20), RowPresence::Unknown).unwrap();
		right.put_row(&mut txn, &h(0xAAA), rn(3), &row(0x30), RowPresence::Unknown).unwrap();

		let restarted = test_membership();
		let left = Store::new(node, JoinSide::Left, restarted.clone());
		let right = Store::new(node, JoinSide::Right, restarted);

		assert!(left.remove_row(&mut txn, &h(0xAAA), rn(1)).unwrap());
		assert!(
			left.contains_key(&mut txn, &h(0xAAA)).unwrap(),
			"a rehydrated key with two rows must not go absent after one removal"
		);
		assert!(
			right.contains_key(&mut txn, &h(0xAAA)).unwrap(),
			"the same scan must have installed the other side too"
		);

		assert!(left.remove_row(&mut txn, &h(0xAAA), rn(2)).unwrap());
		assert!(!left.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(right.contains_key(&mut txn, &h(0xAAA)).unwrap(), "sides must not share instance counts");
	}
}
