// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{
	encoded::{
		row::EncodedRow,
		shape::{RowShape, RowShapeField, cache::RowShapeCacheCell, fingerprint::RowShapeFingerprint},
	},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
#[cfg(test)]
use reifydb_core::interface::catalog::config::{ConfigKey, GetConfig};
use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{blob::Blob, row_number::RowNumber},
};

use super::state::JoinSide;
use crate::{
	error::FlowStateError,
	operator::stateful::{
		membership::{KeyspaceMembership, MembershipAnswer, fold_hash128},
		utils::{state_drop, state_get, state_range, state_range_versioned, state_remove, state_set},
	},
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

pub(crate) struct Store {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
	schema_prefix: u8,
	shape_cache: RowShapeCacheCell,
	membership: Arc<KeyspaceMembership>,
}

impl Store {
	pub(crate) fn new(node_id: FlowNodeId, side: JoinSide, membership: Arc<KeyspaceMembership>) -> Self {
		let (prefix, schema_byte) = match side {
			JoinSide::Left => (vec![0x01], 0x03u8),
			JoinSide::Right => (vec![0x02], 0x04u8),
		};
		Self {
			node_id,
			prefix,
			schema_prefix: schema_byte,
			shape_cache: RowShapeCacheCell::new(SHAPE_CACHE_CAPACITY),
			membership,
		}
	}

	fn hash_from_row_key(&self, key: &[u8]) -> Option<Hash128> {
		if key.len() != self.prefix.len() + HASH_BYTES + ROW_NUMBER_BYTES || !key.starts_with(&self.prefix) {
			return None;
		}
		let mut bytes = [0u8; HASH_BYTES];
		bytes.copy_from_slice(&key[self.prefix.len()..self.prefix.len() + HASH_BYTES]);
		Some(Hash128(u128::from_le_bytes(bytes)))
	}

	fn ensure_membership_hydrated(&self, txn: &mut FlowTransaction) -> Result<()> {
		if self.membership.is_hydrated() {
			return Ok(());
		}
		let mut hashes: Vec<u64> = Vec::new();
		for entry in state_range(self.node_id, txn, EncodedKeyRange::prefix(&self.prefix)) {
			let (key, _) = entry?;
			if let Some(hash) = self.hash_from_row_key(key.as_ref()) {
				hashes.push(fold_hash128(&hash));
			}
		}
		self.membership.install(&hashes);
		Ok(())
	}

	fn schema_key(&self, fingerprint: RowShapeFingerprint) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + 8);
		bytes.push(self.schema_prefix);
		bytes.extend_from_slice(&fingerprint.to_le_bytes());
		EncodedKey::new(bytes)
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
		presence: RowPresence,
	) -> Result<()> {
		self.ensure_membership_hydrated(txn)?;
		match presence {
			RowPresence::Live => {}
			RowPresence::New | RowPresence::Unknown => self.membership.insert(fold_hash128(hash)),
		}
		let key = self.row_key(hash, row_number);
		state_set(self.node_id, txn, &key, encoded.clone())
	}

	pub(crate) fn get_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<Option<EncodedRow>> {
		self.ensure_membership_hydrated(txn)?;
		if self.membership.probe(fold_hash128(hash)) == MembershipAnswer::DefinitelyAbsent {
			return Ok(None);
		}
		let key = self.row_key(hash, row_number);
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
		if self.membership.probe(fold_hash128(hash)) == MembershipAnswer::DefinitelyAbsent {
			return Ok(false);
		}
		let key = self.row_key(hash, row_number);
		if state_get(self.node_id, txn, &key)?.is_none() {
			return Ok(false);
		}
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
		if self.membership.probe(fold_hash128(hash)) == MembershipAnswer::DefinitelyAbsent {
			return Ok(false);
		}
		let key = self.row_key(hash, row_number);
		let existed = state_get(self.node_id, txn, &key)?.is_some();
		if existed {
			state_remove(self.node_id, txn, &key)?;
			self.membership.remove(fold_hash128(hash));
		}
		Ok(existed)
	}

	pub(crate) fn evict_expired(
		&self,
		txn: &mut FlowTransaction,
		cutoff_version: CommitVersion,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<()> {
		let base = EncodedKeyRange::prefix(&self.prefix);
		let start = match cursor.clone() {
			Some(c) => Bound::Excluded(c),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch =
			state_range_versioned(self.node_id, txn, range).take(batch_size).collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < batch_size;
		let last_key = batch.last().map(|(key, _, _)| key.clone());

		for (key, version, _row) in batch {
			if version > cutoff_version {
				continue;
			}
			state_drop(self.node_id, txn, &key)?;
			if let Some(hash) = self.hash_from_row_key(key.as_ref()) {
				self.membership.remove(fold_hash128(&hash));
			}
		}

		*cursor = if reached_end {
			None
		} else {
			last_key
		};
		Ok(())
	}

	pub(crate) fn rows_for_key_block(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		after: Option<&RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedRow)>> {
		self.ensure_membership_hydrated(txn)?;
		let answer = self.membership.probe(fold_hash128(hash));
		if after.is_none() && answer == MembershipAnswer::DefinitelyAbsent {
			return Ok(Vec::new());
		}
		let prefix = self.hash_prefix(hash);
		let mut range = EncodedKeyRange::prefix(&prefix);
		if let Some(after) = after {
			range.start = Bound::Excluded(self.row_key(hash, *after));
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
			self.membership.record_store_miss();
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
		let answer = self.membership.probe(fold_hash128(hash));
		if answer == MembershipAnswer::DefinitelyAbsent {
			return Ok(false);
		}
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		let found = state_range(self.node_id, txn, range).next().transpose()?.is_some();
		if !found && answer == MembershipAnswer::MaybePresent {
			self.membership.record_store_miss();
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
	Some(RowNumber(u64::from_be_bytes(suffix)))
}

#[cfg(test)]
mod tests {
	use reifydb_codec::encoded::row::EncodedRow;
	use reifydb_core::common::CommitVersion;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::value::value_type::ValueType;

	use super::*;
	use crate::operator::stateful::membership::MEMBERSHIP_BYTE_CAP;

	fn test_membership() -> Arc<KeyspaceMembership> {
		Arc::new(KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP))
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
		// every block. DefinitelyAbsent short-circuits before any store access, so
		// the absences_served counter plus the zero-read point probes pin that the
		// answer came from RAM.
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

		let completeness = membership.completeness();
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

		let completeness = membership.completeness();
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
		let absences_before = membership.completeness().absences_served.as_u64();
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert_eq!(
			membership.completeness().absences_served.as_u64(),
			absences_before + 1,
			"the post-removal emptiness check must be served by membership, not a range scan"
		);
		assert_eq!(membership.completeness().false_positives.as_u64(), 0);
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
		assert_eq!(membership.completeness().false_positives.as_u64(), 0);
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
		assert_eq!(membership.completeness().false_positives.as_u64(), 1);
	}

	#[test]
	fn eviction_maintains_membership_for_every_dropped_row() {
		// The TTL sweep drops rows outside any probe path; if it left the filter
		// untouched every expired key would read as maybe-present forever and the
		// filter would degrade to a pass-through. Dropped keys must become RAM
		// absences.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(2)).deferred();
		let membership = test_membership();
		let store = Store::new(FlowNodeId(44), JoinSide::Left, membership.clone());
		store.put_row(&mut txn, &h(0xAAA), rn(1), &row(0x10), RowPresence::Unknown).unwrap();
		store.put_row(&mut txn, &h(0xBBB), rn(2), &row(0x20), RowPresence::Unknown).unwrap();

		let mut cursor = None;
		store.evict_expired(&mut txn, CommitVersion(u64::MAX), &mut cursor, 128).unwrap();
		assert!(cursor.is_none(), "a single batch must clear the whole side");

		let absences_before = membership.completeness().absences_served.as_u64();
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(!store.contains_key(&mut txn, &h(0xBBB)).unwrap());
		assert_eq!(
			membership.completeness().absences_served.as_u64(),
			absences_before + 2,
			"evicted keys must be RAM absences, not range scans"
		);
		assert_eq!(membership.completeness().false_positives.as_u64(), 0);
	}

	#[test]
	fn a_restarted_store_hydrates_membership_from_the_persisted_side() {
		// After a restart the filter is rebuilt by scanning the side prefix. A key
		// persisted before the restart must read maybe-present (no false absence),
		// and an unknown key must be a RAM absence again.
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
		let completeness = restarted_membership.completeness();
		assert!(completeness.membership_complete);
		assert_eq!(completeness.absences_served.as_u64(), 1);
		assert_eq!(completeness.false_positives.as_u64(), 0);
	}
}
