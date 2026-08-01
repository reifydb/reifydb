// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::{decode_u64_asc, encode_u64_asc, encoded::EncodedKeyRange},
};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Diff},
	key::operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey},
	value::column::columns::Columns,
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	error::Error,
	util::{
		cowvec::CowVec,
		hash::{Hash128, xxh3_64},
	},
	value::{blob::Blob, row_number::RowNumber},
};
use serde::{Deserialize, Serialize};

use crate::{
	error::FlowStateError,
	operator::{
		join::{
			Identity,
			operator::JoinOperator,
			store::Store,
			strategy::{
				UpdateKeys,
				hash::{columns_from_block, stream_join_blocks_encoded},
			},
		},
		stateful::utils::{state_get, state_range, state_remove, state_set},
	},
};

const ROW_NUMBER_BYTES: usize = 8;
const TAG_BYTES: usize = 1;
const TAG_JOINED: u8 = 0;
const TAG_UNMATCHED: u8 = 1;
const SLOT: RowNumber = RowNumber::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishedRight {
	Row(RowNumber),
	Unmatched,
}

pub(crate) fn snapshot_ledger_keyspaces(snapshot: bool) -> Vec<Keyspace> {
	match snapshot {
		true => vec![Keyspace::JOIN_PUBLISHED, Keyspace::JOIN_PIN],
		false => Vec::new(),
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ContentVersion(u64);

impl ContentVersion {
	pub(crate) fn of(encoded: &EncodedRow) -> Self {
		Self(xxh3_64(&encoded.0).0)
	}
}

#[derive(Serialize, Deserialize)]
struct Pin {
	refs: u64,

	retired: Option<Vec<u8>>,
}

pub(crate) struct SnapshotLedger {
	node_id: OperatorId,
}

impl SnapshotLedger {
	pub(crate) fn new(node_id: OperatorId) -> Self {
		Self {
			node_id,
		}
	}

	fn published_key(&self, group: GroupId, left: RowNumber, right: RowNumber) -> GroupStateKey {
		self.tagged_key(group, left, TAG_JOINED, right)
	}

	fn unmatched_key(&self, group: GroupId, left: RowNumber) -> GroupStateKey {
		self.tagged_key(group, left, TAG_UNMATCHED, RowNumber(0))
	}

	fn tagged_key(&self, group: GroupId, left: RowNumber, tag: u8, right: RowNumber) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(2 * ROW_NUMBER_BYTES + TAG_BYTES);
		suffix.extend_from_slice(&encode_u64_asc(left.0));
		suffix.push(tag);
		suffix.extend_from_slice(&encode_u64_asc(right.0));
		OperatorGroupStateKey::inner_encoded(group, Keyspace::JOIN_PUBLISHED, suffix)
	}

	fn published_prefix(&self, group: GroupId, left: RowNumber) -> EncodedKeyRange {
		let prefix =
			OperatorGroupStateKey::inner_encoded(group, Keyspace::JOIN_PUBLISHED, encode_u64_asc(left.0));
		EncodedKeyRange::prefix(prefix.as_ref())
	}

	fn pin_key(&self, group: GroupId, right: RowNumber, version: ContentVersion) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(2 * ROW_NUMBER_BYTES);
		suffix.extend_from_slice(&encode_u64_asc(right.0));
		suffix.extend_from_slice(&encode_u64_asc(version.0));
		OperatorGroupStateKey::inner_encoded(group, Keyspace::JOIN_PIN, suffix)
	}

	pub(crate) fn publish(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		left: RowNumber,
		right: RowNumber,
		content: &EncodedRow,
	) -> Result<()> {
		let version = ContentVersion::of(content);
		let key = self.published_key(group, left, right);
		if let Some(existing) = state_get(self.node_id, txn, &key)? {
			let previous = decode_version(&existing)?;
			if previous == version {
				return Ok(());
			}
			self.unpin(txn, group, right, previous)?;
		}
		txn.stamp_side(self.node_id, group, Keyspace::JOIN_PUBLISHED)?;
		txn.stamp_side(self.node_id, group, Keyspace::JOIN_PIN)?;
		state_set(self.node_id, txn, &key, encode_version(version))?;
		self.pin(txn, group, right, version)
	}

	pub(crate) fn published(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		left: RowNumber,
	) -> Result<Vec<(PublishedRight, ContentVersion)>> {
		let mut out = Vec::new();
		for entry in state_range(self.node_id, txn, self.published_prefix(group, left)) {
			let (key, row) = entry?;
			let Some(right) = decode_published(key.as_slice()) else {
				continue;
			};
			out.push((right, decode_version(&row)?));
		}
		Ok(out)
	}

	pub(crate) fn publish_unmatched(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		left: RowNumber,
	) -> Result<()> {
		let key = self.unmatched_key(group, left);
		if state_get(self.node_id, txn, &key)?.is_some() {
			return Ok(());
		}
		txn.stamp_side(self.node_id, group, Keyspace::JOIN_PUBLISHED)?;
		txn.stamp_side(self.node_id, group, Keyspace::JOIN_PIN)?;
		state_set(self.node_id, txn, &key, encode_version(ContentVersion(0)))
	}

	pub(crate) fn release_unmatched(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		left: RowNumber,
	) -> Result<()> {
		state_remove(self.node_id, txn, &self.unmatched_key(group, left))
	}

	pub(crate) fn release(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		left: RowNumber,
		right: RowNumber,
	) -> Result<Option<EncodedRow>> {
		let key = self.published_key(group, left, right);
		let Some(row) = state_get(self.node_id, txn, &key)? else {
			return Ok(None);
		};
		let version = decode_version(&row)?;
		state_remove(self.node_id, txn, &key)?;
		self.unpin(txn, group, right, version)
	}

	pub(crate) fn retire(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		right: RowNumber,
		content: &EncodedRow,
	) -> Result<()> {
		let version = ContentVersion::of(content);
		let key = self.pin_key(group, right, version);
		let Some(existing) = state_get(self.node_id, txn, &key)? else {
			return Ok(());
		};
		let mut pin = decode_pin(&existing)?;
		if pin.retired.is_some() {
			return Ok(());
		}
		pin.retired = Some(content.0.to_vec());
		state_set(self.node_id, txn, &key, encode_pin(&pin)?)
	}

	fn pin(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		right: RowNumber,
		version: ContentVersion,
	) -> Result<()> {
		let key = self.pin_key(group, right, version);
		let mut pin = match state_get(self.node_id, txn, &key)? {
			Some(existing) => decode_pin(&existing)?,
			None => Pin {
				refs: 0,
				retired: None,
			},
		};
		pin.refs += 1;
		state_set(self.node_id, txn, &key, encode_pin(&pin)?)
	}

	fn unpin(
		&self,
		txn: &mut FlowTransaction,
		group: GroupId,
		right: RowNumber,
		version: ContentVersion,
	) -> Result<Option<EncodedRow>> {
		let key = self.pin_key(group, right, version);
		let Some(existing) = state_get(self.node_id, txn, &key)? else {
			return Ok(None);
		};
		let mut pin = decode_pin(&existing)?;
		pin.refs = pin.refs.saturating_sub(1);
		let content = pin.retired.clone().map(|bytes| EncodedRow(CowVec::new(bytes)));
		match pin.refs {
			0 => state_remove(self.node_id, txn, &key)?,
			_ => state_set(self.node_id, txn, &key, encode_pin(&pin)?)?,
		}
		Ok(content)
	}
}

fn decode_published(bytes: &[u8]) -> Option<PublishedRight> {
	if bytes.len() < ROW_NUMBER_BYTES + TAG_BYTES {
		return None;
	}
	match bytes[bytes.len() - ROW_NUMBER_BYTES - TAG_BYTES] {
		TAG_UNMATCHED => Some(PublishedRight::Unmatched),
		TAG_JOINED => {
			let suffix: [u8; ROW_NUMBER_BYTES] = bytes[bytes.len() - ROW_NUMBER_BYTES..].try_into().ok()?;
			Some(PublishedRight::Row(RowNumber(decode_u64_asc(suffix))))
		}
		_ => None,
	}
}

fn encode_version(version: ContentVersion) -> EncodedRow {
	let shape = RowShape::operator_state();
	let mut row = shape.allocate();
	shape.set_blob(&mut row, 0, &Blob::from(version.0.to_le_bytes().to_vec()));
	row
}

fn decode_version(row: &EncodedRow) -> Result<ContentVersion> {
	let shape = RowShape::operator_state();
	let blob = shape.get_blob(row, 0);
	let bytes: [u8; 8] = blob.as_bytes().try_into().map_err(|_| {
		Error::from(FlowStateError::Decode {
			state: "snapshot published version",
			cause: "expected eight bytes".to_string(),
		})
	})?;
	Ok(ContentVersion(u64::from_le_bytes(bytes)))
}

fn encode_pin(pin: &Pin) -> Result<EncodedRow> {
	let serialized = to_stdvec(pin).map_err(|e| {
		Error::from(FlowStateError::Encode {
			state: "snapshot pin",
			cause: e.to_string(),
		})
	})?;
	let shape = RowShape::operator_state();
	let mut row = shape.allocate();
	shape.set_blob(&mut row, 0, &Blob::from(serialized));
	Ok(row)
}

fn decode_pin(row: &EncodedRow) -> Result<Pin> {
	let shape = RowShape::operator_state();
	let blob = shape.get_blob(row, 0);
	from_bytes(blob.as_ref()).map_err(|e| {
		Error::from(FlowStateError::Decode {
			state: "snapshot pin",
			cause: e.to_string(),
		})
	})
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;

	use super::*;

	const NODE: OperatorId = OperatorId(90);
	const GROUP: GroupId = GroupId(3);

	fn ledger() -> SnapshotLedger {
		SnapshotLedger::new(NODE)
	}

	fn row(payload: &[u8]) -> EncodedRow {
		let shape = RowShape::operator_state();
		let mut r = shape.allocate();
		shape.set_blob(&mut r, 0, &Blob::from(payload.to_vec()));
		r
	}

	fn rn(v: u64) -> RowNumber {
		RowNumber(v)
	}

	fn published_order(right: &PublishedRight) -> (u8, u64) {
		match right {
			PublishedRight::Unmatched => (0, 0),
			PublishedRight::Row(number) => (1, number.0),
		}
	}

	#[test]
	fn a_released_pair_reads_back_the_version_it_was_published_against() {
		// The right row moves on after the left row read it, and the withdrawal still has to carry
		// what was actually published - not what the right side holds now, and not nothing.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();
		let published_against = row(b"v1");

		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &published_against).unwrap();
		ledger.retire(&mut txn, GROUP, rn(7), &published_against).unwrap();

		let released = ledger.release(&mut txn, GROUP, rn(1), rn(7)).unwrap();
		assert_eq!(released, Some(published_against), "the retired version must come back verbatim");
	}

	#[test]
	fn a_version_still_live_on_the_right_side_is_not_copied() {
		// A snapshot join whose right side never changes must store no row copies at all - only the
		// counts. Copying eagerly would make the ledger cost a duplicate of the whole right side.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();

		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &row(b"v1")).unwrap();

		assert_eq!(
			ledger.release(&mut txn, GROUP, rn(1), rn(7)).unwrap(),
			None,
			"an un-retired version must send the caller to the right side rather than duplicate it"
		);
	}

	#[test]
	fn one_retired_version_serves_every_left_row_that_published_against_it() {
		// The pin is keyed on the version rather than the pair so a right row matched by many
		// left rows is stored once, not once per match.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();
		let shared = row(b"shared");

		for left in 1..=3u64 {
			ledger.publish(&mut txn, GROUP, rn(left), rn(7), &shared).unwrap();
		}
		ledger.retire(&mut txn, GROUP, rn(7), &shared).unwrap();

		for left in 1..=3u64 {
			assert_eq!(
				ledger.release(&mut txn, GROUP, rn(left), rn(7)).unwrap(),
				Some(shared.clone()),
				"left row {left} must still see the version it published against"
			);
		}
	}

	#[test]
	fn a_pin_outlives_every_reference_and_no_longer() {
		// A record that lingered past its last reference would leave one retired copy per right
		// row the join ever changed, for the life of the node.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();
		let content = row(b"v1");
		let version = ContentVersion::of(&content);

		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &content).unwrap();
		ledger.publish(&mut txn, GROUP, rn(2), rn(7), &content).unwrap();
		ledger.retire(&mut txn, GROUP, rn(7), &content).unwrap();

		ledger.release(&mut txn, GROUP, rn(1), rn(7)).unwrap();
		let key = ledger.pin_key(GROUP, rn(7), version);
		assert!(
			state_get(NODE, &mut txn, &key).unwrap().is_some(),
			"a version another left row still references must stay"
		);

		ledger.release(&mut txn, GROUP, rn(2), rn(7)).unwrap();
		assert!(
			state_get(NODE, &mut txn, &key).unwrap().is_none(),
			"the last release must take the record with it"
		);
	}

	#[test]
	fn two_versions_of_one_right_row_are_pinned_apart() {
		// Different left rows can hold different versions of one right row, so a single slot per
		// row would let the second retirement overwrite the first and a left row would withdraw
		// content it never published.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();
		let first = row(b"v1");
		let second = row(b"v2");

		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &first).unwrap();
		ledger.retire(&mut txn, GROUP, rn(7), &first).unwrap();
		ledger.publish(&mut txn, GROUP, rn(2), rn(7), &second).unwrap();
		ledger.retire(&mut txn, GROUP, rn(7), &second).unwrap();

		assert_eq!(ledger.release(&mut txn, GROUP, rn(1), rn(7)).unwrap(), Some(first));
		assert_eq!(ledger.release(&mut txn, GROUP, rn(2), rn(7)).unwrap(), Some(second));
	}

	#[test]
	fn republishing_a_left_row_moves_its_reference_to_the_new_version() {
		// A left row touched again reads the right side afresh, so its old reference has to go or
		// the version it used to hold is pinned for the left row's whole life.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();
		let first = row(b"v1");
		let second = row(b"v2");
		let stale = ledger.pin_key(GROUP, rn(7), ContentVersion::of(&first));

		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &first).unwrap();
		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &second).unwrap();

		assert!(
			state_get(NODE, &mut txn, &stale).unwrap().is_none(),
			"the version the left row no longer holds must be released"
		);
		assert_eq!(
			ledger.published(&mut txn, GROUP, rn(1)).unwrap(),
			vec![(PublishedRight::Row(rn(7)), ContentVersion::of(&second))],
			"and the pair must now name the version it was republished against"
		);
	}

	#[test]
	fn the_published_set_is_scoped_to_one_left_row() {
		// Withdrawal walks this set by left row, so a scan that reached a neighbour would withdraw
		// rows belonging to a left row that is still live.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();

		ledger.publish(&mut txn, GROUP, rn(1), rn(7), &row(b"a")).unwrap();
		ledger.publish(&mut txn, GROUP, rn(1), rn(8), &row(b"b")).unwrap();
		ledger.publish(&mut txn, GROUP, rn(2), rn(9), &row(b"c")).unwrap();

		let mut rights: Vec<PublishedRight> =
			ledger.published(&mut txn, GROUP, rn(1)).unwrap().into_iter().map(|(r, _)| r).collect();
		rights.sort_by_key(published_order);
		assert_eq!(rights, vec![PublishedRight::Row(rn(7)), PublishedRight::Row(rn(8))]);
		assert_eq!(
			ledger.published(&mut txn, GROUP, rn(2))
				.unwrap()
				.into_iter()
				.map(|(r, _)| r)
				.collect::<Vec<_>>(),
			vec![PublishedRight::Row(rn(9))]
		);
	}

	#[test]
	fn releasing_a_pair_that_was_never_published_changes_nothing() {
		// Withdrawal also runs for left rows that published nothing (an inner join whose key had
		// no matches), so it must be a no-op rather than an error or a phantom diff.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();

		assert_eq!(ledger.release(&mut txn, GROUP, rn(1), rn(7)).unwrap(), None);
		assert!(ledger.published(&mut txn, GROUP, rn(1)).unwrap().is_empty());
	}

	#[test]
	fn retiring_a_version_nothing_published_against_stores_nothing() {
		// On a busy right side rows change before any left row reads them; copying there would
		// make the ledger cost track right-side churn instead of what was published.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let ledger = ledger();
		let content = row(b"v1");

		ledger.retire(&mut txn, GROUP, rn(7), &content).unwrap();

		let key = ledger.pin_key(GROUP, rn(7), ContentVersion::of(&content));
		assert!(state_get(NODE, &mut txn, &key).unwrap().is_none());
	}
}

pub(crate) struct SnapshotJoinContext<'a> {
	pub(crate) ledger: &'a SnapshotLedger,
	pub(crate) operator: &'a JoinOperator,
	pub(crate) right_store: &'a Store,
}

pub(crate) fn publish_joined(
	txn: &mut FlowTransaction,
	ctx: &SnapshotJoinContext,
	key_hash: &Hash128,
	left: &Columns,
	left_indices: &[usize],
	outer: bool,
) -> Result<Vec<Diff>> {
	if left_indices.is_empty() {
		return Ok(Vec::new());
	}
	let group = ctx.right_store.group_for(txn, key_hash)?;
	let left_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers()[idx]).collect();

	let mut diffs = stream_join_blocks_encoded(txn, ctx.right_store, key_hash, true, |txn, opposite, encoded| {
		let opposite_indices: Vec<usize> = (0..opposite.row_count()).collect();
		let joined = ctx.operator.join_columns_cartesian(
			txn,
			left,
			left_indices,
			opposite,
			&opposite_indices,
			Identity::Mint,
		)?;
		if joined.is_empty() {
			return Ok(Vec::new());
		}
		for left_number in &left_numbers {
			for (right_number, content) in encoded {
				ctx.ledger.publish(txn, group, *left_number, *right_number, content)?;
			}
		}
		Ok(joined.published())
	})?;

	if !diffs.is_empty() || !outer {
		return Ok(diffs);
	}
	for left_number in &left_numbers {
		ctx.ledger.publish_unmatched(txn, group, *left_number)?;
	}
	diffs.extend(ctx.operator.unmatched_left_columns_batch(txn, left, left_indices, Identity::Mint)?.published());
	Ok(diffs)
}

pub(crate) fn withdraw_joined(
	txn: &mut FlowTransaction,
	ctx: &SnapshotJoinContext,
	key_hash: &Hash128,
	left: &Columns,
	left_idx: usize,
) -> Result<Vec<Diff>> {
	let Some(group) = ctx.right_store.group_of(txn, key_hash)? else {
		return Ok(Vec::new());
	};
	let left_number = left.row_numbers()[left_idx];
	let mut out = Vec::new();
	for (right, _) in ctx.ledger.published(txn, group, left_number)? {
		let right_number = match right {
			PublishedRight::Unmatched => {
				ctx.ledger.release_unmatched(txn, group, left_number)?;
				let unmatched =
					ctx.operator.unmatched_left_columns(txn, left, left_idx, Identity::Consume)?;
				out.extend(unmatched.withdrawn());
				continue;
			}
			PublishedRight::Row(right_number) => right_number,
		};
		let released = ctx.ledger.release(txn, group, left_number, right_number)?;
		let content = match released {
			Some(retired) => Some(retired),
			None => ctx.right_store.get_row(txn, key_hash, right_number)?,
		};
		let Some(content) = content else {
			continue;
		};
		let opposite = columns_from_block(txn, ctx.right_store, vec![(right_number, content)])?;
		let joined = ctx.operator.join_columns_cartesian(
			txn,
			left,
			&[left_idx],
			&opposite,
			&[0],
			Identity::Consume,
		)?;
		out.extend(joined.withdrawn());
	}
	Ok(out)
}

pub(crate) fn resync_joined(
	txn: &mut FlowTransaction,
	ctx: &SnapshotJoinContext,
	keys: UpdateKeys,
	pre: &Columns,
	post: &Columns,
	left_idx: usize,
	outer: bool,
) -> Result<Vec<Diff>> {
	let mut out = withdraw_joined(txn, ctx, keys.pre, pre, left_idx)?;
	out.extend(publish_joined(txn, ctx, keys.post, post, &[left_idx], outer)?);
	Ok(out)
}

pub(crate) fn publish_slot(
	txn: &mut FlowTransaction,
	ctx: &SnapshotJoinContext,
	key_hash: &Hash128,
	left: &Columns,
	left_indices: &[usize],
	outer: bool,
) -> Result<Option<Columns>> {
	if left_indices.is_empty() {
		return Ok(None);
	}
	let group = ctx.right_store.group_for(txn, key_hash)?;
	let left_numbers: Vec<RowNumber> = left_indices.iter().map(|&idx| left.row_numbers()[idx]).collect();

	let Some(content) = ctx.right_store.get_row(txn, key_hash, SLOT)? else {
		if !outer {
			return Ok(None);
		}
		for left_number in &left_numbers {
			ctx.ledger.publish_unmatched(txn, group, *left_number)?;
		}
		return Ok(Some(ctx.operator.unmatched_left_latest(left, left_indices)));
	};

	for left_number in &left_numbers {
		ctx.ledger.publish(txn, group, *left_number, SLOT, &content)?;
	}
	let slot = columns_from_block(txn, ctx.right_store, vec![(SLOT, content)])?;
	Ok(Some(ctx.operator.join_left_with_slot(left, left_indices, &slot)))
}

pub(crate) fn withdraw_slot(
	txn: &mut FlowTransaction,
	ctx: &SnapshotJoinContext,
	key_hash: &Hash128,
	left: &Columns,
	left_idx: usize,
) -> Result<Option<Columns>> {
	let Some(group) = ctx.right_store.group_of(txn, key_hash)? else {
		return Ok(None);
	};
	let left_number = left.row_numbers()[left_idx];
	for (right, _) in ctx.ledger.published(txn, group, left_number)? {
		let right_number = match right {
			PublishedRight::Unmatched => {
				ctx.ledger.release_unmatched(txn, group, left_number)?;
				return Ok(Some(ctx.operator.unmatched_left_latest(left, &[left_idx])));
			}
			PublishedRight::Row(right_number) => right_number,
		};
		let released = ctx.ledger.release(txn, group, left_number, right_number)?;
		let content = match released {
			Some(retired) => Some(retired),
			None => ctx.right_store.get_row(txn, key_hash, right_number)?,
		};
		let Some(content) = content else {
			continue;
		};
		let slot = columns_from_block(txn, ctx.right_store, vec![(right_number, content)])?;
		return Ok(Some(ctx.operator.join_left_with_slot(left, &[left_idx], &slot)));
	}
	Ok(None)
}

pub(crate) fn retire_slot(txn: &mut FlowTransaction, ctx: &SnapshotJoinContext, key_hash: &Hash128) -> Result<()> {
	retire_right(txn, ctx, key_hash, SLOT)
}

pub(crate) fn retire_right(
	txn: &mut FlowTransaction,
	ctx: &SnapshotJoinContext,
	key_hash: &Hash128,
	row_number: RowNumber,
) -> Result<()> {
	let Some(group) = ctx.right_store.group_of(txn, key_hash)? else {
		return Ok(());
	};
	let Some(content) = ctx.right_store.get_row(txn, key_hash, row_number)? else {
		return Ok(());
	};
	ctx.ledger.retire(txn, group, row_number, &content)
}
