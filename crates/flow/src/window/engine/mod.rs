// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Schema-agnostic windowing state-machine engines. An engine owns accumulator state, late
//! rejection, eviction and diff routing; the caller (the "face") owns extraction and output
//! construction, handing over pre-bucketed events and translating [`WindowResult`]s into diffs.

pub mod config;
pub mod rolling;
pub mod rolling_incremental;
pub mod rolling_top_k;
pub mod tumbling;
pub mod tumbling_carry;

use std::collections::HashMap;

use reifydb_codec::{
	key::{
		encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey},
	},
	row::operator::state::{OperatorState, decode},
};
use reifydb_core::{
	key::operator_state::{
		GroupId, GroupStateKey, IntoGroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range,
	},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::row_number::RowNumber};
use tracing::{debug, instrument};

use crate::{
	operator::{
		state::seal::coord::Coord,
		state_access::{get, modify, remove},
	},
	window::span::{Slot, WindowSpan},
};

/// One contribution routed to a window accumulator.
pub enum AccumulatorEvent<C> {
	Add(C),
	Remove(C),
}

fn note_when_expiry_capped(expired: usize, expire_batch: usize) {
	if expired >= expire_batch {
		debug!(expired, expire_batch, "window expiry hit per-tick batch cap, backlog deferred to next tick");
	}
}

/// How a finalized window value should be emitted downstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitKind {
	Insert,
	Update,
	Remove,
}

/// A finalized window the engine produced; the face turns it into a diff.
pub struct WindowResult<G, Coord, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub span: WindowSpan<Coord>,
	pub value: Output,
	/// The finalized value before this batch's events, for faces that emit a real pre on
	/// Update/Remove. `None` for a brand-new window.
	pub prior: Option<Output>,
	pub kind: EmitKind,
}

/// The highest window start seen for a group, used to drop late events for already-closed windows.
#[operator_state]
#[derive(Debug, Clone)]
pub struct GroupMeta<K> {
	pub high_water: Option<K>,
}

impl<K> Default for GroupMeta<K> {
	fn default() -> Self {
		Self {
			high_water: None,
		}
	}
}

impl<K> HeapSize for GroupMeta<K> {
	fn heap_size(&self) -> usize {
		0
	}
}

/// The group's high-water anchor as a comparable order key, so the meta sweep is uniform across
/// engines. Callers sweep at the seal horizon, so a group below it has seen nothing since its own
/// windows sealed and the late-event rejection this meta drives has nothing left to reject.
pub(crate) trait MetaHighWater: OperatorState {
	fn high_water_order(&self) -> Option<u64>;
}

impl<C: Slot> MetaHighWater for GroupMeta<C> {
	fn high_water_order(&self) -> Option<u64> {
		self.high_water.map(|hw| hw.order_key().to_order())
	}
}

/// `initial` is the persisted value snapshotted at load, served archived without materializing;
/// `bumped` is the batch-local monotonic advance. Only groups with a bump persist.
pub(crate) struct BatchMeta<C> {
	pub(crate) initial: Option<C>,
	pub(crate) bumped: Option<C>,
}

impl<C> Default for BatchMeta<C> {
	fn default() -> Self {
		Self {
			initial: None,
			bumped: None,
		}
	}
}

impl<C: Slot> BatchMeta<C> {
	pub(crate) fn observe(&mut self, coord: C) {
		match self.high_water() {
			Some(hw) if coord > hw => self.bumped = Some(coord),
			None => self.bumped = Some(coord),
			_ => {}
		}
	}

	pub(crate) fn high_water(&self) -> Option<C> {
		self.bumped.or(self.initial)
	}
}

pub(crate) fn load_batch_meta<C>(store: &mut dyn StateStore, key: &MetaKey) -> Result<BatchMeta<C>>
where
	C: Slot,
{
	let initial = get::<_, GroupMeta<C>>(store, key)?.and_then(|meta| meta.high_water);
	Ok(BatchMeta {
		initial,
		bumped: None,
	})
}

pub(crate) fn persist_batch_meta<G, C>(store: &mut dyn StateStore, loaded: HashMap<G, BatchMeta<C>>) -> Result<()>
where
	for<'a> &'a G: IntoEncodedKey,
	C: Slot,
{
	for (group, batch) in loaded {
		let Some(bumped) = batch.bumped else {
			continue;
		};
		modify(store, &meta_key_for(&group), |native: &mut GroupMeta<C>| {
			if native.high_water.is_none_or(|hw| bumped > hw) {
				native.high_water = Some(bumped);
			}
		})?;
	}
	Ok(())
}

/// The internal-key range covering every per-group meta. It lives in the root group because the meta
/// is keyed by partition while a window group is (partition, window), so it fits inside neither group's range.
pub(crate) fn meta_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, Keyspace::WINDOW_META)
}

/// Reclaim every group meta whose high water is strictly below `threshold`.
///
/// `low_water` is a lower bound on the current minimum high water, so a bound already at or above
/// the threshold skips the scan entirely and keeps the steady-state sweep O(1). Staleness is a
/// value, not a key prefix, so the scan itself must cover the whole meta keyspace.
#[instrument(name = "flow::window::sweep_stale_meta", level = "debug", skip_all)]
pub(crate) fn sweep_stale_meta<M>(
	store: &mut dyn StateStore,
	threshold: u64,
	low_water: &mut Option<u64>,
) -> Result<usize>
where
	M: MetaHighWater + Clone + OperatorState + HeapSize,
{
	if low_water.is_some_and(|lw| lw >= threshold) {
		return Ok(0);
	}
	let mut stale: Vec<MetaKey> = Vec::new();
	let mut min_surviving: Option<u64> = None;
	store.state_range_visit(meta_range(), None, &mut |key, bytes| {
		if let Some(hw) = decode::<M>(&bytes)?.high_water_order() {
			if hw < threshold {
				let Some(key) = decode_meta_key(key.as_encoded()) else {
					return Ok(());
				};
				stale.push(key);
			} else {
				min_surviving = Some(min_surviving.map_or(hw, |m| m.min(hw)));
			}
		}
		Ok(())
	})?;
	*low_water = min_surviving;
	let count = stale.len();
	for key in &stale {
		remove(store, key)?;
	}
	Ok(count)
}

/// State-cache key for a group's [`GroupMeta`], tagged into a keyspace distinct from the
/// per-window accumulators.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct MetaKey(pub EncodedKey);

impl HeapSize for MetaKey {
	fn heap_size(&self) -> usize {
		self.0.heap_size()
	}
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct RunningKey {
	pub group: GroupId,
	pub slot: EncodedKey,
}

impl RunningKey {
	pub fn new(group: GroupId, slot: EncodedKey) -> Self {
		Self {
			group,
			slot,
		}
	}

	pub fn of_row(group: GroupId, row: RowNumber) -> Self {
		Self::new(group, EncodedKey::new(encode_u64_asc(row.0)))
	}
}

impl HeapSize for RunningKey {
	fn heap_size(&self) -> usize {
		match &self.slot {
			EncodedKey::Inline {
				..
			} => 0,
			EncodedKey::Shared(bytes) => bytes.len(),
		}
	}
}

impl IntoGroupStateKey for &RunningKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::RUNNING, self.slot.as_bytes())
	}
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct WindowStateKey {
	pub group: GroupId,
	pub slot: EncodedKey,
}

impl WindowStateKey {
	pub fn new(group: GroupId, slot: EncodedKey) -> Self {
		Self {
			group,
			slot,
		}
	}

	pub fn root(slot: EncodedKey) -> Self {
		Self::new(GroupId::ROOT, slot)
	}

	pub fn of_row(group: GroupId, row: RowNumber) -> Self {
		Self::new(group, EncodedKey::new(encode_u64_asc(row.0)))
	}
}

impl HeapSize for WindowStateKey {
	fn heap_size(&self) -> usize {
		match &self.slot {
			EncodedKey::Inline {
				..
			} => 0,
			EncodedKey::Shared(bytes) => bytes.len(),
		}
	}
}

impl IntoGroupStateKey for &WindowStateKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::ACCUMULATOR, self.slot.as_bytes())
	}
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct BufferKey {
	pub group: GroupId,
	pub slot: EncodedKey,
}

impl BufferKey {
	pub fn new(group: GroupId, slot: EncodedKey) -> Self {
		Self {
			group,
			slot,
		}
	}

	pub fn of_row(group: GroupId, row: RowNumber) -> Self {
		Self::new(group, EncodedKey::new(encode_u64_asc(row.0)))
	}
}

impl HeapSize for BufferKey {
	fn heap_size(&self) -> usize {
		match &self.slot {
			EncodedKey::Inline {
				..
			} => 0,
			EncodedKey::Shared(bytes) => bytes.len(),
		}
	}
}

impl IntoGroupStateKey for &BufferKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::BUFFER, self.slot.as_bytes())
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct EmitKey {
	pub group: GroupId,
	pub row: RowNumber,
}

impl EmitKey {
	pub fn new(group: GroupId, row: RowNumber) -> Self {
		Self {
			group,
			row,
		}
	}
}

impl HeapSize for EmitKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &EmitKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::EMIT, encode_u64_asc(self.row.0))
	}
}

impl IntoGroupStateKey for &MetaKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::WINDOW_META, &self.0)
	}
}

pub fn meta_key_for<G>(group: &G) -> MetaKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	MetaKey(group.into_encoded_key())
}

/// Which coordinate a window's expiry-index entry is ordered by, and so which coordinate its seal
/// horizon is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryAnchor {
	/// Never indexed, never swept: count-based and plain aggregation windows have no time
	/// coordinate to expire against.
	Unindexed,
	/// Fixed grid, so a late event can neither extend the horizon nor resurrect a swept window.
	WindowStart,
	/// A session, whose horizon rides the newest event because that is what keeps it open.
	LastEvent,
}

impl ExpiryAnchor {
	/// `last_event` is `None` only when no event time is known, never zero-as-absent: an event at
	/// the epoch is a legitimate coordinate, and reading its zero as "no information" lets a
	/// long-sealed window keep admitting rows.
	pub fn of(&self, window_start: u64, last_event: Option<u64>) -> Option<u64> {
		match self {
			ExpiryAnchor::Unindexed => None,
			ExpiryAnchor::WindowStart => Some(window_start),
			ExpiryAnchor::LastEvent => last_event,
		}
	}
}

pub(crate) fn decode_window_state_key(key: &EncodedKey) -> Option<WindowStateKey> {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	if keyspace != Keyspace::ACCUMULATOR {
		return None;
	}
	Some(WindowStateKey::new(group, EncodedKey::new(suffix)))
}

pub(crate) fn decode_meta_key(key: &EncodedKey) -> Option<MetaKey> {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	(group == GroupId::ROOT && keyspace == Keyspace::WINDOW_META).then(|| MetaKey(EncodedKey::new(suffix)))
}

#[cfg(test)]
mod archived_projection_tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	/// Projects the high water the way `sweep_stale_meta` does: encode, decode, then read it.
	fn via_storage<M: MetaHighWater>(meta: &M) -> Option<u64> {
		let bytes = meta.encode_state().unwrap();
		decode::<M>(&bytes).unwrap().high_water_order()
	}

	#[test]
	fn stored_high_water_yields_the_slot_order_key() {
		// A wrong order key silently drops a live group's meta or keeps dead meta forever.
		let instant = DateTime::from_epoch_millis(1_700_000_000_123).unwrap();
		let datetime_meta = GroupMeta {
			high_water: Some(instant),
		};
		assert_eq!(
			via_storage(&datetime_meta),
			Some(instant.to_order()),
			"the order key is the stored layout, so the projection must round-trip exactly"
		);

		// Adjacent representable instants must keep distinct order keys, or a live group is swept with a dead
		// one.
		let next_instant = GroupMeta {
			high_water: Some(DateTime::from_bits(instant.to_bits() + 1)),
		};
		assert!(via_storage(&next_instant) > via_storage(&datetime_meta));
	}

	#[test]
	fn a_group_that_never_advanced_projects_to_none_through_storage() {
		// An accidental Some(0) compares below every threshold and reclaims meta for live groups.
		let empty: GroupMeta<DateTime> = GroupMeta {
			high_water: None,
		};
		assert_eq!(via_storage(&empty), None);
	}
}
