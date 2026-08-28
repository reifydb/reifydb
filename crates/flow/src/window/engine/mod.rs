// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod config;
pub mod rolling;
pub mod rolling_incremental;
pub mod rolling_top_k;
pub mod tumbling;
pub mod tumbling_carry;

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::{
		encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey},
	},
	row::operator::state::{OperatorState, decode},
};
use reifydb_core::{
	key::operator_state::{
		GroupId, GroupStateKey, IntoGroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range,
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
		state_access::{get_classified, remove, set},
	},
	window::span::{Slot, WindowSpan},
};

pub enum AccumulatorEvent<Contribution> {
	Add(Contribution),
	Remove(Contribution),
}

fn note_when_expiry_capped(expired: usize, expire_batch: usize) {
	if expired >= expire_batch {
		debug!(expired, expire_batch, "window expiry hit per-tick batch cap, backlog deferred to next tick");
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitKind {
	Insert,
	Update,
	Remove,
}

pub struct WindowResult<G, Coord, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub span: WindowSpan<Coord>,
	pub value: Output,
	pub prior: Option<Output>,
	pub kind: EmitKind,
}

#[operator_state]
#[derive(Debug, Clone)]
pub struct GroupMeta<S> {
	pub high_water: Option<S>,
}

impl<S> Default for GroupMeta<S> {
	fn default() -> Self {
		Self {
			high_water: None,
		}
	}
}

impl<S> HeapSize for GroupMeta<S> {
	fn heap_size(&self) -> usize {
		0
	}
}

pub(crate) trait MetaHighWater: OperatorState {
	fn high_water_order(&self) -> Option<u64>;
}

impl<S: Slot> MetaHighWater for GroupMeta<S> {
	fn high_water_order(&self) -> Option<u64> {
		self.high_water.map(|hw| hw.order_key().to_order())
	}
}

pub(crate) struct BatchMeta<S> {
	pub(crate) initial: Option<S>,
	pub(crate) bumped: Option<S>,
}

impl<S> Default for BatchMeta<S> {
	fn default() -> Self {
		Self {
			initial: None,
			bumped: None,
		}
	}
}

impl<S: Slot> BatchMeta<S> {
	pub(crate) fn observe(&mut self, slot: S) {
		match self.high_water() {
			Some(hw) if slot > hw => self.bumped = Some(slot),
			None => self.bumped = Some(slot),
			_ => {}
		}
	}

	pub(crate) fn high_water(&self) -> Option<S> {
		self.bumped.or(self.initial)
	}
}

pub(crate) fn load_batch_meta<S>(store: &mut dyn StateStore, key: &MetaKey) -> Result<BatchMeta<S>>
where
	S: Slot,
{
	let initial = get_classified::<_, GroupMeta<S>>(store, key)?.and_then(|meta| meta.high_water);
	Ok(BatchMeta {
		initial,
		bumped: None,
	})
}

pub(crate) fn persist_batch_meta<G, S>(store: &mut dyn StateStore, loaded: HashMap<G, BatchMeta<S>>) -> Result<()>
where
	for<'a> &'a G: IntoEncodedKey,
	S: Slot,
{
	for (group, batch) in loaded {
		let Some(bumped) = batch.bumped else {
			continue;
		};
		set(
			store,
			&meta_key_for(&group),
			&GroupMeta {
				high_water: Some(bumped),
			},
		)?;
	}
	Ok(())
}

pub(crate) fn meta_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, KeyspaceId::WINDOW_META)
}

const META_SWEEP_PAGE: usize = 1024;

#[derive(Default)]
pub(crate) struct MetaSweep {
	low_water: Option<u64>,
	cursor: Option<EncodedKey>,
	surviving: Option<u64>,
}

impl MetaSweep {
	#[instrument(name = "flow::window::sweep_stale_meta", level = "debug", skip_all)]
	pub(crate) fn sweep<M>(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<usize>
	where
		M: MetaHighWater + Clone + OperatorState + HeapSize,
	{
		if self.cursor.is_none() && self.low_water.is_some_and(|lw| lw >= threshold) {
			return Ok(0);
		}
		let full = meta_range();
		let range = match self.cursor.take() {
			Some(key) => EncodedKeyRange::new(Bound::Excluded(key), full.end),
			None => full,
		};
		let mut stale: Vec<MetaKey> = Vec::new();
		let mut surviving = self.surviving;
		let mut visited = 0usize;
		let mut furthest: Option<EncodedKey> = None;
		store.state_range_visit(range, Some(META_SWEEP_PAGE), &mut |key, bytes| {
			visited += 1;
			let high_water = decode::<M>(&bytes)?.high_water_order();
			let encoded = key.into_encoded();
			if let Some(hw) = high_water {
				if hw < threshold {
					if let Some(meta) = decode_meta_key(&encoded) {
						stale.push(meta);
					}
				} else {
					surviving = Some(surviving.map_or(hw, |m| m.min(hw)));
				}
			}
			furthest = Some(encoded);
			Ok(())
		})?;
		if visited < META_SWEEP_PAGE {
			self.low_water = surviving;
			self.surviving = None;
		} else {
			self.low_water = None;
			self.surviving = surviving;
			self.cursor = furthest;
		}
		let count = stale.len();
		for key in &stale {
			remove(store, key)?;
		}
		Ok(count)
	}
}

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
		OperatorStateKey::inner_encoded(self.group, KeyspaceId::RUNNING, self.slot.as_bytes())
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
		OperatorStateKey::inner_encoded(self.group, KeyspaceId::ACCUMULATOR, self.slot.as_bytes())
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
		OperatorStateKey::inner_encoded(self.group, KeyspaceId::BUFFER, self.slot.as_bytes())
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
		OperatorStateKey::inner_encoded(self.group, KeyspaceId::EMIT, encode_u64_asc(self.row.0))
	}
}

impl IntoGroupStateKey for &MetaKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::WINDOW_META, &self.0)
	}
}

pub fn meta_key_for<G>(group: &G) -> MetaKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	MetaKey(group.into_encoded_key())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryAnchor {
	Unindexed,
	WindowStart,
	LastEvent,
}

impl ExpiryAnchor {
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
	if keyspace != KeyspaceId::ACCUMULATOR {
		return None;
	}
	Some(WindowStateKey::new(group, EncodedKey::new(suffix)))
}

pub(crate) fn decode_meta_key(key: &EncodedKey) -> Option<MetaKey> {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	(group == GroupId::ROOT && keyspace == KeyspaceId::WINDOW_META).then(|| MetaKey(EncodedKey::new(suffix)))
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

#[cfg(test)]
mod meta_sweep_tests {
	use reifydb_value::{factory::time::at_millis, value::datetime::DateTime};

	use super::*;
	use crate::operator::state::mock::MockStore;

	fn meta_key(index: u32) -> MetaKey {
		MetaKey(EncodedKey::new(index.to_be_bytes().to_vec()))
	}

	fn seed(store: &mut MockStore, count: u32, high_water: impl Fn(u32) -> DateTime) {
		for index in 0..count {
			set(
				store,
				&meta_key(index),
				&GroupMeta {
					high_water: Some(high_water(index)),
				},
			)
			.expect("seeding a group meta must succeed");
		}
	}

	#[test]
	fn a_meta_sweep_stops_at_one_page_and_resumes_past_its_cursor() {
		// The sweep runs on every window apply over a keyspace that grows with the group count, so
		// one call must never walk more than a page. Without the parked cursor the next call
		// restarts at the first key and the tail is never reached at all.
		let mut store = MockStore::default();
		let total = META_SWEEP_PAGE as u32 + 3;
		seed(&mut store, total, |_| at_millis(200));

		let mut sweep = MetaSweep::default();
		let threshold = at_millis(50).to_order();

		assert_eq!(sweep.sweep::<GroupMeta<DateTime>>(&mut store, threshold).unwrap(), 0);
		assert_eq!(store.rows_visited(), META_SWEEP_PAGE, "one call must visit at most one page");

		assert_eq!(sweep.sweep::<GroupMeta<DateTime>>(&mut store, threshold).unwrap(), 0);
		assert_eq!(
			store.rows_visited(),
			total as usize,
			"the next call must resume past the cursor rather than rescan the first page"
		);
	}

	#[test]
	fn a_paged_meta_sweep_publishes_the_low_water_of_every_page_it_walked() {
		// The low-water guard skips the whole scan while the smallest surviving high water is at or
		// above the threshold. A pass that spans several pages must fold every page into that
		// minimum: publishing only the final page's minimum makes the guard skip a group that has
		// since gone stale, and its meta then leaks forever.
		let mut store = MockStore::default();
		let total = META_SWEEP_PAGE as u32 + 3;
		seed(&mut store, total, |index| {
			if index == 0 {
				at_millis(100)
			} else {
				at_millis(200)
			}
		});

		let mut sweep = MetaSweep::default();
		let early = at_millis(50).to_order();
		assert_eq!(sweep.sweep::<GroupMeta<DateTime>>(&mut store, early).unwrap(), 0);
		assert_eq!(sweep.sweep::<GroupMeta<DateTime>>(&mut store, early).unwrap(), 0);

		let walked = store.rows_visited();
		assert_eq!(sweep.sweep::<GroupMeta<DateTime>>(&mut store, at_millis(90).to_order()).unwrap(), 0);
		assert_eq!(
			store.rows_visited(),
			walked,
			"a completed revolution must publish a low water the guard can skip on"
		);

		assert_eq!(
			sweep.sweep::<GroupMeta<DateTime>>(&mut store, at_millis(150).to_order()).unwrap(),
			1,
			"the group whose high water sits below the threshold is stale and must be reclaimed"
		);
	}
}
