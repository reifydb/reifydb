// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Schema-agnostic windowing state-machine engines. An engine owns accumulator state, late
//! rejection, eviction and diff routing; the caller (the "face") owns extraction and output
//! construction, handing over pre-bucketed events and translating [`WindowResult`]s into diffs.

pub mod config;
pub(crate) mod expiry;
pub mod rolling;
pub mod rolling_incremental;
pub mod rolling_top_k;
pub mod tumbling;
pub mod tumbling_carry;

use std::collections::HashMap;

use reifydb_codec::{
	key::{
		encode_u64, encode_u64_asc,
		encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey},
	},
	row::operator::{OperatorState, decode},
};
use reifydb_core::{
	key::operator_state::{
		GroupId, GroupStateKey, IntoGroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range,
	},
	metrics::heap::HeapSize,
	state::{cache::StateCache, store::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::row_number::RowNumber};
use tracing::{debug, instrument};

use crate::window::span::{Slot, WindowCoord, WindowSpan};

/// One contribution routed to a window accumulator.
pub enum AccumulatorEvent<C> {
	Add(C),
	Remove(C),
}

/// Anchors strictly below the returned horizon are sealed: immutable and eligible for reclamation.
///
/// `seal_after` is the coordinate's own span type, which is what stops a millisecond span being
/// subtracted from a nanosecond instant.
pub fn seal_horizon<C: WindowCoord>(watermark: C, seal_after: C::Span) -> C {
	watermark.saturating_sub_span(seal_after)
}

/// Strictly below: a window sitting exactly one seal span behind the watermark is still reachable
/// by a late event, so sealing it would discard a legitimate retraction.
pub fn is_sealed<C: WindowCoord>(anchor: C, horizon: C) -> bool {
	anchor < horizon
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

pub(crate) fn load_batch_meta<S, C>(
	store: &mut S,
	meta: &mut StateCache<MetaKey, GroupMeta<C>>,
	key: &MetaKey,
) -> Result<BatchMeta<C>>
where
	S: StateStore,
	C: Slot,
{
	let initial = meta.get(store, key)?.and_then(|meta| meta.high_water);
	Ok(BatchMeta {
		initial,
		bumped: None,
	})
}

pub(crate) fn persist_batch_meta<S, G, C>(
	store: &mut S,
	meta: &mut StateCache<MetaKey, GroupMeta<C>>,
	loaded: HashMap<G, BatchMeta<C>>,
) -> Result<()>
where
	S: StateStore,
	for<'a> &'a G: IntoEncodedKey,
	C: Slot,
{
	for (group, batch) in loaded {
		let Some(bumped) = batch.bumped else {
			continue;
		};
		meta.modify(store, &meta_key_for(&group), |native| {
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
pub(crate) fn sweep_stale_meta<S, M>(
	store: &mut S,
	meta: &mut StateCache<MetaKey, M>,
	threshold: u64,
	low_water: &mut Option<u64>,
) -> Result<usize>
where
	S: StateStore,
	M: MetaHighWater + Clone + OperatorState + HeapSize,
{
	if low_water.is_some_and(|lw| lw >= threshold) {
		return Ok(0);
	}
	meta.flush(store)?;
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
		meta.remove(store, key)?;
	}
	meta.flush(store)?;
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

/// Every accumulator kept in the root group, for engines whose windows are not yet interned as groups. A
/// group-scoped engine cannot hydrate through one range; its accumulators sit inside their own group.
pub(crate) fn accumulator_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, Keyspace::ACCUMULATOR)
}

pub(crate) fn buffer_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, Keyspace::BUFFER)
}

pub(crate) fn running_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, Keyspace::RUNNING)
}

/// The due-ordered expiry index lives in the root group so a group's entries survive the phase-1 range
/// delete and drain on their own.
pub(crate) fn expiry_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::ROOT, Keyspace::EXPIRY)
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

pub fn expiry_key<G>(expiry: u64, group: &G, suffix: &[u8]) -> GroupStateKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	let group = group.into_encoded_key();
	let group = group.as_ref();
	let mut tail = Vec::with_capacity(8 + group.len() + suffix.len());
	tail.extend_from_slice(&encode_u64(expiry));
	tail.extend_from_slice(group);
	tail.extend_from_slice(suffix);
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::EXPIRY, tail)
}

fn decode_group_slot_key(keyspace: Keyspace, key: &EncodedKey) -> Option<(GroupId, EncodedKey)> {
	let (group, found, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	if found != keyspace {
		return None;
	}
	Some((group, EncodedKey::new(suffix)))
}

pub(crate) fn decode_buffer_key(key: &EncodedKey) -> Option<BufferKey> {
	decode_group_slot_key(Keyspace::BUFFER, key).map(|(group, slot)| BufferKey::new(group, slot))
}

pub(crate) fn decode_running_key(key: &EncodedKey) -> Option<RunningKey> {
	decode_group_slot_key(Keyspace::RUNNING, key).map(|(group, slot)| RunningKey::new(group, slot))
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
pub(crate) mod test_support {
	use std::{
		collections::{BTreeMap, HashMap},
		ops::Bound,
	};

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::operator::{EncodedOperatorRow, decode},
	};
	use reifydb_core::{
		key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
		metrics::heap::HeapSize,
		state::store::StateStore,
	};
	use reifydb_macro::operator_state;
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use crate::{timer::Timer, window::accumulator::WindowAccumulator};

	/// One wheel mutation a shell issued, in issue order. Arm and disarm are distinct variants
	/// because the pair is order-sensitive: a disarm landing after its arm cancels a live timer.
	#[derive(Debug, Clone, PartialEq, Eq)]
	pub(crate) enum RecordedTimer {
		Armed(Timer),
		Disarmed(Timer),
	}

	impl RecordedTimer {
		pub(crate) fn armed(at: DateTime, kind: TimerKind, key: EncodedKey) -> Self {
			Self::Armed(Timer {
				at,
				kind,
				key,
			})
		}

		pub(crate) fn disarmed(at: DateTime, kind: TimerKind, key: EncodedKey) -> Self {
			Self::Disarmed(Timer {
				at,
				kind,
				key,
			})
		}
	}

	#[derive(Default)]
	pub(crate) struct MockStore {
		data: HashMap<Vec<u8>, EncodedOperatorRow>,
		groups: HashMap<Vec<u8>, GroupId>,
		rows: HashMap<(GroupId, Vec<u8>), u64>,
		next_row: u64,
		accumulator_reads: usize,
		timers: Option<Vec<RecordedTimer>>,
		flow_watermark: Option<DateTime>,
	}

	impl MockStore {
		/// Opt in to recording wheel mutations. The default store still refuses them, so the
		/// engine suites keep proving that the engine itself never touches the wheel.
		pub(crate) fn recording_timers() -> Self {
			Self {
				timers: Some(Vec::new()),
				..Self::default()
			}
		}

		pub(crate) fn timers(&self) -> &[RecordedTimer] {
			self.timers.as_deref().unwrap_or_default()
		}

		fn record_timer(&mut self, recorded: RecordedTimer) -> Result<()> {
			let Some(timers) = self.timers.as_mut() else {
				unreachable!("the window engine never touches timers; only the shell above it does")
			};
			timers.push(recorded);
			Ok(())
		}

		/// Batched lookups that reached the accumulator keyspace. Point reads and range scans are
		/// not counted: the question this serves is how many futile round trips a batch pays.
		pub(crate) fn accumulator_reads(&self) -> usize {
			self.accumulator_reads
		}

		fn note_reads(&mut self, keys: &[GroupStateKey]) {
			self.accumulator_reads += keys
				.iter()
				.filter(|key| {
					OperatorStateKey::decode_inner(key.as_slice())
						.is_some_and(|(_, found, _)| found == Keyspace::ACCUMULATOR)
				})
				.count();
		}

		fn keyspace_count(&self, keyspace: Keyspace) -> usize {
			self.data
				.keys()
				.filter(|k| {
					OperatorStateKey::decode_inner(k).is_some_and(|(_, found, _)| found == keyspace)
				})
				.count()
		}

		pub(crate) fn index_entry_count(&mut self) -> usize {
			self.keyspace_count(Keyspace::EXPIRY)
		}

		pub(crate) fn buffer_entry_count(&mut self) -> usize {
			self.keyspace_count(Keyspace::BUFFER)
		}

		pub(crate) fn buffer_coord_count<A: WindowAccumulator>(&mut self) -> usize {
			self.data
				.iter()
				.filter(|(k, _)| {
					OperatorStateKey::decode_inner(k)
						.is_some_and(|(_, found, _)| found == Keyspace::BUFFER)
				})
				.map(|(_, bytes)| {
					decode::<BTreeMap<u64, A>>(bytes)
						.expect("persisted window buffer must decode")
						.len()
				})
				.sum()
		}

		pub(crate) fn running_entry_count(&mut self) -> usize {
			self.keyspace_count(Keyspace::RUNNING)
		}

		pub(crate) fn meta_entry_count(&mut self) -> usize {
			self.keyspace_count(Keyspace::WINDOW_META)
		}

		/// Simulates phase-1 group reclamation: the accumulators are erased while the
		/// due-ordered expiry index, which lives outside the group's range, is left behind.
		pub(crate) fn drop_accumulator_entries(&mut self) -> usize {
			let keys: Vec<Vec<u8>> = self
				.data
				.keys()
				.filter(|k| {
					OperatorStateKey::decode_inner(k)
						.is_some_and(|(_, found, _)| found == Keyspace::ACCUMULATOR)
				})
				.cloned()
				.collect();
			for key in &keys {
				self.data.remove(key);
			}
			keys.len()
		}

		/// The same phase, widened to every data keyspace a group can hold - the shape engines that
		/// keep no ACCUMULATOR see. The root group is spared, and the row-number mapping survives on
		/// top of that because it is an identity keyspace rather than a data one.
		pub(crate) fn drop_group_data_entries(&mut self) -> usize {
			let keys: Vec<Vec<u8>> = self
				.data
				.keys()
				.filter(|k| {
					OperatorStateKey::decode_inner(k)
						.is_some_and(|(group, found, _)| !group.is_root() && found.is_data())
				})
				.cloned()
				.collect();
			for key in &keys {
				self.data.remove(key);
			}
			keys.len()
		}

		pub(crate) fn mapping_entry_count(&mut self) -> usize {
			self.keyspace_count(Keyspace::ROW_NUMBER_MAPPING)
		}

		pub(crate) fn seed_mapping_key(&mut self, suffix: u8) {
			self.data.insert(
				OperatorStateKey::inner_encoded(
					GroupId::ROOT,
					Keyspace::ROW_NUMBER_MAPPING,
					vec![suffix],
				)
				.as_slice()
				.to_vec(),
				EncodedOperatorRow::new(&[0u8], DateTime::EPOCH),
			);
		}

		pub(crate) fn contains_row_mapping(&self, group: GroupId, key: &EncodedKey) -> bool {
			self.rows.contains_key(&(group, key.as_bytes().to_vec()))
		}
	}

	use reifydb_core::state::store::TimerKind;

	impl StateStore for MockStore {
		fn arm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
			self.record_timer(RecordedTimer::armed(at, kind, key.clone()))
		}

		fn disarm_timer(&mut self, at: DateTime, kind: TimerKind, key: &EncodedKey) -> Result<()> {
			self.record_timer(RecordedTimer::disarmed(at, kind, key.clone()))
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(self.flow_watermark)
		}

		fn intern_group(&mut self, group: &EncodedKey) -> Result<GroupId> {
			let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
			Ok(*self.groups.entry(group.as_bytes().to_vec()).or_insert(next))
		}

		fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
			Ok(self.groups.get(group.as_bytes()).copied())
		}

		fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedOperatorRow>> {
			Ok(self.data.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[GroupStateKey],
			visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
		) -> Result<()> {
			self.note_reads(keys);
			for key in keys {
				if let Some(b) = self.data.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> Result<()> {
			self.data.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
			self.data.remove(key.as_slice());
			Ok(())
		}
		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> Result<()>,
		) -> Result<()> {
			let after_start = |k: &[u8]| match &range.start {
				Bound::Included(s) => k >= s.as_bytes(),
				Bound::Excluded(s) => k > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let before_end = |k: &[u8]| match &range.end {
				Bound::Included(e) => k <= e.as_bytes(),
				Bound::Excluded(e) => k < e.as_bytes(),
				Bound::Unbounded => true,
			};
			let mut matched: Vec<(Vec<u8>, EncodedOperatorRow)> = self
				.data
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			for (k, b) in matched {
				let Some(k) = GroupStateKey::from_framed(EncodedKey::new(k)) else {
					continue;
				};
				visit(k, b)?;
			}
			Ok(())
		}
		fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
			let slot = (group, key.as_bytes().to_vec());
			if let Some(rn) = self.rows.get(&slot) {
				return Ok((RowNumber(*rn), false));
			}
			self.next_row += 1;
			self.rows.insert(slot, self.next_row);
			Ok((RowNumber(self.next_row), true))
		}
		fn get_or_create_row_numbers(
			&mut self,
			group: GroupId,
			keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			keys.iter().map(|k| self.get_or_create_row_number(group, k)).collect()
		}
		fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
			self.rows.remove(&(group, key.as_bytes().to_vec()));
			Ok(())
		}
		fn written_at(&self) -> DateTime {
			DateTime::EPOCH
		}
	}

	#[operator_state]
	#[derive(Clone, Debug, Default)]
	pub(crate) struct SumAccumulator {
		pub sum: i64,
		pub count: u64,
	}

	impl HeapSize for SumAccumulator {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for SumAccumulator {
		type Contribution = i64;
		type Output = i64;

		fn add(&mut self, contribution: &i64) {
			self.sum += *contribution;
			self.count += 1;
		}
		fn remove(&mut self, contribution: &i64) {
			self.sum -= *contribution;
			self.count = self.count.saturating_sub(1);
		}
		fn finalize(&self) -> Option<i64> {
			if self.count == 0 {
				None
			} else {
				Some(self.sum)
			}
		}
		fn is_empty(&self) -> bool {
			self.count == 0
		}
		fn merge(&mut self, other: &Self) {
			self.sum += other.sum;
			self.count += other.count;
		}
		fn unmerge(&mut self, other: &Self) {
			self.sum -= other.sum;
			self.count = self.count.saturating_sub(other.count);
		}
	}
}

#[cfg(test)]
mod archived_projection_tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	/// Projects the high water the way `sweep_stale_meta` does: encode, decode, then read it.
	fn via_storage<M: MetaHighWater>(meta: &M) -> Option<u64> {
		let bytes = meta.encode_state(DateTime::EPOCH).unwrap();
		decode::<M>(&bytes).unwrap().high_water_order()
	}

	#[test]
	fn stored_high_water_yields_the_slot_order_key() {
		// A wrong order key silently drops a live group's meta or keeps dead meta forever.
		let millis = 1_700_000_000_123u64;
		let datetime_meta = GroupMeta {
			high_water: Some(DateTime::from_epoch_millis(millis).unwrap()),
		};
		assert_eq!(
			via_storage(&datetime_meta),
			Some(millis),
			"DateTime orders by milliseconds, not by stored layout"
		);

		// Nanoseconds surviving into the order key would sweep every live group on the first tick.
		let sub_milli = GroupMeta {
			high_water: Some(DateTime::from_nanos(millis * 1_000_000 + 999_999)),
		};
		assert_eq!(via_storage(&sub_milli), Some(millis));
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
