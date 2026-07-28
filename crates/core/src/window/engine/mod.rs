// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Schema-agnostic windowing state-machine engines.
//!
//! Each engine owns the per-(group,window) accumulator state, high-water late
//! rejection, eviction, and diff routing (`Insert -> add`,
//! `Update -> remove(pre) + add(post)`, `Remove -> remove(pre)`). The caller
//! (the "face") owns extraction (`row -> (group, coord, contribution)`) and
//! output construction; it hands the engine pre-bucketed events and receives
//! [`WindowResult`]s to translate into diffs.

pub mod config;
pub(crate) mod expiry;
pub mod multi_rolling;
pub mod rolling;
pub mod rolling_incremental;
pub mod tumbling;
pub mod tumbling_carry;

use std::collections::HashMap;

use reifydb_codec::{
	key::{
		encode_u64,
		encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey},
	},
	state::OperatorState,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::row_number::RowNumber};
use rkyv::{munge::munge, option::ArchivedOption, seal::Seal};

use crate::{
	key::operator_state::{GroupId, IntoStateKey, Keyspace, OperatorStateKey, StateKey, keyspace_inner_range},
	metrics::heap::HeapSize,
	state::{
		cache::{StateCache, StateView},
		store::StateStore,
	},
	window::span::{Slot, WindowSpan},
};

/// One contribution routed to a window accumulator.
pub enum AccumulatorEvent<C> {
	Add(C),
	Remove(C),
}

/// The seal horizon: window anchors (window start for bucketed engines, the
/// coordinate for rolling ledgers) strictly below this value are sealed -
/// immutable and eligible for state reclamation. Computed by the face as
/// `watermark - seal_after`, where `seal_after` folds the window span and the
/// grace duration into one number in coordinate units.
pub fn seal_horizon(watermark: u64, seal_after: u64) -> u64 {
	watermark.saturating_sub(seal_after)
}

pub fn is_sealed(anchor: u64, horizon: u64) -> bool {
	anchor < horizon
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
	/// The finalized value before this batch's events, when the window was
	/// non-empty (used by faces that emit a real pre on Update/Remove). `None`
	/// for a brand-new window. Faces that don't need it (the sdk drivers)
	/// ignore it.
	pub prior: Option<Output>,
	pub kind: EmitKind,
}

/// Per-group metadata: the highest window start seen, used to drop late events
/// for already-closed windows.
#[operator_state(seal)]
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

/// Read the group's high-water anchor as a comparable order key, shared by every
/// engine's per-group meta so the meta sweep is uniform. A group whose high water
/// has fallen below the sweep threshold has stopped advancing (no recent events)
/// and its meta is safe to reclaim: the meta only drives late-event rejection, and
/// by the time the threshold (>= the operator's lateness/retention) passes it, any
/// late event for the group is already past its horizon.
pub(crate) trait MetaHighWater: OperatorState {
	fn archived_high_water_order(archived: &Self::Archived) -> Option<u64>;
}

impl<C: Slot> MetaHighWater for GroupMeta<C> {
	fn archived_high_water_order(archived: &Self::Archived) -> Option<u64> {
		archived.high_water.as_ref().map(C::archived_order_key)
	}
}

impl<C: Slot> GroupMeta<C> {
	fn seal_bump(seal: Seal<'_, ArchivedGroupMeta<C>>, bumped: C) -> Option<()> {
		munge!(let ArchivedGroupMeta { high_water } = seal);
		let payload = ArchivedOption::as_seal(high_water)?;
		C::seal_write(payload, bumped).then_some(())
	}
}

/// Per-batch high-water tracking for one group: `initial` is the persisted
/// value snapshotted at load (served archived, no materialize), `bumped` the
/// batch-local monotonic advance. Only groups with a bump persist, via a
/// sealed in-place write when the archive carries a payload.
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
	let initial = meta
		.read(store, key, |view| match view {
			StateView::Archived(archived) => {
				GroupMeta::<C>::archived_high_water_order(archived).map(C::from_order_key)
			}
			StateView::Native(native) => native.high_water,
		})?
		.flatten();
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
		meta.modify_in_place(
			store,
			&meta_key_for(&group),
			|seal| GroupMeta::<C>::seal_bump(seal, bumped),
			|native| {
				if native.high_water.is_none_or(|hw| bumped > hw) {
					native.high_water = Some(bumped);
				}
			},
		)?;
	}
	Ok(())
}

/// The internal-key range covering every per-group meta, used by the sweep.
///
/// Node scoped: the meta is keyed by partition while a window group is
/// (partition, window), so it cannot live inside either group's range.
pub(crate) fn meta_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::WINDOW_META)
}

/// Reclaim every group meta whose high water is strictly below `threshold`.
///
/// `low_water` is the smallest high water among the groups that survived the previous sweep - a lower
/// bound on the current minimum, since a group's high water only advances and a newly-seen group starts
/// at an unsealed window (>= the caller's seal horizon >= `threshold`, so it can never be the stale
/// minimum). When the bound is already at/above the threshold nothing can be stale and the whole scan is
/// skipped - the steady-state case, so most apply-time sweeps are O(1). The full scan runs only when the
/// threshold has crossed that minimum (the oldest group has genuinely gone stale); it then drops every
/// stale meta in one pass and recomputes the bound to the smallest surviving high water.
///
/// Staleness is a value, not a key prefix, so the scan must cover the whole meta keyspace (a key-bounded
/// scan would only ever see the lowest-keyed groups). It flushes the meta cache first so the scan sees
/// the latest high water, drops stale keys through the cache (never bypassing it), and flushes the drops.
/// Scoped to the meta keyspace, so row-number mappings and accumulators are untouched.
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
		if let Some(hw) = M::archived_high_water_order(M::archived(&bytes)?) {
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

/// State-cache key for a group's [`GroupMeta`], tagged so it lives in a
/// distinct keyspace from the per-window accumulators.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct MetaKey(pub EncodedKey);

impl HeapSize for MetaKey {
	fn heap_size(&self) -> usize {
		self.0.heap_size()
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct RunningKey {
	pub group: GroupId,
	pub row: RowNumber,
}

impl RunningKey {
	pub fn new(group: GroupId, row: RowNumber) -> Self {
		Self {
			group,
			row,
		}
	}

	pub fn node_scoped(row: RowNumber) -> Self {
		Self::new(GroupId::NODE_SCOPE, row)
	}
}

impl HeapSize for RunningKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &RunningKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::RUNNING, self.row.0.to_be_bytes().to_vec())
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct WindowStateKey {
	pub group: GroupId,
	pub row: RowNumber,
}

impl WindowStateKey {
	pub fn new(group: GroupId, row: RowNumber) -> Self {
		Self {
			group,
			row,
		}
	}

	pub fn node_scoped(row: RowNumber) -> Self {
		Self::new(GroupId::NODE_SCOPE, row)
	}
}

impl HeapSize for WindowStateKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &WindowStateKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::ACCUMULATOR, self.row.0.to_be_bytes().to_vec())
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct BufferKey {
	pub group: GroupId,
	pub row: RowNumber,
}

impl BufferKey {
	pub fn new(group: GroupId, row: RowNumber) -> Self {
		Self {
			group,
			row,
		}
	}

	pub fn node_scoped(row: RowNumber) -> Self {
		Self::new(GroupId::NODE_SCOPE, row)
	}
}

impl HeapSize for BufferKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &BufferKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::BUFFER, self.row.0.to_be_bytes().to_vec())
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

impl IntoStateKey for &EmitKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.group, Keyspace::EMIT, self.row.0.to_be_bytes())
	}
}

impl IntoStateKey for &MetaKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::WINDOW_META, &self.0)
	}
}

pub fn meta_key_for<G>(group: &G) -> MetaKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	MetaKey(group.into_encoded_key())
}

/// Every node-scoped accumulator, for engines whose windows are not yet interned
/// as groups. A group-scoped engine cannot hydrate through one range: its
/// accumulators sit inside their own group, not in a shared keyspace.
pub(crate) fn accumulator_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::ACCUMULATOR)
}

pub(crate) fn buffer_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::BUFFER)
}

pub(crate) fn running_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::RUNNING)
}

/// The due-ordered expiry index, node scoped so a group's entries survive the
/// phase-1 range delete and drain on their own.
pub(crate) fn expiry_range() -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::EXPIRY)
}

pub fn expiry_key<G>(expiry: u64, group: &G, suffix: &[u8]) -> StateKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	let group = group.into_encoded_key();
	let group = group.as_ref();
	let mut tail = Vec::with_capacity(8 + group.len() + suffix.len());
	tail.extend_from_slice(&encode_u64(expiry));
	tail.extend_from_slice(group);
	tail.extend_from_slice(suffix);
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::EXPIRY, tail)
}

fn decode_group_row_key(keyspace: Keyspace, key: &EncodedKey) -> Option<(GroupId, RowNumber)> {
	let (group, found, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	if found != keyspace {
		return None;
	}
	let row = u64::from_be_bytes(suffix.try_into().ok()?);
	Some((group, RowNumber(row)))
}

pub(crate) fn decode_buffer_key(key: &EncodedKey) -> Option<BufferKey> {
	decode_group_row_key(Keyspace::BUFFER, key).map(|(group, row)| BufferKey::new(group, row))
}

pub(crate) fn decode_running_key(key: &EncodedKey) -> Option<RunningKey> {
	decode_group_row_key(Keyspace::RUNNING, key).map(|(group, row)| RunningKey::new(group, row))
}

pub(crate) fn decode_window_state_key(key: &EncodedKey) -> Option<WindowStateKey> {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	if keyspace != Keyspace::ACCUMULATOR {
		return None;
	}
	let row = u64::from_be_bytes(suffix.try_into().ok()?);
	Some(WindowStateKey::new(group, RowNumber(row)))
}

pub(crate) fn decode_meta_key(key: &EncodedKey) -> Option<MetaKey> {
	let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	(group == GroupId::NODE_SCOPE && keyspace == Keyspace::WINDOW_META).then(|| MetaKey(EncodedKey::new(suffix)))
}

#[cfg(test)]
pub(crate) mod test_support {
	use std::{collections::HashMap, ops::Bound};

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		state::{StateBytes, decode_state},
	};
	use reifydb_macro::operator_state;
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use crate::{
		key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey},
		metrics::heap::HeapSize,
		state::{horizon::GroupPosition, map::PersistedMap, store::StateStore},
		window::accumulator::WindowAccumulator,
	};

	#[derive(Default)]
	pub(crate) struct MockStore {
		data: HashMap<Vec<u8>, StateBytes>,
		groups: HashMap<Vec<u8>, GroupId>,
		rows: HashMap<(GroupId, Vec<u8>), u64>,
		next_row: u64,
		accumulator_reads: usize,
	}

	impl MockStore {
		/// Point and batch lookups that reached the accumulator keyspace. Range scans
		/// (hydration) are deliberately not counted: the question these serve is how
		/// many futile round trips a batch pays, not how it warms.
		pub(crate) fn accumulator_reads(&self) -> usize {
			self.accumulator_reads
		}

		fn note_reads(&mut self, keys: &[StateKey]) {
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
					decode_state::<PersistedMap<u64, A>>(bytes)
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

		pub(crate) fn mapping_entry_count(&mut self) -> usize {
			self.keyspace_count(Keyspace::ROW_NUMBER_MAPPING)
		}

		pub(crate) fn seed_mapping_key(&mut self, suffix: u8) {
			self.data.insert(
				OperatorStateKey::inner_encoded(
					GroupId::NODE_SCOPE,
					Keyspace::ROW_NUMBER_MAPPING,
					vec![suffix],
				)
				.as_slice()
				.to_vec(),
				StateBytes::from_archive(&[0u8], DateTime::EPOCH),
			);
		}

		pub(crate) fn contains_row_mapping(&self, group: GroupId, key: &EncodedKey) -> bool {
			self.rows.contains_key(&(group, key.as_bytes().to_vec()))
		}
	}

	impl StateStore for MockStore {
		fn intern_group(&mut self, group: &EncodedKey, _position: GroupPosition) -> Result<GroupId> {
			let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
			Ok(*self.groups.entry(group.as_bytes().to_vec()).or_insert(next))
		}

		fn lookup_group(&mut self, group: &EncodedKey) -> Result<Option<GroupId>> {
			Ok(self.groups.get(group.as_bytes()).copied())
		}

		fn state_get(&mut self, key: &StateKey) -> Result<Option<StateBytes>> {
			Ok(self.data.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[StateKey],
			visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			self.note_reads(keys);
			for key in keys {
				if let Some(b) = self.data.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &StateKey, payload: StateBytes) -> Result<()> {
			self.data.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &StateKey) -> Result<()> {
			self.data.remove(key.as_slice());
			Ok(())
		}
		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(StateKey, StateBytes) -> Result<()>,
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
			let mut matched: Vec<(Vec<u8>, StateBytes)> = self
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
				let Some(k) = StateKey::from_framed(EncodedKey::new(k)) else {
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
		fn clock_now(&self) -> DateTime {
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

	#[operator_state]
	#[derive(Clone, Debug, Default)]
	pub(crate) struct StampedSum {
		pub sum: i64,
		pub count: u64,
		pub stamp: Option<u64>,
	}

	impl HeapSize for StampedSum {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for StampedSum {
		type Contribution = (i64, u64);
		type Output = i64;

		fn add(&mut self, contribution: &(i64, u64)) {
			self.sum += contribution.0;
			self.count += 1;
			self.stamp = Some(self.stamp.map_or(contribution.1, |s| s.max(contribution.1)));
		}
		fn remove(&mut self, contribution: &(i64, u64)) {
			self.sum -= contribution.0;
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
		fn stamp(&self) -> Option<u64> {
			self.stamp
		}
	}
}

#[cfg(test)]
mod archived_projection_tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	/// Project the high water the way `sweep_stale_meta` does: encode, then
	/// read the archive without ever materializing the value.
	fn via_archive<M: MetaHighWater>(meta: &M) -> Option<u64> {
		let bytes = meta.encode_state(DateTime::EPOCH).unwrap();
		M::archived_high_water_order(M::archived(&bytes).unwrap())
	}

	#[test]
	fn archived_high_water_yields_the_slot_order_key() {
		// sweep_stale_meta reclaims purely on this projection, so a wrong order
		// key here silently drops the meta of a live group (breaking late-event
		// rejection) or keeps dead meta forever. The expected values are spelled
		// out rather than derived, so the archive is checked against the meaning
		// of the order key and not against another implementation of it. Slots
		// whose order key is not the identity matter most: that conversion is
		// where a wrong archived read hides.
		let u64_meta = GroupMeta {
			high_water: Some(4242u64),
		};
		assert_eq!(via_archive(&u64_meta), Some(4242));

		let millis = 1_700_000_000_123u64;
		let datetime_meta = GroupMeta {
			high_water: Some(DateTime::from_timestamp_millis(millis).unwrap()),
		};
		assert_eq!(
			via_archive(&datetime_meta),
			Some(millis),
			"DateTime orders by milliseconds, not by archived layout"
		);

		// Sub-millisecond detail is below the coordinate resolution, so it must not reach the
		// order key through either path. If the archived read kept nanoseconds it would compare
		// against a millisecond cutoff and sweep every live group on the first tick.
		let sub_milli = GroupMeta {
			high_water: Some(DateTime::from_nanos(millis * 1_000_000 + 999_999)),
		};
		assert_eq!(via_archive(&sub_milli), Some(millis));
	}

	#[test]
	fn a_group_that_never_advanced_projects_to_none_through_the_archive() {
		// None must survive the archive as None. An accidental Some(0) here
		// would compare below every threshold and make the sweep reclaim meta
		// for groups that simply have not seen an event yet.
		let empty: GroupMeta<u64> = GroupMeta {
			high_water: None,
		};
		assert_eq!(via_archive(&empty), None);
	}
}
