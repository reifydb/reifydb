// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::size_of;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey};
use reifydb_core::{
	key::operator_state::{GroupId, GroupSet, Keyspace, OperatorStateKey, keyspace_inner_range},
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{budget::OperatorStateBudgetHandle, cache::StateCache, store::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, row_number::RowNumber},
};

#[operator_state]
#[derive(Clone, Default)]
pub(super) struct WatermarkState {
	pub value: u64,
}

impl HeapSize for WatermarkState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub(super) struct CountState {
	pub value: u64,
}

impl HeapSize for CountState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub(super) struct RowIndexState {
	pub window_ids: Vec<u64>,
}

impl HeapSize for RowIndexState {
	fn heap_size(&self) -> usize {
		self.window_ids.len() * size_of::<u64>()
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub(super) struct SessionState {
	pub session_id: u64,
	pub last_event_time: u64,
	pub session_start: u64,
}

impl HeapSize for SessionState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub(super) struct EngineMeta {
	pub group_hash: u128,
	pub window_start: u64,
	pub row_number: u64,
	pub last_event_time: u64,
	pub group_values: Vec<Value>,
}

impl HeapSize for EngineMeta {
	fn heap_size(&self) -> usize {
		self.group_values.capacity() * size_of::<Value>()
			+ self.group_values.iter().map(|v| v.heap_size()).sum::<usize>()
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub(super) struct RollingMeta {
	pub group_hash: u128,
	pub row_number: u64,
	pub group_values: Vec<Value>,
	pub last_value: Vec<Value>,
}

impl HeapSize for RollingMeta {
	fn heap_size(&self) -> usize {
		(self.group_values.capacity() + self.last_value.capacity()) * size_of::<Value>()
			+ self.group_values.iter().map(|v| v.heap_size()).sum::<usize>()
			+ self.last_value.iter().map(|v| v.heap_size()).sum::<usize>()
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) enum WatermarkKind {
	Event,
	Expiry,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct WatermarkKey(pub WatermarkKind);

impl HeapSize for WatermarkKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &WatermarkKey {
	fn into_encoded_key(self) -> EncodedKey {
		let disc = match self.0 {
			WatermarkKind::Event => 0u8,
			WatermarkKind::Expiry => 1u8,
		};
		OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::WATERMARK, vec![disc])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct CountKey(pub Hash128);

impl HeapSize for CountKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &CountKey {
	fn into_encoded_key(self) -> EncodedKey {
		encode_partition(Keyspace::COUNT, self.0)
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct RowIndexKey(pub Hash128, pub RowNumber);

impl HeapSize for RowIndexKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &RowIndexKey {
	fn into_encoded_key(self) -> EncodedKey {
		let mut suffix = Vec::with_capacity(16 + 8);
		suffix.extend_from_slice(&self.0.0.to_be_bytes());
		suffix.extend_from_slice(&self.1.0.to_be_bytes());
		OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::ROW_INDEX, suffix)
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct SessionKey(pub Hash128);

impl HeapSize for SessionKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &SessionKey {
	fn into_encoded_key(self) -> EncodedKey {
		encode_partition(Keyspace::SESSION, self.0)
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct EngineMetaKey(pub GroupId);

impl HeapSize for EngineMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &EngineMetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ENGINE_META, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct RollingMetaKey(pub GroupId);

impl HeapSize for RollingMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &RollingMetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ROLLING_META, vec![])
	}
}

/// Aux state is keyed by PARTITION, while a window group is (partition, window
/// coordinate), so none of it fits inside a group and all of it stays node scoped.
fn encode_partition(keyspace: Keyspace, partition: Hash128) -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, keyspace, partition.0.to_be_bytes())
}

fn decode_partition(keyspace: Keyspace, key: &EncodedKey) -> Option<Hash128> {
	let (group, found, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	if group != GroupId::NODE_SCOPE || found != keyspace {
		return None;
	}
	Some(Hash128(u128::from_be_bytes(suffix.try_into().ok()?)))
}

fn node_scoped_range(keyspace: Keyspace) -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, keyspace)
}

fn node_scoped_suffix(keyspace: Keyspace, key: &EncodedKey) -> Option<Vec<u8>> {
	let (group, found, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	(group == GroupId::NODE_SCOPE && found == keyspace).then_some(suffix)
}

fn decode_watermark_key(key: &EncodedKey) -> Option<WatermarkKey> {
	let suffix = node_scoped_suffix(Keyspace::WATERMARK, key)?;
	match suffix.as_slice() {
		[0] => Some(WatermarkKey(WatermarkKind::Event)),
		[1] => Some(WatermarkKey(WatermarkKind::Expiry)),
		_ => None,
	}
}

fn decode_count_key(key: &EncodedKey) -> Option<CountKey> {
	decode_partition(Keyspace::COUNT, key).map(CountKey)
}

fn decode_session_key(key: &EncodedKey) -> Option<SessionKey> {
	decode_partition(Keyspace::SESSION, key).map(SessionKey)
}

fn decode_row_index_key(key: &EncodedKey) -> Option<RowIndexKey> {
	let suffix = node_scoped_suffix(Keyspace::ROW_INDEX, key)?;
	if suffix.len() != 16 + 8 {
		return None;
	}
	let partition = Hash128(u128::from_be_bytes(suffix[..16].try_into().ok()?));
	let row = u64::from_be_bytes(suffix[16..].try_into().ok()?);
	Some(RowIndexKey(partition, RowNumber(row)))
}

pub(super) struct WindowAux {
	watermark: StateCache<WatermarkKey, WatermarkState>,
	count: StateCache<CountKey, CountState>,
	row_index: StateCache<RowIndexKey, RowIndexState>,
	session: StateCache<SessionKey, SessionState>,
	rolling_meta: StateCache<RollingMetaKey, RollingMeta>,
	hydrated: bool,
}

impl WindowAux {
	pub(super) fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			watermark: StateCache::new_internal(budget.clone()),
			count: StateCache::new_internal(budget.clone()),
			row_index: StateCache::new_internal(budget.clone()),
			session: StateCache::new_internal(budget.clone()),
			rolling_meta: StateCache::new_internal(budget),
			hydrated: false,
		}
	}

	pub(super) fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.watermark.hydrate(store, node_scoped_range(Keyspace::WATERMARK), decode_watermark_key)?;
		self.count.hydrate(store, node_scoped_range(Keyspace::COUNT), decode_count_key)?;
		self.row_index.hydrate(store, node_scoped_range(Keyspace::ROW_INDEX), decode_row_index_key)?;
		self.session.hydrate(store, node_scoped_range(Keyspace::SESSION), decode_session_key)?;
		self.hydrated = true;
		Ok(())
	}

	pub(super) fn invalidate_groups(&mut self, groups: &GroupSet) -> usize {
		self.rolling_meta.invalidate_group_data(groups)
	}

	pub(super) fn flush<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		self.watermark.flush(store)?;
		self.count.flush(store)?;
		self.row_index.flush(store)?;
		self.session.flush(store)?;
		self.rolling_meta.flush(store)?;
		Ok(())
	}

	pub(super) fn sample_parts(&self) -> (StateMemory, StateMemory, StateMemory, StateCompleteness) {
		let mut memory = StateMemory::ZERO;
		let mut dirty = StateMemory::ZERO;
		let mut membership = StateMemory::ZERO;
		let mut completeness = StateCompleteness::MERGE_IDENTITY;
		macro_rules! fold {
			($cache:expr) => {{
				memory = memory + $cache.approximate_memory();
				dirty = dirty + $cache.dirty_memory();
				membership = membership + $cache.membership_memory();
				completeness = completeness.merge($cache.completeness());
			}};
		}
		fold!(self.watermark);
		fold!(self.count);
		fold!(self.row_index);
		fold!(self.session);
		fold!(self.rolling_meta);
		(memory, dirty, membership, completeness)
	}

	pub(super) fn event_watermark<S: StateStore>(&mut self, store: &mut S) -> Result<u64> {
		Ok(self.watermark.get_or_default(store, &WatermarkKey(WatermarkKind::Event))?.value)
	}

	pub(super) fn advance_event_watermark<S: StateStore>(&mut self, store: &mut S, coord: u64) -> Result<()> {
		if coord > self.event_watermark(store)? {
			self.watermark.put(
				store,
				&WatermarkKey(WatermarkKind::Event),
				WatermarkState {
					value: coord,
				},
			)?;
		}
		Ok(())
	}

	pub(super) fn expiry_watermark<S: StateStore>(&mut self, store: &mut S) -> Result<u64> {
		Ok(self.watermark.get_or_default(store, &WatermarkKey(WatermarkKind::Expiry))?.value)
	}

	pub(super) fn advance_expiry_watermark<S: StateStore>(&mut self, store: &mut S, coord: u64) -> Result<()> {
		if coord > self.expiry_watermark(store)? {
			self.watermark.put(
				store,
				&WatermarkKey(WatermarkKind::Expiry),
				WatermarkState {
					value: coord,
				},
			)?;
		}
		Ok(())
	}

	pub(super) fn get_and_increment_count<S: StateStore>(&mut self, store: &mut S, group: Hash128) -> Result<u64> {
		let key = CountKey(group);
		let current = self.count.get_or_default(store, &key)?.value;
		self.count.put(
			store,
			&key,
			CountState {
				value: current + 1,
			},
		)?;
		Ok(current)
	}

	pub(super) fn lookup_row_index<S: StateStore>(
		&mut self,
		store: &mut S,
		group: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		Ok(self.row_index.get_or_default(store, &RowIndexKey(group, row_number))?.window_ids)
	}

	pub(super) fn store_row_index<S: StateStore>(
		&mut self,
		store: &mut S,
		group: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let key = RowIndexKey(group, row_number);
		let mut state = self.row_index.get_or_default(store, &key)?;
		if !state.window_ids.contains(&window_id) {
			state.window_ids.push(window_id);
		}
		self.row_index.put(store, &key, state)
	}

	pub(super) fn load_session<S: StateStore>(&mut self, store: &mut S, group: Hash128) -> Result<(u64, u64, u64)> {
		let state = self.session.get_or_default(store, &SessionKey(group))?;
		Ok((state.session_id, state.last_event_time, state.session_start))
	}

	pub(super) fn save_session<S: StateStore>(
		&mut self,
		store: &mut S,
		group: Hash128,
		session_id: u64,
		last_event_time: u64,
		session_start: u64,
	) -> Result<()> {
		self.session.put(
			store,
			&SessionKey(group),
			SessionState {
				session_id,
				last_event_time,
				session_start,
			},
		)
	}

	pub(super) fn rolling_meta<S: StateStore>(
		&mut self,
		store: &mut S,
		group: GroupId,
	) -> Result<Option<RollingMeta>> {
		self.rolling_meta.get(store, &RollingMetaKey(group))
	}

	pub(super) fn put_rolling_meta<S: StateStore>(
		&mut self,
		store: &mut S,
		group: GroupId,
		meta: RollingMeta,
	) -> Result<()> {
		self.rolling_meta.put(store, &RollingMetaKey(group), meta)
	}

	pub(super) fn drop_rolling_meta<S: StateStore>(&mut self, store: &mut S, group: GroupId) -> Result<()> {
		self.rolling_meta.remove(store, &RollingMetaKey(group))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::IntoEncodedKey;
	use reifydb_core::key::operator_state::{GroupId, OperatorStateKey};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use super::{
		CountKey, RowIndexKey, SessionKey, WatermarkKey, WatermarkKind, decode_count_key, decode_row_index_key,
		decode_session_key, decode_watermark_key,
	};

	const PARTITION: Hash128 = Hash128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);

	#[test]
	fn every_aux_key_round_trips() {
		// Each cache hydrates through its own keyspace range and rebuilds its keys with
		// these decoders, so a key that does not survive the round trip is silently
		// dropped from the cache and its state is re-derived from nothing.
		for kind in [WatermarkKind::Event, WatermarkKind::Expiry] {
			let key = WatermarkKey(kind);
			assert!(
				decode_watermark_key(&(&key).into_encoded_key()) == Some(key),
				"watermark key did not survive the round trip"
			);
		}
		let count = CountKey(PARTITION);
		assert!(decode_count_key(&(&count).into_encoded_key()) == Some(count), "count key");
		let session = SessionKey(PARTITION);
		assert!(decode_session_key(&(&session).into_encoded_key()) == Some(session), "session key");
		let row_index = RowIndexKey(PARTITION, RowNumber(7));
		assert!(decode_row_index_key(&(&row_index).into_encoded_key()) == Some(row_index), "row index key");
	}

	#[test]
	fn count_and_session_keys_do_not_decode_each_other() {
		// Both are a bare partition hash and now differ ONLY by the keyspace byte, so a
		// decoder handed the wrong Keyspace constant would read count state as session
		// state - two u64 payloads that would deserialize happily and corrupt session
		// assignment with an event ordinal. Nothing else in the shape can catch it.
		let count_encoded = (&CountKey(PARTITION)).into_encoded_key();
		let session_encoded = (&SessionKey(PARTITION)).into_encoded_key();
		assert!(count_encoded != session_encoded, "count and session must not share a key");
		assert!(decode_session_key(&count_encoded).is_none(), "a count key must not decode as a session key");
		assert!(decode_count_key(&session_encoded).is_none(), "a session key must not decode as a count key");
	}

	#[test]
	fn aux_keys_stay_out_of_every_group_range() {
		// Aux state is keyed by partition while a window group is (partition, coordinate),
		// so none of it belongs to a group. If one of these landed under a real group id,
		// reclaiming that group would erase the partition's watermark or session tracker
		// while the partition was still live.
		let encoded = [
			(&WatermarkKey(WatermarkKind::Event)).into_encoded_key(),
			(&CountKey(PARTITION)).into_encoded_key(),
			(&SessionKey(PARTITION)).into_encoded_key(),
			(&RowIndexKey(PARTITION, RowNumber(7))).into_encoded_key(),
		];
		for key in encoded {
			let (group, keyspace, _) =
				OperatorStateKey::decode_inner(key.as_bytes()).expect("aux keys are structured");
			assert!(group == GroupId::NODE_SCOPE, "aux key {keyspace:?} escaped into a group");
			assert!(keyspace.is_data(), "aux keyspace {keyspace:?} must be a data keyspace");
		}
	}
}
