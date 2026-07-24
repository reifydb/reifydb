// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::size_of, ops::Bound};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange, IntoEncodedKey};
use reifydb_core::{
	key::flow_node_internal_state::FlowNodeInternalStateKey,
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
		EncodedKey::new(vec![FlowNodeInternalStateKey::WINDOW_WATERMARK_TAG, disc])
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
		encode_group(FlowNodeInternalStateKey::WINDOW_COUNT_TAG, self.0)
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
		let mut bytes = Vec::with_capacity(1 + 16 + 8);
		bytes.push(FlowNodeInternalStateKey::WINDOW_ROW_INDEX_TAG);
		bytes.extend_from_slice(&self.0.0.to_be_bytes());
		bytes.extend_from_slice(&self.1.0.to_be_bytes());
		EncodedKey::new(bytes)
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
		encode_group(FlowNodeInternalStateKey::WINDOW_SESSION_TAG, self.0)
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct EngineMetaKey(pub Hash128, pub u64);

impl HeapSize for EngineMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &EngineMetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + 16 + 8);
		bytes.push(FlowNodeInternalStateKey::WINDOW_ENGINE_META_TAG);
		bytes.extend_from_slice(&self.0.0.to_be_bytes());
		bytes.extend_from_slice(&self.1.to_be_bytes());
		EncodedKey::new(bytes)
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct RollingMetaKey(pub Hash128);

impl HeapSize for RollingMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoEncodedKey for &RollingMetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		encode_group(FlowNodeInternalStateKey::WINDOW_ROLLING_META_TAG, self.0)
	}
}

fn encode_group(tag: u8, group: Hash128) -> EncodedKey {
	let mut bytes = Vec::with_capacity(1 + 16);
	bytes.push(tag);
	bytes.extend_from_slice(&group.0.to_be_bytes());
	EncodedKey::new(bytes)
}

fn read_group(tag: u8, key: &EncodedKey) -> Option<Hash128> {
	let bytes = key.as_bytes();
	if bytes.first() != Some(&tag) || bytes.len() != 1 + 16 {
		return None;
	}
	Some(Hash128(u128::from_be_bytes(bytes[1..17].try_into().ok()?)))
}

fn decode_watermark_key(key: &EncodedKey) -> Option<WatermarkKey> {
	let bytes = key.as_bytes();
	if bytes.first() != Some(&FlowNodeInternalStateKey::WINDOW_WATERMARK_TAG) || bytes.len() != 2 {
		return None;
	}
	match bytes[1] {
		0 => Some(WatermarkKey(WatermarkKind::Event)),
		1 => Some(WatermarkKey(WatermarkKind::Expiry)),
		_ => None,
	}
}

fn decode_count_key(key: &EncodedKey) -> Option<CountKey> {
	read_group(FlowNodeInternalStateKey::WINDOW_COUNT_TAG, key).map(CountKey)
}

fn decode_session_key(key: &EncodedKey) -> Option<SessionKey> {
	read_group(FlowNodeInternalStateKey::WINDOW_SESSION_TAG, key).map(SessionKey)
}

fn decode_row_index_key(key: &EncodedKey) -> Option<RowIndexKey> {
	let bytes = key.as_bytes();
	if bytes.first() != Some(&FlowNodeInternalStateKey::WINDOW_ROW_INDEX_TAG) || bytes.len() != 1 + 16 + 8 {
		return None;
	}
	let group = Hash128(u128::from_be_bytes(bytes[1..17].try_into().ok()?));
	let row = u64::from_be_bytes(bytes[17..25].try_into().ok()?);
	Some(RowIndexKey(group, RowNumber(row)))
}

pub(super) fn decode_engine_meta_key(key: &EncodedKey) -> Option<EngineMetaKey> {
	let bytes = key.as_bytes();
	if bytes.first() != Some(&FlowNodeInternalStateKey::WINDOW_ENGINE_META_TAG) || bytes.len() != 1 + 16 + 8 {
		return None;
	}
	let group = Hash128(u128::from_be_bytes(bytes[1..17].try_into().ok()?));
	let window_start = u64::from_be_bytes(bytes[17..25].try_into().ok()?);
	Some(EngineMetaKey(group, window_start))
}

fn decode_rolling_meta_key(key: &EncodedKey) -> Option<RollingMetaKey> {
	read_group(FlowNodeInternalStateKey::WINDOW_ROLLING_META_TAG, key).map(RollingMetaKey)
}

fn tag_range(tag: u8) -> EncodedKeyRange {
	EncodedKeyRange::new(
		Bound::Included(EncodedKey::new(vec![tag])),
		Bound::Excluded(EncodedKey::new(vec![tag + 1])),
	)
}

pub(super) fn engine_meta_range() -> EncodedKeyRange {
	tag_range(FlowNodeInternalStateKey::WINDOW_ENGINE_META_TAG)
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
		self.watermark.hydrate(
			store,
			tag_range(FlowNodeInternalStateKey::WINDOW_WATERMARK_TAG),
			decode_watermark_key,
		)?;
		self.count.hydrate(store, tag_range(FlowNodeInternalStateKey::WINDOW_COUNT_TAG), decode_count_key)?;
		self.row_index.hydrate(
			store,
			tag_range(FlowNodeInternalStateKey::WINDOW_ROW_INDEX_TAG),
			decode_row_index_key,
		)?;
		self.session.hydrate(
			store,
			tag_range(FlowNodeInternalStateKey::WINDOW_SESSION_TAG),
			decode_session_key,
		)?;
		self.rolling_meta.hydrate(
			store,
			tag_range(FlowNodeInternalStateKey::WINDOW_ROLLING_META_TAG),
			decode_rolling_meta_key,
		)?;
		self.hydrated = true;
		Ok(())
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
		group: Hash128,
	) -> Result<Option<RollingMeta>> {
		self.rolling_meta.get(store, &RollingMetaKey(group))
	}

	pub(super) fn put_rolling_meta<S: StateStore>(
		&mut self,
		store: &mut S,
		group: Hash128,
		meta: RollingMeta,
	) -> Result<()> {
		self.rolling_meta.put(store, &RollingMetaKey(group), meta)
	}

	pub(super) fn drop_rolling_meta<S: StateStore>(&mut self, store: &mut S, group: Hash128) -> Result<()> {
		self.rolling_meta.remove(store, &RollingMetaKey(group))
	}
}
