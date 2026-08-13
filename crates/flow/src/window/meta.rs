// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::size_of;

use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey, IntoGroupStateKey, Keyspace, OperatorStateKey},
	metrics::heap::HeapSize,
	state::{cache::StateCache, store::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};

use crate::{
	state::seal::{
		coord::Coord,
		ledger::{SealLedgerState, seal_ledger_key},
	},
	window::kind::session::SessionTracker,
};

#[operator_state]
#[derive(Clone, Default)]
pub struct CountState {
	pub value: u64,
}

impl HeapSize for CountState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub struct RowIndexState {
	pub window_ids: Vec<u64>,
}

impl HeapSize for RowIndexState {
	fn heap_size(&self) -> usize {
		self.window_ids.len() * size_of::<u64>()
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub struct SessionState {
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
pub struct EngineMeta {
	pub last_event_time: u64,
}

impl HeapSize for EngineMeta {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub struct RollingMeta {
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
pub struct SealLedgerKey;

impl HeapSize for SealLedgerKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &SealLedgerKey {
	fn into_group_state_key(self) -> GroupStateKey {
		seal_ledger_key()
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct CountKey(pub GroupId);

impl HeapSize for CountKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &CountKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::COUNT, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct RowIndexKey(pub GroupId, pub RowNumber);

impl HeapSize for RowIndexKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &RowIndexKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ROW_INDEX, self.1.0.to_be_bytes())
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct SessionKey(pub GroupId);

impl HeapSize for SessionKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &SessionKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::SESSION, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct EngineMetaKey(pub GroupId);

impl HeapSize for EngineMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &EngineMetaKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ENGINE_META, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct RollingMetaKey(pub GroupId);

impl HeapSize for RollingMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &RollingMetaKey {
	fn into_group_state_key(self) -> GroupStateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ROLLING_META, vec![])
	}
}

pub struct WindowMeta {
	seal_ledger: StateCache<SealLedgerKey, SealLedgerState>,
	count: StateCache<CountKey, CountState>,
	row_index: StateCache<RowIndexKey, RowIndexState>,
	session: StateCache<SessionKey, SessionState>,
	rolling_meta: StateCache<RollingMetaKey, RollingMeta>,
}

impl Default for WindowMeta {
	fn default() -> Self {
		Self::new()
	}
}

impl WindowMeta {
	pub fn new() -> Self {
		Self {
			seal_ledger: StateCache::new(),
			count: StateCache::new(),
			row_index: StateCache::new(),
			session: StateCache::new(),
			rolling_meta: StateCache::new(),
		}
	}

	pub fn seal_ledger(&mut self, store: &mut dyn StateStore) -> Result<u64> {
		Ok(self.seal_ledger.get_or_default(store, &SealLedgerKey)?.sealed_through)
	}

	pub fn advance_seal_ledger(&mut self, store: &mut dyn StateStore, coord: u64) -> Result<()> {
		if coord > self.seal_ledger(store)? {
			self.seal_ledger.put(
				store,
				&SealLedgerKey,
				SealLedgerState {
					sealed_through: coord,
				},
			)?;
		}
		Ok(())
	}

	pub fn get_and_increment_count(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<u64> {
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

	pub fn lookup_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		Ok(self.row_index.get_or_default(store, &RowIndexKey(group, row_number))?.window_ids)
	}

	pub fn store_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
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

	pub fn drop_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		self.row_index.remove(store, &RowIndexKey(group, row_number))
	}

	pub fn load_session(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<SessionTracker> {
		let Some(state) = self.session.get(store, &SessionKey(group))? else {
			return Ok(SessionTracker::default());
		};
		Ok(SessionTracker::resumed(
			state.session_id,
			<DateTime as Coord>::from_order(state.last_event_time),
			<DateTime as Coord>::from_order(state.session_start),
		))
	}

	pub fn save_session(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		tracker: &SessionTracker,
	) -> Result<()> {
		self.session.put(
			store,
			&SessionKey(group),
			SessionState {
				session_id: tracker.session_id,
				last_event_time: tracker.last.to_order(),
				session_start: tracker.start.to_order(),
			},
		)
	}

	pub fn rolling_meta(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<Option<RollingMeta>> {
		self.rolling_meta.get(store, &RollingMetaKey(group))
	}

	pub fn put_rolling_meta(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		meta: RollingMeta,
	) -> Result<()> {
		self.rolling_meta.put(store, &RollingMetaKey(group), meta)
	}

	pub fn drop_rolling_meta(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<()> {
		self.rolling_meta.remove(store, &RollingMetaKey(group))
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::key::operator_state::{GroupId, IntoGroupStateKey, OperatorStateKey, group_data_inner_range};
	use reifydb_value::{factory::time::at_millis, value::row_number::RowNumber};

	use super::{CountKey, RowIndexKey, SealLedgerKey, SessionKey, WindowMeta};
	use crate::{state::mock::MockStore, window::kind::session::SessionTracker};

	const GROUP: GroupId = GroupId(42);

	fn contains(range: &EncodedKeyRange, key: &[u8]) -> bool {
		let above = match &range.start {
			Included(bound) => key >= bound.as_slice(),
			Excluded(bound) => key > bound.as_slice(),
			Unbounded => true,
		};
		let below = match &range.end {
			Included(bound) => key <= bound.as_slice(),
			Excluded(bound) => key < bound.as_slice(),
			Unbounded => true,
		};
		above && below
	}

	#[test]
	fn partition_scoped_meta_lands_inside_the_group_the_substrate_reclaims() {
		// landing this in the root group would leave no group range able to reach it, stranding one row per
		// partition forever, so it lives in the partition group's data range instead
		let range = group_data_inner_range(GROUP);
		for key in [
			(&CountKey(GROUP)).into_group_state_key(),
			(&SessionKey(GROUP)).into_group_state_key(),
			(&RowIndexKey(GROUP, RowNumber(7))).into_group_state_key(),
		] {
			let (group, keyspace, _) =
				OperatorStateKey::decode_inner(key.as_bytes()).expect("meta keys are structured");
			assert_eq!(group, GROUP, "partition-scoped meta escaped its group");
			assert!(keyspace.is_data(), "{keyspace:?} must be a data keyspace to be reclaimed by phase 1");
			assert!(contains(&range, key.as_bytes()), "{keyspace:?} landed outside the group data range");
		}
	}

	#[test]
	fn the_seal_ledger_stays_out_of_every_group_range() {
		// The seal ledger is per operator, one entry for the whole operator. Under a real group id,
		// reclaiming that group would reset it and every later event would look admissible again.
		let key = (&SealLedgerKey).into_group_state_key();
		let (group, _, _) = OperatorStateKey::decode_inner(key.as_bytes()).expect("meta keys are structured");
		assert_eq!(group, GroupId::ROOT);
		assert!(!contains(&group_data_inner_range(GROUP), key.as_bytes()));
	}

	#[test]
	fn count_and_session_share_a_group_and_are_told_apart_only_by_the_keyspace() {
		// Both are a bare partition group with an empty suffix, so the keyspace byte is all that
		// separates them. Reading one as the other deserializes happily - two u64 payloads - and
		// corrupts session assignment with an event ordinal.
		let count = (&CountKey(GROUP)).into_group_state_key();
		let session = (&SessionKey(GROUP)).into_group_state_key();
		assert_ne!(count, session, "count and session must not share a key");

		let (count_group, count_ks, count_suffix) = OperatorStateKey::decode_inner(count.as_bytes()).unwrap();
		let (session_group, session_ks, session_suffix) =
			OperatorStateKey::decode_inner(session.as_bytes()).unwrap();
		assert_eq!(count_group, session_group, "both belong to the same partition");
		assert_ne!(count_ks, session_ks, "only the keyspace may distinguish them");
		assert!(count_suffix.is_empty() && session_suffix.is_empty());
	}

	#[test]
	fn a_session_persisted_at_the_epoch_reloads_as_open_rather_than_as_a_fresh_tracker() {
		// A SessionState row exists only once a group has opened a session, so row presence IS the
		// openness bit and load_session must read it as an Option. Defaulting an absent row would
		// make an all-zero session, which is one opened at the epoch, read as never-seen.
		let mut meta = WindowMeta::new();
		let mut store = MockStore::default();

		assert_eq!(
			meta.load_session(&mut store, GROUP).unwrap(),
			SessionTracker::default(),
			"a group with no persisted session must load as unopened"
		);

		meta.save_session(&mut store, GROUP, &SessionTracker::resumed(0, at_millis(0), at_millis(0))).unwrap();

		assert_eq!(
			meta.load_session(&mut store, GROUP).unwrap(),
			SessionTracker::resumed(0, at_millis(0), at_millis(0))
		);
	}
}
