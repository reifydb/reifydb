// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::size_of, slice::from_ref};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	key::{
		operator::{
			keyspace::window::{
				EngineMeta as EngineMetaSpace, RollingMeta as RollingMetaSpace, RowIndex, Session,
				SessionMember,
			},
			state::{GroupId, GroupStateKey, IntoGroupStateKey},
		},
		typed::direction::Asc,
	},
	metrics::heap::HeapSize,
	state::{timer::StateStore, typed::typed_key},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};

use crate::{
	operator::{
		state::seal::{
			coord::Coord,
			ledger::{SealLedgerState, seal_ledger_key},
		},
		state_access::{get, get_classified, remove, set},
	},
	window::kind::session::SessionTracker,
};

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
pub struct RowIndexKey(pub GroupId, pub RowNumber);

impl HeapSize for RowIndexKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &RowIndexKey {
	fn into_group_state_key(self) -> GroupStateKey {
		typed_key::<RowIndex>(self.0, &Asc(self.1))
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct SessionMemberKey(pub GroupId, pub RowNumber);

impl HeapSize for SessionMemberKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoGroupStateKey for &SessionMemberKey {
	fn into_group_state_key(self) -> GroupStateKey {
		typed_key::<SessionMember>(self.0, &Asc(self.1))
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
		typed_key::<Session>(self.0, &())
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
		typed_key::<EngineMetaSpace>(self.0, &())
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
		typed_key::<RollingMetaSpace>(self.0, &())
	}
}

#[derive(Default)]
pub struct WindowMeta;

impl WindowMeta {
	pub fn new() -> Self {
		Self
	}

	pub fn seal_ledger(&mut self, store: &mut dyn StateStore) -> Result<u64> {
		Ok(get::<_, SealLedgerState>(store, from_ref(&SealLedgerKey))?
			.remove(0)
			.unwrap_or_default()
			.sealed_through)
	}

	pub fn advance_seal_ledger(&mut self, store: &mut dyn StateStore, coord: u64) -> Result<()> {
		if coord > self.seal_ledger(store)? {
			set(
				store,
				vec![(
					SealLedgerKey,
					SealLedgerState {
						sealed_through: coord,
					},
				)],
			)?;
		}
		Ok(())
	}

	pub fn lookup_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		Ok(get::<_, RowIndexState>(store, from_ref(&RowIndexKey(group, row_number)))?
			.remove(0)
			.unwrap_or_default()
			.window_ids)
	}

	pub fn store_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let key = RowIndexKey(group, row_number);
		let mut state: RowIndexState = get(store, from_ref(&key))?.remove(0).unwrap_or_default();
		if !state.window_ids.contains(&window_id) {
			state.window_ids.push(window_id);
		}
		set(store, vec![(key, state)])
	}

	pub fn store_row_indexes(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
		window_ids: &[u64],
	) -> Result<()> {
		if window_ids.is_empty() {
			return Ok(());
		}
		let key = RowIndexKey(group, row_number);
		let mut state: RowIndexState = get(store, from_ref(&key))?.remove(0).unwrap_or_default();
		for window_id in window_ids {
			if !state.window_ids.contains(window_id) {
				state.window_ids.push(*window_id);
			}
		}
		set(store, vec![(key, state)])
	}

	pub fn withdraw_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let key = RowIndexKey(group, row_number);
		let Some(mut state) = get_classified::<_, RowIndexState>(store, from_ref(&key))?.remove(0) else {
			return Ok(());
		};
		let held = state.window_ids.len();
		state.window_ids.retain(|id| *id != window_id);
		if state.window_ids.len() == held {
			return Ok(());
		}
		if state.window_ids.is_empty() {
			remove(store, from_ref(&key))
		} else {
			set(store, vec![(key, state)])
		}
	}

	pub fn drop_row_index(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		remove(store, from_ref(&RowIndexKey(group, row_number)))
	}

	pub fn store_session_member(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		store.state_set(vec![(
			((&SessionMemberKey(group, row_number)).into_group_state_key()).clone(),
			EncodedPodRow::new(&[]),
		)])
	}

	pub fn drop_session_member(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		remove(store, from_ref(&SessionMemberKey(group, row_number)))
	}

	pub fn load_session(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<SessionTracker> {
		Ok(self.stored_session(store, group)?.unwrap_or_default())
	}

	pub fn stored_session(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<Option<SessionTracker>> {
		Ok(get_classified::<_, SessionState>(store, from_ref(&SessionKey(group)))?
			.remove(0)
			.map(resumed_session))
	}

	pub fn stored_sessions(
		&mut self,
		store: &mut dyn StateStore,
		groups: &[GroupId],
	) -> Result<Vec<Option<SessionTracker>>> {
		let keys: Vec<SessionKey> = groups.iter().map(|group| SessionKey(*group)).collect();
		Ok(get_classified::<_, SessionState>(store, &keys)?
			.into_iter()
			.map(|state| state.map(resumed_session))
			.collect())
	}

	pub fn drop_sessions(&mut self, store: &mut dyn StateStore, groups: &[GroupId]) -> Result<()> {
		if groups.is_empty() {
			return Ok(());
		}
		let keys: Vec<GroupStateKey> =
			groups.iter().map(|group| (&SessionKey(*group)).into_group_state_key()).collect();
		store.state_remove(&keys)
	}

	pub fn save_session(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		tracker: &SessionTracker,
	) -> Result<()> {
		set(
			store,
			vec![(
				SessionKey(group),
				SessionState {
					session_id: tracker.session_id,
					last_event_time: tracker.last.to_order(),
					session_start: tracker.start.to_order(),
				},
			)],
		)
	}

	pub fn rolling_meta(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<Option<RollingMeta>> {
		Ok(get_classified(store, from_ref(&RollingMetaKey(group)))?.remove(0))
	}

	pub fn put_rolling_meta(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		meta: RollingMeta,
	) -> Result<()> {
		set(store, vec![(RollingMetaKey(group), meta)])
	}

	pub fn drop_rolling_meta(&mut self, store: &mut dyn StateStore, group: GroupId) -> Result<()> {
		remove(store, from_ref(&RollingMetaKey(group)))
	}
}

fn resumed_session(state: SessionState) -> SessionTracker {
	SessionTracker::resumed(
		state.session_id,
		<DateTime as Coord>::from_order(state.last_event_time),
		<DateTime as Coord>::from_order(state.session_start),
	)
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::key::operator::state::{
		GroupId, IntoGroupStateKey, OperatorStateKey, group_data_inner_range,
	};
	use reifydb_value::{factory::time::at_millis, util::hash::Hash128, value::row_number::RowNumber};

	use super::{RowIndexKey, SealLedgerKey, SessionKey, WindowMeta};
	use crate::{operator::state::mock::MockStore, window::kind::session::SessionTracker};

	fn group_id() -> GroupId {
		GroupId::hashed(Hash128(42))
	}

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
		let range = group_data_inner_range(group_id());
		for key in [
			(&SessionKey(group_id())).into_group_state_key(),
			(&RowIndexKey(group_id(), RowNumber(7))).into_group_state_key(),
		] {
			let (group, keyspace, _) =
				OperatorStateKey::decode_inner(key.as_bytes()).expect("meta keys are structured");
			assert_eq!(group, group_id(), "partition-scoped meta escaped its group");
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
		assert!(!contains(&group_data_inner_range(group_id()), key.as_bytes()));
	}

	#[test]
	fn a_session_persisted_at_the_epoch_reloads_as_open_rather_than_as_a_fresh_tracker() {
		// A SessionState row exists only once a group has opened a session, so row presence IS the
		// openness bit and load_session must read it as an Option. Defaulting an absent row would
		// make an all-zero session, which is one opened at the epoch, read as never-seen.
		let mut meta = WindowMeta::new();
		let mut store = MockStore::default();

		assert_eq!(
			meta.load_session(&mut store, group_id()).unwrap(),
			SessionTracker::default(),
			"a group with no persisted session must load as unopened"
		);

		meta.save_session(&mut store, group_id(), &SessionTracker::resumed(0, at_millis(0), at_millis(0)))
			.unwrap();

		assert_eq!(
			meta.load_session(&mut store, group_id()).unwrap(),
			SessionTracker::resumed(0, at_millis(0), at_millis(0))
		);
	}
}
