// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::size_of, slice::from_ref};

use reifydb_codec::row::operator::state::{decode_body, derive::Serialize, encode};
use reifydb_core::{
	internal_err,
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
	value::{datetime::DateTime, row_number::RowNumber},
};
use serde::de::DeserializeOwned;

use crate::operator::state_access::{get, get_classified, remove, set};

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
	pub first_time: DateTime,
}

impl HeapSize for SessionState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Clone, Default)]
pub struct OpenSessionsState {
	pub sessions: Vec<(u64, Vec<(u64, u64)>)>,
	pub sealed_last: Option<u64>,
}

impl HeapSize for OpenSessionsState {
	fn heap_size(&self) -> usize {
		self.sessions.iter().map(|(_, members)| size_of::<u64>() + members.len() * 2 * size_of::<u64>()).sum()
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
#[derive(Clone, Debug, PartialEq)]
pub struct RollingPublished<K, O> {
	pub row_number: u64,
	pub key: K,
	pub last_value: O,
	pub first_time: DateTime,
}

impl<K: HeapSize, O: HeapSize> HeapSize for RollingPublished<K, O> {
	fn heap_size(&self) -> usize {
		self.key.heap_size() + self.last_value.heap_size()
	}
}

#[operator_state]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoKey;

impl HeapSize for NoKey {
	fn heap_size(&self) -> usize {
		0
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

	pub fn store_session_member<T: Serialize>(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
		member: &T,
	) -> Result<()> {
		store.state_set(vec![((&SessionMemberKey(group, row_number)).into_group_state_key(), encode(member)?)])
	}

	pub fn load_session_member<T: DeserializeOwned>(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<T> {
		let key = (&SessionMemberKey(group, row_number)).into_group_state_key();
		match store.state_get(from_ref(&key))?.pop().flatten() {
			Some(row) => Ok(decode_body::<T>(&row)?),
			None => internal_err!(
				"session member {:?} of {:?} has no stored contribution; the open sessions and the member \
				 rows disagree",
				row_number,
				group
			),
		}
	}

	pub fn load_open_sessions_of(
		&mut self,
		store: &mut dyn StateStore,
		groups: &[GroupId],
	) -> Result<Vec<Option<OpenSessionsState>>> {
		let keys: Vec<SessionKey> = groups.iter().map(|group| SessionKey(*group)).collect();
		get_classified::<_, OpenSessionsState>(store, &keys)
	}

	pub fn save_open_sessions(
		&mut self,
		store: &mut dyn StateStore,
		states: Vec<(GroupId, OpenSessionsState)>,
	) -> Result<()> {
		if states.is_empty() {
			return Ok(());
		}
		set(store, states.into_iter().map(|(group, state)| (SessionKey(group), state)).collect())
	}

	pub fn drop_session_member(
		&mut self,
		store: &mut dyn StateStore,
		group: GroupId,
		row_number: RowNumber,
	) -> Result<()> {
		remove(store, from_ref(&SessionMemberKey(group, row_number)))
	}

	pub fn drop_sessions(&mut self, store: &mut dyn StateStore, groups: &[GroupId]) -> Result<()> {
		if groups.is_empty() {
			return Ok(());
		}
		let keys: Vec<GroupStateKey> =
			groups.iter().map(|group| (&SessionKey(*group)).into_group_state_key()).collect();
		store.state_remove(&keys)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::key::operator::state::{
		GroupId, IntoGroupStateKey, OperatorStateKey, group_data_inner_range,
	};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use super::{RowIndexKey, SessionKey};

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
}
