// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::size_of;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	key::operator_state::{
		GroupId, GroupSet, IntoStateKey, Keyspace, OperatorStateKey, StateKey, keyspace_inner_range,
	},
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{budget::OperatorStateBudgetHandle, cache::StateCache, store::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{
	Result,
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
pub(super) struct SealLedgerKey;

impl HeapSize for SealLedgerKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &SealLedgerKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::WATERMARK, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct CountKey(pub GroupId);

impl HeapSize for CountKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &CountKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::COUNT, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct RowIndexKey(pub GroupId, pub RowNumber);

impl HeapSize for RowIndexKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &RowIndexKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ROW_INDEX, self.1.0.to_be_bytes())
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct SessionKey(pub GroupId);

impl HeapSize for SessionKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &SessionKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::SESSION, vec![])
	}
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(super) struct EngineMetaKey(pub GroupId);

impl HeapSize for EngineMetaKey {
	fn heap_size(&self) -> usize {
		0
	}
}

impl IntoStateKey for &EngineMetaKey {
	fn into_state_key(self) -> StateKey {
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

impl IntoStateKey for &RollingMetaKey {
	fn into_state_key(self) -> StateKey {
		OperatorStateKey::inner_encoded(self.0, Keyspace::ROLLING_META, vec![])
	}
}

fn node_scoped_range(keyspace: Keyspace) -> EncodedKeyRange {
	keyspace_inner_range(GroupId::NODE_SCOPE, keyspace)
}

fn node_scoped_suffix(keyspace: Keyspace, key: &EncodedKey) -> Option<Vec<u8>> {
	let (group, found, suffix) = OperatorStateKey::decode_inner(key.as_bytes())?;
	(group == GroupId::NODE_SCOPE && found == keyspace).then_some(suffix)
}

fn decode_seal_ledger_key(key: &EncodedKey) -> Option<SealLedgerKey> {
	let suffix = node_scoped_suffix(Keyspace::WATERMARK, key)?;
	suffix.is_empty().then_some(SealLedgerKey)
}

pub(super) struct WindowAux {
	watermark: StateCache<SealLedgerKey, WatermarkState>,
	count: StateCache<CountKey, CountState>,
	row_index: StateCache<RowIndexKey, RowIndexState>,
	session: StateCache<SessionKey, SessionState>,
	rolling_meta: StateCache<RollingMetaKey, RollingMeta>,
	hydrated: bool,
}

impl WindowAux {
	pub(super) fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			watermark: StateCache::new(budget.clone()),
			count: StateCache::new(budget.clone()),
			row_index: StateCache::new(budget.clone()),
			session: StateCache::new(budget.clone()),
			rolling_meta: StateCache::new(budget),
			hydrated: false,
		}
	}

	pub(super) fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.watermark.hydrate(store, node_scoped_range(Keyspace::WATERMARK), decode_seal_ledger_key)?;
		self.hydrated = true;
		Ok(())
	}

	pub(super) fn invalidate_groups(&mut self, groups: &GroupSet) -> usize {
		let mut dropped = self.rolling_meta.invalidate_group_data(groups);
		dropped += self.count.invalidate_group_data(groups);
		dropped += self.row_index.invalidate_group_data(groups);
		dropped += self.session.invalidate_group_data(groups);
		dropped
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

	pub(super) fn seal_ledger<S: StateStore>(&mut self, store: &mut S) -> Result<u64> {
		Ok(self.watermark.get_or_default(store, &SealLedgerKey)?.value)
	}

	pub(super) fn advance_seal_ledger<S: StateStore>(&mut self, store: &mut S, coord: u64) -> Result<()> {
		if coord > self.seal_ledger(store)? {
			self.watermark.put(
				store,
				&SealLedgerKey,
				WatermarkState {
					value: coord,
				},
			)?;
		}
		Ok(())
	}

	pub(super) fn get_and_increment_count<S: StateStore>(&mut self, store: &mut S, group: GroupId) -> Result<u64> {
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
		group: GroupId,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		Ok(self.row_index.get_or_default(store, &RowIndexKey(group, row_number))?.window_ids)
	}

	pub(super) fn store_row_index<S: StateStore>(
		&mut self,
		store: &mut S,
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

	pub(super) fn load_session<S: StateStore>(&mut self, store: &mut S, group: GroupId) -> Result<(u64, u64, u64)> {
		let state = self.session.get_or_default(store, &SessionKey(group))?;
		Ok((state.session_id, state.last_event_time, state.session_start))
	}

	pub(super) fn save_session<S: StateStore>(
		&mut self,
		store: &mut S,
		group: GroupId,
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
	use std::ops::Bound::{Excluded, Included, Unbounded};

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		key::operator_state::{GroupId, GroupSet, IntoStateKey, OperatorStateKey, group_data_inner_range},
		state::budget::OperatorStateBudgetHandle,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use super::{CountKey, RowIndexKey, SealLedgerKey, SessionKey, WindowAux, decode_seal_ledger_key};
	use crate::operator::{
		store::OperatorStateStore,
		window::tumbling::{partition_group_key, window_group_key},
	};

	const PARTITION: Hash128 = Hash128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
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
	fn the_seal_ledger_key_round_trips() {
		// The seal ledger is the one aux cache that still hydrates through a keyspace range and
		// rebuilds its key with a decoder, so a key that does not survive the round trip is
		// silently dropped from the cache and the ledger is re-derived from nothing - which
		// reads as "nothing has been sealed" and readmits every late row the gate exists to
		// drop.
		assert!(
			decode_seal_ledger_key((&SealLedgerKey).into_state_key().as_encoded()) == Some(SealLedgerKey),
			"seal ledger key did not survive the round trip"
		);
	}

	#[test]
	fn partition_scoped_aux_lands_inside_the_group_the_substrate_reclaims() {
		// The whole point of the partition group: this state spans every window of one
		// partition, so it fits in no window group and used to sit at node scope where no
		// group range could reach it - one row per partition, kept forever. Landing it in the
		// partition group's DATA range is what makes reclaim_group_data take it.
		let range = group_data_inner_range(GROUP);
		for key in [
			(&CountKey(GROUP)).into_state_key(),
			(&SessionKey(GROUP)).into_state_key(),
			(&RowIndexKey(GROUP, RowNumber(7))).into_state_key(),
		] {
			let (group, keyspace, _) =
				OperatorStateKey::decode_inner(key.as_bytes()).expect("aux keys are structured");
			assert_eq!(group, GROUP, "partition-scoped aux escaped its group");
			assert!(keyspace.is_data(), "{keyspace:?} must be a data keyspace to be reclaimed by phase 1");
			assert!(contains(&range, key.as_bytes()), "{keyspace:?} landed outside the group data range");
		}
	}

	#[test]
	fn the_seal_ledger_stays_out_of_every_group_range() {
		// The seal ledger is per NODE, not per partition - one entry for the whole operator. If
		// it landed under a real group id, reclaiming that one group would reset the node's
		// seal ledger and every later event would look admissible again.
		let key = (&SealLedgerKey).into_state_key();
		let (group, _, _) = OperatorStateKey::decode_inner(key.as_bytes()).expect("aux keys are structured");
		assert_eq!(group, GroupId::NODE_SCOPE);
		assert!(!contains(&group_data_inner_range(GROUP), key.as_bytes()));
	}

	#[test]
	fn count_and_session_share_a_group_and_are_told_apart_only_by_the_keyspace() {
		// Both are now a bare partition group with an EMPTY suffix, so the keyspace byte is the
		// only thing separating them. Reading one as the other would deserialize happily - two
		// u64 payloads - and corrupt session assignment with an event ordinal.
		let count = (&CountKey(GROUP)).into_state_key();
		let session = (&SessionKey(GROUP)).into_state_key();
		assert_ne!(count, session, "count and session must not share a key");

		let (count_group, count_ks, count_suffix) = OperatorStateKey::decode_inner(count.as_bytes()).unwrap();
		let (session_group, session_ks, session_suffix) =
			OperatorStateKey::decode_inner(session.as_bytes()).unwrap();
		assert_eq!(count_group, session_group, "both belong to the same partition");
		assert_ne!(count_ks, session_ks, "only the keyspace may distinguish them");
		assert!(count_suffix.is_empty() && session_suffix.is_empty());
	}

	#[test]
	fn reclaiming_a_partition_group_drops_its_aux_state_from_ram() {
		// Moving the keys into the group only makes the STORE rows reclaimable. The substrate
		// deletes them behind the operator's back and reports the group id, so anything still
		// sitting in the clean tier would keep answering from RAM for a partition whose rows are
		// gone - a session tracker that outlives its own state, resurrecting a closed session.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let mut aux = WindowAux::new(OperatorStateBudgetHandle::default());
		let mut store = OperatorStateStore::new(&mut txn, FlowNodeId(1));

		aux.save_session(&mut store, GROUP, 1, 2, 3).unwrap();
		aux.get_and_increment_count(&mut store, GROUP).unwrap();
		aux.store_row_index(&mut store, GROUP, RowNumber(7), 11).unwrap();
		aux.flush(&mut store).unwrap();

		assert_eq!(
			aux.invalidate_groups(&GroupSet::new([GROUP])),
			3,
			"every partition-scoped cache must drop the reclaimed group, not just rolling meta"
		);
	}

	#[test]
	fn a_partition_group_can_never_collide_with_a_window_group() {
		// The two kinds share one dictionary. Without the leading discriminator they would be
		// separated only by length, which holds solely because the window coordinate happens to
		// be fixed width - a collision would alias a partition's session tracker onto some
		// window's accumulators and reclaiming either would erase the other.
		let partition = partition_group_key(PARTITION);
		for window_id in [0u64, 1, u64::MAX] {
			let window = window_group_key(PARTITION, window_id);
			assert_ne!(partition, window);
			assert_ne!(
				partition.as_bytes()[0],
				window.as_bytes()[0],
				"the discriminator, not the length, must be what separates the two kinds"
			);
		}
	}
}
