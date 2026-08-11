// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, slice::from_ref, sync::Arc};

use dashmap::DashMap;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		bytes::EncodedBytes,
		operator::{EncodedOperatorRow, OperatorState, decode},
	},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	},
	state::group::GroupRecord,
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{Result, byte_size::ByteSize, reifydb_assertions, value::datetime::DateTime};

use crate::transaction::{DepFlowTransaction, interface::FlowTransaction};

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const HYDRATE_CHUNK: usize = 8_192;

fn entry_bytes(key: &EncodedKey) -> u64 {
	SlabLru::<EncodedKey, GroupId>::entry_struct_bytes() as u64 + key.heap_bytes() as u64
}

fn dictionary_key(group: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::GROUP_DICTIONARY, group)
}

fn record_key(id: GroupId) -> GroupStateKey {
	OperatorStateKey::inner_encoded(id, Keyspace::GROUP_RECORD, vec![])
}

fn counter_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::NODE_COUNTER, vec![])
}

pub(super) fn encode_payload<T: OperatorState>(value: &T, now: DateTime) -> Result<EncodedOperatorRow> {
	Ok(value.encode_state(now)?)
}

pub(super) fn decode_payload<T: OperatorState>(row: &EncodedOperatorRow) -> Result<T> {
	Ok(decode(row)?)
}

pub(super) fn decode_bytes<T: OperatorState>(bytes: &EncodedBytes) -> Result<T> {
	decode_payload(&EncodedOperatorRow::try_from(bytes.clone())?)
}

struct NodeState {
	cache: SlabLru<EncodedKey, GroupId>,
	cache_size: ByteSize,
	hydrated: bool,
	complete: bool,
	next: Option<u64>,
}

impl Default for NodeState {
	fn default() -> Self {
		Self {
			cache: SlabLru::unbounded(),
			cache_size: ByteSize::ZERO,
			hydrated: false,
			complete: false,
			next: None,
		}
	}
}

impl NodeState {
	fn remember(&mut self, group: &EncodedKey, id: GroupId) {
		if self.cache.put(group.clone(), id).is_none() {
			self.cache_size = self.cache_size.saturating_add(ByteSize::from_bytes(entry_bytes(group)));
		}
	}

	fn forget(&mut self, group: &EncodedKey) -> bool {
		if self.cache.remove(group).is_some() {
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(group)));
			true
		} else {
			false
		}
	}

	fn revoke_complete(&mut self) {
		self.complete = false;
	}

	fn evict_to_budget(&mut self, budget: ByteSize) {
		while self.cache_size > budget {
			let Some((group, _)) = self.cache.pop_tail() else {
				break;
			};
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(&group)));
			self.revoke_complete();
		}
	}
}

#[derive(Clone)]
pub struct GroupInterner {
	inner: Arc<GroupInternerInner>,
}

struct GroupInternerInner {
	operators: DashMap<OperatorId, NodeState>,
	budget: ByteSize,
}

impl Default for GroupInterner {
	fn default() -> Self {
		Self::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET))
	}
}

impl GroupInterner {
	pub fn new(budget: ByteSize) -> Self {
		Self {
			inner: Arc::new(GroupInternerInner {
				operators: DashMap::new(),
				budget,
			}),
		}
	}

	pub fn intern(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		group: &EncodedKey,
	) -> Result<(GroupId, bool)> {
		Ok(self.intern_many(operator, txn, from_ref(group))?.into_iter().next().unwrap())
	}

	pub fn intern_many(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		let now = txn.written_at();
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;
		let mut results: Vec<Option<(GroupId, bool)>> = (0..groups.len()).map(|_| None).collect();
		let mut to_resolve: Vec<usize> = Vec::new();
		for (i, group) in groups.iter().enumerate() {
			match state.cache.get(group) {
				Some(id) => results[i] = Some((id, false)),
				None => to_resolve.push(i),
			}
		}
		if to_resolve.is_empty() {
			state.evict_to_budget(budget);
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let dictionary_keys: Vec<GroupStateKey> =
			to_resolve.iter().map(|i| dictionary_key(&groups[*i])).collect();

		let found: HashMap<Vec<u8>, EncodedBytes> = if state.complete {
			HashMap::new()
		} else {
			let batch = txn.state_get_many(operator, &dictionary_keys)?;
			let mut found = HashMap::with_capacity(batch.items.len());
			for item in batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_get_many must return OperatorState keys");
				found.insert(decoded.inner().as_slice().to_vec(), item.bytes);
			}
			found
		};

		let mut resolved_from_store: Vec<(usize, GroupId)> = Vec::new();
		let mut new_slots: Vec<bool> = vec![false; dictionary_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<Vec<u8>, usize> = HashMap::new();
		for (slot, dictionary) in dictionary_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(dictionary.as_slice()) {
				Some(existing) => {
					let id = GroupId(decode_bytes::<u64>(existing)?);
					resolved_from_store.push((i, id));
					results[i] = Some((id, false));
				}
				None => {
					new_slots[slot] = true;
					if !first_new_slot.contains_key(dictionary.as_slice()) {
						first_new_slot.insert(dictionary.as_slice().to_vec(), slot);
						distinct_new.push(slot);
					}
				}
			}
		}

		if !distinct_new.is_empty() {
			let start = Self::mint(state, operator, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<Vec<u8>, GroupId> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let dictionary = &dictionary_keys[slot];
				let id = GroupId(start + offset as u64);
				txn.state_set(operator, dictionary, encode_payload(&id.0, now)?)?;
				Self::stamp(txn, operator, id, &groups[i], now)?;
				state.remember(&groups[i], id);
				assigned.insert(dictionary.as_slice().to_vec(), id);
			}
			for (slot, dictionary) in dictionary_keys.iter().enumerate() {
				if new_slots[slot] {
					let i = to_resolve[slot];
					let id = assigned[dictionary.as_slice()];
					let is_new = first_new_slot.get(dictionary.as_slice()) == Some(&slot);
					results[i] = Some((id, is_new));
				}
			}
		}

		for (i, id) in resolved_from_store {
			Self::stamp(txn, operator, id, &groups[i], now)?;
			state.remember(&groups[i], id);
		}

		state.evict_to_budget(budget);

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	fn stamp(
		txn: &mut impl FlowTransaction,
		operator: OperatorId,
		id: GroupId,
		group: &EncodedKey,
		now: DateTime,
	) -> Result<()> {
		txn.state_set(
			operator,
			&record_key(id),
			encode_payload(&GroupRecord::new(group.as_ref().to_vec()), now)?,
		)
	}

	pub fn lookup(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		group: &EncodedKey,
	) -> Result<Option<GroupId>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;

		if let Some(id) = state.cache.get(group) {
			return Ok(Some(id));
		}
		if state.complete {
			return Ok(None);
		}
		let Some(row) = txn.state_get(operator, &dictionary_key(group))? else {
			return Ok(None);
		};
		let id = GroupId(decode_payload::<u64>(&row)?);
		state.remember(group, id);
		state.evict_to_budget(budget);
		Ok(Some(id))
	}

	pub fn forget(&self, operator: OperatorId, txn: &mut impl FlowTransaction, group: &EncodedKey) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;

		let cached = state.cache.get(group);
		state.forget(group);
		let existed = cached.is_some() || !state.complete;
		txn.state_remove(operator, &dictionary_key(group))?;
		Ok(existed)
	}

	pub fn group_bytes(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		id: GroupId,
	) -> Result<Option<EncodedKey>> {
		let Some(row) = txn.state_get(operator, &record_key(id))? else {
			return Ok(None);
		};
		Ok(Some(EncodedKey::new(decode_payload::<GroupRecord>(&row)?.group)))
	}

	fn hydrate_once(
		state: &mut NodeState,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		budget: ByteSize,
	) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		state.complete = true;
		let base = keyspace_inner_range(GroupId::ROOT, Keyspace::GROUP_DICTIONARY);
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(HYDRATE_CHUNK), "group::hydrate")?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				reifydb_assertions! {
					let (group_id, keyspace) = (decoded.group, decoded.keyspace);
					assert!(
						group_id == GroupId::ROOT
							&& keyspace == Keyspace::GROUP_DICTIONARY,
						"the dictionary range scan must only yield operator-scope dictionary keys; \
						 anything else means the range bounds are wrong and hydration would \
						 poison the interning cache with another keyspace's payloads \
						 (group={group_id:?}, keyspace={keyspace:?})"
					);
				}
				let group = EncodedKey::new(decoded.suffix.clone());
				let id = GroupId(decode_bytes::<u64>(&item.bytes)?);
				state.remember(&group, id);
				last_inner = Some(decoded.inner());
			}
			state.evict_to_budget(budget);
			if !batch.has_more {
				break;
			}
			let Some(last) = last_inner else {
				break;
			};
			start = Bound::Excluded(last);
		}
		Ok(())
	}

	fn mint(
		state: &mut NodeState,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		count: u64,
	) -> Result<u64> {
		let seed = match state.next {
			Some(next) => next,
			None => match txn.state_get(operator, &counter_key())? {
				Some(row) => decode_payload::<u64>(&row)?,
				None => GroupId::FIRST.0,
			},
		};
		reifydb_assertions! {
			assert!(
				seed >= GroupId::FIRST.0,
				"group id 0 is reserved for operator scope, where the interning dictionary and the \
				 counter live; minting it would put a real group's state on top of the table that \
				 resolves every group (seed={seed})"
			);
		}
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.written_at();
		txn.state_set(operator, &counter_key(), encode_payload(&high_water, now)?)?;
		Ok(seed)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{actors::pending::PendingLayers, key::operator_state::group_data_inner_range};
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;
	use crate::transaction::{
		DeferredParams,
		substrate::{FlowSubstrate, apply_operator_state},
	};

	const NODE: OperatorId = OperatorId(1);

	fn group(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn deferred(engine: &TestEngine) -> DepFlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		DepFlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
		})
	}

	fn commit_pending(engine: &TestEngine, txn: &mut DepFlowTransaction) {
		let pending = txn.take_pending();
		apply_operator_state(&engine.inner().operator_state(), &pending);
	}

	fn intern_at(
		interner: &GroupInterner,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		group: &EncodedKey,
	) -> Result<(GroupId, bool)> {
		interner.intern(operator, txn, group)
	}

	fn intern_many_at(
		interner: &GroupInterner,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		interner.intern_many(operator, txn, groups)
	}

	#[test]
	fn eviction_charge_covers_what_an_entry_actually_retains() {
		let mut state = NodeState::default();
		for i in 0..256u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1));
		}

		let retained = state.cache.len() as u64 * SlabLru::<EncodedKey, GroupId>::entry_struct_bytes() as u64;
		assert!(
			state.cache_size.as_bytes() >= retained,
			"charged {} for {} entries that retain {}",
			state.cache_size.as_bytes(),
			state.cache.len(),
			retained
		);
	}

	#[test]
	fn a_budget_bounds_the_memory_its_surviving_entries_retain() {
		let budget = ByteSize::from_bytes(64 * 1024);
		let mut state = NodeState::default();
		for i in 0..4096u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1));
		}

		state.evict_to_budget(budget);

		let retained = state.cache.len() as u64 * SlabLru::<EncodedKey, GroupId>::entry_struct_bytes() as u64;
		assert!(
			retained <= budget.as_bytes(),
			"{} entries survived a {} byte budget and retain {}",
			state.cache.len(),
			budget.as_bytes(),
			retained
		);
	}

	#[test]
	fn the_first_group_interns_to_the_first_usable_id() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (id, is_new) = intern_at(&interner, NODE, &mut txn, &group("first")).unwrap();

		assert_eq!(id, GroupId::FIRST, "the first group must not take the operator-scope id");
		assert!(!id.is_root());
		assert!(is_new, "a never-seen group must report as newly interned");
	}

	#[test]
	fn a_repeated_group_resolves_to_the_same_id() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (first, new_first) = intern_at(&interner, NODE, &mut txn, &group("mint")).unwrap();
		let (second, new_second) = intern_at(&interner, NODE, &mut txn, &group("mint")).unwrap();

		assert_eq!(first, second, "the same group bytes must always resolve to the same id");
		assert!(new_first);
		assert!(!new_second, "only the first sighting is newly interned");
	}

	#[test]
	fn distinct_groups_get_distinct_ids() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let ids: Vec<GroupId> = (0..5)
			.map(|i| intern_at(&interner, NODE, &mut txn, &group(&format!("g{i}"))).unwrap().0)
			.collect();

		let mut unique = ids.clone();
		unique.sort_unstable();
		unique.dedup();
		assert_eq!(unique.len(), ids.len(), "two groups sharing an id would share a state range");
	}

	#[test]
	fn a_batch_dedupes_repeated_groups_and_reports_one_mint() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let batch = vec![group("a"), group("b"), group("a"), group("b"), group("a")];
		let resolved = intern_many_at(&interner, NODE, &mut txn, &batch).unwrap();

		assert_eq!(resolved[0].0, resolved[2].0);
		assert_eq!(resolved[0].0, resolved[4].0);
		assert_eq!(resolved[1].0, resolved[3].0);
		assert_ne!(resolved[0].0, resolved[1].0);
		assert_eq!(
			resolved.iter().filter(|(_, is_new)| *is_new).count(),
			2,
			"a batch of two distinct groups must report exactly two mints"
		);
	}

	#[test]
	fn ids_survive_a_restart() {
		let engine = TestEngine::new();
		let before = {
			let interner = GroupInterner::default();
			let mut txn = deferred(&engine);
			let id = intern_at(&interner, NODE, &mut txn, &group("survivor")).unwrap().0;
			intern_at(&interner, NODE, &mut txn, &group("other")).unwrap();
			commit_pending(&engine, &mut txn);
			id
		};

		let cold = GroupInterner::default();
		let mut txn = deferred(&engine);
		let (after, is_new) = intern_at(&cold, NODE, &mut txn, &group("survivor")).unwrap();

		assert_eq!(after, before, "a restarted interner must resolve an existing group to its stored id");
		assert!(!is_new, "an existing group must not be reported as newly interned after a restart");
	}

	#[test]
	fn a_reborn_group_never_reuses_the_id_of_the_generation_before_it() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let original = intern_at(&interner, NODE, &mut txn, &group("reborn")).unwrap().0;
		interner.forget(NODE, &mut txn, &group("reborn")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		let (reborn, is_new) = intern_at(&interner, NODE, &mut txn, &group("reborn")).unwrap();

		assert!(is_new, "a forgotten group is unknown again and must mint afresh");
		assert_ne!(reborn, original, "a reclaimed id must never be handed back out");
	}

	#[test]
	fn a_forgotten_group_stops_resolving() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("gone")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		interner.forget(NODE, &mut txn, &group("gone")).unwrap();
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::default();
		let mut txn = deferred(&engine);
		assert_eq!(
			cold.lookup(NODE, &mut txn, &group("gone")).unwrap(),
			None,
			"a forgotten group must not resurrect through hydration"
		);
	}

	#[test]
	fn an_id_resolves_back_to_the_bytes_it_was_interned_from() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		let bytes = group("two-address-key");

		let (id, _) = intern_at(&interner, NODE, &mut txn, &bytes).unwrap();

		assert_eq!(
			interner.group_bytes(NODE, &mut txn, id).unwrap(),
			Some(bytes),
			"an interned group must be resolvable from its id alone"
		);
	}

	#[test]
	fn the_reverse_record_lives_outside_the_group_data_range() {
		// Floor compaction cancels rows in the group's data keyspaces only, so the reverse record
		// must sit outside that range or a floored group could no longer resolve its own bytes.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		let bytes = group("outlives-its-data");
		let (id, _) = intern_at(&interner, NODE, &mut txn, &bytes).unwrap();

		let batch = txn
			.state_range(NODE, group_data_inner_range(id), None, "test")
			.expect("the group data range must scan");
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key).expect("state keys decode");
			let inner = GroupStateKey::from_framed(decoded.inner())
				.expect("the data range yields framed inner keys");
			txn.state_remove(NODE, &inner).unwrap();
		}

		assert_eq!(
			interner.group_bytes(NODE, &mut txn, id).unwrap(),
			Some(bytes),
			"erasing every data row must not take the record identity depends on"
		);
	}

	#[test]
	fn lookup_does_not_intern() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		assert_eq!(interner.lookup(NODE, &mut txn, &group("absent")).unwrap(), None);

		let (id, is_new) = intern_at(&interner, NODE, &mut txn, &group("absent")).unwrap();
		assert!(is_new, "the earlier lookup must not have interned the group");
		assert_eq!(id, GroupId::FIRST, "a lookup must not consume an id from the counter");
	}

	#[test]
	fn nodes_intern_independently() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let first = intern_at(&interner, OperatorId(1), &mut txn, &group("shared")).unwrap().0;
		let second = intern_at(&interner, OperatorId(2), &mut txn, &group("shared")).unwrap().0;

		assert_eq!(first, second, "each operator numbers its own groups from the same starting point");

		let other = intern_at(&interner, OperatorId(2), &mut txn, &group("only-on-two")).unwrap().0;
		let mut txn = deferred(&engine);
		assert_eq!(
			interner.lookup(OperatorId(1), &mut txn, &group("only-on-two")).unwrap(),
			None,
			"a group interned on one operator must not resolve on another"
		);
		assert_ne!(other, first);
	}
}

impl DepFlowTransaction {
	pub fn intern_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<(GroupId, bool)> {
		let interner = self.group_interner();
		let (id, is_new) = interner.intern(operator, self, group)?;
		if is_new {
			self.row_numbers().mark_fresh(operator, id);
		}
		Ok((id, is_new))
	}

	pub fn intern_groups(&mut self, operator: OperatorId, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		let interner = self.group_interner();
		let results = interner.intern_many(operator, self, groups)?;
		let provider = self.row_numbers();
		for (id, is_new) in &results {
			if *is_new {
				provider.mark_fresh(operator, *id);
			}
		}
		Ok(results)
	}

	pub fn lookup_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<Option<GroupId>> {
		let interner = self.group_interner();
		interner.lookup(operator, self, group)
	}

	pub fn forget_group(&mut self, operator: OperatorId, group: &EncodedKey) -> Result<bool> {
		let interner = self.group_interner();
		interner.forget(operator, self, group)
	}

	pub fn group_bytes(&mut self, operator: OperatorId, id: GroupId) -> Result<Option<EncodedKey>> {
		let interner = self.group_interner();
		interner.group_bytes(operator, self, id)
	}
}
