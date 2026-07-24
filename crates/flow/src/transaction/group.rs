// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::size_of, ops::Bound, slice::from_ref, sync::Arc};

use dashmap::DashMap;
use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::{
		EncodableKey,
		flow_node_internal_state::FlowNodeInternalStateKey,
		operator_state::{GroupId, Keyspace, OperatorStateKey, keyspace_inner_range},
	},
	metrics::heap::{StateCompleteness, StateMemory},
	state::membership::{MEMBERSHIP_BYTE_CAP, MembershipTracker},
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{Result, byte_size::ByteSize, count::Count, reifydb_assertions, util::hash::xxh3_64};

use super::FlowTransaction;

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const ENTRY_OVERHEAD: u64 = (size_of::<usize>() * 2) as u64;
const HYDRATE_CHUNK: usize = 8_192;

fn entry_bytes(key: &EncodedKey) -> u64 {
	(size_of::<EncodedKey>() + size_of::<GroupId>()) as u64 + ENTRY_OVERHEAD + key.as_ref().len() as u64
}

fn membership_hash(key: &EncodedKey) -> u64 {
	xxh3_64(key.as_ref()).0
}

fn dictionary_key(group: &EncodedKey) -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY, group.as_ref().to_vec())
}

fn counter_key() -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, vec![])
}

fn encode_payload<T: OperatorState>(value: &T, now_nanos: u64) -> Result<EncodedRow> {
	Ok(value.encode_state(now_nanos)?.into_row())
}

fn decode_payload<T: OperatorState>(row: &EncodedRow) -> Result<T> {
	Ok(decode_state(&StateBytes::from_row(row.clone())?)?)
}

struct NodeState {
	cache: SlabLru<EncodedKey, GroupId>,
	cache_size: ByteSize,
	membership: MembershipTracker,
	hydrated: bool,
	complete: bool,
	next: Option<u64>,
	revocations: u64,
}

impl Default for NodeState {
	fn default() -> Self {
		Self {
			cache: SlabLru::unbounded(),
			cache_size: ByteSize::ZERO,
			membership: MembershipTracker::new(MEMBERSHIP_BYTE_CAP),
			hydrated: false,
			complete: false,
			next: None,
			revocations: 0,
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
		if self.complete {
			self.complete = false;
			self.revocations += 1;
		}
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

	fn completeness(&self) -> StateCompleteness {
		if !self.hydrated {
			return StateCompleteness::MERGE_IDENTITY;
		}
		StateCompleteness {
			values_complete: self.complete,
			membership_complete: self.membership.is_tracked(),
			absences_served: Count::new(self.membership.absences_served()),
			false_positives: Count::new(self.membership.false_positives()),
			revocations: Count::new(self.revocations),
		}
	}

	fn memory(&self) -> StateMemory {
		let bytes = self.cache_size.saturating_add(ByteSize::from_bytes(self.cache.struct_bytes() as u64));
		StateMemory::new(Count::new(self.cache.len() as u64), bytes)
	}
}

pub struct GroupInternerSample {
	pub cache: StateMemory,
	pub membership: StateMemory,
	pub completeness: StateCompleteness,
}

#[derive(Clone)]
pub struct GroupInterner {
	inner: Arc<GroupInternerInner>,
}

struct GroupInternerInner {
	nodes: DashMap<FlowNodeId, NodeState>,
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
				nodes: DashMap::new(),
				budget,
			}),
		}
	}

	pub fn intern(&self, node: FlowNodeId, txn: &mut FlowTransaction, group: &EncodedKey) -> Result<(GroupId, bool)> {
		Ok(self.intern_many(node, txn, from_ref(group))?.into_iter().next().unwrap())
	}

	pub fn intern_many(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		let now = txn.clock().now_nanos();
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
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
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let dictionary_keys: Vec<EncodedKey> = to_resolve.iter().map(|i| dictionary_key(&groups[*i])).collect();

		let mut consulted_store: Vec<bool> = Vec::new();
		let found: HashMap<Vec<u8>, EncodedRow> = if state.complete {
			HashMap::new()
		} else {
			let mut lookup: Vec<EncodedKey> = Vec::new();
			for (slot, i) in to_resolve.iter().enumerate() {
				let maybe = state.membership.contains(membership_hash(&groups[*i])).unwrap_or(true);
				consulted_store.push(maybe);
				if maybe {
					lookup.push(dictionary_keys[slot].clone());
				} else {
					state.membership.count_absence();
				}
			}
			if lookup.is_empty() {
				HashMap::new()
			} else {
				let batch = txn.internal_state_get_many(node, &lookup)?;
				let mut found = HashMap::with_capacity(batch.items.len());
				for item in batch.items {
					let decoded = FlowNodeInternalStateKey::decode(&item.key)
						.expect("internal_state_get_many must return FlowNodeInternalState keys");
					found.insert(decoded.key, item.row);
				}
				found
			}
		};

		let mut new_slots: Vec<bool> = vec![false; dictionary_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<Vec<u8>, usize> = HashMap::new();
		for (slot, dictionary) in dictionary_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(dictionary.as_ref()) {
				Some(existing) => {
					let id = GroupId(decode_payload::<u64>(existing)?);
					state.remember(&groups[i], id);
					results[i] = Some((id, false));
				}
				None => {
					if consulted_store.get(slot) == Some(&true) {
						state.membership.record_store_miss();
					}
					new_slots[slot] = true;
					if !first_new_slot.contains_key(dictionary.as_ref()) {
						first_new_slot.insert(dictionary.as_ref().to_vec(), slot);
						distinct_new.push(slot);
					}
				}
			}
		}

		if !distinct_new.is_empty() {
			let start = Self::mint(state, node, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<Vec<u8>, GroupId> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let dictionary = &dictionary_keys[slot];
				let id = GroupId(start + offset as u64);
				txn.internal_state_set(node, dictionary, encode_payload(&id.0, now)?)?;
				state.remember(&groups[i], id);
				state.membership.insert(membership_hash(&groups[i]));
				assigned.insert(dictionary.as_ref().to_vec(), id);
			}
			for (slot, dictionary) in dictionary_keys.iter().enumerate() {
				if new_slots[slot] {
					let i = to_resolve[slot];
					let id = assigned[dictionary.as_ref()];
					let is_new = first_new_slot.get(dictionary.as_ref()) == Some(&slot);
					results[i] = Some((id, is_new));
				}
			}
		}

		state.evict_to_budget(budget);

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	pub fn lookup(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
	) -> Result<Option<GroupId>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;

		if let Some(id) = state.cache.get(group) {
			return Ok(Some(id));
		}
		if state.complete {
			return Ok(None);
		}
		if state.membership.contains(membership_hash(group)) == Some(false) {
			state.membership.count_absence();
			return Ok(None);
		}
		let Some(row) = txn.internal_state_get(node, &dictionary_key(group))? else {
			state.membership.record_store_miss();
			return Ok(None);
		};
		let id = GroupId(decode_payload::<u64>(&row)?);
		state.remember(group, id);
		state.evict_to_budget(budget);
		Ok(Some(id))
	}

	pub fn forget(&self, node: FlowNodeId, txn: &mut FlowTransaction, group: &EncodedKey) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;

		let cached = state.forget(group);
		state.membership.remove(membership_hash(group));
		let existed = cached || !state.complete;
		txn.internal_state_remove(node, &dictionary_key(group))?;
		Ok(existed)
	}

	pub fn sample(&self, node: FlowNodeId) -> Option<GroupInternerSample> {
		self.inner.nodes.get(&node).map(|state| GroupInternerSample {
			cache: state.memory(),
			membership: state.membership.memory(),
			completeness: state.completeness(),
		})
	}

	fn hydrate_once(
		state: &mut NodeState,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		budget: ByteSize,
	) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		state.complete = true;
		let base = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY);
		let mut hashes: Vec<u64> = Vec::new();
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.internal_state_range(node, range, Some(HYDRATE_CHUNK))?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = FlowNodeInternalStateKey::decode(&item.key)
					.expect("internal_state_range must return FlowNodeInternalState keys");
				let (group_id, keyspace, suffix) = OperatorStateKey::decode_inner(&decoded.key)
					.expect("the dictionary range must yield structured operator state keys");
				reifydb_assertions! {
					assert!(
						group_id == GroupId::NODE_SCOPE
							&& keyspace == Keyspace::GROUP_DICTIONARY,
						"the dictionary range scan must only yield node-scope dictionary keys; \
						 anything else means the range bounds are wrong and hydration would \
						 poison the interning cache with another keyspace's payloads \
						 (group={group_id:?}, keyspace={keyspace:?})"
					);
				}
				let group = EncodedKey::new(suffix);
				hashes.push(membership_hash(&group));
				state.remember(&group, GroupId(decode_payload::<u64>(&item.row)?));
				last_inner = Some(EncodedKey::new(decoded.key.clone()));
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
		state.membership.install(&hashes);
		Ok(())
	}

	fn mint(state: &mut NodeState, node: FlowNodeId, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		let seed = match state.next {
			Some(next) => next,
			None => match txn.internal_state_get(node, &counter_key())? {
				Some(row) => decode_payload::<u64>(&row)?,
				None => GroupId::FIRST.0,
			},
		};
		reifydb_assertions! {
			assert!(
				seed >= GroupId::FIRST.0,
				"group id 0 is reserved for node scope, where the interning dictionary and the \
				 counter live; minting it would put a real group's state on top of the table that \
				 resolves every group (seed={seed})"
			);
		}
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.clock().now_nanos();
		txn.internal_state_set(node, &counter_key(), encode_payload(&high_water, now)?)?;
		Ok(seed)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::actors::pending::PendingWrite;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	const NODE: FlowNodeId = FlowNodeId(1);

	fn group(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(
			&parent,
			version,
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(0)),
		)
	}

	// Persist a deferred transaction's pending writes so a cold interner resolves them the way a
	// restarted process would.
	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		for (k, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => cmd.set(k, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(k).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(k).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
	}

	#[test]
	fn the_first_group_interns_to_the_first_usable_id() {
		// Id 0 is reserved for node scope, where the dictionary and the counter live. If minting
		// started at 0 a real group's state would land on top of the table that resolves every
		// group, and reclaiming that group would erase the substrate's own address book.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (id, is_new) = interner.intern(NODE, &mut txn, &group("first")).unwrap();

		assert_eq!(id, GroupId::FIRST, "the first group must not take the node-scope id");
		assert!(!id.is_node_scope());
		assert!(is_new, "a never-seen group must report as newly interned");
	}

	#[test]
	fn a_repeated_group_resolves_to_the_same_id() {
		// The id IS the state address. If interning were not stable, a group's second batch would
		// write to a different range than its first and its accumulated state would be orphaned -
		// invisible to reads and unreachable by reclamation.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (first, new_first) = interner.intern(NODE, &mut txn, &group("mint")).unwrap();
		let (second, new_second) = interner.intern(NODE, &mut txn, &group("mint")).unwrap();

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
			.map(|i| interner.intern(NODE, &mut txn, &group(&format!("g{i}"))).unwrap().0)
			.collect();

		let mut unique = ids.clone();
		unique.sort_unstable();
		unique.dedup();
		assert_eq!(unique.len(), ids.len(), "two groups sharing an id would share a state range");
	}

	#[test]
	fn a_batch_dedupes_repeated_groups_and_reports_one_mint() {
		// Drivers intern a whole batch at once and batches repeat groups constantly. Minting twice
		// for one group would strand the state written under the first id.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let batch = vec![group("a"), group("b"), group("a"), group("b"), group("a")];
		let resolved = interner.intern_many(NODE, &mut txn, &batch).unwrap();

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
		// Hydration is the whole reason the dictionary is durable. A restarted process that
		// re-interned from zero would address every existing group's state at a fresh id, silently
		// abandoning everything on disk and double-counting from an empty accumulator.
		let engine = TestEngine::new();
		let before = {
			let interner = GroupInterner::default();
			let mut txn = deferred(&engine);
			let id = interner.intern(NODE, &mut txn, &group("survivor")).unwrap().0;
			interner.intern(NODE, &mut txn, &group("other")).unwrap();
			commit_pending(&engine, &mut txn);
			id
		};

		let cold = GroupInterner::default();
		let mut txn = deferred(&engine);
		let (after, is_new) = cold.intern(NODE, &mut txn, &group("survivor")).unwrap();

		assert_eq!(after, before, "a restarted interner must resolve an existing group to its stored id");
		assert!(!is_new, "an existing group must not be reported as newly interned after a restart");
	}

	#[test]
	fn a_reborn_group_never_reuses_the_id_of_the_generation_before_it() {
		// Phase-2 identity reclamation forgets a group, but its sink rows can outlive it. Handing the
		// id back out would point a fresh generation's state at the previous one's range, mixing two
		// unrelated groups' data. The counter is monotone precisely so this cannot happen.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let original = interner.intern(NODE, &mut txn, &group("reborn")).unwrap().0;
		interner.forget(NODE, &mut txn, &group("reborn")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		let (reborn, is_new) = interner.intern(NODE, &mut txn, &group("reborn")).unwrap();

		assert!(is_new, "a forgotten group is unknown again and must mint afresh");
		assert_ne!(reborn, original, "a reclaimed id must never be handed back out");
	}

	#[test]
	fn a_forgotten_group_stops_resolving() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		interner.intern(NODE, &mut txn, &group("gone")).unwrap();
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
	fn lookup_does_not_intern() {
		// Reclamation and diagnostics ask whether a group exists. If asking created it, a scan over
		// dead groups would resurrect every one of them and the dictionary could never shrink.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		assert_eq!(interner.lookup(NODE, &mut txn, &group("absent")).unwrap(), None);

		let (id, is_new) = interner.intern(NODE, &mut txn, &group("absent")).unwrap();
		assert!(is_new, "the earlier lookup must not have interned the group");
		assert_eq!(id, GroupId::FIRST, "a lookup must not consume an id from the counter");
	}

	#[test]
	fn nodes_intern_independently() {
		// Ids are per node, so two nodes may hold the same id for different groups. A shared counter
		// or a shared dictionary would let one node's reclamation erase another's state.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let first = interner.intern(FlowNodeId(1), &mut txn, &group("shared")).unwrap().0;
		let second = interner.intern(FlowNodeId(2), &mut txn, &group("shared")).unwrap().0;

		assert_eq!(first, second, "each node numbers its own groups from the same starting point");

		let other = interner.intern(FlowNodeId(2), &mut txn, &group("only-on-two")).unwrap().0;
		let mut txn = deferred(&engine);
		assert_eq!(
			interner.lookup(FlowNodeId(1), &mut txn, &group("only-on-two")).unwrap(),
			None,
			"a group interned on one node must not resolve on another"
		);
		assert_ne!(other, first);
	}
}

impl FlowTransaction {
	pub fn intern_group(&mut self, node: FlowNodeId, group: &EncodedKey) -> Result<(GroupId, bool)> {
		let interner = self.group_interner();
		interner.intern(node, self, group)
	}

	pub fn intern_groups(&mut self, node: FlowNodeId, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
		let interner = self.group_interner();
		interner.intern_many(node, self, groups)
	}

	pub fn lookup_group(&mut self, node: FlowNodeId, group: &EncodedKey) -> Result<Option<GroupId>> {
		let interner = self.group_interner();
		interner.lookup(node, self, group)
	}

	pub fn forget_group(&mut self, node: FlowNodeId, group: &EncodedKey) -> Result<bool> {
		let interner = self.group_interner();
		interner.forget(node, self, group)
	}
}
