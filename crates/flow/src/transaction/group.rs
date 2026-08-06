// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, slice::from_ref, sync::Arc};

use dashmap::DashMap;
use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey, keyspace_inner_range},
		operator_state::OperatorStateKey,
	},
	metrics::heap::{StateCompleteness, StateMemory},
	state::{
		group::{ActivityBuckets, GroupRecord},
		horizon::{Position, activity_buckets},
		membership::{MEMBERSHIP_BYTE_CAP, MembershipTracker},
	},
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	reifydb_assertions,
	util::hash::xxh3_64,
	value::{datetime::DateTime, duration::Duration},
};

use super::FlowTransaction;

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const HYDRATE_CHUNK: usize = 8_192;
const DEFAULT_ACTIVITY_BUCKET_WIDTH: u64 = 1 << 20;

fn entry_bytes(key: &EncodedKey) -> u64 {
	SlabLru::<EncodedKey, GroupId>::entry_struct_bytes() as u64 + key.heap_bytes() as u64
}

fn membership_hash(key: &EncodedKey) -> u64 {
	xxh3_64(key.as_ref()).0
}

fn dictionary_key(group: &EncodedKey) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY, group)
}

fn record_key(id: GroupId) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(id, Keyspace::GROUP_RECORD, vec![])
}

fn watermark_key() -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_WATERMARK, vec![])
}

fn counter_key() -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, vec![])
}

pub(super) fn encode_payload<T: OperatorState>(value: &T, now: DateTime) -> Result<EncodedRow> {
	Ok(value.encode_state(now)?.into_row())
}

pub(super) fn decode_payload<T: OperatorState>(row: &EncodedRow) -> Result<T> {
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
	position: Option<Position>,
	buckets: Option<ActivityBuckets>,
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
			position: None,
			buckets: None,
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
		let key_heap: u64 = self.cache.keys().map(|key| key.heap_bytes() as u64).sum();
		let bytes = ByteSize::from_bytes(self.cache.struct_bytes() as u64 + key_heap);
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
	operators: DashMap<OperatorId, NodeState>,
	budget: ByteSize,
	buckets: ActivityBuckets,
}

impl Default for GroupInterner {
	fn default() -> Self {
		Self::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), DEFAULT_ACTIVITY_BUCKET_WIDTH)
	}
}

impl GroupInterner {
	pub fn new(budget: ByteSize, activity_bucket_width: u64) -> Self {
		Self {
			inner: Arc::new(GroupInternerInner {
				operators: DashMap::new(),
				budget,
				buckets: ActivityBuckets::undeclared(activity_bucket_width),
			}),
		}
	}

	pub fn set_activity_grid(&self, operator: OperatorId, scale: Option<Duration>) {
		let mut state = self.inner.operators.entry(operator).or_default();
		state.buckets = Some(activity_buckets(scale));
	}

	pub fn buckets(&self, operator: OperatorId) -> ActivityBuckets {
		self.buckets_of(operator)
	}

	fn buckets_of(&self, operator: OperatorId) -> ActivityBuckets {
		self.inner.operators.get(&operator).and_then(|state| state.buckets).unwrap_or(self.inner.buckets)
	}

	pub fn intern(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
	) -> Result<(GroupId, bool)> {
		Ok(self.intern_many(operator, txn, from_ref(group))?.into_iter().next().unwrap())
	}

	pub fn intern_many(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
	) -> Result<Vec<(GroupId, bool)>> {
		let now = txn.written_at();
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;
		let position = Self::stamp_position(txn);

		let buckets = state.buckets.unwrap_or(self.inner.buckets);
		Self::advance_position(state, operator, txn, position, buckets, now)?;

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

		let mut consulted_store: Vec<bool> = Vec::new();
		let found: HashMap<Vec<u8>, EncodedRow> = if state.complete {
			HashMap::new()
		} else {
			let mut lookup: Vec<GroupStateKey> = Vec::new();
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
				let batch = txn.state_get_many(operator, &lookup)?;
				let mut found = HashMap::with_capacity(batch.items.len());
				for item in batch.items {
					let decoded = OperatorStateKey::decode(&item.key)
						.expect("state_get_many must return OperatorState keys");
					found.insert(decoded.key, item.row);
				}
				found
			}
		};

		let mut resolved_from_store: Vec<(usize, GroupId)> = Vec::new();
		let mut new_slots: Vec<bool> = vec![false; dictionary_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<Vec<u8>, usize> = HashMap::new();
		for (slot, dictionary) in dictionary_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(dictionary.as_slice()) {
				Some(existing) => {
					let id = GroupId(decode_payload::<u64>(existing)?);
					resolved_from_store.push((i, id));
					results[i] = Some((id, false));
				}
				None => {
					if consulted_store.get(slot) == Some(&true) {
						state.membership.record_store_miss();
					}
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
				state.membership.insert(membership_hash(&groups[i]));
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

	fn stamp_position(txn: &FlowTransaction) -> Position {
		let coordinate = txn.change_coordinate();
		reifydb_assertions! {
			assert!(
				coordinate.is_some(),
				"an intern ran before the substrate set the change coordinate; operators can \
				 no longer supply a position, so a missing coordinate means the executor \
				 skipped set_change_coordinate for this dispatch"
			);
		}
		Position(coordinate.expect("an intern requires the substrate change coordinate").at)
	}

	fn stamp(
		txn: &mut FlowTransaction,
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

	fn advance_position(
		state: &mut NodeState,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		position: Position,
		buckets: ActivityBuckets,
		now: DateTime,
	) -> Result<()> {
		let persist = match state.position {
			Some(previous) => {
				if position.raw() <= previous.raw() {
					return Ok(());
				}
				buckets.of(position) > buckets.of(previous)
			}
			None => true,
		};
		state.position = Some(position);
		if persist {
			txn.state_set(operator, &watermark_key(), encode_payload(&position.raw(), now)?)?;
		}
		Ok(())
	}

	pub fn position(&self, operator: OperatorId, txn: &mut FlowTransaction) -> Result<Position> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		Ok(guard.position.unwrap_or(Position::from_raw(0)))
	}

	pub fn lookup(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
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
		if state.membership.contains(membership_hash(group)) == Some(false) {
			state.membership.count_absence();
			return Ok(None);
		}
		let Some(row) = txn.state_get(operator, &dictionary_key(group))? else {
			state.membership.record_store_miss();
			return Ok(None);
		};
		let id = GroupId(decode_payload::<u64>(&row)?);
		state.remember(group, id);
		state.evict_to_budget(budget);
		Ok(Some(id))
	}

	pub fn forget(&self, operator: OperatorId, txn: &mut FlowTransaction, group: &EncodedKey) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_once(&mut guard, operator, txn, budget)?;
		let state = &mut *guard;

		let cached = state.cache.get(group);
		state.forget(group);
		state.membership.remove(membership_hash(group));
		let existed = cached.is_some() || !state.complete;
		txn.state_remove(operator, &dictionary_key(group))?;
		Ok(existed)
	}

	pub fn group_bytes(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		id: GroupId,
	) -> Result<Option<EncodedKey>> {
		let Some(row) = txn.state_get(operator, &record_key(id))? else {
			return Ok(None);
		};
		Ok(Some(EncodedKey::new(decode_payload::<GroupRecord>(&row)?.group)))
	}

	pub fn samples(&self) -> Vec<(OperatorId, GroupInternerSample)> {
		let mut out: Vec<(OperatorId, GroupInternerSample)> = self
			.inner
			.operators
			.iter()
			.map(|entry| {
				let state = entry.value();
				(
					*entry.key(),
					GroupInternerSample {
						cache: state.memory(),
						membership: state.membership.memory(),
						completeness: state.completeness(),
					},
				)
			})
			.collect();
		out.sort_by_key(|(operator, _)| *operator);
		out
	}

	fn hydrate_once(
		state: &mut NodeState,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		budget: ByteSize,
	) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		state.complete = true;
		if let Some(row) = txn.state_get(operator, &watermark_key())? {
			state.position = Some(Position::from_raw(decode_payload::<u64>(&row)?));
		}
		let base = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::GROUP_DICTIONARY);
		let mut hashes: Vec<u64> = Vec::new();
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(HYDRATE_CHUNK), "group::hydrate")?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
					.expect("the dictionary range must yield structured operator state keys");
				reifydb_assertions! {
					let (group_id, keyspace) = (inner.0, inner.1);
					assert!(
						group_id == GroupId::NODE_SCOPE
							&& keyspace == Keyspace::GROUP_DICTIONARY,
						"the dictionary range scan must only yield operator-scope dictionary keys; \
						 anything else means the range bounds are wrong and hydration would \
						 poison the interning cache with another keyspace's payloads \
						 (group={group_id:?}, keyspace={keyspace:?})"
					);
				}
				let group = EncodedKey::new(inner.2);
				hashes.push(membership_hash(&group));
				let id = GroupId(decode_payload::<u64>(&item.row)?);
				state.remember(&group, id);
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

	fn mint(state: &mut NodeState, operator: OperatorId, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
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
	use reifydb_core::{
		actors::pending::{Pending, PendingLayers},
		common::CommitVersion,
		key::operator_group_state::group_data_inner_range,
		state::budget::OperatorStateBudgetHandle,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::{duration::Duration, identity::IdentityId};

	use super::*;
	use crate::transaction::{
		ChangeCoordinate, DeferredParams,
		substrate::{FlowSubstrate, apply_operator_state},
	};

	const NODE: OperatorId = OperatorId(1);

	fn group(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: Pending::new(),
			base_pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			single: parent.single.clone(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(MockClock::from_millis(0)),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
			state_budget: OperatorStateBudgetHandle::default(),
		})
	}

	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		let pending = txn.take_pending();
		apply_operator_state(&engine.inner().operator_state(), txn.version(), &pending);
	}

	fn set_position(txn: &mut FlowTransaction, position: Position) {
		txn.set_change_coordinate(ChangeCoordinate {
			at: position.instant(),
			version: CommitVersion(0),
		});
	}

	fn intern_at(
		interner: &GroupInterner,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		group: &EncodedKey,
		position: Position,
	) -> Result<(GroupId, bool)> {
		set_position(txn, position);
		interner.intern(operator, txn, group)
	}

	fn intern_many_at(
		interner: &GroupInterner,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		groups: &[EncodedKey],
		position: Position,
	) -> Result<Vec<(GroupId, bool)>> {
		set_position(txn, position);
		interner.intern_many(operator, txn, groups)
	}

	#[test]
	fn reported_memory_counts_retained_containers_not_entry_bookkeeping() {
		let mut state = NodeState::default();
		for i in 0..64u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1));
		}

		assert!(
			state.cache.keys().all(|k| k.heap_bytes() == 0),
			"short group keys must stay inline or this test proves nothing"
		);
		assert_eq!(state.memory().entries.as_u64(), 64);
		assert_eq!(state.memory().bytes.as_bytes(), state.cache.struct_bytes() as u64);
	}

	#[test]
	fn reported_memory_counts_a_shared_out_of_line_key_once() {
		let long = EncodedKey::new(vec![7u8; 200]);
		assert!(long.heap_bytes() > 0, "key must spill out of line or this test proves nothing");

		let mut state = NodeState::default();
		state.remember(&long, GroupId(1));

		assert_eq!(
			state.memory().bytes.as_bytes(),
			state.cache.struct_bytes() as u64 + long.heap_bytes() as u64
		);
	}

	#[test]
	fn reported_memory_survives_eviction_of_every_entry() {
		let mut state = NodeState::default();
		for i in 0..64u64 {
			state.remember(&group(&format!("g{i}")), GroupId(i + 1));
		}
		let full = state.memory().bytes.as_bytes();

		state.evict_to_budget(ByteSize::ZERO);

		assert_eq!(state.memory().entries.as_u64(), 0, "budget of zero must drain every entry");
		assert_eq!(
			state.memory().bytes.as_bytes(),
			state.cache.struct_bytes() as u64,
			"a drained cache holds no key payload, so it reports exactly its containers"
		);

		assert!(
			state.memory().bytes.as_bytes() >= full,
			"retained capacity must not shrink on eviction: {} < {}",
			state.memory().bytes.as_bytes(),
			full
		);
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

		let (id, is_new) =
			intern_at(&interner, NODE, &mut txn, &group("first"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert_eq!(id, GroupId::FIRST, "the first group must not take the operator-scope id");
		assert!(!id.is_node_scope());
		assert!(is_new, "a never-seen group must report as newly interned");
	}

	#[test]
	fn minting_a_group_never_reads_the_record_it_is_about_to_write() {
		// An Append mints one group per source row, so a read here is a store round trip per row.
		// Measured at 1,550,000 load_record calls in 150s against zero reads from every other
		// path in the same operator. The id comes straight from the counter, so the record cannot
		// exist and the read can only ever return None.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		// Warms the one-time hydrate and the counter read so neither is billed to the mints below.
		intern_at(&interner, NODE, &mut txn, &group("warmup"), Position(DateTime::from_nanos(0))).unwrap();
		let before = txn.store_reads();

		for i in 0..16 {
			let (_, is_new) = intern_at(
				&interner,
				NODE,
				&mut txn,
				&group(&format!("fresh-{i}")),
				Position(DateTime::from_nanos(0)),
			)
			.unwrap();
			assert!(is_new, "precondition: every key here must be newly minted");
		}

		assert_eq!(
			txn.store_reads(),
			before,
			"minting groups must not reach the store; the membership filter proves absence and the \
			 record cannot exist for an id the counter just handed out"
		);
	}

	#[test]
	fn re_interning_a_cached_group_never_reaches_the_store() {
		// A Join probes one group per join key on every batch, so any read on this path is a store
		// round trip per probe. Measured at 88,208 point reads per profile window before the path
		// stopped reading; a cache hit must answer from the id alone and consult nothing.
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		interner.set_activity_grid(NODE, Some(Duration::from_seconds(60).unwrap()));
		let key = group("hot");

		// The record must be committed, not pending. A read served by the transaction's own
		// overlay never reaches the store and so never counts, which makes a same-transaction
		// version of this test pass even with the read restored.
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &key, Position(DateTime::from_nanos(0))).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		let before = txn.store_reads();

		for step in 1..=8u64 {
			intern_at(
				&interner,
				NODE,
				&mut txn,
				&key,
				Position(DateTime::from_nanos(step * 120_000_000_000)),
			)
			.unwrap();
		}

		assert_eq!(
			txn.store_reads(),
			before,
			"a cached group already resolves to its id; reading its record back is the round trip \
			 this path exists to avoid"
		);
	}

	#[test]
	fn a_repeated_group_resolves_to_the_same_id() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let (first, new_first) =
			intern_at(&interner, NODE, &mut txn, &group("mint"), Position(DateTime::from_nanos(0)))
				.unwrap();
		let (second, new_second) =
			intern_at(&interner, NODE, &mut txn, &group("mint"), Position(DateTime::from_nanos(0)))
				.unwrap();

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
			.map(|i| {
				intern_at(
					&interner,
					NODE,
					&mut txn,
					&group(&format!("g{i}")),
					Position(DateTime::from_nanos(0)),
				)
				.unwrap()
				.0
			})
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
		let resolved =
			intern_many_at(&interner, NODE, &mut txn, &batch, Position(DateTime::from_nanos(0))).unwrap();

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
			let id = intern_at(
				&interner,
				NODE,
				&mut txn,
				&group("survivor"),
				Position(DateTime::from_nanos(0)),
			)
			.unwrap()
			.0;
			intern_at(&interner, NODE, &mut txn, &group("other"), Position(DateTime::from_nanos(0)))
				.unwrap();
			commit_pending(&engine, &mut txn);
			id
		};

		let cold = GroupInterner::default();
		let mut txn = deferred(&engine);
		let (after, is_new) =
			intern_at(&cold, NODE, &mut txn, &group("survivor"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert_eq!(after, before, "a restarted interner must resolve an existing group to its stored id");
		assert!(!is_new, "an existing group must not be reported as newly interned after a restart");
	}

	#[test]
	fn a_reborn_group_never_reuses_the_id_of_the_generation_before_it() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let original =
			intern_at(&interner, NODE, &mut txn, &group("reborn"), Position(DateTime::from_nanos(0)))
				.unwrap()
				.0;
		interner.forget(NODE, &mut txn, &group("reborn")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut txn = deferred(&engine);
		let (reborn, is_new) =
			intern_at(&interner, NODE, &mut txn, &group("reborn"), Position(DateTime::from_nanos(0)))
				.unwrap();

		assert!(is_new, "a forgotten group is unknown again and must mint afresh");
		assert_ne!(reborn, original, "a reclaimed id must never be handed back out");
	}

	#[test]
	fn a_forgotten_group_stops_resolving() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("gone"), Position(DateTime::from_nanos(0))).unwrap();
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

		let (id, _) = intern_at(&interner, NODE, &mut txn, &bytes, Position(DateTime::from_nanos(0))).unwrap();

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
		let (id, _) = intern_at(&interner, NODE, &mut txn, &bytes, Position(DateTime::from_nanos(0))).unwrap();

		let batch = txn
			.state_range(NODE, group_data_inner_range(id), None, "test")
			.expect("the group data range must scan");
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key).expect("state keys decode");
			let inner = GroupStateKey::from_framed(EncodedKey::new(decoded.key))
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
	fn staying_inside_one_bucket_writes_nothing() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(10))).unwrap();
		let baseline = txn.take_pending().iter_sorted().count();

		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(20))).unwrap();
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(30))).unwrap();
		intern_at(&interner, NODE, &mut txn, &group("chatty"), Position(DateTime::from_nanos(99))).unwrap();

		assert_eq!(
			txn.take_pending().iter_sorted().count(),
			0,
			"repeat activity inside one bucket must not write at all (first batch wrote {baseline})"
		);
	}

	#[test]
	fn each_node_buckets_activity_at_its_own_width() {
		// register.rs declares a grid per operator and the node watermark is only persisted when the
		// position crosses one. A operator that borrowed another's grid would persist on a schedule
		// its own retention never asked for, and one that lost its declaration would fall back to the
		// interner default and stop tracking event time at all.
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 1_000);

		interner.set_activity_grid(OperatorId(2), Some(Duration::from_milliseconds(1_600).unwrap()));
		let mut txn = deferred(&engine);

		intern_at(&interner, OperatorId(1), &mut txn, &group("wide"), Position(DateTime::from_nanos(150)))
			.unwrap();
		intern_at(&interner, OperatorId(2), &mut txn, &group("narrow"), Position(DateTime::from_millis(150)))
			.unwrap();

		let ActivityBuckets::Undeclared(wide_grid) = interner.buckets(OperatorId(1)) else {
			panic!("an unconfigured operator declares no domain to bucket in");
		};
		assert_eq!(wide_grid.width(), 1_000, "an unconfigured operator keeps the default");
		assert_eq!(
			interner.buckets(OperatorId(2))
				.event_grid()
				.expect("a seal horizon buckets in event time")
				.width(),
			Duration::from_milliseconds(100).unwrap()
		);
	}

	#[test]
	fn the_node_position_is_the_high_water_of_everything_ever_stamped() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		intern_at(&interner, NODE, &mut txn, &group("a"), Position(DateTime::from_nanos(150))).unwrap();
		assert_eq!(interner.position(NODE, &mut txn).unwrap(), Position(DateTime::from_nanos(150)));

		intern_at(&interner, NODE, &mut txn, &group("b"), Position(DateTime::from_nanos(50))).unwrap();
		assert_eq!(
			interner.position(NODE, &mut txn).unwrap(),
			Position(DateTime::from_nanos(150)),
			"an out-of-order event must not lower it"
		);
	}

	#[test]
	fn the_node_position_survives_a_restart() {
		let engine = TestEngine::new();
		let interner = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);
		intern_at(&interner, NODE, &mut txn, &group("persisted"), Position(DateTime::from_nanos(4_500)))
			.unwrap();
		commit_pending(&engine, &mut txn);

		let cold = GroupInterner::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET), 100);
		let mut txn = deferred(&engine);

		assert_eq!(cold.position(NODE, &mut txn).unwrap(), Position(DateTime::from_nanos(4_500)));
	}

	#[test]
	fn lookup_does_not_intern() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		assert_eq!(interner.lookup(NODE, &mut txn, &group("absent")).unwrap(), None);

		let (id, is_new) =
			intern_at(&interner, NODE, &mut txn, &group("absent"), Position(DateTime::from_nanos(0)))
				.unwrap();
		assert!(is_new, "the earlier lookup must not have interned the group");
		assert_eq!(id, GroupId::FIRST, "a lookup must not consume an id from the counter");
	}

	#[test]
	fn nodes_intern_independently() {
		let engine = TestEngine::new();
		let interner = GroupInterner::default();
		let mut txn = deferred(&engine);

		let first = intern_at(
			&interner,
			OperatorId(1),
			&mut txn,
			&group("shared"),
			Position(DateTime::from_nanos(0)),
		)
		.unwrap()
		.0;
		let second = intern_at(
			&interner,
			OperatorId(2),
			&mut txn,
			&group("shared"),
			Position(DateTime::from_nanos(0)),
		)
		.unwrap()
		.0;

		assert_eq!(first, second, "each operator numbers its own groups from the same starting point");

		let other = intern_at(
			&interner,
			OperatorId(2),
			&mut txn,
			&group("only-on-two"),
			Position(DateTime::from_nanos(0)),
		)
		.unwrap()
		.0;
		let mut txn = deferred(&engine);
		assert_eq!(
			interner.lookup(OperatorId(1), &mut txn, &group("only-on-two")).unwrap(),
			None,
			"a group interned on one operator must not resolve on another"
		);
		assert_ne!(other, first);
	}
}

impl FlowTransaction {
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

	pub fn node_position(&mut self, operator: OperatorId) -> Result<Position> {
		let interner = self.group_interner();
		interner.position(operator, self)
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
