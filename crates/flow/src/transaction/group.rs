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

use crate::transaction::FlowTransaction;

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

pub struct NodeState {
	pub cache: SlabLru<EncodedKey, GroupId>,
	pub cache_size: ByteSize,
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
	pub fn remember(&mut self, group: &EncodedKey, id: GroupId) {
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

	pub fn evict_to_budget(&mut self, budget: ByteSize) {
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

