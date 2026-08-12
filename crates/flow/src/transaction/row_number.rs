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
		operator_state::{GroupId, GroupSet, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	},
	metrics::heap::{StateCompleteness, StateMemory},
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{Result, byte_size::ByteSize, count::Count, reifydb_assertions, value::row_number::RowNumber};

use crate::transaction::{FlowTransaction, state::StateTxn};

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const HYDRATE_CHUNK: usize = 8_192;
const ROW_NUMBER_COUNTER_SUFFIX: &[u8] = b"rn";

pub fn entry_bytes(key: &EncodedKey) -> u64 {
	SlabLru::<(GroupId, EncodedKey), RowNumber>::entry_struct_bytes() as u64 + key.heap_bytes() as u64
}

pub fn mapping_key(group: GroupId, key: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(group, Keyspace::ROW_NUMBER_MAPPING, key)
}

fn mapping_range(group: GroupId) -> EncodedKeyRange {
	keyspace_inner_range(group, Keyspace::ROW_NUMBER_MAPPING)
}

pub fn counter_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::NODE_COUNTER, ROW_NUMBER_COUNTER_SUFFIX)
}

fn decode_bytes<T: OperatorState>(bytes: &EncodedBytes) -> Result<T> {
	Ok(decode(&EncodedOperatorRow::try_from(bytes.clone())?)?)
}

#[derive(Clone, Copy)]
struct GroupMeta {
	hydrated: bool,
	complete: bool,
}

pub struct NodeState {
	pub cache: SlabLru<(GroupId, EncodedKey), RowNumber>,
	pub cache_size: ByteSize,
	groups: HashMap<GroupId, GroupMeta>,
	next: Option<u64>,
	revocations: u64,
	absences_served: u64,
}

impl Default for NodeState {
	fn default() -> Self {
		Self {
			cache: SlabLru::unbounded(),
			cache_size: ByteSize::ZERO,
			groups: HashMap::new(),
			next: None,
			revocations: 0,
			absences_served: 0,
		}
	}
}

impl NodeState {
	pub fn remember(&mut self, group: GroupId, key: &EncodedKey, row_number: RowNumber) {
		if self.cache.put((group, key.clone()), row_number).is_none() {
			self.cache_size = self.cache_size.saturating_add(ByteSize::from_bytes(entry_bytes(key)));
		}
	}

	fn forget(&mut self, group: GroupId, key: &EncodedKey) -> bool {
		if self.cache.remove(&(group, key.clone())).is_some() {
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(key)));
			true
		} else {
			false
		}
	}

	fn revoke_complete(&mut self, group: GroupId) {
		if let Some(meta) = self.groups.get_mut(&group)
			&& meta.complete
		{
			meta.complete = false;
			self.revocations += 1;
		}
	}

	pub fn evict_to_budget(&mut self, budget: ByteSize) {
		while self.cache_size > budget {
			let Some(((group, key), _)) = self.cache.pop_tail() else {
				break;
			};
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(&key)));
			self.revoke_complete(group);
		}
	}

	fn is_complete(&self, group: GroupId) -> bool {
		self.groups.get(&group).is_some_and(|meta| meta.complete)
	}

	fn completeness(&self) -> StateCompleteness {
		if self.groups.is_empty() {
			return StateCompleteness::MERGE_IDENTITY;
		}
		let values_complete = self.groups.values().all(|meta| meta.complete);
		let membership_complete = self.groups.values().all(|meta| meta.hydrated);
		StateCompleteness {
			values_complete,
			membership_complete,
			absences_served: Count::new(self.absences_served),
			false_positives: Count::new(0),
			revocations: Count::new(self.revocations),
		}
	}

	pub fn memory(&self) -> StateMemory {
		let key_heap: u64 = self.cache.keys().map(|(_, key)| key.heap_bytes() as u64).sum();
		let bytes = ByteSize::from_bytes(self.cache.struct_bytes() as u64 + key_heap);
		StateMemory::new(Count::new(self.cache.len() as u64), bytes)
	}
}

pub struct RowNumberSample {
	pub cache: StateMemory,
	pub completeness: StateCompleteness,
}

#[derive(Clone)]
pub struct RowNumberProvider {
	inner: Arc<RowNumberProviderInner>,
}

struct RowNumberProviderInner {
	operators: DashMap<OperatorId, NodeState>,
	budget: ByteSize,
}

impl Default for RowNumberProvider {
	fn default() -> Self {
		Self::new(ByteSize::from_bytes(DEFAULT_BYTE_BUDGET))
	}
}

impl RowNumberProvider {
	pub fn new(budget: ByteSize) -> Self {
		Self {
			inner: Arc::new(RowNumberProviderInner {
				operators: DashMap::new(),
				budget,
			}),
		}
	}

	pub fn mark_fresh(&self, operator: OperatorId, group: GroupId) {
		if group.is_root() {
			return;
		}
		let mut state = self.inner.operators.entry(operator).or_default();
		state.groups.insert(
			group,
			GroupMeta {
				hydrated: true,
				complete: true,
			},
		);
	}

	pub fn get_or_create_row_number(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(operator, group, txn, from_ref(key))?.into_iter().next().unwrap())
	}

	pub fn get_or_create_row_numbers(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let now = txn.written_at();
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_group(&mut guard, operator, txn, group, budget)?;
		let state = &mut *guard;
		let complete = state.is_complete(group);

		let mut results: Vec<Option<(RowNumber, bool)>> = (0..keys.len()).map(|_| None).collect();
		let mut to_resolve: Vec<usize> = Vec::new();
		for (i, key) in keys.iter().enumerate() {
			match state.cache.get(&(group, key.clone())) {
				Some(row_number) => results[i] = Some((row_number, false)),
				None => to_resolve.push(i),
			}
		}
		if to_resolve.is_empty() {
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let map_keys: Vec<GroupStateKey> = to_resolve.iter().map(|i| mapping_key(group, &keys[*i])).collect();

		let found: HashMap<EncodedKey, EncodedBytes> = if complete {
			state.absences_served += to_resolve.len() as u64;
			HashMap::new()
		} else {
			let batch = txn.state_get_many(operator, &map_keys)?;
			let mut found = HashMap::with_capacity(batch.items.len());
			for item in batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_get_many must return OperatorState keys");
				found.insert(decoded.inner(), item.bytes);
			}
			found
		};

		let mut new_slots: Vec<bool> = vec![false; map_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<GroupStateKey, usize> = HashMap::new();
		for (slot, map_key) in map_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(map_key.as_slice()) {
				Some(existing_row) => {
					let row_number = RowNumber(decode_bytes::<u64>(existing_row)?);
					state.remember(group, &keys[i], row_number);
					results[i] = Some((row_number, false));
				}
				None => {
					new_slots[slot] = true;
					if !first_new_slot.contains_key(map_key) {
						first_new_slot.insert(map_key.clone(), slot);
						distinct_new.push(slot);
					}
				}
			}
		}

		if !distinct_new.is_empty() {
			let start = Self::mint(state, operator, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<GroupStateKey, RowNumber> =
				HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let map_key = &map_keys[slot];
				let row_number = RowNumber(start + offset as u64);
				txn.state_set(operator, map_key, row_number.0.encode_state(now)?)?;
				state.remember(group, &keys[i], row_number);
				assigned.insert(map_key.clone(), row_number);
			}
			for (slot, map_key) in map_keys.iter().enumerate() {
				if new_slots[slot] {
					let i = to_resolve[slot];
					let row_number = assigned[map_key];
					let is_new = first_new_slot.get(map_key) == Some(&slot);
					results[i] = Some((row_number, is_new));
				}
			}
		}

		state.evict_to_budget(budget);

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	pub fn get_row_numbers(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_group(&mut guard, operator, txn, group, budget)?;
		let state = &mut *guard;
		let complete = state.is_complete(group);

		let mut results: Vec<Option<RowNumber>> = vec![None; keys.len()];
		let mut to_resolve: Vec<usize> = Vec::new();
		for (i, key) in keys.iter().enumerate() {
			match state.cache.get(&(group, key.clone())) {
				Some(row_number) => results[i] = Some(row_number),
				None => to_resolve.push(i),
			}
		}
		if to_resolve.is_empty() {
			return Ok(results);
		}
		if complete {
			state.absences_served += to_resolve.len() as u64;
			return Ok(results);
		}

		let map_keys: Vec<GroupStateKey> = to_resolve.iter().map(|i| mapping_key(group, &keys[*i])).collect();
		let batch = txn.state_get_many(operator, &map_keys)?;
		let mut found: HashMap<EncodedKey, EncodedBytes> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_get_many must return OperatorState keys");
			found.insert(decoded.inner(), item.bytes);
		}
		for (slot, map_key) in map_keys.iter().enumerate() {
			let i = to_resolve[slot];
			if let Some(existing_row) = found.get(map_key.as_slice()) {
				let row_number = RowNumber(decode_bytes::<u64>(existing_row)?);
				state.remember(group, &keys[i], row_number);
				results[i] = Some(row_number);
			}
		}

		state.evict_to_budget(budget);
		Ok(results)
	}

	pub fn get_row_number(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_group(&mut guard, operator, txn, group, budget)?;
		let state = &mut *guard;
		if let Some(row_number) = state.cache.get(&(group, key.clone())) {
			return Ok(Some(row_number));
		}
		if state.is_complete(group) {
			state.absences_served += 1;
			return Ok(None);
		}
		match txn.state_get(operator, &mapping_key(group, key))? {
			Some(existing_row) => {
				let row_number = RowNumber(decode::<u64>(&existing_row)?);
				state.remember(group, key, row_number);
				state.evict_to_budget(budget);
				Ok(Some(row_number))
			}
			None => Ok(None),
		}
	}

	pub fn remove_row_number(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		key: &EncodedKey,
	) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.operators.entry(operator).or_default();
		Self::hydrate_group(&mut guard, operator, txn, group, budget)?;
		let state = &mut *guard;
		let cached = state.forget(group, key);
		let map_key = mapping_key(group, key);
		if !cached {
			if state.is_complete(group) {
				return Ok(false);
			}
			if txn.state_get(operator, &map_key)?.is_none() {
				return Ok(false);
			}
		}
		txn.state_remove(operator, &map_key)?;
		Ok(true)
	}

	pub fn drop_below(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let base = mapping_range(group);
		let boundary = mapping_key(group, upper);
		let range = EncodedKeyRange::new(Bound::Excluded(boundary.into_encoded()), base.end.clone());
		let batch = txn.state_range(operator, range, None, "rownum::drop_below")?;

		let mut guard = self.inner.operators.entry(operator).or_default();
		let state = &mut *guard;
		let mut dropped = Vec::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorStateKey::inner_encoded(
				decoded.group,
				decoded.keyspace,
				decoded.suffix.clone(),
			);
			let original = EncodedKey::new(decoded.suffix);
			let row_number = RowNumber(decode_bytes::<u64>(&item.bytes)?);
			txn.state_remove(operator, &inner)?;
			state.forget(group, &original);
			dropped.push(row_number);
		}
		Ok(dropped)
	}

	pub fn remove_by_prefix(
		&self,
		operator: OperatorId,
		group: GroupId,
		txn: &mut impl FlowTransaction,
		key_prefix: &[u8],
	) -> Result<()> {
		let inner_prefix = OperatorStateKey::inner_encoded(group, Keyspace::ROW_NUMBER_MAPPING, key_prefix);
		let range = EncodedKeyRange::prefix(inner_prefix.as_ref());
		let batch = txn.state_range(operator, range, None, "rownum::remove_by_prefix")?;

		let mut guard = self.inner.operators.entry(operator).or_default();
		let state = &mut *guard;
		for item in batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorStateKey::inner_encoded(
				decoded.group,
				decoded.keyspace,
				decoded.suffix.clone(),
			);
			let original = EncodedKey::new(decoded.suffix);
			txn.state_remove(operator, &inner)?;
			state.forget(group, &original);
		}
		Ok(())
	}

	pub fn invalidate_groups(&self, operator: OperatorId, groups: &GroupSet) {
		if groups.is_empty() {
			return;
		}
		let Some(mut guard) = self.inner.operators.get_mut(&operator) else {
			return;
		};
		let state = &mut *guard;
		let victims: Vec<(GroupId, EncodedKey)> =
			state.cache.keys().filter(|(g, _)| groups.contains(*g)).cloned().collect();
		for victim in victims {
			if state.cache.remove(&victim).is_some() {
				state.cache_size =
					state.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(&victim.1)));
			}
		}
		for group in groups.as_slice() {
			state.groups.remove(group);
		}
	}

	pub fn memory(&self, operator: OperatorId) -> StateMemory {
		self.inner.operators.get(&operator).map_or(StateMemory::ZERO, |state| state.memory())
	}

	pub fn samples(&self) -> Vec<(OperatorId, RowNumberSample)> {
		let mut out: Vec<(OperatorId, RowNumberSample)> = self
			.inner
			.operators
			.iter()
			.map(|entry| {
				let state = entry.value();
				(
					*entry.key(),
					RowNumberSample {
						cache: state.memory(),
						completeness: state.completeness(),
					},
				)
			})
			.collect();
		out.sort_by_key(|(operator, _)| *operator);
		out
	}

	pub fn evict(&self, operator: OperatorId) {
		self.inner.operators.remove(&operator);
	}

	fn hydrate_group(
		state: &mut NodeState,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		group: GroupId,
		budget: ByteSize,
	) -> Result<()> {
		if state.groups.get(&group).is_some_and(|meta| meta.hydrated) {
			return Ok(());
		}
		state.groups.insert(
			group,
			GroupMeta {
				hydrated: false,
				complete: true,
			},
		);
		let base = mapping_range(group);
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(HYDRATE_CHUNK), "rownum::hydrate")?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				reifydb_assertions! {
					let (found_group, keyspace) = (decoded.group, decoded.keyspace);
					assert!(
						found_group == group && keyspace == Keyspace::ROW_NUMBER_MAPPING,
						"the mapping-range scan must only yield this group's mapping keys; any \
						 other group or keyspace here means the range bounds are wrong and \
						 hydration would poison the cache with unrelated payloads \
						 (wanted group={group:?}, found group={found_group:?}, \
						 keyspace={keyspace:?})"
					);
				}
				let original = EncodedKey::new(decoded.suffix.clone());
				state.remember(group, &original, RowNumber(decode_bytes::<u64>(&item.bytes)?));
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
		state.groups.get_mut(&group).expect("the group meta was just inserted").hydrated = true;
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
				Some(row) => decode::<u64>(&row)?,
				None => 1,
			},
		};
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.written_at();
		txn.state_set(operator, &counter_key(), high_water.encode_state(now)?)?;
		Ok(seed)
	}
}

pub trait RowNumberTxn: FlowTransaction {
	fn get_or_create_row_number(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		let provider = self.row_numbers();
		provider.get_or_create_row_number(operator, group, self, key)
	}

	fn get_or_create_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let provider = self.row_numbers();
		provider.get_or_create_row_numbers(operator, group, self, keys)
	}

	fn get_row_number(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		let provider = self.row_numbers();
		provider.get_row_number(operator, group, self, key)
	}

	fn get_row_numbers(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>> {
		let provider = self.row_numbers();
		provider.get_row_numbers(operator, group, self, keys)
	}

	fn remove_row_number(&mut self, operator: OperatorId, group: GroupId, key: &EncodedKey) -> Result<bool> {
		let provider = self.row_numbers();
		provider.remove_row_number(operator, group, self, key)
	}

	fn remove_row_numbers_below(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let provider = self.row_numbers();
		provider.drop_below(operator, group, self, upper)
	}

	fn remove_row_numbers_by_prefix(
		&mut self,
		operator: OperatorId,
		group: GroupId,
		key_prefix: &[u8],
	) -> Result<()> {
		let provider = self.row_numbers();
		provider.remove_by_prefix(operator, group, self, key_prefix)
	}

	fn invalidate_row_number_groups(&mut self, operator: OperatorId, groups: &GroupSet) {
		let provider = self.row_numbers();
		provider.invalidate_groups(operator, groups)
	}
}

impl<T: FlowTransaction> RowNumberTxn for T {}
