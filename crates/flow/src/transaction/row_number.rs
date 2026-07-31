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
	interface::catalog::flow::FlowNodeId,
	key::{
		EncodableKey,
		flow_node_state::FlowNodeStateKey,
		operator_state::{GroupId, GroupSet, Keyspace, OperatorStateKey, StateKey, keyspace_inner_range},
	},
	metrics::heap::{StateCompleteness, StateMemory},
	state::horizon::Cutoff,
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	reifydb_assertions,
	value::{datetime::DateTime, row_number::RowNumber},
};

use super::FlowTransaction;

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const HYDRATE_CHUNK: usize = 8_192;
const ROW_NUMBER_COUNTER_SUFFIX: &[u8] = b"rn";

fn entry_bytes(key: &EncodedKey) -> u64 {
	SlabLru::<(GroupId, EncodedKey), RowNumber>::entry_struct_bytes() as u64 + key.heap_bytes() as u64
}

fn mapping_key(group: GroupId, key: &EncodedKey) -> StateKey {
	OperatorStateKey::inner_encoded(group, Keyspace::ROW_NUMBER_MAPPING, key)
}

fn mapping_range(group: GroupId) -> EncodedKeyRange {
	keyspace_inner_range(group, Keyspace::ROW_NUMBER_MAPPING)
}

fn counter_key() -> StateKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, ROW_NUMBER_COUNTER_SUFFIX)
}

fn encode_payload<T: OperatorState>(value: &T, now: DateTime) -> Result<EncodedRow> {
	Ok(value.encode_state(now)?.into_row())
}

fn decode_payload<T: OperatorState>(row: &EncodedRow) -> Result<T> {
	Ok(decode_state(&StateBytes::from_row(row.clone())?)?)
}

#[derive(Clone, Copy)]
struct GroupMeta {
	hydrated: bool,
	complete: bool,
}

struct NodeState {
	cache: SlabLru<(GroupId, EncodedKey), RowNumber>,
	cache_size: ByteSize,
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
	fn remember(&mut self, group: GroupId, key: &EncodedKey, row_number: RowNumber) {
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

	fn evict_to_budget(&mut self, budget: ByteSize) {
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

	fn memory(&self) -> StateMemory {
		let key_heap: u64 = self.cache.keys().map(|(_, key)| key.heap_bytes() as u64).sum();
		let bytes = ByteSize::from_bytes(self.cache.struct_bytes() as u64 + key_heap);
		StateMemory::new(Count::new(self.cache.len() as u64), bytes)
	}
}

pub struct RowNumberSample {
	pub cache: StateMemory,
	pub membership: StateMemory,
	pub completeness: StateCompleteness,
}

#[derive(Clone)]
pub struct RowNumberProvider {
	inner: Arc<RowNumberProviderInner>,
}

struct RowNumberProviderInner {
	nodes: DashMap<FlowNodeId, NodeState>,
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
				nodes: DashMap::new(),
				budget,
			}),
		}
	}

	pub fn mark_fresh(&self, node: FlowNodeId, group: GroupId) {
		if group.is_node_scope() {
			return;
		}
		let mut state = self.inner.nodes.entry(node).or_default();
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
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(node, group, txn, from_ref(key))?.into_iter().next().unwrap())
	}

	pub fn get_or_create_row_numbers(
		&self,
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let now = Self::mapping_time(txn);
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_group(&mut guard, node, txn, group, budget)?;
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

		let map_keys: Vec<StateKey> = to_resolve.iter().map(|i| mapping_key(group, &keys[*i])).collect();

		let found: HashMap<EncodedKey, EncodedRow> = if complete {
			state.absences_served += to_resolve.len() as u64;
			HashMap::new()
		} else {
			let batch = txn.state_get_many(node, &map_keys)?;
			let mut found = HashMap::with_capacity(batch.items.len());
			for item in batch.items {
				let decoded = FlowNodeStateKey::decode(&item.key)
					.expect("state_get_many must return FlowNodeState keys");
				found.insert(EncodedKey::new(decoded.key), item.row);
			}
			found
		};

		let mut new_slots: Vec<bool> = vec![false; map_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<StateKey, usize> = HashMap::new();
		for (slot, map_key) in map_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(map_key.as_slice()) {
				Some(existing_row) => {
					let row_number = RowNumber(decode_payload::<u64>(existing_row)?);
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
			let start = Self::mint(state, node, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<StateKey, RowNumber> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let map_key = &map_keys[slot];
				let row_number = RowNumber(start + offset as u64);
				txn.state_set(node, map_key, encode_payload(&row_number.0, now)?)?;
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
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_group(&mut guard, node, txn, group, budget)?;
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

		let map_keys: Vec<StateKey> = to_resolve.iter().map(|i| mapping_key(group, &keys[*i])).collect();
		let batch = txn.state_get_many(node, &map_keys)?;
		let mut found: HashMap<EncodedKey, EncodedRow> = HashMap::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = FlowNodeStateKey::decode(&item.key)
				.expect("state_get_many must return FlowNodeState keys");
			found.insert(EncodedKey::new(decoded.key), item.row);
		}
		for (slot, map_key) in map_keys.iter().enumerate() {
			let i = to_resolve[slot];
			if let Some(existing_row) = found.get(map_key.as_slice()) {
				let row_number = RowNumber(decode_payload::<u64>(existing_row)?);
				state.remember(group, &keys[i], row_number);
				results[i] = Some(row_number);
			}
		}

		state.evict_to_budget(budget);
		Ok(results)
	}

	pub fn get_row_number(
		&self,
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_group(&mut guard, node, txn, group, budget)?;
		let state = &mut *guard;
		if let Some(row_number) = state.cache.get(&(group, key.clone())) {
			return Ok(Some(row_number));
		}
		if state.is_complete(group) {
			state.absences_served += 1;
			return Ok(None);
		}
		match txn.state_get(node, &mapping_key(group, key))? {
			Some(existing_row) => {
				let row_number = RowNumber(decode_payload::<u64>(&existing_row)?);
				state.remember(group, key, row_number);
				state.evict_to_budget(budget);
				Ok(Some(row_number))
			}
			None => Ok(None),
		}
	}

	pub fn remove_row_number(
		&self,
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<bool> {
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_group(&mut guard, node, txn, group, budget)?;
		let state = &mut *guard;
		let cached = state.forget(group, key);
		let map_key = mapping_key(group, key);
		if !cached {
			if state.is_complete(group) {
				return Ok(false);
			}
			if txn.state_get(node, &map_key)?.is_none() {
				return Ok(false);
			}
		}
		txn.state_remove(node, &map_key)?;
		Ok(true)
	}

	pub fn drop_below(
		&self,
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let base = mapping_range(group);
		let boundary = mapping_key(group, upper);
		let range = EncodedKeyRange::new(Bound::Excluded(boundary.into_encoded()), base.end.clone());
		let batch = txn.state_range(node, range, None)?;

		let mut guard = self.inner.nodes.entry(node).or_default();
		let state = &mut *guard;
		let mut dropped = Vec::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = FlowNodeStateKey::decode(&item.key)
				.expect("state_range must return FlowNodeState keys");
			let inner = StateKey::from_framed(EncodedKey::new(decoded.key))
				.expect("the mapping range yields framed inner keys");
			let (_, _, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
				.expect("the mapping range must yield structured operator state keys");
			let original = EncodedKey::new(suffix);
			let row_number = RowNumber(decode_payload::<u64>(&item.row)?);
			txn.state_remove(node, &inner)?;
			state.forget(group, &original);
			dropped.push(row_number);
		}
		Ok(dropped)
	}

	pub fn remove_by_prefix(
		&self,
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		key_prefix: &[u8],
	) -> Result<()> {
		let inner_prefix = OperatorStateKey::inner_encoded(group, Keyspace::ROW_NUMBER_MAPPING, key_prefix);
		let range = EncodedKeyRange::prefix(inner_prefix.as_ref());
		let batch = txn.state_range(node, range, None)?;

		let mut guard = self.inner.nodes.entry(node).or_default();
		let state = &mut *guard;
		for item in batch.items {
			let decoded = FlowNodeStateKey::decode(&item.key)
				.expect("state_range must return FlowNodeState keys");
			let inner = StateKey::from_framed(EncodedKey::new(decoded.key))
				.expect("the mapping range yields framed inner keys");
			let (_, _, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
				.expect("the mapping range must yield structured operator state keys");
			let original = EncodedKey::new(suffix);
			txn.state_remove(node, &inner)?;
			state.forget(group, &original);
		}
		Ok(())
	}

	fn mapping_time(txn: &FlowTransaction) -> DateTime {
		match txn.change_coordinate() {
			Some(coordinate) => coordinate.at,
			None => txn.clock().now(),
		}
	}

	pub fn evict_expired(
		&self,
		node: FlowNodeId,
		group: GroupId,
		txn: &mut FlowTransaction,
		cutoff: Cutoff,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<usize> {
		let base = mapping_range(group);
		let start = match cursor.clone() {
			Some(c) => Bound::Excluded(c),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch = txn.state_range(node, range, Some(batch_size))?;
		let reached_end = !batch.has_more;
		let last_key = batch.items.last().map(|item| {
			EncodedKey::new(
				FlowNodeStateKey::decode(&item.key)
					.expect("state_range must return FlowNodeState keys")
					.key,
			)
		});

		let mut guard = self.inner.nodes.entry(node).or_default();
		let state = &mut *guard;
		let mut removed = 0;
		for item in batch.items {
			if item.row.updated_at() > cutoff.instant() {
				continue;
			}
			let inner = StateKey::from_framed(EncodedKey::new(
				FlowNodeStateKey::decode(&item.key)
					.expect("state_range must return FlowNodeState keys")
					.key,
			))
			.expect("the mapping range yields framed inner keys");
			let (_, _, suffix) = OperatorStateKey::decode_inner(inner.as_slice())
				.expect("the mapping range must yield structured operator state keys");
			let original = EncodedKey::new(suffix);
			txn.state_remove(node, &inner)?;
			state.forget(group, &original);
			removed += 1;
		}

		*cursor = if reached_end {
			None
		} else {
			last_key
		};
		Ok(removed)
	}

	pub fn invalidate_groups(&self, node: FlowNodeId, groups: &GroupSet) {
		if groups.is_empty() {
			return;
		}
		let Some(mut guard) = self.inner.nodes.get_mut(&node) else {
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

	pub fn completeness(&self, node: FlowNodeId) -> StateCompleteness {
		self.inner.nodes.get(&node).map_or(StateCompleteness::MERGE_IDENTITY, |state| state.completeness())
	}

	pub fn memory(&self, node: FlowNodeId) -> StateMemory {
		self.inner.nodes.get(&node).map_or(StateMemory::ZERO, |state| state.memory())
	}

	pub fn membership_memory(&self, _node: FlowNodeId) -> StateMemory {
		StateMemory::ZERO
	}

	pub fn samples(&self) -> Vec<(FlowNodeId, RowNumberSample)> {
		let mut out: Vec<(FlowNodeId, RowNumberSample)> = self
			.inner
			.nodes
			.iter()
			.map(|entry| {
				let state = entry.value();
				(
					*entry.key(),
					RowNumberSample {
						cache: state.memory(),
						membership: StateMemory::ZERO,
						completeness: state.completeness(),
					},
				)
			})
			.collect();
		out.sort_by_key(|(node, _)| *node);
		out
	}

	pub fn evict(&self, node: FlowNodeId) {
		self.inner.nodes.remove(&node);
	}

	fn hydrate_group(
		state: &mut NodeState,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
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
			let batch = txn.state_range(node, range, Some(HYDRATE_CHUNK))?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = FlowNodeStateKey::decode(&item.key)
					.expect("state_range must return FlowNodeState keys");
				let inner = OperatorStateKey::decode_inner(&decoded.key)
					.expect("the mapping range must yield structured operator state keys");
				reifydb_assertions! {
					let (found_group, keyspace) = (inner.0, inner.1);
					assert!(
						found_group == group && keyspace == Keyspace::ROW_NUMBER_MAPPING,
						"the mapping-range scan must only yield this group's mapping keys; any \
						 other group or keyspace here means the range bounds are wrong and \
						 hydration would poison the cache with unrelated payloads \
						 (wanted group={group:?}, found group={found_group:?}, \
						 keyspace={keyspace:?})"
					);
				}
				let original = EncodedKey::new(inner.2);
				state.remember(group, &original, RowNumber(decode_payload::<u64>(&item.row)?));
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
		state.groups.get_mut(&group).expect("the group meta was just inserted").hydrated = true;
		Ok(())
	}

	fn mint(state: &mut NodeState, node: FlowNodeId, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		let seed = match state.next {
			Some(next) => next,
			None => match txn.state_get(node, &counter_key())? {
				Some(row) => decode_payload::<u64>(&row)?,
				None => 1,
			},
		};
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.clock().now();
		txn.state_set(node, &counter_key(), encode_payload(&high_water, now)?)?;
		Ok(seed)
	}
}

impl FlowTransaction {
	pub fn get_or_create_row_number(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		let provider = self.row_numbers();
		provider.get_or_create_row_number(node, group, self, key)
	}

	pub fn get_or_create_row_numbers(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let provider = self.row_numbers();
		provider.get_or_create_row_numbers(node, group, self, keys)
	}

	pub fn get_row_number(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		let provider = self.row_numbers();
		provider.get_row_number(node, group, self, key)
	}

	pub fn get_row_numbers(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		keys: &[EncodedKey],
	) -> Result<Vec<Option<RowNumber>>> {
		let provider = self.row_numbers();
		provider.get_row_numbers(node, group, self, keys)
	}

	pub fn remove_row_number(&mut self, node: FlowNodeId, group: GroupId, key: &EncodedKey) -> Result<bool> {
		let provider = self.row_numbers();
		provider.remove_row_number(node, group, self, key)
	}

	pub fn remove_row_numbers_below(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let provider = self.row_numbers();
		provider.drop_below(node, group, self, upper)
	}

	pub fn remove_row_numbers_by_prefix(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		key_prefix: &[u8],
	) -> Result<()> {
		let provider = self.row_numbers();
		provider.remove_by_prefix(node, group, self, key_prefix)
	}

	pub fn evict_row_numbers(
		&mut self,
		node: FlowNodeId,
		group: GroupId,
		cutoff: Cutoff,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<usize> {
		let provider = self.row_numbers();
		provider.evict_expired(node, group, self, cutoff, cursor, batch_size)
	}

	pub fn invalidate_row_number_groups(&mut self, node: FlowNodeId, groups: &GroupSet) {
		let provider = self.row_numbers();
		provider.invalidate_groups(node, groups)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{actors::pending::PendingWrite, common::CommitVersion};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;
	use crate::transaction::ChangeCoordinate;

	const NODE: FlowNodeId = FlowNodeId(1);
	const GROUP: GroupId = GroupId(7);
	const NEIGHBOUR: GroupId = GroupId(8);

	fn key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	// The shape the block operators reclaim over: (slot, base, quote).
	fn slot_key(slot: u64) -> EncodedKey {
		EncodedKey::builder().u64(slot).u32(1u32).u32(2u32).build()
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

	// Persist a deferred transaction's pending row-number writes to the shared store, so a
	// subsequent transaction (or a cold provider) resolves them the way a committed flow would.
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
	fn reported_memory_counts_retained_containers_not_entry_bookkeeping() {
		// memory() must report what the allocator actually holds. SlabLru stores each key twice
		// (once in the slab node, once in the map) and struct_bytes() already counts both copies
		// at capacity. Inline keys carry their payload inside that 64-byte EncodedKey, so a cache
		// of inline keys retains exactly struct_bytes() and nothing more. Adding the per-entry
		// entry_bytes() charge on top counts the same storage a third time, which is what inflated
		// flow_node::*::row_number_cache_bytes in the memory registry.
		let mut state = NodeState::default();
		for i in 0..64u64 {
			state.remember(GROUP, &slot_key(i), RowNumber(i));
		}

		assert!(
			state.cache.keys().all(|(_, k)| k.heap_bytes() == 0),
			"slot keys must stay inline or this test proves nothing"
		);
		assert_eq!(state.memory().entries.as_u64(), 64);
		assert_eq!(state.memory().bytes.as_bytes(), state.cache.struct_bytes() as u64);
	}

	#[test]
	fn reported_memory_counts_a_shared_out_of_line_key_once() {
		// A key past EncodedKey::INLINE_CAP spills to a refcounted Arc. SlabLru still clones it into both the
		// slab node and the map, but the clones share one allocation, so the out-of-line payload is resident
		// once. Charging it per copy over-reports caches keyed by long keys, which would evict them early.
		let long = EncodedKey::new(vec![7u8; 200]);
		assert!(long.heap_bytes() > 0, "key must spill out of line or this test proves nothing");

		let mut state = NodeState::default();
		state.remember(GROUP, &long, RowNumber(1));

		assert_eq!(
			state.memory().bytes.as_bytes(),
			state.cache.struct_bytes() as u64 + long.heap_bytes() as u64
		);
	}

	#[test]
	fn reported_memory_survives_eviction_of_every_entry() {
		// Eviction frees entries but neither the slab Vec nor the map returns its capacity, so the
		// pages stay resident. Reported memory must follow the retained containers, not the live
		// entry count, or a cache that has churned looks free while still holding its peak.
		let mut state = NodeState::default();
		for i in 0..64u64 {
			state.remember(GROUP, &slot_key(i), RowNumber(i));
		}
		let full = state.memory().bytes.as_bytes();

		state.evict_to_budget(ByteSize::ZERO);

		assert_eq!(state.memory().entries.as_u64(), 0, "budget of zero must drain every entry");
		assert_eq!(
			state.memory().bytes.as_bytes(),
			state.cache.struct_bytes() as u64,
			"a drained cache holds no key payload, so it reports exactly its containers"
		);
		// Not merely equal to `full`: releasing a slot pushes its index onto the free list, so a
		// fully drained cache retains slightly more than a full one. What must never happen is
		// reported memory falling as entries leave.
		assert!(
			state.memory().bytes.as_bytes() >= full,
			"retained capacity must not shrink on eviction: {} < {}",
			state.memory().bytes.as_bytes(),
			full
		);
	}

	#[test]
	fn eviction_charge_covers_what_an_entry_actually_retains() {
		// A budget only means something if the per-entry charge covers what the entry actually
		// retains: the slab slot plus the map bucket, both of which outlive the caller. Charging
		// less lets a nominal 1 MiB cache hold several MiB. The original charge was 96 bytes
		// against ~205 bytes retained, so every node held ~2.5x its budget.
		let mut state = NodeState::default();
		for i in 0..256u64 {
			state.remember(GROUP, &slot_key(i), RowNumber(i));
		}

		let retained = state.cache.len() as u64
			* SlabLru::<(GroupId, EncodedKey), RowNumber>::entry_struct_bytes() as u64;
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
			state.remember(GROUP, &slot_key(i), RowNumber(i));
		}

		state.evict_to_budget(budget);

		let retained = state.cache.len() as u64
			* SlabLru::<(GroupId, EncodedKey), RowNumber>::entry_struct_bytes() as u64;
		assert!(
			retained <= budget.as_bytes(),
			"{} entries survived a {} byte budget and retain {}",
			state.cache.len(),
			budget.as_bytes(),
			retained
		);
	}

	#[test]
	fn first_key_mints_one_and_is_new() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (rn, is_new) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("first")).unwrap();
		assert_eq!(rn.0, 1);
		assert!(is_new, "a never-seen key must report as newly minted");
	}

	#[test]
	fn distinct_keys_mint_sequential_numbers() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		for i in 1..=5u64 {
			let (rn, is_new) = provider
				.get_or_create_row_number(NODE, GROUP, &mut txn, &key(&format!("k{i}")))
				.unwrap();
			assert_eq!(rn.0, i, "distinct keys mint a contiguous ascending sequence");
			assert!(is_new);
		}
	}

	#[test]
	fn a_repeated_key_returns_the_same_number() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (first, new1) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("dup")).unwrap();
		let (second, new2) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("dup")).unwrap();
		assert_eq!(first, second, "the same key must always resolve to the same row number");
		assert!(new1);
		assert!(!new2, "a re-seen key must not report as new");
	}

	#[test]
	fn duplicate_keys_in_one_batch_share_a_single_row_number() {
		// The group_sum fixture emits one record per input row (not per distinct group), so a
		// single batch carries the same key twice. Both occurrences must resolve to the SAME
		// freshly-minted number and only the first must report is_new - otherwise the operator
		// emits two output rows for one group. This is the regression the flow suite caught.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let batch = [key("food"), key("transport"), key("food"), key("drinks")];
		let results = provider.get_or_create_row_numbers(NODE, GROUP, &mut txn, &batch).unwrap();

		assert_eq!(results[0].0, results[2].0, "both 'food' slots must share one row number");
		assert!(results[0].1, "the first occurrence of a new key is new");
		assert!(!results[2].1, "the duplicate occurrence must not report as new");
		assert_ne!(results[0].0, results[1].0, "distinct keys keep distinct numbers");
		assert_ne!(results[0].0, results[3].0);
		let mut distinct: Vec<u64> = results.iter().map(|(rn, _)| rn.0).collect();
		distinct.sort_unstable();
		distinct.dedup();
		assert_eq!(distinct.len(), 3, "four slots over three distinct keys mint three numbers");
	}

	#[test]
	fn a_batch_mixes_existing_and_new_keys() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (a, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("a")).unwrap();
		let (b, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("b")).unwrap();

		let batch = [key("b"), key("c"), key("a")];
		let results = provider.get_or_create_row_numbers(NODE, GROUP, &mut txn, &batch).unwrap();
		assert_eq!(results[0], (b, false), "existing key b keeps its number, not new");
		assert!(results[1].1, "c is freshly minted");
		assert_eq!(results[1].0.0, 3, "c takes the next sequential number");
		assert_eq!(results[2], (a, false), "existing key a keeps its number, not new");
	}

	#[test]
	fn a_known_mapping_is_served_from_the_cache_across_transactions() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		let (minted, new1) = provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("k")).unwrap();
		assert!(new1);
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		let (resolved, new2) = provider.get_or_create_row_number(NODE, GROUP, &mut second, &key("k")).unwrap();
		assert_eq!(resolved, minted, "a persisted mapping must resolve to the original number");
		assert!(!new2, "an existing mapping must not be re-minted");
	}

	#[test]
	fn a_cold_provider_resolves_persisted_mappings_from_the_store() {
		// A restart is a fresh provider with an empty cache. It must hydrate the persisted group
		// mappings from the store rather than re-minting - re-minting would hand a downstream
		// consumer a different row number for a row it already tracks.
		let engine = TestEngine::new();
		let minted = {
			let seed = RowNumberProvider::default();
			let mut txn = deferred(&engine);
			let (rn, _) = seed.get_or_create_row_number(NODE, GROUP, &mut txn, &key("survivor")).unwrap();
			commit_pending(&engine, &mut txn);
			rn
		};

		let restarted = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (resolved, is_new) =
			restarted.get_or_create_row_number(NODE, GROUP, &mut txn, &key("survivor")).unwrap();
		assert_eq!(resolved, minted, "the cold provider must reuse the persisted number");
		assert!(!is_new, "resolving a persisted mapping is not a mint");
	}

	#[test]
	fn the_counter_high_water_survives_a_restart() {
		// The monotonic counter is seeded from the persisted high-water on a cold provider, so a
		// restart never re-issues a number a prior run already handed out. The counter is node
		// scoped, so ids stay unique across the node's groups.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::default();
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(NODE, GROUP, &mut txn, &key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (rn, is_new) = restarted.get_or_create_row_number(NODE, GROUP, &mut txn, &key("k4")).unwrap();
		assert!(is_new);
		assert_eq!(rn.0, 4, "a fresh key after a restart continues the sequence, never reusing 1..=3");
	}

	#[test]
	fn the_counter_is_shared_across_a_nodes_groups() {
		// Row numbers must be unique per node, not per group: a downstream consumer tracks a row by
		// its number across every group of the node. Two groups minting from independent sequences
		// would hand the same number to two different rows.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);

		let (a, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("shared")).unwrap();
		let (b, _) = provider.get_or_create_row_number(NODE, NEIGHBOUR, &mut txn, &key("shared")).unwrap();

		assert_ne!(a, b, "the same key in two groups must not collide on one row number");
		assert_eq!(a.0, 1);
		assert_eq!(b.0, 2, "the second group's mint continues the node's sequence");

		let (a_again, is_new) =
			provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("shared")).unwrap();
		assert_eq!(a_again, a, "each group's mapping is stable and independent");
		assert!(!is_new);
	}

	#[test]
	fn get_row_number_returns_none_for_unknown_and_never_mints() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		assert_eq!(provider.get_row_number(NODE, GROUP, &mut txn, &key("ghost")).unwrap(), None);
		let (rn, is_new) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("real")).unwrap();
		assert_eq!(rn.0, 1, "a failed lookup must not advance the counter");
		assert!(is_new);
	}

	#[test]
	fn get_row_number_returns_an_existing_mapping_without_minting() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (minted, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("here")).unwrap();
		assert_eq!(provider.get_row_number(NODE, GROUP, &mut txn, &key("here")).unwrap(), Some(minted));
	}

	#[test]
	fn dropping_a_mapping_removes_it_and_a_re_lookup_mints_a_fresh_number() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		let (minted, _) = provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("victim")).unwrap();
		assert!(
			provider.remove_row_number(NODE, GROUP, &mut first, &key("victim")).unwrap(),
			"dropping a present key returns true"
		);
		assert_eq!(
			provider.get_row_number(NODE, GROUP, &mut first, &key("victim")).unwrap(),
			None,
			"the dropped mapping is gone from the cache"
		);
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		let (reminted, is_new) =
			provider.get_or_create_row_number(NODE, GROUP, &mut second, &key("victim")).unwrap();
		assert!(is_new, "a dropped key mints fresh on re-lookup");
		assert_ne!(reminted, minted, "a dropped row number is never reused");
	}

	#[test]
	fn dropping_an_absent_mapping_is_idempotent() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		assert!(
			!provider.remove_row_number(NODE, GROUP, &mut txn, &key("nope")).unwrap(),
			"dropping an absent key returns false, not an error"
		);
	}

	#[test]
	fn nodes_are_isolated() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (a, _) = provider.get_or_create_row_number(FlowNodeId(1), GROUP, &mut txn, &key("shared")).unwrap();
		let (b, _) = provider.get_or_create_row_number(FlowNodeId(2), GROUP, &mut txn, &key("shared")).unwrap();
		assert_eq!(a.0, 1, "each node mints from its own sequence");
		assert_eq!(b.0, 1, "the same key under a different node is an independent mapping");
	}

	#[test]
	fn an_idle_node_merges_as_the_completeness_identity() {
		// A node that has never resolved anything proves nothing and must merge as the identity,
		// so a healthy operator's completeness is not dragged down by an untouched provider.
		let provider = RowNumberProvider::default();
		assert_eq!(provider.completeness(NODE), StateCompleteness::MERGE_IDENTITY);
	}

	#[test]
	fn a_complete_group_proves_absence_without_a_store_read() {
		// The membership filter is gone: a group that has been fully hydrated (or freshly interned)
		// is complete, and a complete group answers "never minted" from the cache alone. This is the
		// property that keeps the firehose new-key path off the store, now at group granularity.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("known")).unwrap();
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		// Warm the group so the assertion measures absence proofs, not the hydration scan.
		provider.get_row_number(NODE, GROUP, &mut second, &key("known")).unwrap();
		let reads_before = second.store_reads();
		assert_eq!(provider.get_row_number(NODE, GROUP, &mut second, &key("unknown")).unwrap(), None);
		assert_eq!(
			second.store_reads() - reads_before,
			0,
			"a never-minted key in a complete group must be proven absent from memory alone"
		);
	}

	#[test]
	fn a_freshly_interned_group_mints_new_keys_without_a_store_read() {
		// mark_fresh is what txn.intern_group calls when the interner reports a brand-new group. A
		// fresh group's mapping keyspace is provably empty, so its keys mint with zero store reads -
		// preserving the firehose no-read path for new assets without any membership filter.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		provider.mark_fresh(NODE, GROUP);
		// Seed the node counter once so the assertion measures per-key reads, not the one-time
		// counter-seed read the first mint on a cold provider always pays.
		provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("warmup")).unwrap();

		let reads_before = txn.store_reads();
		let fresh = [key("new_a"), key("new_b"), key("new_c")];
		let results = provider.get_or_create_row_numbers(NODE, GROUP, &mut txn, &fresh).unwrap();
		assert!(results.iter().all(|(_, is_new)| *is_new), "all three keys are brand new");
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"a freshly interned group must mint further keys without consulting the store"
		);
	}

	#[test]
	fn an_over_capacity_group_falls_back_to_the_store_for_absence() {
		// The deliberate consequence of dropping the membership filter: a group holding more mapping
		// keys than the byte budget cannot stay complete, so hydration evicts and the group can no
		// longer prove absence from memory - it pays a store read. This never bites distinct or the
		// windowed operators, which hold one mapping key per group, but the substrate must document
		// it rather than silently over-claim a RAM absence.
		let engine = TestEngine::new();
		let budget = ByteSize::from_bytes(entry_bytes(&key("k1")) * 2);
		{
			let seed = RowNumberProvider::new(budget);
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(NODE, GROUP, &mut txn, &key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::new(budget);
		let mut txn = deferred(&engine);
		restarted.get_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap();

		assert!(
			!restarted.completeness(NODE).values_complete,
			"three mappings cannot be values-complete at capacity two"
		);
		let reads_before = txn.store_reads();
		assert_eq!(restarted.get_row_number(NODE, GROUP, &mut txn, &key("never_minted")).unwrap(), None);
		assert!(
			txn.store_reads() - reads_before > 0,
			"an over-capacity, incomplete group must consult the store to prove absence"
		);
	}

	#[test]
	fn a_confirmed_removal_keeps_absence_in_memory() {
		// remove_row_number retires the key from the cache while the group stays complete, so every
		// later probe of the removed key is answered from memory rather than paying a store read.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut txn = deferred(&engine);
		provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap();
		provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("k2")).unwrap();
		assert!(provider.remove_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap());

		let reads_before = txn.store_reads();
		assert_eq!(provider.get_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap(), None);
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"the removed key's absence must be answered from the complete group, not the store"
		);
	}

	#[test]
	fn eviction_drops_only_expired_mappings_and_keeps_the_rest_in_memory() {
		// The reclaim sweep runs evict_expired against every node that declares a mapping span.
		// Evicting by clearing the whole cache silently downgrades the provider to one store
		// roundtrip per key for the rest of its life - the surviving mapping and its completeness
		// must both outlive the sweep. The cutoff is now an instant compared against the mapping's
		// own stamp rather than a commit version, so the two mappings are separated in event time
		// here instead of by separate commits; the assertions are unchanged.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		first.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(0),
			version: CommitVersion(0),
		});
		let (minted_old, _) = provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("old")).unwrap();
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		second.set_change_coordinate(ChangeCoordinate {
			at: DateTime::from_millis(100),
			version: CommitVersion(0),
		});
		let (minted_young, _) =
			provider.get_or_create_row_number(NODE, GROUP, &mut second, &key("young")).unwrap();
		commit_pending(&engine, &mut second);

		let mut third = deferred(&engine);
		provider.evict_expired(NODE, GROUP, &mut third, Cutoff(DateTime::from_millis(50)), &mut None, 100)
			.unwrap();

		let reads_before = third.store_reads();
		let (resolved, is_new) =
			provider.get_or_create_row_number(NODE, GROUP, &mut third, &key("young")).unwrap();
		assert!(!is_new, "the surviving mapping must not be re-minted");
		assert_eq!(resolved, minted_young, "the surviving mapping keeps its row number");
		assert_eq!(
			third.store_reads() - reads_before,
			0,
			"an eviction must not cost the survivor its in-memory resolution"
		);

		let (reminted, is_new) =
			provider.get_or_create_row_number(NODE, GROUP, &mut third, &key("old")).unwrap();
		assert!(is_new, "the expired mapping is gone, so it re-mints");
		assert_ne!(reminted, minted_old, "row numbers are never reused");
	}

	#[test]
	fn drop_below_reclaims_only_mappings_under_the_bound() {
		// The block operators reclaim finished slots with drop_below. Keys lead with a slot, so
		// dropping below a bound must reclaim exactly the lower slots and leave the rest mapped.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);

		let (rn10, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(10)).unwrap();
		let (rn20, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(20)).unwrap();
		let (rn30, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(30)).unwrap();

		let upper = EncodedKey::builder().u64(25u64).u32(0u32).u32(0u32).build();
		let mut dropped = provider.drop_below(NODE, GROUP, &mut txn, &upper).unwrap();
		dropped.sort_by_key(|rn| rn.0);
		assert_eq!(dropped, vec![rn10, rn20], "exactly the below-bound mappings are reclaimed");

		let (rn30_again, is_new30) =
			provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(30)).unwrap();
		assert!(!is_new30, "slot 30 sat above the bound and must remain mapped");
		assert_eq!(rn30, rn30_again);

		let (rn10_again, is_new10) =
			provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(10)).unwrap();
		assert!(is_new10, "reclaimed slot 10 mints fresh");
		assert_ne!(rn10, rn10_again, "a reclaimed row number is never reused");
	}

	#[test]
	fn invalidating_a_group_drops_its_cache_without_serving_a_ghost() {
		// After phase-2 identity reclamation deletes a group's mapping rows, the cache
		// still names them. Serving that stale row number is a ghost - a row number for a mapping
		// that no longer exists. invalidate_groups must clear the reclaimed group's cache while
		// leaving every other group's mappings intact.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);

		let (doomed, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("x")).unwrap();
		let (kept, _) = provider.get_or_create_row_number(NODE, NEIGHBOUR, &mut txn, &key("x")).unwrap();

		provider.invalidate_groups(NODE, &GroupSet::new([GROUP]));

		// Emulate phase 2 erasing the reclaimed group's mapping row from the store.
		txn.state_remove(NODE, &mapping_key(GROUP, &key("x"))).unwrap();

		assert_eq!(
			provider.get_row_number(NODE, GROUP, &mut txn, &key("x")).unwrap(),
			None,
			"the reclaimed group must not serve a ghost row number from a dropped cache entry"
		);
		assert_eq!(
			provider.get_row_number(NODE, NEIGHBOUR, &mut txn, &key("x")).unwrap(),
			Some(kept),
			"an unrelated group's mapping must survive the invalidation"
		);
		assert_ne!(doomed, kept);
	}

	#[test]
	fn the_row_number_counter_never_collides_with_the_interners_group_counter() {
		// Both node counters live in the node-scope NODE_COUNTER keyspace. If they shared a cell, a
		// group mint would advance the row-number sequence and vice versa, breaking the contiguity
		// row-number consumers depend on. A distinct suffix keeps them in separate cells.
		let group_counter =
			OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::NODE_COUNTER, vec![]);
		assert_ne!(counter_key(), group_counter, "the row-number counter must not alias the group-id counter");
		assert_ne!(
			mapping_key(GROUP, &key("x")),
			counter_key(),
			"a mapping key must never equal the counter key"
		);
	}
}
