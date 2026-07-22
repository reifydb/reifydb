// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem::size_of,
	ops::Bound,
	slice::from_ref,
	sync::Arc,
};

use dashmap::DashMap;
use reifydb_codec::{
	encoded::row::EncodedRow,
	key::{
		deserializer::KeyDeserializer,
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	},
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	metrics::heap::{StateCompleteness, StateMemory},
	state::membership::MembershipIndex,
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result, byte_size::ByteSize, count::Count, reifydb_assertions, util::hash::xxh3_64,
	value::row_number::RowNumber,
};

use super::FlowTransaction;

const DEFAULT_BYTE_BUDGET: u64 = 1024 * 1024;
const ENTRY_OVERHEAD: u64 = (size_of::<usize>() * 2) as u64;
const HYDRATE_CHUNK: usize = 8_192;
const MEMBERSHIP_BYTE_CAP: u64 = 16 * 1024 * 1024;

fn entry_bytes(key: &EncodedKey) -> u64 {
	(size_of::<EncodedKey>() + size_of::<RowNumber>()) as u64 + ENTRY_OVERHEAD + key.as_ref().len() as u64
}

fn membership_hash(key: &EncodedKey) -> u64 {
	xxh3_64(key.as_ref()).0
}

fn mapping_prefix() -> EncodedKey {
	let mut serializer = KeySerializer::new();
	serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
	serializer.finish()
}

fn make_map_key(key: &EncodedKey) -> EncodedKey {
	let mut serializer = KeySerializer::new();
	serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
	serializer.extend_bytes(key.as_ref());
	serializer.finish()
}

fn counter_key() -> EncodedKey {
	let mut serializer = KeySerializer::new();
	serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG);
	serializer.finish()
}

fn encode_payload<T: OperatorState>(value: &T, now_nanos: u64) -> Result<EncodedRow> {
	Ok(value.encode_state(now_nanos)?.into_row())
}

fn decode_payload<T: OperatorState>(row: &EncodedRow) -> Result<T> {
	Ok(decode_state(&StateBytes::from_row(row.clone())?)?)
}

struct NodeState {
	cache: SlabLru<EncodedKey, RowNumber>,
	cache_size: ByteSize,
	membership: Option<MembershipIndex>,
	hydrated: bool,
	complete: bool,
	next: Option<u64>,
	absences_served: u64,
	false_positives: u64,
	revocations: u64,
}

impl Default for NodeState {
	fn default() -> Self {
		Self {
			cache: SlabLru::unbounded(),
			cache_size: ByteSize::ZERO,
			membership: None,
			hydrated: false,
			complete: false,
			next: None,
			absences_served: 0,
			false_positives: 0,
			revocations: 0,
		}
	}
}

impl NodeState {
	fn remember(&mut self, key: &EncodedKey, row_number: RowNumber) {
		if self.cache.put(key.clone(), row_number).is_none() {
			self.cache_size = self.cache_size.saturating_add(ByteSize::from_bytes(entry_bytes(key)));
		}
	}

	fn forget(&mut self, key: &EncodedKey) -> bool {
		if self.cache.remove(key).is_some() {
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(key)));
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
			let Some((key, _)) = self.cache.pop_tail() else {
				break;
			};
			self.cache_size = self.cache_size.saturating_sub(ByteSize::from_bytes(entry_bytes(&key)));
			self.revoke_complete();
		}
	}

	fn membership_insert(&mut self, key: &EncodedKey) {
		if let Some(index) = self.membership.as_mut()
			&& !index.insert(membership_hash(key))
		{
			self.membership = None;
		}
	}

	fn membership_remove(&mut self, key: &EncodedKey) {
		if let Some(index) = self.membership.as_mut() {
			index.remove(membership_hash(key));
		}
	}

	fn membership_contains(&self, key: &EncodedKey) -> Option<bool> {
		self.membership.as_ref().map(|index| index.contains(membership_hash(key)))
	}

	fn count_absence(&mut self) {
		self.absences_served += 1;
	}

	fn count_false_positive(&mut self) {
		if self.membership.is_some() {
			self.false_positives += 1;
		}
	}

	fn completeness(&self) -> StateCompleteness {
		if !self.hydrated {
			return StateCompleteness::MERGE_IDENTITY;
		}
		StateCompleteness {
			values_complete: self.complete,
			membership_complete: self.membership.is_some(),
			absences_served: Count::new(self.absences_served),
			false_positives: Count::new(self.false_positives),
			revocations: Count::new(self.revocations),
		}
	}

	fn memory(&self) -> StateMemory {
		let bytes = self.cache_size.saturating_add(ByteSize::from_bytes(self.cache.struct_bytes() as u64));
		StateMemory::new(Count::new(self.cache.len() as u64), bytes)
	}

	fn membership_memory(&self) -> StateMemory {
		self.membership.as_ref().map_or(StateMemory::ZERO, MembershipIndex::approximate_memory)
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

	pub fn get_or_create_row_number(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(node, txn, from_ref(key))?.into_iter().next().unwrap())
	}

	pub fn get_or_create_row_numbers(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let now = txn.clock().now_nanos();
		let budget = self.inner.budget;
		let mut guard = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut guard, node, txn, budget)?;
		let state = &mut *guard;

		let mut results: Vec<Option<(RowNumber, bool)>> = (0..keys.len()).map(|_| None).collect();
		let mut to_resolve: Vec<usize> = Vec::new();
		for (i, key) in keys.iter().enumerate() {
			match state.cache.get(key) {
				Some(row_number) => results[i] = Some((row_number, false)),
				None => to_resolve.push(i),
			}
		}
		if to_resolve.is_empty() {
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let map_keys: Vec<EncodedKey> = to_resolve.iter().map(|i| make_map_key(&keys[*i])).collect();

		let mut consulted_store: Vec<bool> = Vec::new();
		let found: HashMap<Vec<u8>, EncodedRow> = if state.complete {
			HashMap::new()
		} else {
			let mut lookup: Vec<EncodedKey> = Vec::new();
			for (slot, i) in to_resolve.iter().enumerate() {
				let maybe = state.membership_contains(&keys[*i]).unwrap_or(true);
				consulted_store.push(maybe);
				if maybe {
					lookup.push(map_keys[slot].clone());
				} else {
					state.count_absence();
				}
			}
			if lookup.is_empty() {
				HashMap::new()
			} else {
				let batch = txn.internal_state_get_many(node, &lookup)?;
				let mut found = HashMap::with_capacity(batch.items.len());
				for item in batch.items {
					let decoded = FlowNodeInternalStateKey::decode(&item.key).expect(
						"internal_state_get_many must return FlowNodeInternalState keys",
					);
					found.insert(decoded.key, item.row);
				}
				found
			}
		};

		let mut new_slots: Vec<bool> = vec![false; map_keys.len()];
		let mut distinct_new: Vec<usize> = Vec::new();
		let mut first_new_slot: HashMap<Vec<u8>, usize> = HashMap::new();
		for (slot, map_key) in map_keys.iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(map_key.as_ref()) {
				Some(existing_row) => {
					let row_number = RowNumber(decode_payload::<u64>(existing_row)?);
					state.remember(&keys[i], row_number);
					results[i] = Some((row_number, false));
				}
				None => {
					if consulted_store.get(slot) == Some(&true) {
						state.count_false_positive();
					}
					new_slots[slot] = true;
					if !first_new_slot.contains_key(map_key.as_ref()) {
						first_new_slot.insert(map_key.as_ref().to_vec(), slot);
						distinct_new.push(slot);
					}
				}
			}
		}

		if !distinct_new.is_empty() {
			let start = Self::mint(state, node, txn, distinct_new.len() as u64)?;
			let mut assigned: HashMap<Vec<u8>, RowNumber> = HashMap::with_capacity(distinct_new.len());
			for (offset, &slot) in distinct_new.iter().enumerate() {
				let i = to_resolve[slot];
				let map_key = &map_keys[slot];
				let row_number = RowNumber(start + offset as u64);
				txn.internal_state_set(node, map_key, encode_payload(&row_number.0, now)?)?;
				state.remember(&keys[i], row_number);
				state.membership_insert(&keys[i]);
				assigned.insert(map_key.as_ref().to_vec(), row_number);
			}
			for (slot, map_key) in map_keys.iter().enumerate() {
				if new_slots[slot] {
					let i = to_resolve[slot];
					let row_number = assigned[map_key.as_ref()];
					let is_new = first_new_slot.get(map_key.as_ref()) == Some(&slot);
					results[i] = Some((row_number, is_new));
				}
			}
		}

		state.evict_to_budget(budget);

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	pub fn get_row_number(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<Option<RowNumber>> {
		let budget = self.inner.budget;
		let mut state = self.inner.nodes.entry(node).or_default();
		Self::hydrate_once(&mut state, node, txn, budget)?;
		if let Some(row_number) = state.cache.get(key) {
			return Ok(Some(row_number));
		}
		if state.complete {
			return Ok(None);
		}
		if state.membership_contains(key) == Some(false) {
			state.count_absence();
			return Ok(None);
		}
		let map_key = make_map_key(key);
		match txn.internal_state_get(node, &map_key)? {
			Some(existing_row) => {
				let row_number = RowNumber(decode_payload::<u64>(&existing_row)?);
				state.remember(key, row_number);
				state.evict_to_budget(budget);
				Ok(Some(row_number))
			}
			None => {
				state.count_false_positive();
				Ok(None)
			}
		}
	}

	pub fn drop_row_number(&self, node: FlowNodeId, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<bool> {
		let mut state = self.inner.nodes.entry(node).or_default();
		let cached = state.forget(key);
		let map_key = make_map_key(key);
		if !cached {
			if state.complete {
				return Ok(false);
			}
			if state.membership_contains(key) == Some(false) {
				state.count_absence();
				return Ok(false);
			}
			if txn.internal_state_get(node, &map_key)?.is_none() {
				state.count_false_positive();
				return Ok(false);
			}
		}
		txn.internal_state_drop(node, &map_key)?;
		state.membership_remove(key);
		Ok(true)
	}

	pub fn drop_below(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		upper: &EncodedKey,
	) -> Result<Vec<RowNumber>> {
		let boundary = make_map_key(upper);
		let prefix = mapping_prefix();
		let prefix_range = EncodedKeyRange::prefix(prefix.as_ref());
		let range = EncodedKeyRange::new(Bound::Excluded(boundary), prefix_range.end.clone());
		let batch = txn.internal_state_range(node, range, None)?;

		let mut state = self.inner.nodes.entry(node).or_default();
		let mut dropped = Vec::with_capacity(batch.items.len());
		for item in batch.items {
			let decoded = FlowNodeInternalStateKey::decode(&item.key)
				.expect("internal_state_range must return FlowNodeInternalState keys");
			let inner = EncodedKey::new(decoded.key);
			let row_number = RowNumber(decode_payload::<u64>(&item.row)?);
			let mut de = KeyDeserializer::from_bytes(inner.as_ref());
			de.read_u8()?;
			let original = EncodedKey::new(de.read_bytes()?);
			txn.internal_state_drop(node, &inner)?;
			state.forget(&original);
			state.membership_remove(&original);
			dropped.push(row_number);
		}
		Ok(dropped)
	}

	pub fn remove_by_prefix(&self, node: FlowNodeId, txn: &mut FlowTransaction, key_prefix: &[u8]) -> Result<()> {
		let mut state = self.inner.nodes.entry(node).or_default();
		let cached_matches: Vec<EncodedKey> =
			state.cache.keys().filter(|key| key.as_ref().starts_with(key_prefix)).cloned().collect();
		for key in &cached_matches {
			state.forget(key);
		}

		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		let state_prefix = FlowNodeInternalStateKey::new(node, prefix);
		let full_range = EncodedKeyRange::prefix(&state_prefix.encode());

		let keys_to_remove = {
			let stream = txn.range(full_range, RangeScope::All, 1024);
			let mut keys = Vec::new();
			for result in stream {
				keys.push(result?.key);
			}
			keys
		};

		let mut untracked: HashSet<EncodedKey> = HashSet::new();
		for key in keys_to_remove {
			if let Some(decoded) = FlowNodeInternalStateKey::decode(&key) {
				let mut de = KeyDeserializer::from_bytes(&decoded.key);
				de.read_u8()?;
				let original = EncodedKey::new(de.read_bytes()?);
				if untracked.insert(original.clone()) {
					state.membership_remove(&original);
				}
			}
			txn.remove(&key)?;
		}

		Ok(())
	}

	pub fn evict_expired(
		&self,
		node: FlowNodeId,
		txn: &mut FlowTransaction,
		cutoff_version: CommitVersion,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<()> {
		let prefix = mapping_prefix();
		let base = EncodedKeyRange::prefix(prefix.as_ref());
		let start = match cursor.clone() {
			Some(c) => Bound::Excluded(c),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch = txn.internal_state_range(node, range, Some(batch_size))?;
		let reached_end = !batch.has_more;
		let last_key = batch.items.last().map(|item| {
			EncodedKey::new(
				FlowNodeInternalStateKey::decode(&item.key)
					.expect("internal_state_range must return FlowNodeInternalState keys")
					.key,
			)
		});

		let mut state = self.inner.nodes.entry(node).or_default();
		let mut untracked: HashSet<EncodedKey> = HashSet::new();
		for item in batch.items {
			if item.version > cutoff_version {
				continue;
			}
			let inner = EncodedKey::new(
				FlowNodeInternalStateKey::decode(&item.key)
					.expect("internal_state_range must return FlowNodeInternalState keys")
					.key,
			);
			txn.internal_state_drop(node, &inner)?;
			let mut de = KeyDeserializer::from_bytes(inner.as_ref());
			de.read_u8()?;
			let original = EncodedKey::new(de.read_bytes()?);
			state.forget(&original);
			if untracked.insert(original.clone()) {
				state.membership_remove(&original);
			}
		}

		*cursor = if reached_end {
			None
		} else {
			last_key
		};
		Ok(())
	}

	pub fn completeness(&self, node: FlowNodeId) -> StateCompleteness {
		self.inner.nodes.get(&node).map_or(StateCompleteness::MERGE_IDENTITY, |state| state.completeness())
	}

	pub fn memory(&self, node: FlowNodeId) -> StateMemory {
		self.inner.nodes.get(&node).map_or(StateMemory::ZERO, |state| state.memory())
	}

	pub fn membership_memory(&self, node: FlowNodeId) -> StateMemory {
		self.inner.nodes.get(&node).map_or(StateMemory::ZERO, |state| state.membership_memory())
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
						membership: state.membership_memory(),
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
		let prefix = mapping_prefix();
		let base = EncodedKeyRange::prefix(prefix.as_ref());
		let mut hashes: Vec<u64> = Vec::new();
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.internal_state_range(node, range, Some(HYDRATE_CHUNK))?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = FlowNodeInternalStateKey::decode(&item.key)
					.expect("internal_state_range must return FlowNodeInternalState keys");
				let mut de = KeyDeserializer::from_bytes(&decoded.key);
				reifydb_assertions! {
					let tag = KeyDeserializer::from_bytes(&decoded.key).read_u8()?;
					assert!(
						tag == FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG,
						"the mapping-prefix range scan must only yield 'M' keys; any other tag here \
						 means the prefix bounds are wrong and hydration would poison the cache \
						 (tag={tag})"
					);
				}
				de.read_u8()?;
				let original = EncodedKey::new(de.read_bytes()?);
				hashes.push(membership_hash(&original));
				state.remember(&original, RowNumber(decode_payload::<u64>(&item.row)?));
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
		let mut membership = MembershipIndex::with_capacity(hashes.len(), MEMBERSHIP_BYTE_CAP);
		let tracked = hashes.into_iter().all(|hash| membership.insert(hash));
		state.membership = tracked.then_some(membership);
		Ok(())
	}

	fn mint(state: &mut NodeState, node: FlowNodeId, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		let seed = match state.next {
			Some(next) => next,
			None => match txn.internal_state_get(node, &counter_key())? {
				Some(row) => decode_payload::<u64>(&row)?,
				None => 1,
			},
		};
		let high_water = seed + count;
		state.next = Some(high_water);
		let now = txn.clock().now_nanos();
		txn.internal_state_set(node, &counter_key(), encode_payload(&high_water, now)?)?;
		Ok(seed)
	}
}

impl FlowTransaction {
	pub fn get_or_create_row_number(&mut self, node: FlowNodeId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
		let provider = self.row_numbers();
		provider.get_or_create_row_number(node, self, key)
	}

	pub fn get_or_create_row_numbers(
		&mut self,
		node: FlowNodeId,
		keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		let provider = self.row_numbers();
		provider.get_or_create_row_numbers(node, self, keys)
	}

	pub fn get_row_number(&mut self, node: FlowNodeId, key: &EncodedKey) -> Result<Option<RowNumber>> {
		let provider = self.row_numbers();
		provider.get_row_number(node, self, key)
	}

	pub fn drop_row_number(&mut self, node: FlowNodeId, key: &EncodedKey) -> Result<bool> {
		let provider = self.row_numbers();
		provider.drop_row_number(node, self, key)
	}

	pub fn drop_row_numbers_below(&mut self, node: FlowNodeId, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		let provider = self.row_numbers();
		provider.drop_below(node, self, upper)
	}

	pub fn remove_row_numbers_by_prefix(&mut self, node: FlowNodeId, key_prefix: &[u8]) -> Result<()> {
		let provider = self.row_numbers();
		provider.remove_by_prefix(node, self, key_prefix)
	}

	pub fn evict_row_numbers(
		&mut self,
		node: FlowNodeId,
		cutoff_version: CommitVersion,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<()> {
		let provider = self.row_numbers();
		provider.evict_expired(node, self, cutoff_version, cursor, batch_size)
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

	fn key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
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
				PendingWrite::Remove => cmd.remove(k).unwrap(),
				PendingWrite::Drop => cmd.drop_key(k).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
	}

	#[test]
	fn first_key_mints_one_and_is_new() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (rn, is_new) = provider.get_or_create_row_number(NODE, &mut txn, &key("first")).unwrap();
		assert_eq!(rn.0, 1);
		assert!(is_new, "a never-seen key must report as newly minted");
	}

	#[test]
	fn distinct_keys_mint_sequential_numbers() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		for i in 1..=5u64 {
			let (rn, is_new) =
				provider.get_or_create_row_number(NODE, &mut txn, &key(&format!("k{i}"))).unwrap();
			assert_eq!(rn.0, i, "distinct keys mint a contiguous ascending sequence");
			assert!(is_new);
		}
	}

	#[test]
	fn a_repeated_key_returns_the_same_number() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (first, new1) = provider.get_or_create_row_number(NODE, &mut txn, &key("dup")).unwrap();
		let (second, new2) = provider.get_or_create_row_number(NODE, &mut txn, &key("dup")).unwrap();
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
		let results = provider.get_or_create_row_numbers(NODE, &mut txn, &batch).unwrap();

		assert_eq!(results[0].0, results[2].0, "both 'food' slots must share one row number");
		assert!(results[0].1, "the first occurrence of a new key is new");
		assert!(!results[2].1, "the duplicate occurrence must not report as new");
		assert_ne!(results[0].0, results[1].0, "distinct keys keep distinct numbers");
		assert_ne!(results[0].0, results[3].0);
		// Exactly three distinct numbers were minted for three distinct keys.
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
		let (a, _) = provider.get_or_create_row_number(NODE, &mut txn, &key("a")).unwrap();
		let (b, _) = provider.get_or_create_row_number(NODE, &mut txn, &key("b")).unwrap();

		let batch = [key("b"), key("c"), key("a")];
		let results = provider.get_or_create_row_numbers(NODE, &mut txn, &batch).unwrap();
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
		let (minted, new1) = provider.get_or_create_row_number(NODE, &mut first, &key("k")).unwrap();
		assert!(new1);
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		let (resolved, new2) = provider.get_or_create_row_number(NODE, &mut second, &key("k")).unwrap();
		assert_eq!(resolved, minted, "a persisted mapping must resolve to the original number");
		assert!(!new2, "an existing mapping must not be re-minted");
	}

	#[test]
	fn a_cold_provider_resolves_persisted_mappings_from_the_store() {
		// A restart is a fresh provider with an empty cache. It must hydrate the persisted
		// mappings from the store rather than re-minting - re-minting would hand a downstream
		// consumer a different row number for a row it already tracks.
		let engine = TestEngine::new();
		let minted = {
			let seed = RowNumberProvider::default();
			let mut txn = deferred(&engine);
			let (rn, _) = seed.get_or_create_row_number(NODE, &mut txn, &key("survivor")).unwrap();
			commit_pending(&engine, &mut txn);
			rn
		};

		let restarted = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (resolved, is_new) = restarted.get_or_create_row_number(NODE, &mut txn, &key("survivor")).unwrap();
		assert_eq!(resolved, minted, "the cold provider must reuse the persisted number");
		assert!(!is_new, "resolving a persisted mapping is not a mint");
	}

	#[test]
	fn the_counter_high_water_survives_a_restart() {
		// The monotonic counter is seeded from the persisted high-water on a cold provider, so a
		// restart never re-issues a number a prior run already handed out.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::default();
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(NODE, &mut txn, &key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (rn, is_new) = restarted.get_or_create_row_number(NODE, &mut txn, &key("k4")).unwrap();
		assert!(is_new);
		assert_eq!(rn.0, 4, "a fresh key after a restart continues the sequence, never reusing 1..=3");
	}

	#[test]
	fn get_row_number_returns_none_for_unknown_and_never_mints() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		assert_eq!(provider.get_row_number(NODE, &mut txn, &key("ghost")).unwrap(), None);
		// A pure lookup must not consume a row number: the next mint is still 1.
		let (rn, is_new) = provider.get_or_create_row_number(NODE, &mut txn, &key("real")).unwrap();
		assert_eq!(rn.0, 1, "a failed lookup must not advance the counter");
		assert!(is_new);
	}

	#[test]
	fn get_row_number_returns_an_existing_mapping_without_minting() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (minted, _) = provider.get_or_create_row_number(NODE, &mut txn, &key("here")).unwrap();
		assert_eq!(provider.get_row_number(NODE, &mut txn, &key("here")).unwrap(), Some(minted));
	}

	#[test]
	fn dropping_a_mapping_removes_it_and_a_re_lookup_mints_a_fresh_number() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		let (minted, _) = provider.get_or_create_row_number(NODE, &mut first, &key("victim")).unwrap();
		assert!(
			provider.drop_row_number(NODE, &mut first, &key("victim")).unwrap(),
			"dropping a present key returns true"
		);
		assert_eq!(
			provider.get_row_number(NODE, &mut first, &key("victim")).unwrap(),
			None,
			"the dropped mapping is gone from the cache"
		);
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		let (reminted, is_new) = provider.get_or_create_row_number(NODE, &mut second, &key("victim")).unwrap();
		assert!(is_new, "a dropped key mints fresh on re-lookup");
		assert_ne!(reminted, minted, "a dropped row number is never reused");
	}

	#[test]
	fn dropping_an_absent_mapping_is_idempotent() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		assert!(
			!provider.drop_row_number(NODE, &mut txn, &key("nope")).unwrap(),
			"dropping an absent key returns false, not an error"
		);
	}

	#[test]
	fn nodes_are_isolated() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (a, _) = provider.get_or_create_row_number(FlowNodeId(1), &mut txn, &key("shared")).unwrap();
		let (b, _) = provider.get_or_create_row_number(FlowNodeId(2), &mut txn, &key("shared")).unwrap();
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
	fn a_hydrated_provider_proves_absence_without_a_store_read() {
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		provider.get_or_create_row_number(NODE, &mut first, &key("known")).unwrap();
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		// Warm the provider so the assertion measures absence proofs, not the hydration scan.
		provider.get_row_number(NODE, &mut second, &key("known")).unwrap();
		let reads_before = second.store_reads();
		assert_eq!(provider.get_row_number(NODE, &mut second, &key("unknown")).unwrap(), None);
		assert_eq!(
			second.store_reads() - reads_before,
			0,
			"a never-minted key must be proven absent from memory alone"
		);
	}

	#[test]
	fn an_over_capacity_population_still_proves_absence_from_memory() {
		// The value cache is capacity-bounded, but membership is built from the full hydration
		// scan. A population above capacity must still answer "never minted" without a store read.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::new(ByteSize::from_bytes(entry_bytes(&key("k1")) * 2));
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(NODE, &mut txn, &key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::new(ByteSize::from_bytes(entry_bytes(&key("k1")) * 2));
		let mut txn = deferred(&engine);
		restarted.get_row_number(NODE, &mut txn, &key("k1")).unwrap();

		let reads_before = txn.store_reads();
		assert_eq!(restarted.get_row_number(NODE, &mut txn, &key("never_minted")).unwrap(), None);
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"an over-capacity provider must still serve absence from membership alone"
		);
		let completeness = restarted.completeness(NODE);
		assert!(!completeness.values_complete, "three mappings cannot be values-complete at capacity two");
		assert!(completeness.membership_complete, "membership must cover the full population");
	}

	#[test]
	fn over_capacity_brand_new_keys_are_minted_without_a_store_read() {
		// get_or_create partitions the batch through membership: definitely-new keys skip the
		// get_many entirely. In the firehose workload new keys dominate, so this is the path that
		// keeps get_many::operator_internal off the profile even when the value cache overflows.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::new(ByteSize::from_bytes(entry_bytes(&key("k1")) * 2));
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(NODE, &mut txn, &key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::new(ByteSize::from_bytes(entry_bytes(&key("k1")) * 2));
		let mut txn = deferred(&engine);
		restarted.get_or_create_row_number(NODE, &mut txn, &key("warmup")).unwrap();

		let reads_before = txn.store_reads();
		let fresh = [key("new_a"), key("new_b"), key("new_c")];
		let results = restarted.get_or_create_row_numbers(NODE, &mut txn, &fresh).unwrap();
		assert!(results.iter().all(|(_, is_new)| *is_new), "all three keys are brand new");
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"definitely-new keys must be minted without consulting the store"
		);
	}

	#[test]
	fn a_confirmed_removal_updates_membership_so_absence_stays_in_memory() {
		// drop_row_number must retire the key's membership evidence along with the mapping;
		// otherwise every later probe of the removed key reads as maybe-present and pays a
		// pointless store read forever.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::new(ByteSize::from_bytes(entry_bytes(&key("k1")) * 2));
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(NODE, &mut txn, &key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::new(ByteSize::from_bytes(entry_bytes(&key("k1")) * 2));
		let mut txn = deferred(&engine);
		restarted.get_row_number(NODE, &mut txn, &key("k2")).unwrap();
		assert!(restarted.drop_row_number(NODE, &mut txn, &key("k1")).unwrap());

		let reads_before = txn.store_reads();
		assert_eq!(restarted.get_row_number(NODE, &mut txn, &key("k1")).unwrap(), None);
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"the removed key's absence must be answered by membership, not the store"
		);
	}

	#[test]
	fn tick_eviction_drops_only_expired_mappings_and_keeps_the_rest_in_memory() {
		// Join and append run evict_expired every tick. Evicting by clearing the whole cache
		// silently downgrades the provider to one store roundtrip per key for the rest of its
		// life - the surviving mapping and its completeness must both outlive the tick.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::default();

		let mut first = deferred(&engine);
		let (minted_old, _) = provider.get_or_create_row_number(NODE, &mut first, &key("old")).unwrap();
		commit_pending(&engine, &mut first);
		let cutoff = engine.begin_admin(IdentityId::system()).unwrap().version();

		let mut second = deferred(&engine);
		let (minted_young, _) = provider.get_or_create_row_number(NODE, &mut second, &key("young")).unwrap();
		commit_pending(&engine, &mut second);

		let mut third = deferred(&engine);
		provider.evict_expired(NODE, &mut third, cutoff, &mut None, 100).unwrap();

		let reads_before = third.store_reads();
		let (resolved, is_new) = provider.get_or_create_row_number(NODE, &mut third, &key("young")).unwrap();
		assert!(!is_new, "the surviving mapping must not be re-minted");
		assert_eq!(resolved, minted_young, "the surviving mapping keeps its row number");
		assert_eq!(
			third.store_reads() - reads_before,
			0,
			"a tick eviction must not cost the survivor its in-memory resolution"
		);

		let (reminted, is_new) = provider.get_or_create_row_number(NODE, &mut third, &key("old")).unwrap();
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

		let (rn10, _) = provider.get_or_create_row_number(NODE, &mut txn, &slot_key(10)).unwrap();
		let (rn20, _) = provider.get_or_create_row_number(NODE, &mut txn, &slot_key(20)).unwrap();
		let (rn30, _) = provider.get_or_create_row_number(NODE, &mut txn, &slot_key(30)).unwrap();

		let upper = EncodedKey::builder().u64(25u64).u32(0u32).u32(0u32).build();
		let mut dropped = provider.drop_below(NODE, &mut txn, &upper).unwrap();
		dropped.sort_by_key(|rn| rn.0);
		assert_eq!(dropped, vec![rn10, rn20], "exactly the below-bound mappings are reclaimed");

		let (rn30_again, is_new30) = provider.get_or_create_row_number(NODE, &mut txn, &slot_key(30)).unwrap();
		assert!(!is_new30, "slot 30 sat above the bound and must remain mapped");
		assert_eq!(rn30, rn30_again);

		let (rn10_again, is_new10) = provider.get_or_create_row_number(NODE, &mut txn, &slot_key(10)).unwrap();
		assert!(is_new10, "reclaimed slot 10 mints fresh");
		assert_ne!(rn10, rn10_again, "a reclaimed row number is never reused");
	}

	#[test]
	fn mapping_and_counter_tags_are_distinct() {
		// The mapping keyspace and the counter live under the same internal-state node; if their
		// tag bytes collided a mint would overwrite the counter (or vice versa).
		assert_ne!(
			FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG,
			FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG,
			"mapping and counter tags must never collide"
		);
		assert_ne!(make_map_key(&key("x")), counter_key(), "a mapping key must never equal the counter key");
	}
}
