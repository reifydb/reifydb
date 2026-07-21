// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::{
	cell::{Cell, UnsafeCell},
	collections::{HashMap, HashSet},
	iter::once,
	mem::size_of,
	ops::Bound,
};

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::{
		deserializer::KeyDeserializer,
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	metrics::heap::{StateCompleteness, StateMemory},
	state::membership::MembershipIndex,
};
use reifydb_sdk::state::{decode_payload, encode_payload};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	Result, byte_size::ByteSize, count::Count, reifydb_assertions, util::hash::xxh3_64,
	value::row_number::RowNumber,
};

use crate::operator::stateful::utils::{
	internal_state_drop, internal_state_get, internal_state_range_versioned, internal_state_set,
};
use reifydb_flow::transaction::FlowTransaction;

pub fn allocate_row_numbers(txn: &mut FlowTransaction, node: FlowNodeId, count: u64) -> Result<u64> {
	let registry = txn.row_allocators();
	let counter_key = counter_key();
	let seed = if registry.is_seeded(node) {
		0
	} else {
		match internal_state_get(node, txn, &counter_key)? {
			Some(row) => decode_payload::<u64>(&row)?,
			None => 1,
		}
	};
	let start = registry.allocate(node, count, seed);
	let high_water = registry.high_water(node).expect("node seeded after allocate");
	let now = txn.clock().now_nanos();
	internal_state_set(node, txn, &counter_key, encode_payload(&high_water, now)?)?;
	Ok(start)
}

fn counter_key() -> EncodedKey {
	let mut serializer = KeySerializer::new();
	serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG);
	serializer.finish()
}

const CACHE_CAPACITY: usize = 65_536;
const HYDRATE_CHUNK: usize = 8_192;
const MEMBERSHIP_BYTE_CAP: u64 = 16 * 1024 * 1024;

fn membership_hash(key: &EncodedKey) -> u64 {
	xxh3_64(key.as_ref()).0
}

pub struct RowNumberProvider {
	node: FlowNodeId,
	cache: UnsafeCell<HashMap<EncodedKey, RowNumber>>,
	membership: UnsafeCell<Option<MembershipIndex>>,
	capacity: usize,
	hydrated: Cell<bool>,
	complete: Cell<bool>,
	absences_served: Cell<u64>,
	false_positives: Cell<u64>,
	revocations: Cell<u64>,
}

impl RowNumberProvider {
	pub fn new(node: FlowNodeId) -> Self {
		Self::with_capacity(node, CACHE_CAPACITY)
	}

	fn with_capacity(node: FlowNodeId, capacity: usize) -> Self {
		Self {
			node,
			cache: UnsafeCell::new(HashMap::new()),
			membership: UnsafeCell::new(None),
			capacity,
			hydrated: Cell::new(false),
			complete: Cell::new(false),
			absences_served: Cell::new(0),
			false_positives: Cell::new(0),
			revocations: Cell::new(0),
		}
	}

	fn hydrate_once(&self, txn: &mut FlowTransaction) -> Result<()> {
		if self.hydrated.get() {
			return Ok(());
		}
		self.hydrated.set(true);
		let prefix = {
			let mut serializer = KeySerializer::new();
			serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
			serializer.finish()
		};
		let base = EncodedKeyRange::prefix(prefix.as_ref());
		let cache = self.cache();
		let mut hashes: Vec<u64> = Vec::new();
		let mut values_complete = true;
		let mut start = base.start.clone();
		loop {
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.internal_state_range(self.node, range, Some(HYDRATE_CHUNK))?;
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
				if cache.len() < self.capacity {
					cache.insert(original, RowNumber(decode_payload::<u64>(&item.row)?));
				} else {
					values_complete = false;
				}
				last_inner = Some(EncodedKey::new(decoded.key.clone()));
			}
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
		*self.membership_mut() = tracked.then_some(membership);
		self.complete.set(values_complete);
		Ok(())
	}

	#[allow(clippy::mut_from_ref)]
	fn cache(&self) -> &mut HashMap<EncodedKey, RowNumber> {
		unsafe { &mut *self.cache.get() }
	}

	#[allow(clippy::mut_from_ref)]
	fn membership_mut(&self) -> &mut Option<MembershipIndex> {
		// SAFETY: the provider is confined to its flow actor's thread and no reference

		unsafe { &mut *self.membership.get() }
	}

	fn membership_ref(&self) -> &Option<MembershipIndex> {
		// SAFETY: same single-threaded confinement as membership_mut; shared reads

		unsafe { &*self.membership.get() }
	}

	fn membership_insert(&self, key: &EncodedKey) {
		let membership = self.membership_mut();
		if let Some(index) = membership.as_mut()
			&& !index.insert(membership_hash(key))
		{
			*membership = None;
		}
	}

	fn membership_remove(&self, key: &EncodedKey) {
		if let Some(index) = self.membership_mut().as_mut() {
			index.remove(membership_hash(key));
		}
	}

	fn membership_contains(&self, key: &EncodedKey) -> Option<bool> {
		self.membership_ref().as_ref().map(|index| index.contains(membership_hash(key)))
	}

	fn count_absence(&self) {
		self.absences_served.set(self.absences_served.get() + 1);
	}

	fn count_false_positive(&self) {
		if self.membership_ref().is_some() {
			self.false_positives.set(self.false_positives.get() + 1);
		}
	}

	pub fn memory(&self) -> StateMemory {
		let cache = unsafe { &*self.cache.get() };
		let bytes = cache.capacity() * (size_of::<EncodedKey>() + size_of::<RowNumber>() + 1)
			+ cache.keys().map(|key| key.as_ref().len()).sum::<usize>();
		StateMemory::new(Count::new(cache.len() as u64), ByteSize::from_bytes(bytes as u64))
	}

	pub fn membership_memory(&self) -> StateMemory {
		self.membership_ref().as_ref().map_or(StateMemory::ZERO, MembershipIndex::approximate_memory)
	}

	pub fn completeness(&self) -> StateCompleteness {
		if !self.hydrated.get() {
			return StateCompleteness::MERGE_IDENTITY;
		}
		StateCompleteness {
			values_complete: self.complete.get(),
			membership_complete: self.membership_ref().is_some(),
			absences_served: Count::new(self.absences_served.get()),
			false_positives: Count::new(self.false_positives.get()),
			revocations: Count::new(self.revocations.get()),
		}
	}

	fn remember(&self, key: &EncodedKey, row_number: RowNumber) {
		let cache = self.cache();
		if cache.len() >= self.capacity {
			cache.clear();
			if self.complete.get() {
				self.complete.set(false);
				self.revocations.set(self.revocations.get() + 1);
			}
		}
		cache.insert(key.clone(), row_number);
	}

	pub fn get_or_create_row_numbers<'a, I>(
		&self,
		txn: &mut FlowTransaction,
		keys: I,
	) -> Result<Vec<(RowNumber, bool)>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let now = txn.clock().now_nanos();
		self.hydrate_once(txn)?;
		let keys: Vec<&EncodedKey> = keys.into_iter().collect();

		let mut results: Vec<Option<(RowNumber, bool)>> = (0..keys.len()).map(|_| None).collect();
		let mut to_resolve: Vec<usize> = Vec::new();
		for (i, key) in keys.iter().enumerate() {
			match self.cache().get(*key) {
				Some(row_number) => results[i] = Some((*row_number, false)),
				None => to_resolve.push(i),
			}
		}
		if to_resolve.is_empty() {
			return Ok(results.into_iter().map(|r| r.expect("every position filled")).collect());
		}

		let map_keys: Vec<EncodedKey> = to_resolve.iter().map(|i| self.make_map_key(keys[*i])).collect();

		let mut consulted_store: Vec<bool> = Vec::new();
		let found: HashMap<Vec<u8>, EncodedRow> = if self.complete.get() {
			HashMap::new()
		} else {
			let mut lookup: Vec<EncodedKey> = Vec::new();
			for (slot, i) in to_resolve.iter().enumerate() {
				let maybe = self.membership_contains(keys[*i]).unwrap_or(true);
				consulted_store.push(maybe);
				if maybe {
					lookup.push(map_keys[slot].clone());
				} else {
					self.count_absence();
				}
			}
			if lookup.is_empty() {
				HashMap::new()
			} else {
				let batch = txn.internal_state_get_many(self.node, &lookup)?;
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

		let mut new_positions: Vec<(usize, EncodedKey)> = Vec::new();

		for (slot, map_key) in map_keys.into_iter().enumerate() {
			let i = to_resolve[slot];
			match found.get(map_key.as_ref()) {
				Some(existing_row) => {
					let row_number = RowNumber(decode_payload::<u64>(existing_row)?);
					self.remember(keys[i], row_number);
					results[i] = Some((row_number, false));
				}
				None => {
					if consulted_store.get(slot) == Some(&true) {
						self.count_false_positive();
					}
					new_positions.push((i, map_key));
				}
			}
		}

		if !new_positions.is_empty() {
			let start = self.mint(txn, new_positions.len() as u64)?;
			for (offset, (i, map_key)) in new_positions.iter().enumerate() {
				let row_number = RowNumber(start + offset as u64);
				internal_state_set(self.node, txn, map_key, encode_payload(&row_number.0, now)?)?;
				self.remember(keys[*i], row_number);
				self.membership_insert(keys[*i]);
				results[*i] = Some((row_number, true));
			}
		}

		Ok(results.into_iter().map(|r| r.expect("every position filled")).collect())
	}

	fn mint(&self, txn: &mut FlowTransaction, count: u64) -> Result<u64> {
		allocate_row_numbers(txn, self.node, count)
	}

	pub fn get_or_create_row_number(
		&self,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(txn, once(key))?.into_iter().next().unwrap())
	}

	pub fn get_row_number(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<RowNumber>> {
		self.hydrate_once(txn)?;
		if let Some(row_number) = self.cache().get(key) {
			return Ok(Some(*row_number));
		}
		if self.complete.get() {
			return Ok(None);
		}
		if self.membership_contains(key) == Some(false) {
			self.count_absence();
			return Ok(None);
		}
		let map_key = self.make_map_key(key);
		match internal_state_get(self.node, txn, &map_key)? {
			Some(existing_row) => {
				let row_number = RowNumber(decode_payload::<u64>(&existing_row)?);
				self.remember(key, row_number);
				Ok(Some(row_number))
			}
			None => {
				self.count_false_positive();
				Ok(None)
			}
		}
	}

	pub fn remove_for_key(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<bool> {
		let cached = self.cache().remove(key).is_some();
		let map_key = self.make_map_key(key);
		if !cached {
			if self.complete.get() {
				return Ok(false);
			}
			if self.membership_contains(key) == Some(false) {
				self.count_absence();
				return Ok(false);
			}
			if internal_state_get(self.node, txn, &map_key)?.is_none() {
				self.count_false_positive();
				return Ok(false);
			}
		}
		internal_state_drop(self.node, txn, &map_key)?;
		self.membership_remove(key);
		Ok(true)
	}

	fn make_map_key(&self, key: &EncodedKey) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}

	pub fn remove_by_prefix(&self, txn: &mut FlowTransaction, key_prefix: &[u8]) -> Result<()> {
		self.cache().retain(|key, _| !key.as_ref().starts_with(key_prefix));

		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		let state_prefix = FlowNodeInternalStateKey::new(self.node, prefix.clone());
		let full_range = EncodedKeyRange::prefix(&state_prefix.encode());

		let keys_to_remove = {
			let stream = txn.range(full_range, RangeScope::All, 1024);
			let mut keys = Vec::new();
			for result in stream {
				let multi = result?;
				keys.push(multi.key);
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
					self.membership_remove(&original);
				}
			}
			txn.remove(&key)?;
		}

		Ok(())
	}

	pub fn evict_expired(
		&self,
		txn: &mut FlowTransaction,
		cutoff_version: CommitVersion,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<()> {
		let prefix = {
			let mut serializer = KeySerializer::new();
			serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
			serializer.finish()
		};
		let base = EncodedKeyRange::prefix(prefix.as_ref());
		let start = match cursor.clone() {
			Some(c) => Bound::Excluded(c),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch = internal_state_range_versioned(self.node, txn, range)
			.take(batch_size)
			.collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < batch_size;
		let last_key = batch.last().map(|(key, _, _)| key.clone());

		let mut untracked: HashSet<EncodedKey> = HashSet::new();
		for (key, version, _row) in batch {
			if version > cutoff_version {
				continue;
			}
			internal_state_drop(self.node, txn, &key)?;
			let mut de = KeyDeserializer::from_bytes(key.as_ref());
			de.read_u8()?;
			let original = EncodedKey::new(de.read_bytes()?);
			self.cache().remove(&original);
			if untracked.insert(original.clone()) {
				self.membership_remove(&original);
			}
		}

		*cursor = if reached_end {
			None
		} else {
			last_key
		};
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;
	use reifydb_value::value::identity::IdentityId;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		engine.flow_txn().deferred()
	}

	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		engine.commit_pending(txn);
	}

	#[test]
	fn an_idle_provider_merges_as_the_completeness_identity() {
		// Join, distinct and append samples AND this provider's flags into their
		// membership filters' completeness. A provider that never hydrated has
		// served nothing and proves nothing, so it must merge as the identity:
		// before this fix every healthy join node in the [memory] log reported
		// membership_complete=0 solely because its row-number provider was idle,
		// masking the one real signal the flag exists for (a cap-discarded filter).
		let engine = TestEngine::new();
		let provider = RowNumberProvider::new(FlowNodeId(1));
		assert_eq!(provider.completeness(), StateCompleteness::MERGE_IDENTITY);

		let mut txn = deferred(&engine);
		provider.get_or_create_row_number(&mut txn, &test_key("mint")).unwrap();
		let completeness = provider.completeness();
		assert!(completeness.membership_complete, "a hydrated provider must report its real membership state");
		assert!(completeness.values_complete);
	}

	#[test]
	fn a_known_mapping_is_served_from_the_operator_cache_across_transactions() {
		// Row-number mappings are write-once and immune to the operator-state TTL GC, so a
		// mapping this operator already resolved cannot change under it. Re-reading it from
		// the store on every slice is the read amplification the cache exists to kill: each
		// slice runs in a fresh transaction, so the per-transaction prefetch starts cold and
		// the same handful of window keys were re-fetched once per version.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::new(FlowNodeId(1));
		let key = test_key("mint");

		let mut first = deferred(&engine);
		let (minted, is_new) = provider.get_or_create_row_number(&mut first, &key).unwrap();
		assert!(is_new, "the first resolve mints the mapping");
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		let reads_before = second.store_reads();
		let (resolved, is_new) = provider.get_or_create_row_number(&mut second, &key).unwrap();
		assert_eq!(resolved, minted, "the cached mapping must resolve to the row the first slice minted");
		assert!(!is_new, "a cached mapping is an existing mapping: emitting it as new would double-insert");
		assert_eq!(second.store_reads() - reads_before, 0, "a cached mapping must not reach the store");
	}

	#[test]
	fn a_dropped_mapping_is_never_served_from_the_cache() {
		// The cache may only outlive a slice because nothing else deletes these mappings. The
		// operator itself does, though (session close, join eviction), and a cache that kept
		// serving a dropped key would hand out a row number whose state row is gone.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::new(FlowNodeId(1));
		let key = test_key("dropped");

		let mut first = deferred(&engine);
		let (minted, _) = provider.get_or_create_row_number(&mut first, &key).unwrap();
		provider.remove_for_key(&mut first, &key).unwrap();
		assert_eq!(
			provider.get_row_number(&mut first, &key).unwrap(),
			None,
			"a dropped mapping must not resurface through the cache"
		);
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		let (reminted, is_new) = provider.get_or_create_row_number(&mut second, &key).unwrap();
		assert!(is_new, "the key was dropped, so resolving it again mints a fresh mapping");
		assert_ne!(reminted, minted, "row numbers are never reused");
	}

	#[test]
	fn a_restarted_provider_resolves_persisted_mappings_instead_of_reminting() {
		// A fresh provider claims completeness after hydrating the 'M' keyspace. If
		// hydration silently loaded nothing while completeness was still claimed, every
		// persisted mapping would look absent and get re-minted under a NEW row number -
		// downstream state rows would silently split across two row numbers.
		let engine = TestEngine::new();
		let key = test_key("persisted");

		let minted = {
			let provider = RowNumberProvider::new(FlowNodeId(1));
			let mut txn = deferred(&engine);
			let (minted, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert!(is_new);
			commit_pending(&engine, &mut txn);
			minted
		};

		let restarted = RowNumberProvider::new(FlowNodeId(1));
		let mut txn = deferred(&engine);
		let (resolved, is_new) = restarted.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(!is_new, "a persisted mapping must never be re-minted after a restart");
		assert_eq!(resolved, minted, "the restarted provider must resolve to the original row number");
	}

	#[test]
	fn a_hydrated_provider_proves_absence_without_store_reads() {
		// The point of hydration: an unknown key must not cost a store lookup just to
		// learn its mapping does not exist - that per-new-key roundtrip was the hot-path
		// cost this exists to remove.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let mut first = deferred(&engine);
		provider.get_or_create_row_number(&mut first, &test_key("known")).unwrap();
		commit_pending(&engine, &mut first);

		let mut second = deferred(&engine);
		provider.get_row_number(&mut second, &test_key("known")).unwrap();

		let reads_before = second.store_reads();
		assert_eq!(
			provider.get_row_number(&mut second, &test_key("unknown")).unwrap(),
			None,
			"the key was never minted"
		);
		assert_eq!(
			second.store_reads() - reads_before,
			0,
			"after hydration an absent mapping must be answered from memory alone"
		);
	}

	#[test]
	fn a_mapping_population_beyond_capacity_stays_read_through() {
		// Hydration pins at most `capacity` mappings. When the persisted population is
		// larger, claiming completeness over the truncated load would make the overflow
		// keys look absent and re-mint them; the provider must stay read-through instead.
		let engine = TestEngine::new();
		let minted = {
			let seed = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
			let mut txn = deferred(&engine);
			let minted = seed.get_or_create_row_number(&mut txn, &test_key("k1")).unwrap().0;
			seed.get_or_create_row_number(&mut txn, &test_key("k2")).unwrap();
			seed.get_or_create_row_number(&mut txn, &test_key("k3")).unwrap();
			commit_pending(&engine, &mut txn);
			minted
		};

		let restarted = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
		let mut txn = deferred(&engine);
		let (resolved, is_new) = restarted.get_or_create_row_number(&mut txn, &test_key("k1")).unwrap();
		assert!(!is_new, "an over-capacity population must be resolved through the store, not re-minted");
		assert_eq!(resolved, minted);
	}

	#[test]
	fn an_overflow_cache_clear_revokes_absence_proofs() {
		// remember() clears the whole cache when it hits capacity. The cleared entries'
		// mappings still exist in the store, so if completeness survived the clear, the
		// next resolve of a cleared key would look absent and re-mint it.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::with_capacity(FlowNodeId(1), 2);

		let mut txn = deferred(&engine);
		let minted = provider.get_or_create_row_number(&mut txn, &test_key("k1")).unwrap().0;
		provider.get_or_create_row_number(&mut txn, &test_key("k2")).unwrap();
		provider.get_or_create_row_number(&mut txn, &test_key("k3")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut second = deferred(&engine);
		let (resolved, is_new) = provider.get_or_create_row_number(&mut second, &test_key("k1")).unwrap();
		assert!(!is_new, "a mapping dropped from the cache by the overflow clear still exists in the store");
		assert_eq!(resolved, minted);
	}

	#[test]
	fn an_over_capacity_population_still_proves_absence_from_memory() {
		// The value cache is capacity-bounded, but membership is built from the FULL
		// hydration scan. A population 1.5x the capacity must therefore still answer
		// "this key was never minted" without a store read - the exact per-new-key
		// roundtrip that returns if membership completeness were tied to the value
		// cache fitting.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(&mut txn, &test_key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
		let mut txn = deferred(&engine);
		restarted.get_row_number(&mut txn, &test_key("k1")).unwrap();

		let reads_before = txn.store_reads();
		assert_eq!(restarted.get_row_number(&mut txn, &test_key("never_minted")).unwrap(), None);
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"an over-capacity provider must still serve absence from membership alone"
		);
		let completeness = restarted.completeness();
		assert!(!completeness.values_complete, "3 mappings cannot be values-complete at capacity 2");
		assert!(completeness.membership_complete, "membership must cover the full population");
		assert_eq!(completeness.absences_served.as_u64(), 1);
	}

	#[test]
	fn over_capacity_batch_resolution_skips_the_store_for_brand_new_keys() {
		// get_or_create partitions each batch through membership: keys that are
		// definitely new must go straight to allocation without joining the get_many.
		// In the firehose workload new keys dominate, so this is the path that keeps
		// get_many::operator_internal off the profile even when values overflow.
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(&mut txn, &test_key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
		let mut txn = deferred(&engine);
		// First resolve hydrates and seeds the row-allocator registry (one counter read).
		restarted.get_or_create_row_number(&mut txn, &test_key("warmup")).unwrap();

		let reads_before = txn.store_reads();
		let fresh = [test_key("new_a"), test_key("new_b"), test_key("new_c")];
		let results = restarted.get_or_create_row_numbers(&mut txn, fresh.iter()).unwrap();
		assert!(results.iter().all(|(_, is_new)| *is_new), "all three keys are brand new");
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"definitely-new keys must be minted without consulting the store"
		);
	}

	#[test]
	fn a_confirmed_removal_updates_membership_so_absence_stays_in_memory() {
		// remove_for_key must retire the key's membership evidence along with the
		// mapping; otherwise every later probe of the removed key would read as
		// maybe-present and pay a pointless store read forever (values-incomplete
		// providers have no other absence source).
		let engine = TestEngine::new();
		{
			let seed = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
			let mut txn = deferred(&engine);
			for name in ["k1", "k2", "k3"] {
				seed.get_or_create_row_number(&mut txn, &test_key(name)).unwrap();
			}
			commit_pending(&engine, &mut txn);
		}

		let restarted = RowNumberProvider::with_capacity(FlowNodeId(1), 2);
		let mut txn = deferred(&engine);
		// Hydrate first so the zero-read assertion below measures membership, not the scan.
		restarted.get_row_number(&mut txn, &test_key("k2")).unwrap();
		assert!(restarted.remove_for_key(&mut txn, &test_key("k1")).unwrap());

		let reads_before = txn.store_reads();
		assert_eq!(restarted.get_row_number(&mut txn, &test_key("k1")).unwrap(), None);
		assert_eq!(
			txn.store_reads() - reads_before,
			0,
			"the removed key's absence must be answered by membership, not the store"
		);
	}

	#[test]
	fn tick_eviction_drops_only_expired_mappings_and_keeps_the_rest_served_from_memory() {
		// Join and append operators run evict_expired every tick. Evicting by clearing
		// the whole cache (the old behaviour) silently downgraded the provider to one
		// store roundtrip per key for the rest of its life - the surviving mappings and
		// the completeness claim must both outlive the tick. The zero-store-reads
		// assertion is what fails if eviction ever goes back to a wholesale clear.
		let engine = TestEngine::new();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let mut first = deferred(&engine);
		let (minted_a, _) = provider.get_or_create_row_number(&mut first, &test_key("old")).unwrap();
		commit_pending(&engine, &mut first);
		let cutoff = engine.begin_admin(IdentityId::system()).unwrap().version();

		let mut second = deferred(&engine);
		let (minted_b, _) = provider.get_or_create_row_number(&mut second, &test_key("young")).unwrap();
		commit_pending(&engine, &mut second);

		let mut third = deferred(&engine);
		provider.evict_expired(&mut third, cutoff, &mut None, 100).unwrap();

		let reads_before = third.store_reads();
		let (resolved, is_new) = provider.get_or_create_row_number(&mut third, &test_key("young")).unwrap();
		assert!(!is_new, "the surviving mapping must not be re-minted");
		assert_eq!(resolved, minted_b, "the surviving mapping must keep its row number");
		assert_eq!(
			third.store_reads() - reads_before,
			0,
			"a tick eviction must not cost the surviving mappings their in-memory resolution"
		);

		assert_eq!(
			provider.get_row_number(&mut third, &test_key("old")).unwrap(),
			None,
			"the expired mapping is gone from cache and store alike"
		);
		let (reminted, is_new) = provider.get_or_create_row_number(&mut third, &test_key("old")).unwrap();
		assert!(is_new, "resolving an evicted key mints a fresh mapping");
		assert_ne!(reminted, minted_a, "row numbers are never reused");
	}

	#[test]
	fn test_first_row_number() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("first");
		let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

		assert_eq!(row_num.0, 1);
		assert!(is_new);
	}

	#[test]
	fn test_duplicate_key_same_row_number() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("duplicate");

		// First call - should create new
		let (row_num1, is_new1) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(row_num1.0, 1);
		assert!(is_new1);

		// Second call with same key - should return existing
		let (row_num2, is_new2) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(row_num2.0, 1);
		assert!(!is_new2);

		// Row numbers should be the same
		assert_eq!(row_num1, row_num2);
	}

	#[test]
	fn test_sequential_row_numbers() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create multiple unique keys
		for i in 1..=5 {
			let key = test_key(&format!("key_{}", i));
			let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

			assert_eq!(row_num.0, i as u64);
			assert!(is_new);
		}
	}

	#[test]
	fn test_mixed_new_and_existing() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some keys
		let key1 = test_key("mixed_1");
		let key2 = test_key("mixed_2");
		let key3 = test_key("mixed_3");

		// First round - all new
		let (rn1, new1) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();
		let (rn2, new2) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		let (rn3, new3) = provider.get_or_create_row_number(&mut txn, &key3).unwrap();

		assert_eq!(rn1.0, 1);
		assert!(new1);
		assert_eq!(rn2.0, 2);
		assert!(new2);
		assert_eq!(rn3.0, 3);
		assert!(new3);

		// Second round - mixed
		let key4 = test_key("mixed_4");
		let (rn2_again, new2_again) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		let (rn4, new4) = provider.get_or_create_row_number(&mut txn, &key4).unwrap();
		let (rn1_again, new1_again) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();

		assert_eq!(rn2_again.0, 2);
		assert!(!new2_again);
		assert_eq!(rn4.0, 4); // Next sequential number
		assert!(new4);
		assert_eq!(rn1_again.0, 1);
		assert!(!new1_again);
	}

	#[test]
	fn test_multiple_providers_isolated() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider1 = RowNumberProvider::new(FlowNodeId(1));
		let provider2 = RowNumberProvider::new(FlowNodeId(2));

		let key = test_key("shared_key");

		// Same key in different providers should get different encoded numbers
		let (rn1, _) = provider1.get_or_create_row_number(&mut txn, &key).unwrap();
		let (rn2, _) = provider2.get_or_create_row_number(&mut txn, &key).unwrap();

		assert_eq!(rn1.0, 1);
		assert_eq!(rn2.0, 1);

		// Add more keys to provider1
		let key2 = test_key("key2");
		let (rn1_2, _) = provider1.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn1_2.0, 2);

		// Provider2 should still be at 1 for new keys
		let (rn2_2, _) = provider2.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn2_2.0, 2);
	}

	#[test]
	fn test_counter_persistence() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some encoded numbers
		for i in 1..=3 {
			let key = test_key(&format!("persist_{}", i));
			let (rn, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert_eq!(rn.0, i as u64);
		}

		// Simulate loading counter again (internally happens in get_or_create)
		let new_key = test_key("persist_new");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &new_key).unwrap();

		// Should continue from where we left off
		assert_eq!(rn.0, 4);
		assert!(is_new);
	}

	#[test]
	fn test_large_row_numbers() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create many encoded numbers
		for i in 1..=1000 {
			let key = test_key(&format!("large_{}", i));
			let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert_eq!(rn.0, i as u64);
			assert!(is_new);
		}

		// Verify we can still retrieve early ones
		let key = test_key("large_1");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(rn.0, 1);
		assert!(!is_new);

		// And continue adding new ones
		let key = test_key("large_1001");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(rn.0, 1001);
		assert!(is_new);
	}

	#[test]
	fn test_mixed_existing_and_new_keys() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create 3 initial keys to establish existing row numbers
		let key1 = test_key("key_1");
		let key2 = test_key("key_2");
		let key3 = test_key("key_3");

		let (rn1, _) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();
		assert_eq!(rn1.0, 1);

		let (rn2, _) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn2.0, 2);

		let (rn3, _) = provider.get_or_create_row_number(&mut txn, &key3).unwrap();
		assert_eq!(rn3.0, 3);

		// Now test batch with mix of existing and new keys
		let key4 = test_key("key_4");
		let key5 = test_key("key_5");

		// Batch: [existing key2, new key4, existing key1, new key5, existing key3]
		let keys = vec![&key2, &key4, &key1, &key5, &key3];

		let results = provider.get_or_create_row_numbers(&mut txn, keys.into_iter()).unwrap();

		// Verify results are in correct order and have correct values
		assert_eq!(results.len(), 5);

		// key2 (existing) -> row number 2, not new
		assert_eq!(results[0].0.0, 2);
		assert!(!results[0].1);

		// key4 (new) -> row number 4, is new
		assert_eq!(results[1].0.0, 4);
		assert!(results[1].1);

		// key1 (existing) -> row number 1, not new
		assert_eq!(results[2].0.0, 1);
		assert!(!results[2].1);

		// key5 (new) -> row number 5, is new
		assert_eq!(results[3].0.0, 5);
		assert!(results[3].1);

		// key3 (existing) -> row number 3, not new
		assert_eq!(results[4].0.0, 3);
		assert!(!results[4].1);

		// Verify that counter was only incremented by 2 (for key4 and key5)
		// by checking that the next new key gets row number 6
		let key6 = test_key("key_6");
		let (rn6, is_new6) = provider.get_or_create_row_number(&mut txn, &key6).unwrap();
		assert_eq!(rn6.0, 6);
		assert!(is_new6);

		// Verify all mappings are still correct by retrieving them individually
		let (check_rn4, is_new4) = provider.get_or_create_row_number(&mut txn, &key4).unwrap();
		assert_eq!(check_rn4.0, 4);
		assert!(!is_new4);

		let (check_rn5, is_new5) = provider.get_or_create_row_number(&mut txn, &key5).unwrap();
		assert_eq!(check_rn5.0, 5);
		assert!(!is_new5);
	}

	#[test]
	fn test_get_row_number_returns_none_for_unknown() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("never_seen");
		assert_eq!(provider.get_row_number(&mut txn, &key).unwrap(), None);
	}

	#[test]
	fn test_get_row_number_returns_existing_without_creating() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("lookup_hit");
		let (created, was_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(was_new);

		let looked_up = provider.get_row_number(&mut txn, &key).unwrap();
		assert_eq!(looked_up, Some(created));

		let another = test_key("another_missing");
		assert_eq!(provider.get_row_number(&mut txn, &another).unwrap(), None);
		let (after, was_new_after) = provider.get_or_create_row_number(&mut txn, &another).unwrap();
		assert!(was_new_after);
		assert_ne!(after, created);
	}

	#[test]
	fn test_remove_for_key_clears_mapping() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("to_remove");
		let (_assigned, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(provider.get_row_number(&mut txn, &key).unwrap().is_some());

		let removed = provider.remove_for_key(&mut txn, &key).unwrap();
		assert!(removed);

		assert_eq!(provider.get_row_number(&mut txn, &key).unwrap(), None);
	}

	#[test]
	fn test_remove_for_key_is_idempotent() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("absent");
		assert!(!provider.remove_for_key(&mut txn, &key).unwrap());

		let (_assigned, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(provider.remove_for_key(&mut txn, &key).unwrap());
		assert!(!provider.remove_for_key(&mut txn, &key).unwrap());
	}

	#[test]
	fn test_remove_for_key_then_recreate_assigns_new_number() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("recycled");
		let (first, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(provider.remove_for_key(&mut txn, &key).unwrap());

		let (second, was_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(was_new, "after removal the next mapping should be created fresh");
		assert_ne!(first, second, "counter must keep advancing, not recycle old row numbers");
	}

	#[test]
	fn internal_state_tags_are_pairwise_distinct() {
		// The row-number counter/forward-map keys share the per-node
		// FlowNodeInternalState namespace with window-meta and gate-visibility keys.
		// Every tag must be pairwise distinct, or an operator that mixes them (e.g. a
		// windowed operator that also assigns row numbers) would overwrite another's
		// state in the same node range.
		let tags = [
			FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG,
			FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG,
			FlowNodeInternalStateKey::WINDOW_META_TAG,
			FlowNodeInternalStateKey::GATE_VISIBILITY_TAG,
		];
		for i in 0..tags.len() {
			for j in (i + 1)..tags.len() {
				assert_ne!(tags[i], tags[j], "internal-state tag collision at {:#04x}", tags[i]);
			}
		}
	}

	#[test]
	fn mapping_values_are_postcard_encoded() {
		// The forward map value must be encoded via postcard (encode_payload), not raw
		// big-endian / raw bytes. This pins it: the forward map value decodes as a u64
		// via decode_payload. RED on the old raw-be encoding.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("encoded");
		let (rn, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

		let forward =
			internal_state_get(FlowNodeId(1), &mut txn, &provider.make_map_key(&key)).unwrap().unwrap();
		assert_eq!(decode_payload::<u64>(&forward).unwrap(), rn.0);
	}

	#[test]
	fn test_counter_survives_full_mapping_eviction() {
		// Regression: purging EVERY per-key mapping (full eviction of the provider's
		// state) must not delete the monotonic counter. If it did, a fresh key would
		// reuse a previously issued row number and corrupt any downstream consumer that
		// tracks rows by number.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let keys = [test_key("a"), test_key("b"), test_key("c")];
		let mut issued = Vec::new();
		for key in &keys {
			let (n, was_new) = provider.get_or_create_row_number(&mut txn, key).unwrap();
			assert!(was_new);
			issued.push(n);
		}

		for key in &keys {
			assert!(provider.remove_for_key(&mut txn, key).unwrap());
		}

		let (fresh, was_new) = provider.get_or_create_row_number(&mut txn, &test_key("d")).unwrap();
		assert!(was_new, "a brand-new key after full eviction must be assigned fresh");
		for prev in &issued {
			assert_ne!(&fresh, prev, "row number {:?} was reused after full eviction", prev);
		}
		assert!(
			issued.iter().all(|prev| fresh.0 > prev.0),
			"counter must keep advancing past every previously issued number, got {:?} after {:?}",
			fresh,
			issued
		);
	}
}
