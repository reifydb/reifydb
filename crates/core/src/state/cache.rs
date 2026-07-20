// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, mem, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	state::{OperatorState, StateBytes, decode_state},
};
use reifydb_runtime::cache::slab::SlabLru;
use reifydb_value::{Result, byte_size::ByteSize, count::Count, reifydb_assertions};

use crate::{
	metrics::heap::{HeapSize, StateMemory},
	state::{budget::OperatorStateBudgetHandle, store::StateStore},
};

const ENTRY_OVERHEAD: u64 = (mem::size_of::<usize>() * 2) as u64;

#[derive(Clone, Copy, Debug)]
pub enum StateBackend {
	Data,

	Internal,
}

enum CleanEntry<V> {
	Archived(StateBytes),
	Native(Arc<V>),
}

impl<V> Clone for CleanEntry<V> {
	fn clone(&self) -> Self {
		match self {
			CleanEntry::Archived(bytes) => CleanEntry::Archived(bytes.clone()),
			CleanEntry::Native(arc) => CleanEntry::Native(arc.clone()),
		}
	}
}

enum DirtyEntry<V> {
	Live(Arc<V>),
	Removed,
}

#[derive(Clone, Copy, Debug, Default)]
struct CacheLedger {
	clean: u64,
	dirty: u64,
}

impl CacheLedger {
	fn charge_clean(&mut self, bytes: u64) {
		self.clean = self.clean.saturating_add(bytes);
	}

	fn release_clean(&mut self, bytes: u64) {
		reifydb_assertions! {
			assert!(
				self.clean >= bytes,
				"state cache clean ledger released below zero: held={} released={}",
				self.clean,
				bytes
			);
		}
		self.clean = self.clean.saturating_sub(bytes);
	}

	fn charge_dirty(&mut self, bytes: u64) {
		self.dirty = self.dirty.saturating_add(bytes);
	}

	fn release_dirty(&mut self, bytes: u64) {
		reifydb_assertions! {
			assert!(
				self.dirty >= bytes,
				"state cache dirty ledger released below zero: held={} released={}",
				self.dirty,
				bytes
			);
		}
		self.dirty = self.dirty.saturating_sub(bytes);
	}
}

pub struct StateCache<K, V> {
	clean: SlabLru<K, CleanEntry<V>>,
	dirty: HashMap<K, DirtyEntry<V>>,
	dirty_order: Vec<K>,
	dirty_bytes: HashMap<K, u64>,
	ledger: CacheLedger,
	pool: OperatorStateBudgetHandle,
	backend: StateBackend,
}

fn native_charge<V: HeapSize>(value: &V) -> u64 {
	(mem::size_of::<V>() + value.heap_size()) as u64 + ENTRY_OVERHEAD
}

fn archived_charge(bytes: &StateBytes) -> u64 {
	bytes.byte_size().as_bytes() + ENTRY_OVERHEAD
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + OperatorState + HeapSize,
{
	pub fn new(pool: OperatorStateBudgetHandle) -> Self {
		Self::with_backend(pool, StateBackend::Data)
	}

	pub fn new_internal(pool: OperatorStateBudgetHandle) -> Self {
		Self::with_backend(pool, StateBackend::Internal)
	}

	fn with_backend(pool: OperatorStateBudgetHandle, backend: StateBackend) -> Self {
		Self {
			clean: SlabLru::unbounded(),
			dirty: HashMap::new(),
			dirty_order: Vec::new(),
			dirty_bytes: HashMap::new(),
			ledger: CacheLedger::default(),
			pool,
			backend,
		}
	}

	pub fn get_arc(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<Arc<V>>> {
		if let Some(slot) = self.dirty.get(key) {
			return Ok(match slot {
				DirtyEntry::Live(arc) => Some(arc.clone()),
				DirtyEntry::Removed => None,
			});
		}

		if self.clean.contains_key(key) {
			return Ok(Some(self.promote(key)?));
		}

		let encoded_key = key.into_encoded_key();
		let loaded = match self.backend {
			StateBackend::Data => store.state_get(&encoded_key)?,
			StateBackend::Internal => store.internal_get(&encoded_key)?,
		};
		match loaded {
			Some(bytes) => {
				let value = decode_state::<V>(&bytes)?;
				let arc = Arc::new(value);
				self.insert_clean_native(key.clone(), arc.clone());
				self.evict_to_budget();
				Ok(Some(arc))
			}
			None => Ok(None),
		}
	}

	fn promote(&mut self, key: &K) -> Result<Arc<V>> {
		let entry = self.clean.get(key).expect("promote called for a resident clean key");
		match entry {
			CleanEntry::Native(arc) => Ok(arc),
			CleanEntry::Archived(bytes) => {
				// SAFETY: every Archived entry was validated by

				let archived = unsafe { V::archived_trusted(&bytes) };
				let value = V::materialize(archived)?;
				let arc = Arc::new(value);
				self.insert_clean_native(key.clone(), arc.clone());
				self.evict_to_budget();
				Ok(arc)
			}
		}
	}

	fn insert_clean_native(&mut self, key: K, arc: Arc<V>) {
		let charge = native_charge(arc.as_ref());
		if let Some(old) = self.clean.put(key, CleanEntry::Native(arc)) {
			self.release_clean_entry(&old);
		}
		self.ledger.charge_clean(charge);
		self.pool.charge_clean(ByteSize::from_bytes(charge));
	}

	fn insert_clean_archived(&mut self, key: K, bytes: StateBytes) {
		let charge = archived_charge(&bytes);
		if let Some(old) = self.clean.put(key, CleanEntry::Archived(bytes)) {
			self.release_clean_entry(&old);
		}
		self.ledger.charge_clean(charge);
		self.pool.charge_clean(ByteSize::from_bytes(charge));
	}

	fn release_clean_entry(&mut self, entry: &CleanEntry<V>) {
		let bytes = match entry {
			CleanEntry::Archived(b) => archived_charge(b),
			CleanEntry::Native(arc) => native_charge(arc.as_ref()),
		};
		self.ledger.release_clean(bytes);
		self.pool.release_clean(ByteSize::from_bytes(bytes));
	}

	fn insert_dirty(&mut self, key: K, entry: DirtyEntry<V>) {
		if let Some(old) = self.clean.remove(&key) {
			self.release_clean_entry(&old);
		}
		let charge = match &entry {
			DirtyEntry::Live(arc) => native_charge(arc.as_ref()),
			DirtyEntry::Removed => ENTRY_OVERHEAD,
		};
		if let Some(previous) = self.dirty_bytes.insert(key.clone(), charge) {
			self.ledger.release_dirty(previous);
			self.pool.release_dirty(ByteSize::from_bytes(previous));
		}
		if self.dirty.insert(key.clone(), entry).is_none() {
			self.dirty_order.push(key);
		}
		self.ledger.charge_dirty(charge);
		self.pool.charge_dirty(ByteSize::from_bytes(charge));
		self.evict_to_budget();
	}

	fn evict_to_budget(&mut self) {
		while self.pool.over_budget() {
			let Some((_, entry)) = self.clean.pop_tail() else {
				break;
			};
			let bytes = match &entry {
				CleanEntry::Archived(b) => archived_charge(b),
				CleanEntry::Native(arc) => native_charge(arc.as_ref()),
			};
			self.ledger.release_clean(bytes);
			self.pool.release_clean(ByteSize::from_bytes(bytes));
			self.pool.record_eviction(ByteSize::from_bytes(bytes));
		}
	}

	pub fn get(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<V>> {
		Ok(self.get_arc(store, key)?.map(|arc| (*arc).clone()))
	}

	pub fn take(&mut self, store: &mut impl StateStore, key: &K) -> Result<Option<V>> {
		let Some(arc) = self.get_arc(store, key)? else {
			return Ok(None);
		};
		if let Some(entry) = self.clean.remove(key) {
			self.release_clean_entry(&entry);
		}
		Ok(Some(Arc::try_unwrap(arc).unwrap_or_else(|arc| (*arc).clone())))
	}

	pub fn warm(&mut self, store: &mut impl StateStore, keys: &[K]) -> Result<()> {
		let mut to_load: Vec<K> = Vec::new();
		for key in keys {
			if self.clean.contains_key(key) || self.dirty.contains_key(key) {
				continue;
			}
			to_load.push(key.clone());
		}
		if to_load.is_empty() {
			return Ok(());
		}

		let mut by_encoded: HashMap<Vec<u8>, K> = HashMap::with_capacity(to_load.len());
		let mut encoded_keys: Vec<EncodedKey> = Vec::with_capacity(to_load.len());
		for key in &to_load {
			let encoded = key.into_encoded_key();
			by_encoded.insert(encoded.as_bytes().to_vec(), key.clone());
			encoded_keys.push(encoded);
		}

		let mut loaded: Vec<(K, StateBytes)> = Vec::new();
		let mut visit = |encoded: EncodedKey, bytes: StateBytes| -> Result<()> {
			if let Some(key) = by_encoded.get(encoded.as_bytes()) {
				V::archived(&bytes)?;
				loaded.push((key.clone(), bytes));
			}
			Ok(())
		};
		match self.backend {
			StateBackend::Data => store.state_get_many_visit(&encoded_keys, &mut visit)?,
			StateBackend::Internal => store.internal_get_many_visit(&encoded_keys, &mut visit)?,
		}
		for (key, bytes) in loaded {
			self.insert_clean_archived(key, bytes);
		}
		self.evict_to_budget();
		Ok(())
	}

	pub fn set(&mut self, _store: &mut impl StateStore, key: &K, value: &V) -> Result<()> {
		self.insert_dirty(key.clone(), DirtyEntry::Live(Arc::new(value.clone())));
		Ok(())
	}

	pub fn put(&mut self, _store: &mut impl StateStore, key: &K, value: V) -> Result<()> {
		self.insert_dirty(key.clone(), DirtyEntry::Live(Arc::new(value)));
		Ok(())
	}

	pub fn put_arc(&mut self, _store: &mut impl StateStore, key: &K, value: Arc<V>) -> Result<()> {
		self.insert_dirty(key.clone(), DirtyEntry::Live(value));
		Ok(())
	}

	pub fn modify<F>(&mut self, store: &mut impl StateStore, key: &K, f: F) -> Result<()>
	where
		F: FnOnce(&mut V) -> Result<()>,
		V: Default,
	{
		let mut arc = self.get_arc(store, key)?.unwrap_or_else(|| Arc::new(V::default()));
		f(Arc::make_mut(&mut arc))?;
		self.put_arc(store, key, arc)
	}

	pub fn remove(&mut self, _store: &mut impl StateStore, key: &K) -> Result<()> {
		self.insert_dirty(key.clone(), DirtyEntry::Removed);
		Ok(())
	}

	pub fn flush(&mut self, store: &mut impl StateStore) -> Result<()> {
		let mut dirty = mem::take(&mut self.dirty);
		let order = mem::take(&mut self.dirty_order);
		let mut dirty_bytes = mem::take(&mut self.dirty_bytes);
		let now_nanos = store.clock_now_nanos();
		for key in order {
			let Some(slot) = dirty.remove(&key) else {
				continue;
			};
			let encoded_key = (&key).into_encoded_key();
			match (slot, self.backend) {
				(DirtyEntry::Live(value), backend) => {
					let payload = value.encode_state(now_nanos)?;
					match backend {
						StateBackend::Data => store.state_set(&encoded_key, payload)?,
						StateBackend::Internal => store.internal_set(&encoded_key, payload)?,
					}
					self.release_flushed(&mut dirty_bytes, &key);
					self.insert_clean_native(key, value);
				}
				(DirtyEntry::Removed, StateBackend::Data) => {
					store.state_drop(&encoded_key)?;
					self.release_flushed(&mut dirty_bytes, &key);
				}
				(DirtyEntry::Removed, StateBackend::Internal) => {
					store.internal_drop(&encoded_key)?;
					self.release_flushed(&mut dirty_bytes, &key);
				}
			}
		}
		self.evict_to_budget();
		reifydb_assertions! {
			assert!(
				self.dirty.is_empty() && self.ledger.dirty == 0,
				"flush must drain every dirty entry and its ledger bytes (left={} bytes={})",
				self.dirty.len(),
				self.ledger.dirty
			);
		}
		Ok(())
	}

	fn release_flushed(&mut self, dirty_bytes: &mut HashMap<K, u64>, key: &K) {
		if let Some(bytes) = dirty_bytes.remove(key) {
			self.ledger.release_dirty(bytes);
			self.pool.release_dirty(ByteSize::from_bytes(bytes));
		}
	}

	pub fn clear_cache(&mut self) {
		let released = self.ledger.clean;
		self.clean.clear();
		self.ledger.release_clean(released);
		self.pool.release_clean(ByteSize::from_bytes(released));
	}

	pub fn invalidate(&mut self, key: &K) {
		if let Some(entry) = self.clean.remove(key) {
			self.release_clean_entry(&entry);
		}
	}

	pub fn is_cached(&self, key: &K) -> bool {
		self.clean.contains_key(key) || matches!(self.dirty.get(key), Some(DirtyEntry::Live(_)))
	}

	pub fn len(&self) -> usize {
		self.clean.len() + self.dirty.values().filter(|e| matches!(e, DirtyEntry::Live(_))).count()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> ByteSize {
		self.pool.snapshot().budget
	}

	pub fn approximate_memory(&self) -> StateMemory {
		StateMemory::new(
			Count::new((self.clean.len() + self.dirty.len()) as u64),
			ByteSize::from_bytes(
				self.ledger
					.clean
					.saturating_add(self.ledger.dirty)
					.saturating_add(self.clean.struct_bytes() as u64),
			),
		)
	}

	pub fn dirty_memory(&self) -> StateMemory {
		StateMemory::new(Count::new(self.dirty.len() as u64), ByteSize::from_bytes(self.ledger.dirty))
	}
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Default + OperatorState + HeapSize,
{
	pub fn get_or_default(&mut self, store: &mut impl StateStore, key: &K) -> Result<V> {
		match self.get(store, key)? {
			Some(value) => Ok(value),
			None => Ok(V::default()),
		}
	}

	pub fn update<U>(&mut self, store: &mut impl StateStore, key: &K, updater: U) -> Result<V>
	where
		U: FnOnce(&mut V) -> Result<()>,
	{
		let mut value = self.get_or_default(store, key)?;
		updater(&mut value)?;
		self.set(store, key, &value)?;
		Ok(value)
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, ops::Bound};

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_macro::operator_state;
	use reifydb_value::value::row_number::RowNumber;
	use serde::{Deserialize, Serialize};

	use super::*;

	#[operator_state]
	#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
	struct Cell {
		value: i32,
	}

	impl HeapSize for Cell {
		fn heap_size(&self) -> usize {
			0
		}
	}

	fn cell(value: i32) -> Cell {
		Cell {
			value,
		}
	}

	fn pool_of(bytes: u64) -> OperatorStateBudgetHandle {
		OperatorStateBudgetHandle::new(ByteSize::from_bytes(bytes))
	}

	fn big_pool() -> OperatorStateBudgetHandle {
		pool_of(64 * 1024 * 1024)
	}

	#[derive(Default)]
	struct MockStore {
		data: HashMap<Vec<u8>, StateBytes>,
		internal: HashMap<Vec<u8>, StateBytes>,
		drops: usize,
		removes: usize,
	}

	impl StateStore for MockStore {
		fn state_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
			Ok(self.data.get(key.as_bytes()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_bytes()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
			self.data.insert(key.as_bytes().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.removes += 1;
			self.data.remove(key.as_bytes());
			Ok(())
		}
		fn state_drop(&mut self, key: &EncodedKey) -> Result<()> {
			self.drops += 1;
			self.data.remove(key.as_bytes());
			Ok(())
		}
		fn internal_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
			Ok(self.internal.get(key.as_bytes()).cloned())
		}
		fn internal_get_many_visit(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.internal.get(key.as_bytes()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn internal_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
			self.internal.insert(key.as_bytes().to_vec(), payload);
			Ok(())
		}
		fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.internal.remove(key.as_bytes());
			Ok(())
		}
		fn internal_drop(&mut self, key: &EncodedKey) -> Result<()> {
			self.internal.remove(key.as_bytes());
			Ok(())
		}
		fn internal_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			let after_start = |k: &[u8]| match &range.start {
				Bound::Included(s) => k >= s.as_bytes(),
				Bound::Excluded(s) => k > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let before_end = |k: &[u8]| match &range.end {
				Bound::Included(e) => k <= e.as_bytes(),
				Bound::Excluded(e) => k < e.as_bytes(),
				Bound::Unbounded => true,
			};
			let mut matched: Vec<(Vec<u8>, StateBytes)> = self
				.internal
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			for (k, b) in matched {
				visit(EncodedKey::new(k), b)?;
			}
			Ok(())
		}
		fn get_or_create_row_number(&mut self, _key: &EncodedKey) -> Result<(RowNumber, bool)> {
			Ok((RowNumber(1), true))
		}
		fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
		}
		fn drop_row_number(&mut self, _key: &EncodedKey) -> Result<()> {
			Ok(())
		}
		fn allocate_row_numbers(&mut self, _count: u64) -> Result<RowNumber> {
			Ok(RowNumber(1))
		}
		fn clock_now_nanos(&self) -> u64 {
			0
		}
	}

	#[test]
	fn set_then_flush_persists_to_store_and_survives_cache_clear() {
		let mut store = MockStore::default();
		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());

		// A set is buffered (dirty) and visible via get before flush.
		cache.set(&mut store, &"a".to_string(), &cell(7)).unwrap();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(cell(7)));
		// Nothing reached the backing store yet.
		assert!(store.data.is_empty());

		cache.flush(&mut store).unwrap();
		assert!(!store.data.is_empty(), "flush must write dirty entries to the store");

		// After dropping the in-memory cache, the value must load from the store.
		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(cell(7)));
	}

	#[test]
	fn warm_bulk_loads_present_keys_and_skips_absent() {
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<String, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
			seed.set(&mut store, &"b".to_string(), &cell(2)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());
		let keys = vec!["a".to_string(), "b".to_string(), "missing".to_string()];
		cache.warm(&mut store, &keys).unwrap();

		assert!(cache.is_cached(&"a".to_string()));
		assert!(cache.is_cached(&"b".to_string()));
		assert!(!cache.is_cached(&"missing".to_string()));
	}

	#[test]
	fn dirty_write_shadows_committed_value_during_warm() {
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<String, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());
		cache.set(&mut store, &"a".to_string(), &cell(99)).unwrap();
		cache.warm(&mut store, &["a".to_string()]).unwrap();
		assert_eq!(
			cache.get(&mut store, &"a".to_string()).unwrap(),
			Some(cell(99)),
			"pending write must shadow store"
		);
	}

	#[test]
	fn take_returns_value_and_evicts_it_from_cache() {
		// take() is the load side of the window engines' load-mutate-persist
		// cycle: it must hand the caller an owned value AND remove it from the
		// clean cache, so that after the caller mutates its copy and persists
		// it, no stale cached copy survives to shadow the write.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<String, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &"a".to_string(), &cell(42)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());
		cache.warm(&mut store, &["a".to_string()]).unwrap();
		assert!(cache.is_cached(&"a".to_string()), "warm must populate the cache");

		let taken = cache.take(&mut store, &"a".to_string()).unwrap();
		assert_eq!(taken, Some(cell(42)), "take must return the stored value");
		assert!(!cache.is_cached(&"a".to_string()), "take must evict the entry from the cache");

		// The backing store is untouched by take, so a fresh get reloads it.
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(cell(42)));
	}

	#[test]
	fn take_of_absent_key_is_none() {
		let mut store = MockStore::default();
		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());
		assert_eq!(cache.take(&mut store, &"missing".to_string()).unwrap(), None);
	}

	#[test]
	fn take_then_persist_round_trips_a_mutation() {
		// The full engine cycle in miniature: take the current value out, mutate
		// it, persist via put, flush. The mutation must be the value that lands
		// in the store - proving take+put is a faithful replacement for the old
		// get(clone)+set(clone) pair.
		let mut store = MockStore::default();
		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());
		cache.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
		cache.flush(&mut store).unwrap();

		let mut value = cache.take(&mut store, &"a".to_string()).unwrap().unwrap_or_default();
		value.value += 40;
		cache.put(&mut store, &"a".to_string(), value).unwrap();
		cache.flush(&mut store).unwrap();

		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(cell(41)));
	}

	#[test]
	fn internal_backend_round_trips_through_internal_store() {
		let mut store = MockStore::default();
		let mut cache: StateCache<String, Cell> = StateCache::new_internal(big_pool());
		cache.set(&mut store, &"a".to_string(), &cell(5)).unwrap();
		cache.flush(&mut store).unwrap();
		assert!(store.data.is_empty(), "internal backend must not write to the data store");
		assert!(!store.internal.is_empty(), "internal backend must write to the internal store");
		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(cell(5)));
	}

	#[test]
	fn warm_inserts_archived_entries_without_decode_and_promotes_on_access() {
		// The zero-copy win the redesign exists for: warm must insert
		// Archived entries (exact byte charge, no materialization);
		// the first typed access promotes exactly once, switching the
		// charge from exact to approximate.
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<String, Cell> = StateCache::new(big_pool());
			seed.set(&mut store, &"a".to_string(), &cell(7)).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let pool = big_pool();
		let mut cache: StateCache<String, Cell> = StateCache::new(pool.clone());
		cache.warm(&mut store, &["a".to_string()]).unwrap();

		let stored = store.data.values().next().unwrap();
		let archived_bytes = archived_charge(stored);
		assert_eq!(pool.snapshot().resident.as_bytes(), archived_bytes, "warm charges the exact archived size");

		let value = cache.get(&mut store, &"a".to_string()).unwrap();
		assert_eq!(value, Some(cell(7)));
		let native_bytes = native_charge(&cell(7));
		assert_eq!(
			pool.snapshot().resident.as_bytes(),
			native_bytes,
			"promotion must transfer the charge from exact archived to approximate native"
		);
	}

	#[test]
	fn ledger_clean_plus_dirty_matches_pool_after_interleaved_ops() {
		// The ledger is the accounting the metrics pipeline reads; if
		// it ever drifts from the pool the bound is fiction. Interleave
		// every mutation kind and check the identity at each step.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<String, Cell> = StateCache::new(pool.clone());

		let check = |cache: &StateCache<String, Cell>, pool: &OperatorStateBudgetHandle| {
			let snapshot = pool.snapshot();
			assert_eq!(cache.ledger.clean, snapshot.resident.as_bytes());
			assert_eq!(cache.ledger.dirty, snapshot.dirty.as_bytes());
		};

		cache.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
		check(&cache, &pool);
		cache.set(&mut store, &"a".to_string(), &cell(2)).unwrap();
		check(&cache, &pool);
		cache.put(&mut store, &"b".to_string(), cell(3)).unwrap();
		check(&cache, &pool);
		cache.flush(&mut store).unwrap();
		check(&cache, &pool);
		assert_eq!(cache.ledger.dirty, 0);
		cache.remove(&mut store, &"a".to_string()).unwrap();
		check(&cache, &pool);
		cache.flush(&mut store).unwrap();
		check(&cache, &pool);
		cache.take(&mut store, &"b".to_string()).unwrap();
		check(&cache, &pool);
		cache.clear_cache();
		check(&cache, &pool);
		assert_eq!(pool.snapshot().total(), ByteSize::ZERO);
	}

	#[test]
	fn writing_same_key_twice_transfers_bytes_instead_of_adding() {
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<String, Cell> = StateCache::new(pool.clone());

		cache.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
		let once = pool.snapshot().dirty;
		cache.set(&mut store, &"a".to_string(), &cell(2)).unwrap();
		assert_eq!(pool.snapshot().dirty, once, "rewriting a dirty key must not double-charge");
	}

	#[test]
	fn dirty_entries_are_never_evicted_and_cap_violation_is_visible() {
		// Soft overage pinned: an all-dirty cache over budget must not
		// spin, must not error, must not lose a write, and must report
		// the overage instead of hiding it.
		let mut store = MockStore::default();
		let pool = pool_of(1);
		let mut cache: StateCache<String, Cell> = StateCache::new(pool.clone());

		for i in 0..8 {
			cache.put(&mut store, &format!("k{}", i), cell(i)).unwrap();
		}
		assert!(pool.over_budget());
		assert!(pool.snapshot().overage().as_bytes() > 0);
		assert_eq!(cache.dirty_memory().entries, Count::new(8));

		cache.flush(&mut store).unwrap();
		assert_eq!(store.data.len(), 8, "every dirty write must reach the store despite the overage");
		assert_eq!(cache.ledger.dirty, 0);
	}

	#[test]
	fn flush_makes_entries_clean_and_evictable_restoring_the_bound() {
		// After flush the entries are clean; the eviction pass at the
		// end of flush must bring the pool back under a tiny budget by
		// dropping clean entries (their bytes live in the store now).
		let mut store = MockStore::default();
		let pool = pool_of(1);
		let mut cache: StateCache<String, Cell> = StateCache::new(pool.clone());

		for i in 0..8 {
			cache.put(&mut store, &format!("k{}", i), cell(i)).unwrap();
		}
		cache.flush(&mut store).unwrap();
		assert!(!pool.over_budget(), "flush + eviction must restore the bound once nothing is pinned");
		assert!(pool.snapshot().resident.as_bytes() <= 1);

		// Nothing was lost: evicted entries reload from the store.
		for i in 0..8 {
			assert_eq!(cache.get(&mut store, &format!("k{}", i)).unwrap(), Some(cell(i)));
		}
	}

	#[test]
	fn eviction_issues_no_storage_operation() {
		// Eviction is memory-only: no drop, no remove may reach the
		// store, and the evicted key must reload to its original value.
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut cache: StateCache<String, Cell> = StateCache::new(pool.clone());
		cache.set(&mut store, &"a".to_string(), &cell(9)).unwrap();
		cache.flush(&mut store).unwrap();

		pool.set_budget(ByteSize::from_bytes(1));
		cache.put(&mut store, &"b".to_string(), cell(1)).unwrap();
		assert!(!cache.is_cached(&"a".to_string()), "the clean entry must be evicted under pressure");
		assert_eq!(store.drops, 0, "eviction must not drop stored state");
		assert_eq!(store.removes, 0, "eviction must not remove stored state");
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(cell(9)));
	}

	#[test]
	fn removed_key_shadows_store_and_flush_routes_to_drop() {
		let mut store = MockStore::default();
		let mut cache: StateCache<String, Cell> = StateCache::new(big_pool());
		cache.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
		cache.flush(&mut store).unwrap();

		cache.remove(&mut store, &"a".to_string()).unwrap();
		assert_eq!(
			cache.get(&mut store, &"a".to_string()).unwrap(),
			None,
			"a pending remove must shadow the stored value"
		);
		cache.flush(&mut store).unwrap();
		assert_eq!(store.drops, 1, "flush must route a removed entry to state_drop, not state_remove");
		assert_eq!(store.removes, 0);
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), None);
	}

	#[test]
	fn two_caches_share_one_pool_and_the_toucher_evicts_its_own_tail() {
		let mut store = MockStore::default();
		let pool = big_pool();
		let mut a: StateCache<String, Cell> = StateCache::new(pool.clone());
		let mut b: StateCache<String, Cell> = StateCache::new_internal(pool.clone());

		a.set(&mut store, &"a".to_string(), &cell(1)).unwrap();
		a.flush(&mut store).unwrap();
		b.set(&mut store, &"b".to_string(), &cell(2)).unwrap();
		b.flush(&mut store).unwrap();

		pool.set_budget(ByteSize::from_bytes(1));
		b.put(&mut store, &"c".to_string(), cell(3)).unwrap();
		assert!(!b.is_cached(&"b".to_string()), "the touching cache must evict its own clean tail");
		assert!(
			a.is_cached(&"a".to_string()),
			"an idle cache keeps its entries under another cache's pressure"
		);
	}
}
