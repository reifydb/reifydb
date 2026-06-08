// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, mem, sync::Arc};

use reifydb_value::Result;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	util::lru::slab::SlabLru,
	window::store::WindowStore,
};

#[derive(Clone, Copy, Debug)]
pub enum StateBackend {
	Data,

	Internal,
}

pub struct StateCache<K, V> {
	cache: SlabLru<K, Arc<V>>,
	dirty: HashMap<K, Option<Arc<V>>>,
	backend: StateBackend,
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Serialize + DeserializeOwned,
{
	pub fn new(capacity: usize) -> Self {
		Self::with_backend(capacity, StateBackend::Data)
	}

	pub fn new_internal(capacity: usize) -> Self {
		Self::with_backend(capacity, StateBackend::Internal)
	}

	fn with_backend(capacity: usize, backend: StateBackend) -> Self {
		Self {
			cache: SlabLru::new(capacity),
			dirty: HashMap::new(),
			backend,
		}
	}

	pub fn get_arc(&mut self, store: &mut impl WindowStore, key: &K) -> Result<Option<Arc<V>>> {
		if let Some(cached) = self.cache.get(key) {
			return Ok(Some(cached));
		}

		if let Some(slot) = self.dirty.get(key) {
			return Ok(slot.clone());
		}

		let encoded_key = key.into_encoded_key();
		let loaded = match self.backend {
			StateBackend::Data => store.state_get::<V>(&encoded_key)?,
			StateBackend::Internal => store.internal_get::<V>(&encoded_key)?,
		};
		match loaded {
			Some(value) => {
				let arc = Arc::new(value);
				self.cache.put(key.clone(), arc.clone());
				Ok(Some(arc))
			}
			None => Ok(None),
		}
	}

	pub fn get(&mut self, store: &mut impl WindowStore, key: &K) -> Result<Option<V>> {
		Ok(self.get_arc(store, key)?.map(|arc| (*arc).clone()))
	}

	pub fn warm(&mut self, store: &mut impl WindowStore, keys: &[K]) -> Result<()> {
		let mut to_load: Vec<K> = Vec::new();
		for key in keys {
			if self.cache.contains_key(key) || self.dirty.contains_key(key) {
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

		let cache = &mut self.cache;
		let mut visit = |encoded: EncodedKey, value: V| -> Result<()> {
			if let Some(key) = by_encoded.get(encoded.as_bytes()) {
				cache.put(key.clone(), Arc::new(value));
			}
			Ok(())
		};
		match self.backend {
			StateBackend::Data => store.state_get_many_visit::<V>(&encoded_keys, &mut visit)?,
			StateBackend::Internal => store.internal_get_many_visit::<V>(&encoded_keys, &mut visit)?,
		}
		Ok(())
	}

	pub fn set(&mut self, _store: &mut impl WindowStore, key: &K, value: &V) -> Result<()> {
		let arc = Arc::new(value.clone());
		self.cache.put(key.clone(), arc.clone());
		self.dirty.insert(key.clone(), Some(arc));
		Ok(())
	}

	pub fn put(&mut self, _store: &mut impl WindowStore, key: &K, value: V) -> Result<()> {
		let arc = Arc::new(value);
		self.cache.put(key.clone(), arc.clone());
		self.dirty.insert(key.clone(), Some(arc));
		Ok(())
	}

	pub fn put_arc(&mut self, _store: &mut impl WindowStore, key: &K, value: Arc<V>) -> Result<()> {
		self.cache.put(key.clone(), value.clone());
		self.dirty.insert(key.clone(), Some(value));
		Ok(())
	}

	pub fn modify<F>(&mut self, store: &mut impl WindowStore, key: &K, f: F) -> Result<()>
	where
		F: FnOnce(&mut V) -> Result<()>,
		V: Default,
	{
		let mut arc = self.get_arc(store, key)?.unwrap_or_else(|| Arc::new(V::default()));
		f(Arc::make_mut(&mut arc))?;
		self.put_arc(store, key, arc)
	}

	pub fn remove(&mut self, _store: &mut impl WindowStore, key: &K) -> Result<()> {
		self.cache.remove(key);
		self.dirty.insert(key.clone(), None);
		Ok(())
	}

	pub fn flush(&mut self, store: &mut impl WindowStore) -> Result<()> {
		let dirty = mem::take(&mut self.dirty);
		for (key, slot) in dirty {
			let encoded_key = (&key).into_encoded_key();
			match (slot, self.backend) {
				(Some(value), StateBackend::Data) => store.state_set(&encoded_key, value.as_ref())?,
				(Some(value), StateBackend::Internal) => {
					store.internal_set(&encoded_key, value.as_ref())?
				}
				(None, StateBackend::Data) => store.state_remove(&encoded_key)?,
				(None, StateBackend::Internal) => store.internal_remove(&encoded_key)?,
			}
		}
		Ok(())
	}

	pub fn clear_cache(&mut self) {
		self.cache.clear();
	}

	pub fn invalidate(&mut self, key: &K) {
		self.cache.remove(key);
	}

	pub fn is_cached(&self, key: &K) -> bool {
		self.cache.contains_key(key)
	}

	pub fn len(&self) -> usize {
		self.cache.len()
	}

	pub fn is_empty(&self) -> bool {
		self.cache.is_empty()
	}

	pub fn capacity(&self) -> usize {
		self.cache.capacity()
	}
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Default + Serialize + DeserializeOwned,
{
	pub fn get_or_default(&mut self, store: &mut impl WindowStore, key: &K) -> Result<V> {
		match self.get(store, key)? {
			Some(value) => Ok(value),
			None => Ok(V::default()),
		}
	}

	pub fn update<U>(&mut self, store: &mut impl WindowStore, key: &K, updater: U) -> Result<V>
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

	use postcard::{from_bytes, to_allocvec};
	use reifydb_value::value::row_number::RowNumber;

	use super::*;
	use crate::encoded::key::EncodedKeyRange;

	#[derive(Default)]
	struct MockStore {
		data: HashMap<Vec<u8>, Vec<u8>>,
		internal: HashMap<Vec<u8>, Vec<u8>>,
	}

	impl WindowStore for MockStore {
		fn state_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
			Ok(self.data.get(key.as_bytes()).map(|b| from_bytes(b).expect("decode")))
		}
		fn state_get_many_visit<V: DeserializeOwned>(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_bytes()) {
					visit(key.clone(), from_bytes(b).expect("decode"))?;
				}
			}
			Ok(())
		}
		fn state_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
			self.data.insert(key.as_bytes().to_vec(), to_allocvec(value).expect("encode"));
			Ok(())
		}
		fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.data.remove(key.as_bytes());
			Ok(())
		}
		fn internal_get<V: DeserializeOwned>(&mut self, key: &EncodedKey) -> Result<Option<V>> {
			Ok(self.internal.get(key.as_bytes()).map(|b| from_bytes(b).expect("decode")))
		}
		fn internal_get_many_visit<V: DeserializeOwned>(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.internal.get(key.as_bytes()) {
					visit(key.clone(), from_bytes(b).expect("decode"))?;
				}
			}
			Ok(())
		}
		fn internal_set<V: Serialize>(&mut self, key: &EncodedKey, value: &V) -> Result<()> {
			self.internal.insert(key.as_bytes().to_vec(), to_allocvec(value).expect("encode"));
			Ok(())
		}
		fn internal_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.internal.remove(key.as_bytes());
			Ok(())
		}
		fn internal_range_visit<V: DeserializeOwned>(
			&mut self,
			range: EncodedKeyRange,
			visit: &mut dyn FnMut(EncodedKey, V) -> Result<()>,
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
			let mut matched: Vec<(Vec<u8>, Vec<u8>)> = self
				.internal
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			for (k, b) in matched {
				visit(EncodedKey::new(k), from_bytes(&b).expect("decode"))?;
			}
			Ok(())
		}
		fn get_or_create_row_number(&mut self, _key: &EncodedKey) -> Result<(RowNumber, bool)> {
			Ok((RowNumber(1), true))
		}
		fn get_or_create_row_numbers(&mut self, keys: &[EncodedKey]) -> Result<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
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
		let mut cache: StateCache<String, i32> = StateCache::new(100);

		// A set is buffered (dirty) and visible via get before flush.
		cache.set(&mut store, &"a".to_string(), &7).unwrap();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(7));
		// Nothing reached the backing store yet.
		assert!(store.data.is_empty());

		cache.flush(&mut store).unwrap();
		assert!(!store.data.is_empty(), "flush must write dirty entries to the store");

		// After dropping the in-memory cache, the value must load from the store.
		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(7));
	}

	#[test]
	fn warm_bulk_loads_present_keys_and_skips_absent() {
		let mut store = MockStore::default();
		{
			let mut seed: StateCache<String, i32> = StateCache::new(100);
			seed.set(&mut store, &"a".to_string(), &1).unwrap();
			seed.set(&mut store, &"b".to_string(), &2).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<String, i32> = StateCache::new(100);
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
			let mut seed: StateCache<String, i32> = StateCache::new(100);
			seed.set(&mut store, &"a".to_string(), &1).unwrap();
			seed.flush(&mut store).unwrap();
		}

		let mut cache: StateCache<String, i32> = StateCache::new(100);
		cache.set(&mut store, &"a".to_string(), &99).unwrap();
		cache.warm(&mut store, &["a".to_string()]).unwrap();
		assert_eq!(
			cache.get(&mut store, &"a".to_string()).unwrap(),
			Some(99),
			"pending write must shadow store"
		);
	}

	#[test]
	fn internal_backend_round_trips_through_internal_store() {
		let mut store = MockStore::default();
		let mut cache: StateCache<String, i32> = StateCache::new_internal(100);
		cache.set(&mut store, &"a".to_string(), &5).unwrap();
		cache.flush(&mut store).unwrap();
		assert!(store.data.is_empty(), "internal backend must not write to the data store");
		assert!(!store.internal.is_empty(), "internal backend must write to the internal store");
		cache.clear_cache();
		assert_eq!(cache.get(&mut store, &"a".to_string()).unwrap(), Some(5));
	}
}
