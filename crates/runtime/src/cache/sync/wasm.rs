// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, mem, sync::Mutex};

use reifydb_value::{byte_size::ByteSize, count::Count};

use crate::cache::sync::{CacheMemory, FootprintFn};

pub struct WasmLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	inner: Mutex<Inner<K, V>>,
	capacity: usize,
	footprint: Option<FootprintFn<K, V>>,
}

struct Inner<K, V> {
	map: HashMap<K, Entry<V>>,
	access_counter: u64,
	heap: u64,
	payload: u64,
}

struct Entry<V> {
	value: V,
	last_access: u64,
}

impl<K, V> WasmLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	pub fn new(capacity: usize) -> Self {
		Self::with_footprint(capacity, None)
	}

	pub fn measured(capacity: usize, footprint: FootprintFn<K, V>) -> Self {
		Self::with_footprint(capacity, Some(footprint))
	}

	fn with_footprint(capacity: usize, footprint: Option<FootprintFn<K, V>>) -> Self {
		Self {
			inner: Mutex::new(Inner {
				map: HashMap::with_capacity(capacity),
				access_counter: 0,
				heap: 0,
				payload: 0,
			}),
			capacity,
			footprint,
		}
	}

	pub fn get(&self, key: &K) -> Option<V> {
		let mut inner = self.inner.lock().unwrap();
		let access = inner.access_counter;

		let value = inner.map.get_mut(key).map(|entry| {
			entry.last_access = access;
			entry.value.clone()
		});

		if value.is_some() {
			inner.access_counter += 1;
		}
		value
	}

	pub fn put(&self, key: K, value: V) -> Option<V> {
		let mut inner = self.inner.lock().unwrap();
		let access = inner.access_counter;
		inner.access_counter += 1;

		if let Some(footprint) = self.footprint {
			let added = footprint(&key, &value);
			inner.heap += added.heap as u64;
			inner.payload += added.payload as u64;
		}

		if let Some(entry) = inner.map.get_mut(&key) {
			let old_value = mem::replace(&mut entry.value, value);
			entry.last_access = access;
			if let Some(footprint) = self.footprint {
				let removed = footprint(&key, &old_value);
				inner.heap -= removed.heap as u64;
				inner.payload -= removed.payload as u64;
			}
			return Some(old_value);
		}

		if inner.map.len() >= self.capacity {
			inner.evict_lru(self.footprint);
		}

		inner.map.insert(
			key,
			Entry {
				value,
				last_access: access,
			},
		);
		None
	}

	pub fn remove(&self, key: &K) -> Option<V> {
		let mut inner = self.inner.lock().unwrap();
		let removed = inner.map.remove_entry(key);
		if let (Some(footprint), Some((key, entry))) = (self.footprint, &removed) {
			let footprint = footprint(key, &entry.value);
			inner.heap -= footprint.heap as u64;
			inner.payload -= footprint.payload as u64;
		}
		removed.map(|(_, entry)| entry.value)
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.inner.lock().unwrap().map.contains_key(key)
	}

	pub fn clear(&self) {
		let mut inner = self.inner.lock().unwrap();
		inner.map.clear();
		inner.heap = 0;
		inner.payload = 0;
	}

	pub fn len(&self) -> usize {
		self.inner.lock().unwrap().map.len()
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}

	pub fn run_pending_tasks(&self) {}

	pub fn memory_usage(&self) -> Option<CacheMemory> {
		self.footprint?;
		let inner = self.inner.lock().unwrap();
		let per_entry = (mem::size_of::<K>() + mem::size_of::<Entry<V>>() + 1) as u64;
		let table = inner.map.capacity() as u64 * per_entry;
		Some(CacheMemory {
			entries: Count::new(inner.map.len() as u64),
			resident: ByteSize::from_bytes(table + inner.heap),
			payload: ByteSize::from_bytes(inner.payload),
		})
	}
}

impl<K, V> Inner<K, V>
where
	K: Hash + Eq + Clone,
{
	fn evict_lru(&mut self, footprint: Option<FootprintFn<K, V>>) {
		let mut oldest_key: Option<&K> = None;
		let mut oldest_access = u64::MAX;

		for (key, entry) in self.map.iter() {
			if entry.last_access < oldest_access {
				oldest_access = entry.last_access;
				oldest_key = Some(key);
			}
		}

		if let Some(key) = oldest_key.cloned() {
			let removed = self.map.remove_entry(&key);
			if let (Some(footprint), Some((key, entry))) = (footprint, &removed) {
				let footprint = footprint(key, &entry.value);
				self.heap -= footprint.heap as u64;
				self.payload -= footprint.payload as u64;
			}
		}
	}
}
