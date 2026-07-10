// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, mem, sync::Mutex};

pub struct WasmLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	inner: Mutex<Inner<K, V>>,
	capacity: usize,
}

struct Inner<K, V> {
	map: HashMap<K, Entry<V>>,
	access_counter: u64,
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
		Self {
			inner: Mutex::new(Inner {
				map: HashMap::with_capacity(capacity),
				access_counter: 0,
			}),
			capacity,
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

		if let Some(entry) = inner.map.get_mut(&key) {
			let old_value = mem::replace(&mut entry.value, value);
			entry.last_access = access;
			return Some(old_value);
		}

		if inner.map.len() >= self.capacity {
			inner.evict_lru();
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
		self.inner.lock().unwrap().map.remove(key).map(|entry| entry.value)
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.inner.lock().unwrap().map.contains_key(key)
	}

	pub fn clear(&self) {
		self.inner.lock().unwrap().map.clear();
	}

	pub fn len(&self) -> usize {
		self.inner.lock().unwrap().map.len()
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}
}

impl<K, V> Inner<K, V>
where
	K: Hash + Eq + Clone,
{
	fn evict_lru(&mut self) {
		let mut oldest_key: Option<&K> = None;
		let mut oldest_access = u64::MAX;

		for (key, entry) in self.map.iter() {
			if entry.last_access < oldest_access {
				oldest_access = entry.last_access;
				oldest_key = Some(key);
			}
		}

		if let Some(key) = oldest_key.cloned() {
			self.map.remove(&key);
		}
	}
}
