// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::Hash;

use cfg_if::cfg_if;

#[cfg(not(reifydb_single_threaded))]
pub(crate) mod native;
#[cfg(reifydb_single_threaded)]
pub(crate) mod wasm;

cfg_if! {
	if #[cfg(not(reifydb_single_threaded))] {
		type LruImpl<K, V> = native::NativeLru<K, V>;
	} else {
		type LruImpl<K, V> = wasm::WasmLru<K, V>;
	}
}

pub struct SyncLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	inner: LruImpl<K, V>,
}

impl<K, V> SyncLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			inner: LruImpl::new(capacity),
		}
	}

	pub fn get(&self, key: &K) -> Option<V> {
		self.inner.get(key)
	}

	pub fn put(&self, key: K, value: V) -> Option<V> {
		self.inner.put(key, value)
	}

	pub fn remove(&self, key: &K) -> Option<V> {
		self.inner.remove(key)
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.inner.contains_key(key)
	}

	pub fn clear(&self) {
		self.inner.clear();
	}

	pub fn len(&self) -> usize {
		self.inner.len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	pub fn capacity(&self) -> usize {
		self.inner.capacity()
	}

	pub fn run_pending_tasks(&self) {
		self.inner.run_pending_tasks();
	}
}

#[cfg(test)]
mod tests {
	use super::SyncLru;

	#[test]
	fn test_basic_operations() {
		let cache = SyncLru::new(2);

		assert_eq!(cache.put(1, "a"), None);
		assert_eq!(cache.put(2, "b"), None);
		assert_eq!(cache.get(&1), Some("a"));
		assert_eq!(cache.get(&2), Some("b"));
		cache.run_pending_tasks();
		assert_eq!(cache.len(), 2);
	}

	#[test]
	fn test_eviction() {
		let cache = SyncLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		let evicted = cache.put(3, "c");
		cache.run_pending_tasks();

		assert_eq!(evicted, None);
		assert_eq!(cache.get(&1), None);
		assert_eq!(cache.get(&2), Some("b"));
		assert_eq!(cache.get(&3), Some("c"));
	}

	#[test]
	fn test_lru_order() {
		let cache = SyncLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.run_pending_tasks();
		cache.get(&1); // Access 1, making it more recent than 2
		cache.run_pending_tasks();
		cache.put(3, "c"); // Should evict 2 (least recently used)
		cache.run_pending_tasks();

		assert_eq!(cache.get(&1), Some("a"));
		assert_eq!(cache.get(&2), None);
		assert_eq!(cache.get(&3), Some("c"));
	}

	#[test]
	fn test_update_existing() {
		let cache = SyncLru::new(2);

		cache.put(1, "a");
		let old = cache.put(1, "b");

		assert_eq!(old, Some("a"));
		assert_eq!(cache.get(&1), Some("b"));
		cache.run_pending_tasks();
		assert_eq!(cache.len(), 1);
	}

	#[test]
	fn test_remove() {
		let cache = SyncLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		let removed = cache.remove(&1);

		assert_eq!(removed, Some("a"));
		assert_eq!(cache.get(&1), None);
		cache.run_pending_tasks();
		assert_eq!(cache.len(), 1);
	}

	#[test]
	fn test_clear() {
		let cache = SyncLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.clear();
		cache.run_pending_tasks();

		assert_eq!(cache.len(), 0);
		assert!(cache.is_empty());
	}

	#[test]
	fn test_contains_key() {
		let cache = SyncLru::new(2);

		cache.put(1, "a");
		assert!(cache.contains_key(&1));
		assert!(!cache.contains_key(&2));
	}
}
