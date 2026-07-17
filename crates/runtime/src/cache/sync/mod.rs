// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::Hash;

use cfg_if::cfg_if;
use reifydb_value::{byte_size::ByteSize, count::Count};

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheFootprint {
	pub heap: usize,
	pub payload: usize,
}

pub type FootprintFn<K, V> = fn(&K, &V) -> CacheFootprint;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheMemory {
	pub entries: Count,
	pub resident: ByteSize,
	pub payload: ByteSize,
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

	pub fn measured(capacity: usize, footprint: FootprintFn<K, V>) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			inner: LruImpl::measured(capacity, footprint),
		}
	}

	pub fn memory_usage(&self) -> Option<CacheMemory> {
		self.inner.memory_usage()
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

#[cfg(all(test, not(reifydb_single_threaded)))]
mod tests {
	use std::{mem::size_of, sync::Arc};

	use reifydb_value::{byte_size::ByteSize, count::Count};

	use super::{CacheFootprint, SyncLru};

	// The footprint fn used by measured caches in these tests: heap = the
	// String's buffer, payload = key bytes + string bytes. Mirrors how a
	// real consumer derives it from HeapSize.
	fn footprint(_key: &u64, value: &String) -> CacheFootprint {
		CacheFootprint {
			heap: value.capacity(),
			payload: size_of::<u64>() + value.len(),
		}
	}

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

	#[test]
	fn unmeasured_cache_reports_no_memory_usage() {
		let cache: SyncLru<u64, String> = SyncLru::new(2);
		cache.put(1, "a".to_string());
		assert_eq!(cache.memory_usage(), None);
	}

	#[test]
	fn measured_cache_counts_entries_heap_and_payload() {
		let cache: SyncLru<u64, String> = SyncLru::measured(8, footprint);
		let a = String::with_capacity(16) + "aaaa";
		let b = String::with_capacity(32) + "bbbbbbbb";
		let heap = a.capacity() + b.capacity();
		let payload = (8 + a.len()) + (8 + b.len());

		cache.put(1, a);
		cache.put(2, b);
		cache.run_pending_tasks();

		let usage = cache.memory_usage().expect("measured cache must report usage");
		assert_eq!(usage.entries, Count::new(2));
		assert_eq!(usage.payload, ByteSize::from_bytes(payload as u64));
		// Resident must cover the tracked heap plus a nonzero per-entry
		// structural overhead; equality with heap alone would mean the
		// cache's own bookkeeping is unaccounted.
		assert!(usage.resident.as_bytes() > heap as u64);
	}

	#[test]
	fn replacing_a_key_keeps_single_entry_accounting() {
		let cache: SyncLru<u64, String> = SyncLru::measured(8, footprint);
		cache.put(1, "aaaa".to_string());
		cache.put(1, "bbbbbbbb".to_string());
		cache.run_pending_tasks();

		let usage = cache.memory_usage().expect("measured cache must report usage");
		assert_eq!(usage.entries, Count::new(1), "replacement must not leak the old entry's count");
		assert_eq!(
			usage.payload,
			ByteSize::from_bytes(8 + 8),
			"payload must reflect only the replacement value"
		);
	}

	#[test]
	fn removal_and_clear_release_accounted_memory() {
		let cache: SyncLru<u64, String> = SyncLru::measured(8, footprint);
		cache.put(1, "aaaa".to_string());
		cache.put(2, "bbbb".to_string());
		cache.remove(&1);
		cache.run_pending_tasks();

		let usage = cache.memory_usage().expect("measured cache must report usage");
		assert_eq!(usage.entries, Count::new(1));
		assert_eq!(usage.payload, ByteSize::from_bytes(8 + 4));

		cache.clear();
		cache.run_pending_tasks();

		let usage = cache.memory_usage().expect("measured cache must report usage");
		assert_eq!(usage.entries, Count::ZERO, "clear must release every accounted entry");
		assert_eq!(usage.payload, ByteSize::ZERO);
		assert_eq!(usage.resident, ByteSize::ZERO);
	}

	#[test]
	fn eviction_at_capacity_releases_the_victims_memory() {
		let cache: SyncLru<u64, String> = SyncLru::measured(2, footprint);
		cache.put(1, "aaaa".to_string());
		cache.put(2, "bbbb".to_string());
		cache.run_pending_tasks();
		cache.put(3, "cccc".to_string());
		cache.run_pending_tasks();

		let usage = cache.memory_usage().expect("measured cache must report usage");
		assert_eq!(usage.entries, Count::new(2), "eviction must decrement the entry count");
		assert_eq!(usage.payload, ByteSize::from_bytes(2 * (8 + 4)));
	}

	#[test]
	fn measured_values_shared_via_arc_count_their_heap_once_per_slot() {
		fn arc_footprint(_key: &u64, value: &Arc<str>) -> CacheFootprint {
			CacheFootprint {
				heap: 2 * size_of::<usize>() + value.len(),
				payload: size_of::<u64>() + value.len(),
			}
		}
		let cache: SyncLru<u64, Arc<str>> = SyncLru::measured(8, arc_footprint);
		let shared: Arc<str> = Arc::from("shared-value");
		cache.put(1, shared.clone());
		cache.put(2, shared);
		cache.run_pending_tasks();

		let usage = cache.memory_usage().expect("measured cache must report usage");
		assert_eq!(usage.entries, Count::new(2));
		assert_eq!(usage.payload, ByteSize::from_bytes(2 * (8 + 12)));
	}
}
