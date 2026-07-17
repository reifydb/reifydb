// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::Hash,
	mem::size_of,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use moka::{
	policy::EvictionPolicy,
	sync::{Cache, CacheBuilder},
};
use reifydb_value::{byte_size::ByteSize, count::Count};
use xxhash_rust::xxh3::Xxh3Builder;

use crate::cache::sync::{CacheMemory, FootprintFn};

pub const MOKA_LRU_ENTRY_OVERHEAD: usize = 215;

struct Metrics<K, V> {
	footprint: FootprintFn<K, V>,
	entries: AtomicU64,
	heap: AtomicU64,
	payload: AtomicU64,
}

pub struct NativeLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	cache: Cache<K, V, Xxh3Builder>,
	capacity: usize,
	metrics: Option<Arc<Metrics<K, V>>>,
}

impl<K, V> NativeLru<K, V>
where
	K: Hash + Eq + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	pub fn new(capacity: usize) -> Self {
		let cache = CacheBuilder::new(capacity as u64)
			.eviction_policy(EvictionPolicy::lru())
			.build_with_hasher(Xxh3Builder::new());
		Self {
			cache,
			capacity,
			metrics: None,
		}
	}

	pub fn measured(capacity: usize, footprint: FootprintFn<K, V>) -> Self {
		let metrics = Arc::new(Metrics {
			footprint,
			entries: AtomicU64::new(0),
			heap: AtomicU64::new(0),
			payload: AtomicU64::new(0),
		});
		let listener_metrics = Arc::clone(&metrics);
		let cache = CacheBuilder::new(capacity as u64)
			.eviction_policy(EvictionPolicy::lru())
			.eviction_listener(move |key: Arc<K>, value: V, _cause| {
				let footprint = (listener_metrics.footprint)(&key, &value);
				listener_metrics.entries.fetch_sub(1, Ordering::Relaxed);
				listener_metrics.heap.fetch_sub(footprint.heap as u64, Ordering::Relaxed);
				listener_metrics.payload.fetch_sub(footprint.payload as u64, Ordering::Relaxed);
			})
			.build_with_hasher(Xxh3Builder::new());
		Self {
			cache,
			capacity,
			metrics: Some(metrics),
		}
	}

	pub fn get(&self, key: &K) -> Option<V> {
		self.cache.get(key)
	}

	pub fn put(&self, key: K, value: V) -> Option<V> {
		let old = self.cache.get(&key);
		if let Some(metrics) = &self.metrics {
			let footprint = (metrics.footprint)(&key, &value);
			metrics.entries.fetch_add(1, Ordering::Relaxed);
			metrics.heap.fetch_add(footprint.heap as u64, Ordering::Relaxed);
			metrics.payload.fetch_add(footprint.payload as u64, Ordering::Relaxed);
		}
		self.cache.insert(key, value);
		old
	}

	pub fn remove(&self, key: &K) -> Option<V> {
		let old = self.cache.get(key);
		self.cache.invalidate(key);
		old
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.cache.contains_key(key)
	}

	pub fn clear(&self) {
		self.cache.invalidate_all();
	}

	pub fn len(&self) -> usize {
		self.cache.entry_count() as usize
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}

	pub fn run_pending_tasks(&self) {
		self.cache.run_pending_tasks();
	}

	pub fn memory_usage(&self) -> Option<CacheMemory> {
		let metrics = self.metrics.as_ref()?;
		let entries = metrics.entries.load(Ordering::Relaxed);
		let per_entry = (MOKA_LRU_ENTRY_OVERHEAD + size_of::<K>() + size_of::<V>()) as u64;
		let resident = metrics.heap.load(Ordering::Relaxed) + entries * per_entry;
		Some(CacheMemory {
			entries: Count::new(entries),
			resident: ByteSize::from_bytes(resident),
			payload: ByteSize::from_bytes(metrics.payload.load(Ordering::Relaxed)),
		})
	}
}
