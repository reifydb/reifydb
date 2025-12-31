// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, hash::Hash};

pub struct LruCache<K, V> {
	map: HashMap<K, usize>,
	entries: Vec<Entry<K, V>>,
	head: Option<usize>,
	tail: Option<usize>,
	capacity: usize,
}

struct Entry<K, V> {
	key: K,
	value: V,
	prev: Option<usize>,
	next: Option<usize>,
}

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			map: HashMap::with_capacity(capacity),
			entries: Vec::with_capacity(capacity),
			head: None,
			tail: None,
			capacity,
		}
	}

	pub fn get(&mut self, key: &K) -> Option<&V> {
		if let Some(&idx) = self.map.get(key) {
			self.move_to_front(idx);
			Some(&self.entries[idx].value)
		} else {
			None
		}
	}

	pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
		if let Some(&idx) = self.map.get(key) {
			self.move_to_front(idx);
			Some(&mut self.entries[idx].value)
		} else {
			None
		}
	}

	pub fn put(&mut self, key: K, value: V) -> Option<V> {
		if let Some(&idx) = self.map.get(&key) {
			let old_value = std::mem::replace(&mut self.entries[idx].value, value);
			self.move_to_front(idx);
			return Some(old_value);
		}

		let evicted = if self.entries.len() >= self.capacity {
			self.evict_lru()
		} else {
			None
		};

		let idx = self.entries.len();
		self.entries.push(Entry {
			key: key.clone(),
			value,
			prev: None,
			next: self.head,
		});

		if let Some(old_head) = self.head {
			self.entries[old_head].prev = Some(idx);
		}

		self.head = Some(idx);

		if self.tail.is_none() {
			self.tail = Some(idx);
		}

		self.map.insert(key, idx);
		evicted
	}

	pub fn remove(&mut self, key: &K) -> Option<V> {
		if let Some(idx) = self.map.remove(key) {
			self.unlink(idx);
			let entry = self.entries.swap_remove(idx);

			if idx < self.entries.len() {
				let moved_key = self.entries[idx].key.clone();
				self.map.insert(moved_key, idx);

				if let Some(prev) = self.entries[idx].prev {
					self.entries[prev].next = Some(idx);
				} else {
					self.head = Some(idx);
				}

				if let Some(next) = self.entries[idx].next {
					self.entries[next].prev = Some(idx);
				} else {
					self.tail = Some(idx);
				}
			}

			Some(entry.value)
		} else {
			None
		}
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.map.contains_key(key)
	}

	pub fn clear(&mut self) {
		self.map.clear();
		self.entries.clear();
		self.head = None;
		self.tail = None;
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}

	pub fn iter(&self) -> LruIter<'_, K, V> {
		LruIter {
			entries: &self.entries,
			current: self.head,
		}
	}

	fn move_to_front(&mut self, idx: usize) {
		if self.head == Some(idx) {
			return;
		}

		self.unlink(idx);

		self.entries[idx].prev = None;
		self.entries[idx].next = self.head;

		if let Some(old_head) = self.head {
			self.entries[old_head].prev = Some(idx);
		}

		self.head = Some(idx);

		if self.tail.is_none() {
			self.tail = Some(idx);
		}
	}

	fn unlink(&mut self, idx: usize) {
		let entry = &self.entries[idx];
		let prev = entry.prev;
		let next = entry.next;

		if let Some(p) = prev {
			self.entries[p].next = next;
		} else {
			self.head = next;
		}

		if let Some(n) = next {
			self.entries[n].prev = prev;
		} else {
			self.tail = prev;
		}
	}

	fn evict_lru(&mut self) -> Option<V> {
		if let Some(tail_idx) = self.tail {
			let key = self.entries[tail_idx].key.clone();
			self.remove(&key)
		} else {
			None
		}
	}
}

pub struct LruIter<'a, K, V> {
	entries: &'a [Entry<K, V>],
	current: Option<usize>,
}

impl<'a, K, V> Iterator for LruIter<'a, K, V> {
	type Item = (&'a K, &'a V);

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(idx) = self.current {
			let entry = &self.entries[idx];
			self.current = entry.next;
			Some((&entry.key, &entry.value))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_basic_operations() {
		let mut cache = LruCache::new(2);

		assert_eq!(cache.put(1, "a"), None);
		assert_eq!(cache.put(2, "b"), None);
		assert_eq!(cache.get(&1), Some(&"a"));
		assert_eq!(cache.get(&2), Some(&"b"));
		assert_eq!(cache.len(), 2);
	}

	#[test]
	fn test_eviction() {
		let mut cache = LruCache::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		let evicted = cache.put(3, "c");

		assert_eq!(evicted, Some("a"));
		assert_eq!(cache.get(&1), None);
		assert_eq!(cache.get(&2), Some(&"b"));
		assert_eq!(cache.get(&3), Some(&"c"));
	}

	#[test]
	fn test_lru_order() {
		let mut cache = LruCache::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.get(&1);
		cache.put(3, "c");

		assert_eq!(cache.get(&1), Some(&"a"));
		assert_eq!(cache.get(&2), None);
		assert_eq!(cache.get(&3), Some(&"c"));
	}

	#[test]
	fn test_update_existing() {
		let mut cache = LruCache::new(2);

		cache.put(1, "a");
		let old = cache.put(1, "b");

		assert_eq!(old, Some("a"));
		assert_eq!(cache.get(&1), Some(&"b"));
		assert_eq!(cache.len(), 1);
	}

	#[test]
	fn test_remove() {
		let mut cache = LruCache::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		let removed = cache.remove(&1);

		assert_eq!(removed, Some("a"));
		assert_eq!(cache.get(&1), None);
		assert_eq!(cache.len(), 1);
	}

	#[test]
	fn test_clear() {
		let mut cache = LruCache::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.clear();

		assert_eq!(cache.len(), 0);
		assert!(cache.is_empty());
	}

	#[test]
	fn test_contains_key() {
		let mut cache = LruCache::new(2);

		cache.put(1, "a");
		assert!(cache.contains_key(&1));
		assert!(!cache.contains_key(&2));
	}

	#[test]
	fn test_iter() {
		let mut cache = LruCache::new(3);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.put(3, "c");
		cache.get(&1);

		let items: Vec<_> = cache.iter().collect();
		assert_eq!(items[0], (&1, &"a"));
		assert_eq!(items[1], (&3, &"c"));
		assert_eq!(items[2], (&2, &"b"));
	}
}
