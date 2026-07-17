// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, mem};

struct SlabNode<K, V> {
	key: K,
	value: V,
	prev: Option<usize>,
	next: Option<usize>,
}

pub struct SlabLru<K, V> {
	map: HashMap<K, usize>,
	nodes: Vec<Option<SlabNode<K, V>>>,
	free: Vec<usize>,
	head: Option<usize>,
	tail: Option<usize>,
	capacity: usize,
}

impl<K: Hash + Eq + Clone, V: Clone> SlabLru<K, V> {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "LRU cache capacity must be greater than 0");
		Self {
			map: HashMap::new(),
			nodes: Vec::new(),
			free: Vec::new(),
			head: None,
			tail: None,
			capacity,
		}
	}

	pub fn get(&mut self, key: &K) -> Option<V> {
		if let Some(&idx) = self.map.get(key) {
			self.move_to_front(idx);
			Some(self.node(idx).value.clone())
		} else {
			None
		}
	}

	pub fn put(&mut self, key: K, value: V) -> Option<V> {
		if let Some(&idx) = self.map.get(&key) {
			let old = mem::replace(&mut self.node_mut(idx).value, value);
			self.move_to_front(idx);
			return Some(old);
		}

		if self.map.len() >= self.capacity {
			self.evict_tail();
		}

		let idx = self.alloc_node(key.clone(), value);
		self.map.insert(key, idx);
		self.push_front(idx);
		None
	}

	pub fn remove(&mut self, key: &K) -> Option<V> {
		if let Some(idx) = self.map.remove(key) {
			self.unlink(idx);
			self.free.push(idx);
			self.nodes[idx].take().map(|node| node.value)
		} else {
			None
		}
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.map.contains_key(key)
	}

	pub fn clear(&mut self) {
		self.map.clear();
		self.nodes.clear();
		self.free.clear();
		self.head = None;
		self.tail = None;
	}

	pub fn len(&self) -> usize {
		self.map.len()
	}

	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	pub fn capacity(&self) -> usize {
		self.capacity
	}

	pub fn values(&self) -> impl Iterator<Item = &V> {
		self.nodes.iter().filter_map(|slot| slot.as_ref().map(|node| &node.value))
	}

	pub fn struct_bytes(&self) -> usize {
		self.nodes.capacity() * mem::size_of::<Option<SlabNode<K, V>>>()
			+ self.map.capacity() * (mem::size_of::<K>() + mem::size_of::<usize>() * 2)
			+ self.free.capacity() * mem::size_of::<usize>()
	}

	fn node(&self, idx: usize) -> &SlabNode<K, V> {
		self.nodes[idx].as_ref().expect("occupied slab slot")
	}

	fn node_mut(&mut self, idx: usize) -> &mut SlabNode<K, V> {
		self.nodes[idx].as_mut().expect("occupied slab slot")
	}

	fn alloc_node(&mut self, key: K, value: V) -> usize {
		let node = SlabNode {
			key,
			value,
			prev: None,
			next: None,
		};
		if let Some(idx) = self.free.pop() {
			self.nodes[idx] = Some(node);
			idx
		} else {
			self.nodes.push(Some(node));
			self.nodes.len() - 1
		}
	}

	fn evict_tail(&mut self) {
		if let Some(idx) = self.tail {
			self.unlink(idx);
			if let Some(node) = self.nodes[idx].take() {
				self.map.remove(&node.key);
			}
			self.free.push(idx);
		}
	}

	fn push_front(&mut self, idx: usize) {
		let head = self.head;
		{
			let node = self.node_mut(idx);
			node.prev = None;
			node.next = head;
		}
		if let Some(h) = head {
			self.node_mut(h).prev = Some(idx);
		}
		self.head = Some(idx);
		if self.tail.is_none() {
			self.tail = Some(idx);
		}
	}

	fn unlink(&mut self, idx: usize) {
		let (prev, next) = {
			let node = self.node(idx);
			(node.prev, node.next)
		};
		match prev {
			Some(p) => self.node_mut(p).next = next,
			None => self.head = next,
		}
		match next {
			Some(n) => self.node_mut(n).prev = prev,
			None => self.tail = prev,
		}
		let node = self.node_mut(idx);
		node.prev = None;
		node.next = None;
	}

	fn move_to_front(&mut self, idx: usize) {
		if self.head == Some(idx) {
			return;
		}
		self.unlink(idx);
		self.push_front(idx);
	}
}

#[cfg(test)]
mod tests {
	use super::SlabLru;

	#[test]
	fn test_basic_operations() {
		let mut cache = SlabLru::new(2);

		assert_eq!(cache.put(1, "a"), None);
		assert_eq!(cache.put(2, "b"), None);
		assert_eq!(cache.get(&1), Some("a"));
		assert_eq!(cache.get(&2), Some("b"));
		assert_eq!(cache.len(), 2);
	}

	#[test]
	fn test_eviction_removes_lru() {
		let mut cache = SlabLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		// Inserting a third key past capacity evicts the LRU entry (key 1).
		// New-key insertion returns None, matching ArcLru.
		let evicted = cache.put(3, "c");

		assert_eq!(evicted, None);
		assert_eq!(cache.get(&1), None);
		assert_eq!(cache.get(&2), Some("b"));
		assert_eq!(cache.get(&3), Some("c"));
		assert_eq!(cache.len(), 2);
	}

	#[test]
	fn test_get_promotes_recency() {
		let mut cache = SlabLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.get(&1); // 1 becomes most-recent, so 2 is now the LRU victim
		cache.put(3, "c"); // must evict 2, not 1

		assert_eq!(cache.get(&1), Some("a"));
		assert_eq!(cache.get(&2), None);
		assert_eq!(cache.get(&3), Some("c"));
	}

	#[test]
	fn test_update_existing_returns_old_and_keeps_len() {
		let mut cache = SlabLru::new(2);

		cache.put(1, "a");
		let old = cache.put(1, "b");

		assert_eq!(old, Some("a"));
		assert_eq!(cache.get(&1), Some("b"));
		assert_eq!(cache.len(), 1);
	}

	#[test]
	fn test_remove() {
		let mut cache = SlabLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");

		assert_eq!(cache.remove(&1), Some("a"));
		assert_eq!(cache.get(&1), None);
		assert_eq!(cache.len(), 1);
		assert_eq!(cache.remove(&999), None);
	}

	#[test]
	fn test_clear_then_reuse() {
		let mut cache = SlabLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		cache.clear();

		assert_eq!(cache.len(), 0);
		assert!(cache.is_empty());
		// The cache must remain usable after clear (slab/free-list reset).
		assert_eq!(cache.put(5, "e"), None);
		assert_eq!(cache.get(&5), Some("e"));
	}

	#[test]
	fn test_contains_key_does_not_promote() {
		let mut cache = SlabLru::new(2);

		cache.put(1, "a");
		cache.put(2, "b");
		// contains_key is a pure lookup: it must NOT bump recency, so 1
		// stays the LRU victim and is the one evicted next.
		assert!(cache.contains_key(&1));
		cache.put(3, "c");

		assert_eq!(cache.get(&1), None);
		assert_eq!(cache.get(&2), Some("b"));
	}

	#[test]
	#[should_panic(expected = "capacity must be greater than 0")]
	fn test_zero_capacity_panics() {
		let _cache: SlabLru<i32, i32> = SlabLru::new(0);
	}

	#[test]
	fn test_slab_recycles_slots_no_unbounded_growth() {
		// The point of the fix: streaming far more distinct keys than
		// capacity must recycle evicted slots, never grow the backing node
		// Vec, and never exceed capacity. This is what makes steady-state
		// put/evict allocation-free (vs the old per-put DashMap scan).
		let cap = 8usize;
		let mut cache = SlabLru::new(cap);
		for k in 0..1000i32 {
			cache.put(k, k * 10);
			assert!(cache.len() <= cap);
		}

		assert_eq!(cache.len(), cap);
		// Backing slab stays bounded at capacity - no growth over 1000 inserts.
		assert_eq!(cache.nodes.len(), cap);
		assert!(cache.free.is_empty());

		// Exact-LRU: only the most-recent `cap` keys survive.
		for k in (1000 - cap as i32)..1000 {
			assert_eq!(cache.get(&k), Some(k * 10));
		}
		assert_eq!(cache.get(&0), None);
	}
}
