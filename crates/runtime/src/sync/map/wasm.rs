// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	borrow::Borrow,
	collections::HashMap,
	hash::Hash,
	sync::{Arc, RwLock},
};

pub struct MapInner<K, V>
where
	K: Eq + Hash,
{
	inner: Arc<RwLock<HashMap<K, V>>>,
}

// SAFETY: The inner Arc<RwLock<HashMap>> is Sync, and we need to explicitly mark this

unsafe impl<K, V> Sync for MapInner<K, V>
where
	K: Eq + Hash + Send,
	V: Send,
{
}

impl<K, V> MapInner<K, V>
where
	K: Eq + Hash,
{
	#[inline]
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	#[inline]
	pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
	where
		F: FnOnce() -> V,
		V: Clone,
		K: Clone,
	{
		{
			let map = self.inner.read().unwrap();
			if let Some(value) = map.get(&key) {
				return value.clone();
			}
		}

		let mut map = self.inner.write().unwrap();

		map.entry(key).or_insert_with(f).clone()
	}

	#[inline]
	pub fn get<Q>(&self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		V: Clone,
	{
		let map = self.inner.read().unwrap();
		map.get(key).cloned()
	}

	#[inline]
	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
	{
		let map = self.inner.read().unwrap();
		map.contains_key(key)
	}

	#[inline]
	pub fn with_read<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		F: FnOnce(&V) -> R,
	{
		let map = self.inner.read().unwrap();
		map.get(key).map(f)
	}

	#[inline]
	pub fn insert(&self, key: K, value: V) {
		self.inner.write().unwrap().insert(key, value);
	}

	#[inline]
	pub fn remove<Q>(&self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
	{
		self.inner.write().unwrap().remove(key)
	}

	#[inline]
	pub fn keys(&self) -> Vec<K>
	where
		K: Clone,
	{
		self.inner.read().unwrap().keys().cloned().collect()
	}

	#[inline]
	pub fn keys_into(&self, buf: &mut Vec<K>)
	where
		K: Clone,
	{
		buf.clear();
		let map = self.inner.read().unwrap();
		buf.extend(map.keys().cloned());
	}

	#[inline]
	pub fn with_write<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		F: FnOnce(&mut V) -> R,
	{
		self.inner.write().unwrap().get_mut(key).map(f)
	}

	#[inline]
	pub fn clear(&self) {
		let mut map = self.inner.write().unwrap();
		map.clear();
	}
}

impl<K, V> Default for MapInner<K, V>
where
	K: Eq + Hash,
{
	fn default() -> Self {
		Self::new()
	}
}
