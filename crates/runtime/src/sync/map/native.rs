// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{borrow::Borrow, hash::Hash};

use dashmap::DashMap;

pub struct MapInner<K, V>
where
	K: Eq + Hash,
{
	inner: DashMap<K, V>,
}

impl<K, V> MapInner<K, V>
where
	K: Eq + Hash,
{
	#[inline]
	pub fn new() -> Self {
		Self {
			inner: DashMap::new(),
		}
	}

	#[inline]
	pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
	where
		F: FnOnce() -> V,
		V: Clone,
	{
		self.inner.entry(key).or_insert_with(f).value().clone()
	}

	#[inline]
	pub fn get<Q>(&self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		V: Clone,
	{
		self.inner.get(key).map(|guard| guard.value().clone())
	}

	#[inline]
	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
	{
		self.inner.contains_key(key)
	}

	#[inline]
	pub fn with_read<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		F: FnOnce(&V) -> R,
	{
		self.inner.get(key).map(|guard| f(guard.value()))
	}

	#[inline]
	pub fn insert(&self, key: K, value: V) {
		self.inner.insert(key, value);
	}

	#[inline]
	pub fn remove<Q>(&self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
	{
		self.inner.remove(key).map(|(_, v)| v)
	}

	#[inline]
	pub fn keys(&self) -> Vec<K>
	where
		K: Clone,
	{
		self.inner.iter().map(|entry| entry.key().clone()).collect()
	}

	#[inline]
	pub fn keys_into(&self, buf: &mut Vec<K>)
	where
		K: Clone,
	{
		buf.clear();
		for shard in self.inner.shards() {
			let shard = shard.read();
			buf.extend(shard.iter().map(|(k, _)| k.clone()));
		}
	}

	#[inline]
	pub fn with_write<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		F: FnOnce(&mut V) -> R,
	{
		self.inner.get_mut(key).map(|mut guard| f(guard.value_mut()))
	}

	#[inline]
	pub fn clear(&self) {
		self.inner.clear();
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
