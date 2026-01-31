//! Concurrent map abstraction that provides a unified API across native and WASM targets.
//!
//! On native platforms, this wraps `DashMap` for high-performance concurrent access.
//! On WASM platforms, this wraps `Arc<RwLock<HashMap>>` to provide similar semantics.

use std::{borrow::Borrow, hash::Hash};

use cfg_if::cfg_if;

#[cfg(reifydb_target = "native")]
pub(crate) mod native;

#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

cfg_if! {
    if #[cfg(reifydb_target = "native")] {
	type MapInnerImpl<K, V> = native::MapInner<K, V>;
    } else {
	type MapInnerImpl<K, V> = wasm::MapInner<K, V>;
    }
}

/// A concurrent map that provides a unified API across native and WASM targets.
pub struct Map<K, V>
where
	K: Eq + Hash,
{
	inner: MapInnerImpl<K, V>,
}

impl<K, V> Map<K, V>
where
	K: Eq + Hash,
{
	#[inline]
	pub fn new() -> Self {
		Self {
			inner: MapInnerImpl::new(),
		}
	}

	#[inline]
	pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
	where
		F: FnOnce() -> V,
		V: Clone,
		K: Clone,
	{
		self.inner.get_or_insert_with(key, f)
	}

	#[inline]
	pub fn get<Q>(&self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		V: Clone,
	{
		self.inner.get(key)
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
		self.inner.with_read(key, f)
	}

	#[inline]
	pub fn insert(&self, key: K, value: V)
	where
		K: Clone,
	{
		self.inner.insert(key, value);
	}

	#[inline]
	pub fn remove<Q>(&self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
	{
		self.inner.remove(key)
	}

	#[inline]
	pub fn keys(&self) -> Vec<K>
	where
		K: Clone,
	{
		self.inner.keys()
	}

	#[inline]
	pub fn with_write<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
	where
		K: Borrow<Q>,
		Q: Hash + Eq + ?Sized,
		F: FnOnce(&mut V) -> R,
	{
		self.inner.with_write(key, f)
	}

	#[inline]
	pub fn clear(&self) {
		self.inner.clear();
	}
}

impl<K, V> Default for Map<K, V>
where
	K: Eq + Hash,
{
	#[inline]
	fn default() -> Self {
		Self::new()
	}
}
