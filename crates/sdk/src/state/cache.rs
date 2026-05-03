// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, hash::Hash, mem};

use reifydb_core::{encoded::key::IntoEncodedKey, util::lru::LruCache};
use serde::{Serialize, de::DeserializeOwned};

use crate::{error::Result, operator::context::OperatorContext};

pub struct StateCache<K, V> {
	cache: LruCache<K, V>,
	dirty: HashMap<K, Option<V>>,
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Serialize + DeserializeOwned,
{
	pub fn new(capacity: usize) -> Self {
		Self {
			cache: LruCache::new(capacity),
			dirty: HashMap::new(),
		}
	}

	pub fn get(&mut self, ctx: &mut OperatorContext, key: &K) -> Result<Option<V>> {
		if let Some(cached) = self.cache.get(key) {
			return Ok(Some(cached.clone()));
		}

		let encoded_key = key.into_encoded_key();
		match ctx.state().get::<V>(&encoded_key)? {
			Some(value) => {
				self.cache.put(key.clone(), value.clone());
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	pub fn set(&mut self, _ctx: &mut OperatorContext, key: &K, value: &V) -> Result<()> {
		self.cache.put(key.clone(), value.clone());
		self.dirty.insert(key.clone(), Some(value.clone()));
		Ok(())
	}

	pub fn remove(&mut self, _ctx: &mut OperatorContext, key: &K) -> Result<()> {
		self.cache.remove(key);
		self.dirty.insert(key.clone(), None);
		Ok(())
	}

	pub fn flush(&mut self, ctx: &mut OperatorContext) -> Result<()> {
		let dirty = mem::take(&mut self.dirty);
		for (key, slot) in dirty {
			let encoded_key = (&key).into_encoded_key();
			match slot {
				Some(value) => ctx.state().set(&encoded_key, &value)?,
				None => ctx.state().remove(&encoded_key)?,
			}
		}
		Ok(())
	}

	pub fn clear_cache(&mut self) {
		self.cache.clear();
	}

	pub fn invalidate(&mut self, key: &K) {
		self.cache.remove(key);
	}

	pub fn is_cached(&self, key: &K) -> bool {
		self.cache.contains_key(key)
	}

	pub fn len(&self) -> usize {
		self.cache.len()
	}

	pub fn is_empty(&self) -> bool {
		self.cache.is_empty()
	}

	pub fn capacity(&self) -> usize {
		self.cache.capacity()
	}
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Default + Serialize + DeserializeOwned,
{
	pub fn get_or_default(&mut self, ctx: &mut OperatorContext, key: &K) -> Result<V> {
		match self.get(ctx, key)? {
			Some(value) => Ok(value),
			None => Ok(V::default()),
		}
	}

	pub fn update<U>(&mut self, ctx: &mut OperatorContext, key: &K, updater: U) -> Result<V>
	where
		U: FnOnce(&mut V) -> Result<()>,
	{
		let mut value = self.get_or_default(ctx, key)?;
		updater(&mut value)?;
		self.set(ctx, key, &value)?;
		Ok(value)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::encoded::key::IntoEncodedKey;

	use super::*;

	#[test]
	fn test_cache_capacity() {
		let cache: StateCache<String, i32> = StateCache::new(100);
		assert_eq!(cache.capacity(), 100);
		assert!(cache.is_empty());
		assert_eq!(cache.len(), 0);
	}

	#[test]
	#[should_panic(expected = "capacity must be greater than 0")]
	fn test_zero_capacity_panics() {
		let _cache: StateCache<String, i32> = StateCache::new(0);
	}

	#[test]
	fn test_into_encoded_key_string() {
		let key = "test_key".to_string();
		let encoded = (&key).into_encoded_key();
		assert!(!encoded.as_bytes().is_empty());
	}

	#[test]
	fn test_into_encoded_key_str() {
		let key = "test_key";
		let encoded = key.into_encoded_key();
		assert!(!encoded.as_bytes().is_empty());
	}

	#[test]
	fn test_into_encoded_key_tuple2() {
		let key = ("base".to_string(), "quote".to_string());
		let encoded = (&key).into_encoded_key();
		assert!(!encoded.as_bytes().is_empty());
	}

	#[test]
	fn test_into_encoded_key_tuple3() {
		let key = ("a".to_string(), "b".to_string(), "c".to_string());
		let encoded = (&key).into_encoded_key();
		assert!(!encoded.as_bytes().is_empty());
	}

	#[test]
	fn test_into_encoded_key_consistency() {
		let key1 = ("base".to_string(), "quote".to_string());
		let key2 = ("base".to_string(), "quote".to_string());
		assert_eq!((&key1).into_encoded_key().as_bytes(), (&key2).into_encoded_key().as_bytes());
	}

	#[test]
	fn test_into_encoded_key_different_keys() {
		let key1 = ("a".to_string(), "b".to_string());
		let key2 = ("c".to_string(), "d".to_string());
		assert_ne!((&key1).into_encoded_key().as_bytes(), (&key2).into_encoded_key().as_bytes());
	}
}
