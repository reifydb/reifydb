// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, hash::Hash, mem};

use postcard::{from_bytes, to_allocvec};
use reifydb_core::{
	encoded::{key::IntoEncodedKey, shape::RowShape},
	util::lru::LruCache,
};
use reifydb_type::value::blob::Blob;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	error::{FFIError, Result},
	operator::context::OperatorContext,
};

pub struct StateCache<K, V> {
	cache: LruCache<K, V>,
	/// Keys mutated since the last `flush`. `Some(value)` means a `set`,
	/// `None` means a `remove`. The cache acts as a coalescing buffer:
	/// repeated `set`s of the same key only flush the final value once.
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
		// Check cache first
		if let Some(cached) = self.cache.get(key) {
			return Ok(Some(cached.clone()));
		}

		// Cache miss - load from FFI
		let encoded_key = key.into_encoded_key();
		let state = ctx.state();
		match state.get(&encoded_key)? {
			Some(encoded_row) => {
				let shape = RowShape::operator_state();
				let blob = shape.get_blob(&encoded_row, 0);
				let value: V = from_bytes(blob.as_bytes()).map_err(|e| {
					FFIError::Serialization(format!("deserialization failed: {}", e))
				})?;
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

	/// Drain dirty entries and write them through to host storage.
	///
	/// Called once per txn at commit time, normally from the operator's
	/// `FFIOperator::flush_state` impl. Repeated `set`s of the same key
	/// produce a single write here; coalesced `set` + `remove` produces a
	/// single remove.
	pub fn flush(&mut self, ctx: &mut OperatorContext) -> Result<()> {
		let dirty = mem::take(&mut self.dirty);
		let shape = RowShape::operator_state();
		for (key, slot) in dirty {
			let encoded_key = (&key).into_encoded_key();
			match slot {
				Some(value) => {
					let bytes = to_allocvec(&value).map_err(|e| {
						FFIError::Serialization(format!("serialization failed: {}", e))
					})?;
					let mut row = shape.allocate();
					shape.set_blob(&mut row, 0, &Blob::new(bytes));
					ctx.state().set(&encoded_key, &row)?;
				}
				None => {
					ctx.state().remove(&encoded_key)?;
				}
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
		// Load or create default
		let mut value = self.get_or_default(ctx, key)?;

		// Apply update
		updater(&mut value)?;

		// Save (write-through)
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
