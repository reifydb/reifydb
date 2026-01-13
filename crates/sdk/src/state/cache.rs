// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! LRU cache for operator state - reduces FFI round-trips and deserialization overhead
//!
//! This module provides a generic `StateCache<K, V>` that operators can use to cache
//! deserialized state values. The cache is a write-through cache: reads are served from
//! cache when possible, writes always persist to storage AND update the cache.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_sdk::state::StateCache;
//! use reifydb_core::IntoEncodedKey;
//!
//! // Define your key type and implement IntoEncodedKey for &MyKey
//! #[derive(Hash, Eq, PartialEq, Clone)]
//! struct MyKey {
//!     base_mint: String,
//!     quote_mint: String,
//! }
//!
//! impl IntoEncodedKey for &MyKey {
//!     fn into_encoded_key(self) -> EncodedKey {
//!         EncodedKey::builder()
//!             .str(&self.base_mint)
//!             .str(&self.quote_mint)
//!             .build()
//!     }
//! }
//!
//! pub struct MyOperator {
//!     cache: StateCache<MyKey, MyState>,
//! }
//!
//! impl MyOperator {
//!     fn new() -> Self {
//!         Self {
//!             cache: StateCache::new(1000),
//!         }
//!     }
//!
//!     fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
//!         let key = MyKey { base_mint, quote_mint };
//!
//!         // Clean API - no closures needed!
//!         let state = self.cache.update(ctx, &key, |state| {
//!             state.process(input);
//!             Ok(())
//!         })?;
//!     }
//! }
//! ```

use std::hash::Hash;

use reifydb_core::util::{CowVec, LruCache};
use reifydb_core::value::encoded::EncodedValues;
use reifydb_core::IntoEncodedKey;
use serde::{Serialize, de::DeserializeOwned};

use crate::OperatorContext;
use crate::error::{FFIError, Result};

/// Generic LRU cache for operator state - caches deserialized domain types.
///
/// `K` is the key type (must implement `Hash + Eq + Clone`, and `&K` must implement `IntoEncodedKey`)
/// `V` is the state type (must implement `Clone + Serialize + DeserializeOwned`)
///
/// The cache is thread-safe (`Send + Sync`) using `ConcurrentLruCache` internally.
pub struct StateCache<K, V> {
	cache: LruCache<K, V>,
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Serialize + DeserializeOwned,
{

	/// Create a new state cache with default capacity (1000 entries).
	pub fn new(capacity: usize) -> Self {
		Self {
			cache: LruCache::new(capacity),
		}
	}
	/// Get a value - checks cache first, falls back to FFI + deserialize.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context for FFI calls
	/// * `key` - Application-level key (must implement `IntoEncodedKey`)
	///
	/// # Returns
	///
	/// `Ok(Some(value))` if the key exists, `Ok(None)` if not found.
	pub fn get(&mut self, ctx: &mut OperatorContext, key: &K) -> Result<Option<V>> {
		// Check cache first
		if let Some(cached) = self.cache.get(key) {
			return Ok(Some(cached.clone()));
		}

		// Cache miss - load from FFI
		let encoded_key = key.into_encoded_key();
		let state = ctx.state();
		match state.get(&encoded_key)? {
			Some(encoded_values) => {
				// Deserialize and cache
				let value: V = postcard::from_bytes(encoded_values.as_ref()).map_err(|e| {
					FFIError::Serialization(format!("deserialization failed: {}", e))
				})?;
				self.cache.put(key.clone(), value.clone());
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	/// Set a value - serializes, writes through to FFI, AND updates cache.
	///
	/// This is a write-through cache: the value is always persisted to storage
	/// via FFI, then the cache is updated.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context for FFI calls
	/// * `key` - Application-level key
	/// * `value` - The value to store
	pub fn set(&mut self, ctx: &mut OperatorContext, key: &K, value: &V) -> Result<()> {
		// Serialize the value
		let bytes = postcard::to_allocvec(value)
			.map_err(|e| FFIError::Serialization(format!("serialization failed: {}", e)))?;
		let encoded_values = EncodedValues(CowVec::new(bytes));

		// Write through to FFI
		let encoded_key = key.into_encoded_key();
		ctx.state().set(&encoded_key, &encoded_values)?;

		// Update cache
		self.cache.put(key.clone(), value.clone());

		Ok(())
	}

	/// Remove a value - removes from FFI AND cache.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context for FFI calls
	/// * `key` - Application-level key
	pub fn remove(&mut self, ctx: &mut OperatorContext, key: &K) -> Result<()> {
		// Remove from FFI
		let encoded_key = key.into_encoded_key();
		ctx.state().remove(&encoded_key)?;

		// Remove from cache
		self.cache.remove(key);

		Ok(())
	}

	/// Clear the cache (does NOT clear persistent state).
	///
	/// This only clears the in-memory cache. Persistent state in the FFI layer
	/// is not affected.
	pub fn clear_cache(&mut self) {
		self.cache.clear();
	}

	/// Invalidate a specific key in the cache.
	///
	/// Removes the key from the cache without affecting persistent state.
	/// The next `get()` call for this key will reload from FFI.
	pub fn invalidate(&mut self, key: &K) {
		self.cache.remove(key);
	}

	/// Check if a key is in the cache (without loading from FFI).
	///
	/// Note: This does not update LRU order.
	pub fn is_cached(&self, key: &K) -> bool {
		self.cache.contains_key(key)
	}

	/// Get the current number of cached entries.
	pub fn len(&self) -> usize {
		self.cache.len()
	}

	/// Check if the cache is empty.
	pub fn is_empty(&self) -> bool {
		self.cache.is_empty()
	}

	/// Get the capacity of the cache.
	pub fn capacity(&self) -> usize {
		self.cache.capacity()
	}
}

/// Methods that require `V: Default`
impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Default + Serialize + DeserializeOwned,
{
	/// Get or create - loads from cache/FFI or creates default if not found.
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context for FFI calls
	/// * `key` - Application-level key
	///
	/// # Returns
	///
	/// The existing value or `V::default()` if not found.
	pub fn get_or_default(&mut self, ctx: &mut OperatorContext, key: &K) -> Result<V> {
		match self.get(ctx, key)? {
			Some(value) => Ok(value),
			None => Ok(V::default()),
		}
	}

	/// Update a value with a function - load, modify, save (all cached).
	///
	/// This is the most common pattern for stateful operators:
	/// 1. Load existing state (from cache if available, or create default)
	/// 2. Apply an update function
	/// 3. Save the updated state (write-through)
	/// 4. Return the updated value
	///
	/// # Arguments
	///
	/// * `ctx` - The operator context for FFI calls
	/// * `key` - Application-level key
	/// * `updater` - Function that modifies the value in place
	///
	/// # Returns
	///
	/// The updated value (also cached).
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
mod tests {
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
