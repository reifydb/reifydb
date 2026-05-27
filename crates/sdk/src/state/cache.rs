// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash, mem, sync::Arc};

use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	util::lru::slab::SlabLru,
};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	error::Result,
	operator::context::{InternalStateApi, OperatorContext, StateApi},
};

#[derive(Clone, Copy, Debug)]
pub enum StateBackend {
	Data,

	Internal,
}

pub struct StateCache<K, V> {
	cache: SlabLru<K, Arc<V>>,
	dirty: HashMap<K, Option<Arc<V>>>,
	backend: StateBackend,
}

impl<K, V> StateCache<K, V>
where
	K: Hash + Eq + Clone,
	for<'a> &'a K: IntoEncodedKey,
	V: Clone + Serialize + DeserializeOwned,
{
	pub fn new(capacity: usize) -> Self {
		Self::with_backend(capacity, StateBackend::Data)
	}

	pub fn new_internal(capacity: usize) -> Self {
		Self::with_backend(capacity, StateBackend::Internal)
	}

	fn with_backend(capacity: usize, backend: StateBackend) -> Self {
		Self {
			cache: SlabLru::new(capacity),
			dirty: HashMap::new(),
			backend,
		}
	}

	pub fn get_arc(&mut self, ctx: &mut impl OperatorContext, key: &K) -> Result<Option<Arc<V>>> {
		if let Some(cached) = self.cache.get(key) {
			return Ok(Some(cached));
		}

		if let Some(slot) = self.dirty.get(key) {
			return Ok(slot.clone());
		}

		let encoded_key = key.into_encoded_key();
		let loaded = match self.backend {
			StateBackend::Data => ctx.state().get::<V>(&encoded_key)?,
			StateBackend::Internal => ctx.internal_state().get::<V>(&encoded_key)?,
		};
		match loaded {
			Some(value) => {
				let arc = Arc::new(value);
				self.cache.put(key.clone(), arc.clone());
				Ok(Some(arc))
			}
			None => Ok(None),
		}
	}

	pub fn get(&mut self, ctx: &mut impl OperatorContext, key: &K) -> Result<Option<V>> {
		Ok(self.get_arc(ctx, key)?.map(|arc| (*arc).clone()))
	}

	pub fn warm(&mut self, ctx: &mut impl OperatorContext, keys: &[K]) -> Result<()> {
		let mut to_load: Vec<K> = Vec::new();
		for key in keys {
			if self.cache.contains_key(key) || self.dirty.contains_key(key) {
				continue;
			}
			to_load.push(key.clone());
		}
		if to_load.is_empty() {
			return Ok(());
		}

		let mut by_encoded: HashMap<Vec<u8>, K> = HashMap::with_capacity(to_load.len());
		let mut encoded_keys: Vec<EncodedKey> = Vec::with_capacity(to_load.len());
		for key in &to_load {
			let encoded = key.into_encoded_key();
			by_encoded.insert(encoded.as_bytes().to_vec(), key.clone());
			encoded_keys.push(encoded);
		}

		let cache = &mut self.cache;
		let mut visit = |encoded: EncodedKey, value: V| -> Result<()> {
			if let Some(key) = by_encoded.get(encoded.as_bytes()) {
				cache.put(key.clone(), Arc::new(value));
			}
			Ok(())
		};
		match self.backend {
			StateBackend::Data => ctx.state().get_many_visit::<V>(&encoded_keys, &mut visit)?,
			StateBackend::Internal => {
				ctx.internal_state().get_many_visit::<V>(&encoded_keys, &mut visit)?
			}
		}
		Ok(())
	}

	pub fn set(&mut self, _ctx: &mut impl OperatorContext, key: &K, value: &V) -> Result<()> {
		let arc = Arc::new(value.clone());
		self.cache.put(key.clone(), arc.clone());
		self.dirty.insert(key.clone(), Some(arc));
		Ok(())
	}

	pub fn put(&mut self, _ctx: &mut impl OperatorContext, key: &K, value: V) -> Result<()> {
		let arc = Arc::new(value);
		self.cache.put(key.clone(), arc.clone());
		self.dirty.insert(key.clone(), Some(arc));
		Ok(())
	}

	pub fn put_arc(&mut self, _ctx: &mut impl OperatorContext, key: &K, value: Arc<V>) -> Result<()> {
		self.cache.put(key.clone(), value.clone());
		self.dirty.insert(key.clone(), Some(value));
		Ok(())
	}

	pub fn modify<F>(&mut self, ctx: &mut impl OperatorContext, key: &K, f: F) -> Result<()>
	where
		F: FnOnce(&mut V) -> Result<()>,
		V: Default,
	{
		let mut arc = self.get_arc(ctx, key)?.unwrap_or_else(|| Arc::new(V::default()));
		f(Arc::make_mut(&mut arc))?;
		self.put_arc(ctx, key, arc)
	}

	pub fn remove(&mut self, _ctx: &mut impl OperatorContext, key: &K) -> Result<()> {
		self.cache.remove(key);
		self.dirty.insert(key.clone(), None);
		Ok(())
	}

	pub fn flush(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		let dirty = mem::take(&mut self.dirty);
		for (key, slot) in dirty {
			let encoded_key = (&key).into_encoded_key();
			match (slot, self.backend) {
				(Some(value), StateBackend::Data) => ctx.state().set(&encoded_key, value.as_ref())?,
				(Some(value), StateBackend::Internal) => {
					ctx.internal_state().set(&encoded_key, value.as_ref())?
				}
				(None, StateBackend::Data) => ctx.state().remove(&encoded_key)?,
				(None, StateBackend::Internal) => ctx.internal_state().remove(&encoded_key)?,
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
	pub fn get_or_default(&mut self, ctx: &mut impl OperatorContext, key: &K) -> Result<V> {
		match self.get(ctx, key)? {
			Some(value) => Ok(value),
			None => Ok(V::default()),
		}
	}

	pub fn update<U>(&mut self, ctx: &mut impl OperatorContext, key: &K, updater: U) -> Result<V>
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
	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_core::{encoded::key::IntoEncodedKey, interface::catalog::flow::FlowNodeId};

	use super::*;
	use crate::{
		config::Config,
		operator::{
			FFIOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
			context::ffi::FFIOperatorContext,
		},
		state::RawStatefulOperator,
		testing::{harness::FFIOperatorHarnessBuilder, helpers::encode_key},
	};

	struct WarmTestOperator;

	impl OperatorMetadata for WarmTestOperator {
		const NAME: &'static str = "warm_test";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Test operator for StateCache::warm";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl FFIOperator for WarmTestOperator {
		fn new(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, _ctx: &mut FFIOperatorContext, _input: BorrowedChange<'_>) -> Result<()> {
			Ok(())
		}
	}

	impl RawStatefulOperator for WarmTestOperator {}

	#[test]
	fn test_warm_bulk_loads_present_keys_and_skips_absent() {
		let mut harness = FFIOperatorHarnessBuilder::<WarmTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Seed committed state under the same encoding the cache uses for its keys.
		{
			let mut ctx = harness.create_operator_context();
			let mut state = ctx.state();
			state.set(&encode_key(&"a".to_string()), &1i32).unwrap();
			state.set(&encode_key(&"b".to_string()), &2i32).unwrap();
		}

		let mut cache: StateCache<String, i32> = StateCache::new(100);
		let keys = vec!["a".to_string(), "b".to_string(), "missing".to_string()];

		let mut ctx = harness.create_operator_context();
		cache.warm(&mut ctx, &keys).unwrap();

		// Present keys are now cached without further host reads; absent key is not.
		assert!(cache.is_cached(&"a".to_string()));
		assert!(cache.is_cached(&"b".to_string()));
		assert!(!cache.is_cached(&"missing".to_string()));

		assert_eq!(cache.get(&mut ctx, &"a".to_string()).unwrap(), Some(1));
		assert_eq!(cache.get(&mut ctx, &"b".to_string()).unwrap(), Some(2));
		assert_eq!(cache.get(&mut ctx, &"missing".to_string()).unwrap(), None);
	}

	#[test]
	fn test_warm_internal_backend_bulk_loads_present_keys_and_skips_absent() {
		let mut harness = FFIOperatorHarnessBuilder::<WarmTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Seed committed internal state under the same encoding the cache uses for its keys.
		{
			let mut ctx = harness.create_operator_context();
			let mut state = ctx.internal_state();
			state.set(&encode_key(&"a".to_string()), &1i32).unwrap();
			state.set(&encode_key(&"b".to_string()), &2i32).unwrap();
		}

		// An Internal-backed cache must batch-load via internal_get_many. Before
		// InternalState::get_many existed, warm no-oped on this backend and nothing
		// was cached - this test pins that warm now actually loads internal state.
		let mut cache: StateCache<String, i32> = StateCache::new_internal(100);
		let keys = vec!["a".to_string(), "b".to_string(), "missing".to_string()];

		let mut ctx = harness.create_operator_context();
		cache.warm(&mut ctx, &keys).unwrap();

		assert!(cache.is_cached(&"a".to_string()));
		assert!(cache.is_cached(&"b".to_string()));
		assert!(!cache.is_cached(&"missing".to_string()));

		assert_eq!(cache.get(&mut ctx, &"a".to_string()).unwrap(), Some(1));
		assert_eq!(cache.get(&mut ctx, &"b".to_string()).unwrap(), Some(2));
		assert_eq!(cache.get(&mut ctx, &"missing".to_string()).unwrap(), None);
	}

	#[test]
	fn test_warm_does_not_overwrite_pending() {
		let mut harness = FFIOperatorHarnessBuilder::<WarmTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		{
			let mut ctx = harness.create_operator_context();
			ctx.state().set(&encode_key(&"a".to_string()), &1i32).unwrap();
		}

		let mut cache: StateCache<String, i32> = StateCache::new(100);
		let mut ctx = harness.create_operator_context();

		// A pending (dirty) write must shadow the committed value warm would load.
		cache.set(&mut ctx, &"a".to_string(), &99i32).unwrap();
		cache.warm(&mut ctx, &["a".to_string()]).unwrap();

		assert_eq!(cache.get(&mut ctx, &"a".to_string()).unwrap(), Some(99));
	}

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
