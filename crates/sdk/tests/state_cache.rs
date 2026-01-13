// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Integration tests for StateCache using the operator test harness
//!
//! Note: StateCache is `!Send + !Sync` by design (single-threaded LRU cache),
//! so we test it directly using `create_operator_context()` rather than
//! embedding it in FFIOperator implementations.

use std::collections::HashMap;

use reifydb_sdk::{
	prelude::*,
	state::StateCache,
	testing::{TestFlowChangeBuilder, TestHarnessBuilder},
};
use serde::{Deserialize, Serialize};

// =============================================================================
// Test State Types
// =============================================================================

#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
struct CounterState {
	count: i64,
}

#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
struct SumState {
	total: i64,
}

/// Simple pass-through operator - we only need this to create a harness
/// that provides a valid OperatorContext for testing StateCache directly.
struct PassthroughOperator;

impl FFIOperatorMetadata for PassthroughOperator {
	const NAME: &'static str = "passthrough";
	const API: u32 = 1;
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "Pass-through operator for testing";
	const INPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
	const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
}

impl FFIOperator for PassthroughOperator {
	fn new(_operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
		Ok(input)
	}

	fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<Columns> {
		Ok(Columns::empty())
	}
}

#[test]
fn test_cache_set_and_get() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "test_key".to_string();
	let value = CounterState { count: 42 };

	// Set a value
	let mut ctx = harness.create_operator_context();
	cache.set(&mut ctx, &key, &value).expect("Set failed");

	// Verify it's cached
	assert!(cache.is_cached(&key));

	// Get should return the cached value
	let mut ctx = harness.create_operator_context();
	let retrieved = cache.get(&mut ctx, &key).expect("Get failed");
	assert_eq!(retrieved, Some(value));
}

#[test]
fn test_cache_write_through_persists_to_ffi() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "persist_key".to_string();
	let value = CounterState { count: 100 };

	// Set a value through cache
	let mut ctx = harness.create_operator_context();
	cache.set(&mut ctx, &key, &value).expect("Set failed");

	// Verify state was persisted to FFI (not just cached)
	let state = harness.state();
	assert!(state.len() > 0, "State should be persisted to FFI");
}

#[test]
fn test_cache_get_or_default_creates_default() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "new_key".to_string();

	// get_or_default should create default when key doesn't exist
	let mut ctx = harness.create_operator_context();
	let result = cache
		.get_or_default(&mut ctx, &key)
		.expect("get_or_default failed");

	assert_eq!(result.count, 0); // V::default()
}

#[test]
fn test_cache_get_or_default_returns_existing() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "existing_key".to_string();
	let value = CounterState { count: 50 };

	// Set a value first
	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut ctx, &key, &value).expect("Set failed");
	}

	// get_or_default should return existing value
	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.get_or_default(&mut ctx, &key)
			.expect("get_or_default failed");

		assert_eq!(result.count, 50, "Should return existing value, not default");
	}
}

#[test]
fn test_cache_update() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "counter".to_string();

	// Update should create default and apply updater
	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut ctx, &key, |s| {
				s.count += 10;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.count, 10);
	}

	// Update again should load existing and apply updater
	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut ctx, &key, |s| {
				s.count += 5;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.count, 15);
	}

	// Verify cached
	assert!(cache.is_cached(&key));
}

#[test]
fn test_cache_remove() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "remove_key".to_string();
	let value = CounterState { count: 42 };

	// Set a value
	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut ctx, &key, &value).expect("Set failed");
	}

	// Verify it's cached and in FFI
	assert!(cache.is_cached(&key));
	assert!(harness.state().len() > 0);

	// Remove
	{
		let mut ctx = harness.create_operator_context();
		cache.remove(&mut ctx, &key).expect("Remove failed");
	}

	// Verify removed from cache
	assert!(!cache.is_cached(&key));

	// Get should return None (removed from FFI too)
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut ctx, &key).expect("Get failed");
		assert_eq!(result, None);
	}
}

#[test]
fn test_cache_invalidate_only_clears_cache() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "invalidate_key".to_string();
	let value = CounterState { count: 77 };

	// Set a value
	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut ctx, &key, &value).expect("Set failed");
	}

	// Verify it's cached
	assert!(cache.is_cached(&key));

	// Invalidate (removes from cache only)
	cache.invalidate(&key);

	// Verify removed from cache
	assert!(!cache.is_cached(&key));

	// But FFI state should still exist - get should reload it
	{
		let mut ctx = harness.create_operator_context();
		let retrieved = cache.get(&mut ctx, &key).expect("Get failed");
		assert_eq!(retrieved, Some(value));
	}

	// Now it should be cached again
	assert!(cache.is_cached(&key));
}

#[test]
fn test_cache_clear_cache() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);

	// Set multiple values
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = format!("key_{}", i);
			let value = CounterState { count: i };
			cache.set(&mut ctx, &key, &value).expect("Set failed");
		}
	}

	// Verify all cached
	assert_eq!(cache.len(), 3);

	// Clear cache
	cache.clear_cache();

	// Verify cache is empty
	assert!(cache.is_empty());

	// FFI state should still exist (can reload)
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut ctx, &"key_0".to_string()).expect("Get failed");
		assert!(result.is_some(), "FFI state should still exist");
	}
}

#[test]
fn test_cache_multiple_keys() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, SumState> = StateCache::new(10);

	// Set multiple keys
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..5 {
			let key = format!("sum_{}", i);
			let value = SumState { total: i * 10 };
			cache.set(&mut ctx, &key, &value).expect("Set failed");
		}
	}

	// All should be cached
	assert_eq!(cache.len(), 5);

	// Verify each value
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..5 {
			let key = format!("sum_{}", i);
			let result = cache.get(&mut ctx, &key).expect("Get failed");
			assert_eq!(result, Some(SumState { total: i * 10 }));
		}
	}
}

#[test]
fn test_cache_lru_eviction() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(3);

	// Fill cache to capacity
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = format!("key_{}", i);
			let value = CounterState { count: i };
			cache.set(&mut ctx, &key, &value).expect("Set failed");
		}
	}

	assert_eq!(cache.len(), 3);
	assert!(cache.is_cached(&"key_0".to_string()));
	assert!(cache.is_cached(&"key_1".to_string()));
	assert!(cache.is_cached(&"key_2".to_string()));

	// Insert a 4th item - should evict key_0 (LRU)
	{
		let mut ctx = harness.create_operator_context();
		let key = "key_3".to_string();
		let value = CounterState { count: 3 };
		cache.set(&mut ctx, &key, &value).expect("Set failed");
	}

	// key_0 should be evicted from cache
	assert!(!cache.is_cached(&"key_0".to_string()), "key_0 should be evicted");
	assert!(cache.is_cached(&"key_3".to_string()), "key_3 should be cached");
	assert_eq!(cache.len(), 3);

	// But key_0 should still exist in FFI storage
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut ctx, &"key_0".to_string()).expect("Get failed");
		assert_eq!(result, Some(CounterState { count: 0 }), "key_0 should still exist in FFI");
	}
}

#[test]
fn test_cache_lru_access_updates_order() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(3);

	// Fill cache: key_0, key_1, key_2
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = format!("key_{}", i);
			let value = CounterState { count: i };
			cache.set(&mut ctx, &key, &value).expect("Set failed");
		}
	}

	// Access key_0 to make it most recently used
	{
		let mut ctx = harness.create_operator_context();
		cache.get(&mut ctx, &"key_0".to_string()).expect("Get failed");
	}

	// Insert key_3 - should evict key_1 (now LRU)
	{
		let mut ctx = harness.create_operator_context();
		let key = "key_3".to_string();
		let value = CounterState { count: 3 };
		cache.set(&mut ctx, &key, &value).expect("Set failed");
	}

	// key_0 should still be cached (was accessed recently)
	assert!(cache.is_cached(&"key_0".to_string()), "key_0 should be cached (recently accessed)");
	// key_1 should be evicted (was LRU)
	assert!(!cache.is_cached(&"key_1".to_string()), "key_1 should be evicted (LRU)");
	// key_2 and key_3 should be cached
	assert!(cache.is_cached(&"key_2".to_string()));
	assert!(cache.is_cached(&"key_3".to_string()));
}

#[test]
fn test_cache_tuple_keys() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<(String, String), SumState> = StateCache::new(10);

	let key1 = ("base".to_string(), "quote".to_string());
	let key2 = ("foo".to_string(), "bar".to_string());
	let value1 = SumState { total: 100 };
	let value2 = SumState { total: 200 };

	// Set values with tuple keys
	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut ctx, &key1, &value1).expect("Set failed");
		cache.set(&mut ctx, &key2, &value2).expect("Set failed");
	}

	// Verify cached
	assert!(cache.is_cached(&key1));
	assert!(cache.is_cached(&key2));

	// Get and verify
	{
		let mut ctx = harness.create_operator_context();
		let result1 = cache.get(&mut ctx, &key1).expect("Get failed");
		let result2 = cache.get(&mut ctx, &key2).expect("Get failed");
		assert_eq!(result1, Some(value1));
		assert_eq!(result2, Some(value2));
	}
}

#[test]
fn test_cache_tuple_key_update() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<(String, String), SumState> = StateCache::new(10);
	let key = ("account".to_string(), "balance".to_string());

	// Update with tuple key
	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut ctx, &key, |s| {
				s.total += 500;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.total, 500);
	}

	// Update again
	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut ctx, &key, |s| {
				s.total += 250;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.total, 750);
	}
}

#[test]
fn test_cache_capacity() {
	let cache: StateCache<String, CounterState> = StateCache::new(100);
	assert_eq!(cache.capacity(), 100);
}

#[test]
fn test_cache_len_and_is_empty() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);

	assert!(cache.is_empty());
	assert_eq!(cache.len(), 0);

	// Add some items
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = format!("key_{}", i);
			let value = CounterState { count: i };
			cache.set(&mut ctx, &key, &value).expect("Set failed");
		}
	}

	assert!(!cache.is_empty());
	assert_eq!(cache.len(), 3);
}

#[test]
fn test_cache_miss_then_hit() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<String, CounterState> = StateCache::new(10);
	let key = "miss_hit_key".to_string();
	let value = CounterState { count: 123 };

	// Set value (goes to FFI and cache)
	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut ctx, &key, &value).expect("Set failed");
	}

	// Invalidate to clear cache
	cache.invalidate(&key);
	assert!(!cache.is_cached(&key));

	// First get should be a cache miss (loads from FFI)
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut ctx, &key).expect("Get failed");
		assert_eq!(result, Some(value.clone()));
	}

	// Now it should be cached
	assert!(cache.is_cached(&key));

	// Second get should be a cache hit
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut ctx, &key).expect("Get failed");
		assert_eq!(result, Some(value));
	}
}

#[test]
fn test_cache_with_operator_apply() {
	let mut harness = TestHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	// Create cache outside the operator (since StateCache is !Send+!Sync)
	let mut cache: StateCache<String, CounterState> = StateCache::new(10);

	// Simulate what an operator would do: process input, update state via cache
	let input = TestFlowChangeBuilder::new()
		.insert_row(1, vec![Value::Int8(10i64)])
		.insert_row(2, vec![Value::Int8(20i64)])
		.build();

	// Process the input - count the number of diffs
	{
		let mut ctx = harness.create_operator_context();
		let diff_count = input.diffs.len() as i64;
		cache
			.update(&mut ctx, &"event_counter".to_string(), |s| {
				s.count += diff_count;
				Ok(())
			})
			.expect("Update failed");
	}

	// Process more input
	let input2 = TestFlowChangeBuilder::new().insert_row(3, vec![Value::Int8(30i64)]).build();

	{
		let mut ctx = harness.create_operator_context();
		let diff_count = input2.diffs.len() as i64;
		cache
			.update(&mut ctx, &"event_counter".to_string(), |s| {
				s.count += diff_count;
				Ok(())
			})
			.expect("Update failed");
	}

	// Verify final count
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut ctx, &"event_counter".to_string()).expect("Get failed");
		assert_eq!(result, Some(CounterState { count: 3 }));
	}
}
