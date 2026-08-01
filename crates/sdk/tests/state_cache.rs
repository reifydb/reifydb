// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{GroupStateKey, IntoGroupStateKey, Keyspace},
	metrics::heap::HeapSize,
	state::{budget::OperatorStateBudgetHandle, cache::StateCache},
};
use reifydb_macro::operator_state;
use reifydb_sdk::{
	config::Config,
	error::Result,
	operator::{
		FFIOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
		context::ffi::FFIOperatorContext, windowed::bridge::OperatorContextStore,
	},
};
use reifydb_testing_sdk::{builders::TestChangeBuilder, harness::FFIOperatorHarnessBuilder};
use reifydb_value::{byte_size::ByteSize, value::Value};
use serde::{Deserialize, Serialize};

fn test_pool() -> OperatorStateBudgetHandle {
	OperatorStateBudgetHandle::new(ByteSize::from_bytes(64 * 1024 * 1024))
}

/// A bare `String` cannot be a cache key: `IntoGroupStateKey` exists to force every key through the operator-state
/// framing, so this wrapper frames the test's keys exactly as an operator would.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestKey(String);

impl TestKey {
	fn new(key: &str) -> Self {
		Self(key.to_string())
	}
}

impl HeapSize for TestKey {
	fn heap_size(&self) -> usize {
		self.0.capacity()
	}
}

/// A composite key, framed the same way. Mirrors an operator keyed on more than one value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestPair(TestKey, TestKey);

impl HeapSize for TestPair {
	fn heap_size(&self) -> usize {
		self.0.heap_size() + self.1.heap_size()
	}
}

impl IntoGroupStateKey for &TestPair {
	fn into_group_state_key(self) -> GroupStateKey {
		let mut suffix = Vec::with_capacity(self.0.0.len() + self.1.0.len() + 1);
		suffix.extend_from_slice(self.0.0.as_bytes());
		suffix.push(0xFF);
		suffix.extend_from_slice(self.1.0.as_bytes());
		GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, suffix)
	}
}

impl IntoGroupStateKey for &TestKey {
	fn into_group_state_key(self) -> GroupStateKey {
		GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, self.0.as_bytes())
	}
}

#[operator_state]
#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
struct CounterState {
	count: i64,
}

impl HeapSize for CounterState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq)]
struct SumState {
	total: i64,
}

impl HeapSize for SumState {
	fn heap_size(&self) -> usize {
		0
	}
}

/// Exists only so the harness can hand out a real `FFIOperatorContext`; the cache, not the operator, is under test.
struct PassthroughOperator;

impl OperatorMetadata for PassthroughOperator {
	const NAME: &'static str = "passthrough";
	const API: u32 = 1;
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "Pass-through operator for testing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl FFIOperator for PassthroughOperator {
	fn new(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, _ctx: &mut FFIOperatorContext, _input: BorrowedChange<'_>) -> Result<()> {
		Ok(())
	}
}

#[test]
fn test_cache_set_and_get() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("test_key");
	let value = CounterState {
		count: 42,
	};

	let mut ctx = harness.create_operator_context();
	cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");

	assert!(cache.is_cached(&key));

	let mut ctx = harness.create_operator_context();
	let retrieved = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
	assert_eq!(retrieved, Some(value));
}

#[test]
fn test_cache_flush_persists_to_ffi() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("persist_key");
	let value = CounterState {
		count: 100,
	};

	// Set only marks the key dirty; flush is the sole point at which state reaches host storage.
	let mut ctx = harness.create_operator_context();
	cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
	assert_eq!(harness.state().len(), 0, "Set must not write through pre-flush");

	let mut ctx = harness.create_operator_context();
	cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	assert!(harness.state().len() > 0, "State should be persisted after flush");
}

#[test]
fn test_cache_get_or_default_creates_default() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("new_key");

	let mut ctx = harness.create_operator_context();
	let result = cache.get_or_default(&mut OperatorContextStore(&mut ctx), &key).expect("get_or_default failed");

	assert_eq!(result.count, 0);
}

#[test]
fn test_cache_get_or_default_returns_existing() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("existing_key");
	let value = CounterState {
		count: 50,
	};

	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
	}

	{
		let mut ctx = harness.create_operator_context();
		let result =
			cache.get_or_default(&mut OperatorContextStore(&mut ctx), &key).expect("get_or_default failed");

		assert_eq!(result.count, 50, "Should return existing value, not default");
	}
}

#[test]
fn test_cache_update() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("counter");

	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut OperatorContextStore(&mut ctx), &key, |s| {
				s.count += 10;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.count, 10);
	}

	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut OperatorContextStore(&mut ctx), &key, |s| {
				s.count += 5;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.count, 15);
	}

	assert!(cache.is_cached(&key));
}

#[test]
fn test_cache_drop() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("drop_key");
	let value = CounterState {
		count: 42,
	};

	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}

	assert!(cache.is_cached(&key));
	assert!(harness.state().len() > 0);

	{
		let mut ctx = harness.create_operator_context();
		cache.remove(&mut OperatorContextStore(&mut ctx), &key).expect("Drop failed");
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}

	assert!(!cache.is_cached(&key));

	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, None);
	}
}

#[test]
fn test_cache_invalidate_only_clears_cache() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("invalidate_key");
	let value = CounterState {
		count: 77,
	};

	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}

	assert!(cache.is_cached(&key));

	cache.invalidate(&key);

	assert!(!cache.is_cached(&key));

	// Invalidate must clear the cache entry only, leaving host storage intact for the next get to reload.
	{
		let mut ctx = harness.create_operator_context();
		let retrieved = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(retrieved, Some(value));
	}

	assert!(cache.is_cached(&key));
}

#[test]
fn test_cache_clear_cache() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());

	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = TestKey::new(&format!("key_{}", i));
			let value = CounterState {
				count: i,
			};
			cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		}
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}

	assert_eq!(cache.len(), 3);

	cache.clear_cache();

	assert!(cache.is_empty());

	// clear_cache drops resident entries only; host storage still holds every flushed value.
	{
		let mut ctx = harness.create_operator_context();
		let result =
			cache.get(&mut OperatorContextStore(&mut ctx), &TestKey::new("key_0")).expect("Get failed");
		assert!(result.is_some(), "FFI state should still exist");
	}
}

#[test]
fn test_cache_multiple_keys() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, SumState> = StateCache::new(test_pool());

	{
		let mut ctx = harness.create_operator_context();
		for i in 0..5 {
			let key = TestKey::new(&format!("sum_{}", i));
			let value = SumState {
				total: i * 10,
			};
			cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		}
	}

	assert_eq!(cache.len(), 5);

	{
		let mut ctx = harness.create_operator_context();
		for i in 0..5 {
			let key = TestKey::new(&format!("sum_{}", i));
			let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
			assert_eq!(
				result,
				Some(SumState {
					total: i * 10
				})
			);
		}
	}
}

#[test]
fn test_cache_lru_eviction() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let pool = test_pool();
	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(pool.clone());

	// Pinning the budget to the resident footprint of three clean entries is what forces the fourth insert to
	// evict rather than simply grow the pool.
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = TestKey::new(&format!("key_{}", i));
			let value = CounterState {
				count: i,
			};
			cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		}
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}
	pool.set_budget(pool.snapshot().resident);

	assert_eq!(cache.len(), 3);
	assert!(cache.is_cached(&TestKey::new("key_0")));
	assert!(cache.is_cached(&TestKey::new("key_1")));
	assert!(cache.is_cached(&TestKey::new("key_2")));

	{
		let mut ctx = harness.create_operator_context();
		let key = TestKey::new("key_3");
		let value = CounterState {
			count: 3,
		};
		cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}

	assert!(!cache.is_cached(&TestKey::new("key_0")), "key_0 should be evicted");
	assert!(cache.is_cached(&TestKey::new("key_3")), "key_3 should be cached");
	assert_eq!(cache.len(), 3);

	// Eviction must drop the cache entry only: the value is still in host storage and a get reloads it.
	{
		let mut ctx = harness.create_operator_context();
		let result =
			cache.get(&mut OperatorContextStore(&mut ctx), &TestKey::new("key_0")).expect("Get failed");
		assert_eq!(
			result,
			Some(CounterState {
				count: 0
			}),
			"key_0 should still exist in FFI"
		);
	}
}

#[test]
fn test_cache_lru_access_updates_order() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let pool = test_pool();
	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(pool.clone());

	// Only clean entries are evictable, so the flush is what makes the pinned budget bite on the next insert.
	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = TestKey::new(&format!("key_{}", i));
			let value = CounterState {
				count: i,
			};
			cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		}
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}
	pool.set_budget(pool.snapshot().resident);

	// Touching key_0 must move it off the eviction front, leaving key_1 as least recently used.
	{
		let mut ctx = harness.create_operator_context();
		cache.get(&mut OperatorContextStore(&mut ctx), &TestKey::new("key_0")).expect("Get failed");
	}

	{
		let mut ctx = harness.create_operator_context();
		let key = TestKey::new("key_3");
		let value = CounterState {
			count: 3,
		};
		cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
	}

	assert!(cache.is_cached(&TestKey::new("key_0")), "key_0 should be cached (recently accessed)");
	assert!(!cache.is_cached(&TestKey::new("key_1")), "key_1 should be evicted (LRU)");
	assert!(cache.is_cached(&TestKey::new("key_2")));
	assert!(cache.is_cached(&TestKey::new("key_3")));
}

#[test]
fn test_cache_tuple_keys() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestPair, SumState> = StateCache::new(test_pool());

	let key1 = TestPair(TestKey::new("base"), TestKey::new("quote"));
	let key2 = TestPair(TestKey::new("foo"), TestKey::new("bar"));
	let value1 = SumState {
		total: 100,
	};
	let value2 = SumState {
		total: 200,
	};

	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut OperatorContextStore(&mut ctx), &key1, &value1).expect("Set failed");
		cache.set(&mut OperatorContextStore(&mut ctx), &key2, &value2).expect("Set failed");
	}

	assert!(cache.is_cached(&key1));
	assert!(cache.is_cached(&key2));

	{
		let mut ctx = harness.create_operator_context();
		let result1 = cache.get(&mut OperatorContextStore(&mut ctx), &key1).expect("Get failed");
		let result2 = cache.get(&mut OperatorContextStore(&mut ctx), &key2).expect("Get failed");
		assert_eq!(result1, Some(value1));
		assert_eq!(result2, Some(value2));
	}
}

#[test]
fn test_cache_tuple_key_update() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestPair, SumState> = StateCache::new(test_pool());
	let key = TestPair(TestKey::new("account"), TestKey::new("balance"));

	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut OperatorContextStore(&mut ctx), &key, |s| {
				s.total += 500;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.total, 500);
	}

	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.update(&mut OperatorContextStore(&mut ctx), &key, |s| {
				s.total += 250;
				Ok(())
			})
			.expect("Update failed");

		assert_eq!(result.total, 750);
	}
}

#[test]
fn test_cache_capacity() {
	let cache: StateCache<TestKey, CounterState> =
		StateCache::new(OperatorStateBudgetHandle::new(ByteSize::from_bytes(100)));
	assert_eq!(cache.capacity(), ByteSize::from_bytes(100));
}

#[test]
fn test_cache_len_and_is_empty() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());

	assert!(cache.is_empty());
	assert_eq!(cache.len(), 0);

	{
		let mut ctx = harness.create_operator_context();
		for i in 0..3 {
			let key = TestKey::new(&format!("key_{}", i));
			let value = CounterState {
				count: i,
			};
			cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		}
	}

	assert!(!cache.is_empty());
	assert_eq!(cache.len(), 3);
}

#[test]
fn test_cache_miss_then_hit() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());
	let key = TestKey::new("miss_hit_key");
	let value = CounterState {
		count: 123,
	};

	// Flushing first is what makes the later invalidate a genuine miss-then-reload rather than a lost write.
	{
		let mut ctx = harness.create_operator_context();
		cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
		cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	}

	cache.invalidate(&key);
	assert!(!cache.is_cached(&key));

	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, Some(value.clone()));
	}

	assert!(cache.is_cached(&key));

	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, Some(value));
	}
}

#[test]
fn test_cache_with_operator_apply() {
	let mut harness =
		FFIOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	// StateCache is !Send + !Sync, so it has to live outside the operator rather than in its state.
	let mut cache: StateCache<TestKey, CounterState> = StateCache::new(test_pool());

	let input = TestChangeBuilder::new()
		.insert_row(1, vec![Value::Int8(10i64)])
		.insert_row(2, vec![Value::Int8(20i64)])
		.build();

	{
		let mut ctx = harness.create_operator_context();
		let diff_count = input.diffs.len() as i64;
		cache.update(&mut OperatorContextStore(&mut ctx), &TestKey::new("event_counter"), |s| {
			s.count += diff_count;
			Ok(())
		})
		.expect("Update failed");
	}

	let input2 = TestChangeBuilder::new().insert_row(3, vec![Value::Int8(30i64)]).build();

	{
		let mut ctx = harness.create_operator_context();
		let diff_count = input2.diffs.len() as i64;
		cache.update(&mut OperatorContextStore(&mut ctx), &TestKey::new("event_counter"), |s| {
			s.count += diff_count;
			Ok(())
		})
		.expect("Update failed");
	}

	{
		let mut ctx = harness.create_operator_context();
		let result = cache
			.get(&mut OperatorContextStore(&mut ctx), &TestKey::new("event_counter"))
			.expect("Get failed");
		assert_eq!(
			result,
			Some(CounterState {
				count: 3
			})
		);
	}
}
