// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupStateKey, IntoGroupStateKey, Keyspace},
	metrics::heap::HeapSize,
	state::cache::StateCache,
};
use reifydb_macro::operator_state;
use reifydb_sdk::{
	error::Result,
	operator::{
		ExternCOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
		context::extern_c::ExternCOperatorContext, windowed::bridge::OperatorContextStore,
	},
};
use reifydb_testing_sdk::{builders::TestChangeBuilder, harness::ExternCOperatorHarnessBuilder};
use reifydb_value::{config::Config, value::Value};

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
		GroupStateKey::root(Keyspace::CUSTOM, suffix)
	}
}

impl IntoGroupStateKey for &TestKey {
	fn into_group_state_key(self) -> GroupStateKey {
		GroupStateKey::root(Keyspace::CUSTOM, self.0.as_bytes())
	}
}

#[operator_state]
#[derive(Default, Clone, Debug, PartialEq)]
struct CounterState {
	count: i64,
}

impl HeapSize for CounterState {
	fn heap_size(&self) -> usize {
		0
	}
}

#[operator_state]
#[derive(Default, Clone, Debug, PartialEq)]
struct SumState {
	total: i64,
}

impl HeapSize for SumState {
	fn heap_size(&self) -> usize {
		0
	}
}

/// Exists only so the harness can hand out a real `ExternCOperatorContext`; the cache, not the operator, is under test.
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

impl ExternCOperator for PassthroughOperator {
	fn new(_operator_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, _ctx: &mut ExternCOperatorContext, _input: BorrowedChange<'_>) -> Result<()> {
		Ok(())
	}
}

#[test]
fn test_cache_set_and_get() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();
	let key = TestKey::new("test_key");
	let value = CounterState {
		count: 42,
	};

	let mut ctx = harness.create_operator_context();
	cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");

	// Nothing buffers the write, so the value must be in host storage the moment set returns.
	assert_eq!(harness.state().len(), 1);

	let mut ctx = harness.create_operator_context();
	let retrieved = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
	assert_eq!(retrieved, Some(value));
}

#[test]
fn test_cache_set_persists_to_extern_c_and_flush_is_inert() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();
	let key = TestKey::new("persist_key");
	let value = CounterState {
		count: 100,
	};

	// Set is the sole point at which state reaches host storage; nothing is left pending for flush.
	let mut ctx = harness.create_operator_context();
	cache.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
	let persisted = harness.snapshot_state();
	assert_eq!(persisted.len(), 1, "Set must write through to host storage");

	// Operators still call flush; re-issuing the write here would stamp every key a second time.
	let mut ctx = harness.create_operator_context();
	cache.flush(&mut OperatorContextStore(&mut ctx)).expect("Flush failed");
	assert_eq!(harness.snapshot_state(), persisted, "Flush must leave host storage byte-identical");
}

#[test]
fn test_cache_get_or_default_creates_default() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();
	let key = TestKey::new("new_key");

	let mut ctx = harness.create_operator_context();
	let result = cache.get_or_default(&mut OperatorContextStore(&mut ctx), &key).expect("get_or_default failed");

	assert_eq!(result.count, 0);
}

#[test]
fn test_cache_get_or_default_returns_existing() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();
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
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();
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

	// The returned value must agree with host storage, otherwise the second update read a stale base.
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(
			result,
			Some(CounterState {
				count: 15
			})
		);
	}
}

#[test]
fn test_cache_multiple_keys() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestKey, SumState> = StateCache::new();

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

	// Five distinct keys must frame five distinct rows; a collision would silently overwrite.
	assert_eq!(harness.state().len(), 5);

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
fn test_cache_tuple_keys() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestPair, SumState> = StateCache::new();

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

	// Two composite keys must never frame onto one row, otherwise the second set eats the first.
	assert_eq!(harness.state().len(), 2);

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
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let mut cache: StateCache<TestPair, SumState> = StateCache::new();
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
fn test_cache_get_reloads_from_host_storage() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	let key = TestKey::new("miss_hit_key");
	let value = CounterState {
		count: 123,
	};

	{
		let mut writer: StateCache<TestKey, CounterState> = StateCache::new();
		let mut ctx = harness.create_operator_context();
		writer.set(&mut OperatorContextStore(&mut ctx), &key, &value).expect("Set failed");
	}

	// A reader that never saw the write can only answer from host storage, never from an in-process copy.
	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();

	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, Some(value.clone()));
	}

	// A get must never consume the row, otherwise the next read of the same key strands the operator.
	{
		let mut ctx = harness.create_operator_context();
		let result = cache.get(&mut OperatorContextStore(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, Some(value));
	}
}

#[test]
fn test_cache_with_operator_apply() {
	let mut harness =
		ExternCOperatorHarnessBuilder::<PassthroughOperator>::new().build().expect("Failed to build harness");

	// Every apply gets a fresh context, so the count must accumulate through host storage, never restart at zero.
	let mut cache: StateCache<TestKey, CounterState> = StateCache::new();

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
