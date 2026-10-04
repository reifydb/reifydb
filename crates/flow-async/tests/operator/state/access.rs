// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	actors::pending::PendingWrite,
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::suffix_width_of,
		state::{GroupStateKey, IntoGroupStateKey, KeyspaceId},
	},
	metrics::heap::HeapSize,
};
use reifydb_flow_async::{
	operator::{
		host::TxnHostContext,
		state_access::{get, get_or_default, set},
	},
	transaction::FlowTransaction,
};
use reifydb_macro::operator_state;
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::flow::operator::{mount::context::InProcessContext, windowed::guest_as_host::GuestAsHost};
use reifydb_testing_sdk::in_process::transaction::TestFlowTransaction;

const OPERATOR: OperatorId = OperatorId(1);

/// A bare `String` cannot be a state key: `IntoGroupStateKey` exists to force every key through the operator-state
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
		accumulator_key(&suffix)
	}
}

impl IntoGroupStateKey for &TestKey {
	fn into_group_state_key(self) -> GroupStateKey {
		accumulator_key(self.0.as_bytes())
	}
}

fn accumulator_key(id: &[u8]) -> GroupStateKey {
	let width =
		suffix_width_of(KeyspaceId::GUEST_ACCUMULATOR).expect("the guest accumulator keyspace is catalogued");
	assert!(id.len() <= width, "a fixture id must fit the keyspace's slot width");
	let mut slot = id.to_vec();
	slot.resize(width, 0);
	GroupStateKey::root(KeyspaceId::GUEST_ACCUMULATOR, slot)
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

struct Host {
	txn: TestFlowTransaction,
}

impl Host {
	fn new() -> Self {
		Self {
			txn: TestFlowTransaction::new(CommitVersion(1), Clock::Mock(MockClock::new(0))),
		}
	}

	fn snapshot_state(&self) -> HashMap<EncodedKey, EncodedBytes> {
		self.txn.pending()
			.iter_sorted()
			.filter_map(|(key, write)| match write {
				PendingWrite::Set(bytes) => Some((key.clone(), bytes.clone())),
				PendingWrite::Remove {
					..
				} => None,
			})
			.collect()
	}
}

#[test]
fn test_set_and_get() {
	let mut host = Host::new();

	let key = TestKey::new("test_key");
	let value = CounterState {
		count: 42,
	};

	let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
	set(&mut GuestAsHost(&mut ctx), &key, &value).expect("Set failed");

	// Nothing buffers the write, so the value must be in host storage the moment set returns.
	assert_eq!(host.snapshot_state().len(), 1);

	let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
	let retrieved = get(&mut GuestAsHost(&mut ctx), &key).expect("Get failed");
	assert_eq!(retrieved, Some(value));
}

#[test]
fn test_set_persists_to_host_storage_on_the_set_itself() {
	let mut host = Host::new();

	let key = TestKey::new("persist_key");
	let value = CounterState {
		count: 100,
	};

	// Set is the sole point at which state reaches host storage; a guest that never sets writes nothing.
	let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
	set(&mut GuestAsHost(&mut ctx), &key, &value).expect("Set failed");
	let persisted = host.snapshot_state();
	assert_eq!(persisted.len(), 1, "Set must write through to host storage");

	// A later context must observe the same bytes, or the write only reached the guest side.
	let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
	assert_eq!(
		get(&mut GuestAsHost(&mut ctx), &key).expect("Get failed"),
		Some(value),
		"the persisted row must read back across a fresh context"
	);
	assert_eq!(host.snapshot_state(), persisted, "a read must leave host storage byte-identical");
}

#[test]
fn test_get_or_default_creates_default() {
	let mut host = Host::new();

	let key = TestKey::new("new_key");

	let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
	let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
	let result: CounterState = get_or_default(&mut GuestAsHost(&mut ctx), &key).expect("get_or_default failed");

	assert_eq!(result.count, 0);
}

#[test]
fn test_get_or_default_returns_existing() {
	let mut host = Host::new();

	let key = TestKey::new("existing_key");
	let value = CounterState {
		count: 50,
	};

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		set(&mut GuestAsHost(&mut ctx), &key, &value).expect("Set failed");
	}

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		let result: CounterState =
			get_or_default(&mut GuestAsHost(&mut ctx), &key).expect("get_or_default failed");

		assert_eq!(result.count, 50, "Should return existing value, not default");
	}
}

#[test]
fn test_multiple_keys() {
	let mut host = Host::new();

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		for i in 0..5 {
			let key = TestKey::new(&format!("sum_{}", i));
			let value = SumState {
				total: i * 10,
			};
			set(&mut GuestAsHost(&mut ctx), &key, &value).expect("Set failed");
		}
	}

	// Five distinct keys must frame five distinct rows; a collision would silently overwrite.
	assert_eq!(host.snapshot_state().len(), 5);

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		for i in 0..5 {
			let key = TestKey::new(&format!("sum_{}", i));
			let result: Option<SumState> = get(&mut GuestAsHost(&mut ctx), &key).expect("Get failed");
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
fn test_tuple_keys() {
	let mut host = Host::new();

	let key1 = TestPair(TestKey::new("base"), TestKey::new("quote"));
	let key2 = TestPair(TestKey::new("foo"), TestKey::new("bar"));
	let value1 = SumState {
		total: 100,
	};
	let value2 = SumState {
		total: 200,
	};

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		set(&mut GuestAsHost(&mut ctx), &key1, &value1).expect("Set failed");
		set(&mut GuestAsHost(&mut ctx), &key2, &value2).expect("Set failed");
	}

	// Two composite keys must never frame onto one row, otherwise the second set eats the first.
	assert_eq!(host.snapshot_state().len(), 2);

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		let result1 = get(&mut GuestAsHost(&mut ctx), &key1).expect("Get failed");
		let result2 = get(&mut GuestAsHost(&mut ctx), &key2).expect("Get failed");
		assert_eq!(result1, Some(value1));
		assert_eq!(result2, Some(value2));
	}
}

#[test]
fn test_get_reloads_from_host_storage() {
	let mut host = Host::new();

	let key = TestKey::new("miss_hit_key");
	let value = CounterState {
		count: 123,
	};

	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		set(&mut GuestAsHost(&mut ctx), &key, &value).expect("Set failed");
	}

	// A reader that never saw the write can only answer from host storage, never from an in-process copy.
	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		let result = get(&mut GuestAsHost(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, Some(value.clone()));
	}

	// A get must never consume the row, otherwise the next read of the same key strands the operator.
	{
		let mut txn_host = TxnHostContext::new(&mut host.txn, OPERATOR);
		let mut ctx = InProcessContext::new(&mut txn_host, OPERATOR);
		let result = get(&mut GuestAsHost(&mut ctx), &key).expect("Get failed");
		assert_eq!(result, Some(value));
	}
}
