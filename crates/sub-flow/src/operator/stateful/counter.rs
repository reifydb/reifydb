// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{GroupStateKey, Keyspace},
};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_sdk::state::{decode_payload, encode_payload};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::operator::stateful::utils::{state_get, state_set};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CounterDirection {
	#[default]
	Ascending,

	Descending,
}

pub struct Counter {
	node: OperatorId,
	key: GroupStateKey,
	direction: CounterDirection,
}

impl Counter {
	pub fn with_prefix(node: OperatorId, prefix: u8, direction: CounterDirection) -> Self {
		let key = GroupStateKey::node_scoped(Keyspace::NODE_COUNTER, vec![prefix]);
		Self {
			node,
			key,
			direction,
		}
	}

	pub fn with_key(node: OperatorId, key: GroupStateKey, direction: CounterDirection) -> Self {
		Self {
			node,
			key,
			direction,
		}
	}

	pub fn next(&self, txn: &mut FlowTransaction) -> Result<RowNumber> {
		let current = self.load(txn)?;
		let next_value = self.compute_next(current);
		self.save(txn, next_value)?;
		Ok(RowNumber(current))
	}

	pub fn current(&self, txn: &mut FlowTransaction) -> Result<u64> {
		self.load(txn)
	}

	pub fn set(&self, txn: &mut FlowTransaction, value: u64) -> Result<()> {
		self.save(txn, value)
	}

	fn load(&self, txn: &mut FlowTransaction) -> Result<u64> {
		match state_get(self.node, txn, &self.key)? {
			None => Ok(self.default_value()),
			Some(encoded) => Ok(decode_payload::<u64>(&encoded)?),
		}
	}

	fn save(&self, txn: &mut FlowTransaction, value: u64) -> Result<()> {
		let now = txn.clock().now();
		state_set(self.node, txn, &self.key, encode_payload(&value, now)?)?;
		Ok(())
	}

	fn default_value(&self) -> u64 {
		match self.direction {
			CounterDirection::Ascending => 1,
			CounterDirection::Descending => u64::MAX,
		}
	}

	fn compute_next(&self, current: u64) -> u64 {
		match self.direction {
			CounterDirection::Ascending => current.wrapping_add(1),
			CounterDirection::Descending => current.wrapping_sub(1),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;

	use super::*;

	#[test]
	fn test_counter_starts_at_one() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'T', CounterDirection::Ascending);

		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, 1);
	}

	#[test]
	fn test_counter_increments() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'T', CounterDirection::Ascending);

		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		let v3 = counter.next(&mut txn).unwrap();

		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 2);
		assert_eq!(v3.0, 3);
	}

	#[test]
	fn test_counter_persistence() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = OperatorId(1);

		{
			let counter = Counter::with_prefix(node, b'P', CounterDirection::Ascending);
			counter.next(&mut txn).unwrap();
			counter.next(&mut txn).unwrap();
		}

		// A second instance on the same node and prefix resumes the stored sequence.
		{
			let counter = Counter::with_prefix(node, b'P', CounterDirection::Ascending);
			let value = counter.next(&mut txn).unwrap();
			assert_eq!(value.0, 3);
		}
	}

	#[test]
	fn test_counter_current() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'T', CounterDirection::Ascending);

		let current = counter.current(&mut txn).unwrap();
		assert_eq!(current, 1);

		counter.next(&mut txn).unwrap();
		let current = counter.current(&mut txn).unwrap();
		assert_eq!(current, 2);

		// A read must not advance the counter.
		let current_again = counter.current(&mut txn).unwrap();
		assert_eq!(current_again, 2);
	}

	#[test]
	fn test_counter_set() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'T', CounterDirection::Ascending);

		counter.set(&mut txn, 100).unwrap();

		// next() hands back the current value and then advances.
		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, 100);

		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, 101);
	}

	#[test]
	fn test_counter_with_custom_key() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();

		let custom_key = {
			let mut serializer = KeySerializer::new();
			serializer.extend_bytes(b"subscription-id-123");
			GroupStateKey::node_scoped(Keyspace::NODE_COUNTER, serializer.finish().as_ref().to_vec())
		};

		let counter = Counter::with_key(OperatorId(1), custom_key, CounterDirection::Ascending);

		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();

		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 2);
	}

	#[test]
	fn test_multiple_counters_isolated() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let node = OperatorId(1);

		let counter1 = Counter::with_prefix(node, b'A', CounterDirection::Ascending);
		let counter2 = Counter::with_prefix(node, b'B', CounterDirection::Ascending);

		let v1a = counter1.next(&mut txn).unwrap();
		let v2a = counter2.next(&mut txn).unwrap();
		let v1b = counter1.next(&mut txn).unwrap();
		let v2b = counter2.next(&mut txn).unwrap();

		assert_eq!(v1a.0, 1);
		assert_eq!(v2a.0, 1);
		assert_eq!(v1b.0, 2);
		assert_eq!(v2b.0, 2);
	}

	#[test]
	fn test_different_nodes_isolated() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();

		let counter1 = Counter::with_prefix(OperatorId(1), b'X', CounterDirection::Ascending);
		let counter2 = Counter::with_prefix(OperatorId(2), b'X', CounterDirection::Ascending);

		let v1 = counter1.next(&mut txn).unwrap();
		let v2 = counter2.next(&mut txn).unwrap();

		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 1);
	}

	#[test]
	fn test_wrapping_behavior() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();

		// Exhausting the range wraps rather than panicking on overflow.
		let counter = Counter::with_prefix(OperatorId(1), b'W', CounterDirection::Ascending);
		counter.set(&mut txn, u64::MAX).unwrap();
		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		assert_eq!(v1.0, u64::MAX);
		assert_eq!(v2.0, 0);
	}

	#[test]
	fn test_encoded_keys_sort_descending() {
		// Row numbers are handed out ascending but the key encoding inverts them, so newer rows
		// sort first.
		let mut serializer1 = KeySerializer::new();
		serializer1.extend_u64(1u64);
		let key1 = serializer1.finish();

		let mut serializer2 = KeySerializer::new();
		serializer2.extend_u64(2u64);
		let key2 = serializer2.finish();

		assert!(key1 > key2, "encode(1) > encode(2) for descending order");
	}

	#[test]
	fn test_counter_descending_starts_at_max() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'T', CounterDirection::Descending);

		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, u64::MAX);
	}

	#[test]
	fn test_counter_descending_decrements() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'T', CounterDirection::Descending);

		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		let v3 = counter.next(&mut txn).unwrap();

		assert_eq!(v1.0, u64::MAX);
		assert_eq!(v2.0, u64::MAX - 1);
		assert_eq!(v3.0, u64::MAX - 2);
	}

	#[test]
	fn test_counter_descending_wrapping() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let counter = Counter::with_prefix(OperatorId(1), b'W', CounterDirection::Descending);

		// Descending underflow wraps rather than panicking.
		counter.set(&mut txn, 1).unwrap();
		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 0);
		let v3 = counter.next(&mut txn).unwrap();
		assert_eq!(v3.0, u64::MAX);
	}
}
