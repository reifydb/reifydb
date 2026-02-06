// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::catalog::flow::FlowNodeId,
	util::encoding::keycode::serializer::KeySerializer,
};
use reifydb_type::{util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{
	operator::stateful::utils::{internal_state_get, internal_state_set},
	transaction::FlowTransaction,
};

/// Direction for counter increment/decrement
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterDirection {
	/// Count upwards: 1, 2, 3, ...
	Ascending,
	/// Count downwards: MAX, MAX-1, MAX-2, ...
	Descending,
}

impl Default for CounterDirection {
	fn default() -> Self {
		CounterDirection::Ascending
	}
}

pub struct Counter {
	node: FlowNodeId,
	key: EncodedKey,
	direction: CounterDirection,
}

impl Counter {
	/// Create counter with single-byte prefix key
	pub fn with_prefix(node: FlowNodeId, prefix: u8, direction: CounterDirection) -> Self {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(prefix);
		let key = EncodedKey::new(serializer.finish());
		Self {
			node,
			key,
			direction,
		}
	}

	/// Create counter with custom key (e.g., subscription ID)
	pub fn with_key(node: FlowNodeId, key: EncodedKey, direction: CounterDirection) -> Self {
		Self {
			node,
			key,
			direction,
		}
	}

	/// Get next counter value (atomically: returns current, then increments/decrements)
	pub fn next(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<RowNumber> {
		let current = self.load(txn)?;
		let next_value = self.compute_next(current);
		self.save(txn, next_value)?;
		Ok(RowNumber(current))
	}

	/// Get current value without modifying
	pub fn current(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<u64> {
		self.load(txn)
	}

	/// Set to specific value
	pub fn set(&self, txn: &mut FlowTransaction, value: u64) -> reifydb_type::Result<()> {
		self.save(txn, value)
	}

	// Internal methods
	fn load(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<u64> {
		match internal_state_get(self.node, txn, &self.key)? {
			None => Ok(self.default_value()),
			Some(encoded) => {
				let bytes = encoded.as_ref();
				if bytes.len() >= 8 {
					Ok(u64::from_be_bytes([
						bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
						bytes[7],
					]))
				} else {
					Ok(self.default_value())
				}
			}
		}
	}

	fn save(&self, txn: &mut FlowTransaction, value: u64) -> reifydb_type::Result<()> {
		let bytes = value.to_be_bytes().to_vec();
		internal_state_set(self.node, txn, &self.key, EncodedValues(CowVec::new(bytes)))?;
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
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::common::CommitVersion;
	use reifydb_transaction::interceptor::interceptors::Interceptors;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	#[test]
	fn test_counter_starts_at_one() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'T', CounterDirection::Ascending);

		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, 1);
	}

	#[test]
	fn test_counter_increments() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'T', CounterDirection::Ascending);

		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		let v3 = counter.next(&mut txn).unwrap();

		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 2);
		assert_eq!(v3.0, 3);
	}

	#[test]
	fn test_counter_persistence() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let node = FlowNodeId(1);

		// First counter instance
		{
			let counter = Counter::with_prefix(node, b'P', CounterDirection::Ascending);
			counter.next(&mut txn).unwrap();
			counter.next(&mut txn).unwrap();
		}

		// Second counter instance with same node and prefix
		{
			let counter = Counter::with_prefix(node, b'P', CounterDirection::Ascending);
			let value = counter.next(&mut txn).unwrap();
			// Should continue from where we left off
			assert_eq!(value.0, 3);
		}
	}

	#[test]
	fn test_counter_current() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'T', CounterDirection::Ascending);

		// First call returns default (1)
		let current = counter.current(&mut txn).unwrap();
		assert_eq!(current, 1);

		// After next(), current should reflect the saved value
		counter.next(&mut txn).unwrap();
		let current = counter.current(&mut txn).unwrap();
		assert_eq!(current, 2);

		// current() should not modify the counter
		let current_again = counter.current(&mut txn).unwrap();
		assert_eq!(current_again, 2);
	}

	#[test]
	fn test_counter_set() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'T', CounterDirection::Ascending);

		// Set to a specific value
		counter.set(&mut txn, 100).unwrap();

		// Next should return 100 and advance to 101
		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, 100);

		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, 101);
	}

	#[test]
	fn test_counter_with_custom_key() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());

		// Create a custom key
		let custom_key = {
			let mut serializer = KeySerializer::new();
			serializer.extend_bytes(b"subscription-id-123");
			EncodedKey::new(serializer.finish())
		};

		let counter = Counter::with_key(FlowNodeId(1), custom_key, CounterDirection::Ascending);

		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();

		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 2);
	}

	#[test]
	fn test_multiple_counters_isolated() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let node = FlowNodeId(1);

		// Different prefixes should be isolated
		let counter1 = Counter::with_prefix(node, b'A', CounterDirection::Ascending);
		let counter2 = Counter::with_prefix(node, b'B', CounterDirection::Ascending);

		let v1a = counter1.next(&mut txn).unwrap();
		let v2a = counter2.next(&mut txn).unwrap();
		let v1b = counter1.next(&mut txn).unwrap();
		let v2b = counter2.next(&mut txn).unwrap();

		// Each counter should maintain its own sequence
		assert_eq!(v1a.0, 1);
		assert_eq!(v2a.0, 1);
		assert_eq!(v1b.0, 2);
		assert_eq!(v2b.0, 2);
	}

	#[test]
	fn test_different_nodes_isolated() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());

		// Same prefix, different nodes should be isolated
		let counter1 = Counter::with_prefix(FlowNodeId(1), b'X', CounterDirection::Ascending);
		let counter2 = Counter::with_prefix(FlowNodeId(2), b'X', CounterDirection::Ascending);

		let v1 = counter1.next(&mut txn).unwrap();
		let v2 = counter2.next(&mut txn).unwrap();

		// Each node should have its own counter
		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 1);
	}

	#[test]
	fn test_wrapping_behavior() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());

		// Test wrapping from MAX to 0
		let counter = Counter::with_prefix(FlowNodeId(1), b'W', CounterDirection::Ascending);
		counter.set(&mut txn, u64::MAX).unwrap();
		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		assert_eq!(v1.0, u64::MAX);
		assert_eq!(v2.0, 0); // Wraps to 0
	}

	#[test]
	fn test_encoded_keys_sort_descending() {
		// Verify that when counter values are encoded as keys,
		// they sort in descending order
		let mut serializer1 = KeySerializer::new();
		serializer1.extend_u64(1u64);
		let key1 = serializer1.finish();

		let mut serializer2 = KeySerializer::new();
		serializer2.extend_u64(2u64);
		let key2 = serializer2.finish();

		// Key from value 1 should be > key from value 2
		// (descending order in key space)
		assert!(key1 > key2, "encode(1) > encode(2) for descending order");
	}

	#[test]
	fn test_counter_descending_starts_at_max() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'T', CounterDirection::Descending);

		let value = counter.next(&mut txn).unwrap();
		assert_eq!(value.0, u64::MAX);
	}

	#[test]
	fn test_counter_descending_decrements() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'T', CounterDirection::Descending);

		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		let v3 = counter.next(&mut txn).unwrap();

		assert_eq!(v1.0, u64::MAX);
		assert_eq!(v2.0, u64::MAX - 1);
		assert_eq!(v3.0, u64::MAX - 2);
	}

	#[test]
	fn test_counter_descending_wrapping() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let counter = Counter::with_prefix(FlowNodeId(1), b'W', CounterDirection::Descending);

		// Set to 1, next should give 1, then wrap to 0, then MAX
		counter.set(&mut txn, 1).unwrap();
		let v1 = counter.next(&mut txn).unwrap();
		let v2 = counter.next(&mut txn).unwrap();
		assert_eq!(v1.0, 1);
		assert_eq!(v2.0, 0);
		let v3 = counter.next(&mut txn).unwrap();
		assert_eq!(v3.0, u64::MAX); // Wraps from 0 to MAX
	}
}
