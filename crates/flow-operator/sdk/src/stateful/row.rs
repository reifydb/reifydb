// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Row number provider for stable row numbering in stateful operators

use reifydb_core::{
	EncodedKey,
	interface::FlowNodeId,
	util::{CowVec, encoding::keycode::KeySerializer},
	value::encoded::EncodedValues,
};
use reifydb_type::RowNumber;

use crate::{context::OperatorContext, error::Result, stateful::FFIRawStatefulOperator};

/// Provides stable row numbers for keys with automatic Insert/Update detection
///
/// This component maintains:
/// - A sequential counter for generating new row numbers
/// - A mapping from keys to their assigned row numbers
///
/// When a key is seen for the first time, it gets a new row number and returns
/// true. When a key is seen again, it returns the existing row number and false.
pub struct RowNumberProvider {
	node: FlowNodeId,
}

impl RowNumberProvider {
	/// Create a new RowNumberProvider for the given operator
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}

	/// Get or create a RowNumber for a given key
	/// Returns (RowNumber, is_new) where is_new indicates if it was newly created
	pub fn get_or_create_row_number<O: FFIRawStatefulOperator>(
		&self,
		ctx: &mut OperatorContext,
		operator: &O,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		// Check if we already have a row number for this key
		let map_key = self.make_map_key(key);
		let encoded_map_key = EncodedKey::new(map_key);

		if let Some(existing_row) = operator.state_get(ctx, &encoded_map_key)? {
			// Key exists, return existing row number
			let bytes = existing_row.as_ref();
			if bytes.len() >= 8 {
				let row_num = u64::from_be_bytes([
					bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
				]);
				return Ok((RowNumber(row_num), false));
			}
		}

		// Key doesn't exist, generate a new row number
		let counter = self.load_counter(ctx, operator)?;
		let new_row_number = RowNumber(counter);

		// Save the new counter value
		self.save_counter(ctx, operator, counter + 1)?;

		// Save the mapping from key to row number
		let row_num_bytes = counter.to_be_bytes().to_vec();
		operator.state_set(ctx, &encoded_map_key, &EncodedValues(CowVec::new(row_num_bytes)))?;

		Ok((new_row_number, true))
	}

	/// Load the current counter value
	fn load_counter<O: FFIRawStatefulOperator>(&self, ctx: &mut OperatorContext, operator: &O) -> Result<u64> {
		let key = self.make_counter_key();
		let encoded_key = EncodedKey::new(key);
		match operator.state_get(ctx, &encoded_key)? {
			None => Ok(1), // First time, start at 1
			Some(state_row) => {
				// Parse the stored counter
				let bytes = state_row.as_ref();
				if bytes.len() >= 8 {
					Ok(u64::from_be_bytes([
						bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
						bytes[7],
					]))
				} else {
					Ok(1)
				}
			}
		}
	}

	/// Save the counter value
	fn save_counter<O: FFIRawStatefulOperator>(
		&self,
		ctx: &mut OperatorContext,
		operator: &O,
		counter: u64,
	) -> Result<()> {
		let key = self.make_counter_key();
		let encoded_key = EncodedKey::new(key);
		let value = EncodedValues(CowVec::new(counter.to_be_bytes().to_vec()));
		operator.state_set(ctx, &encoded_key, &value)?;
		Ok(())
	}

	/// Create a key for the counter, including node_id
	fn make_counter_key(&self) -> Vec<u8> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_u8(b'C'); // 'C' for counter
		serializer.finish()
	}

	/// Create a mapping key for a given encoded key, including node_id
	fn make_map_key(&self, key: &EncodedKey) -> Vec<u8> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_u8(b'M'); // 'M' for mapping
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}

	/// Remove all row number mappings with the given prefix
	/// This is useful for cleaning up all join results from a specific left row
	pub fn remove_by_prefix<O: FFIRawStatefulOperator>(
		&self,
		ctx: &mut OperatorContext,
		operator: &O,
		key_prefix: &[u8],
	) -> Result<()> {
		// Create the prefix for scanning
		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_u8(b'M'); // 'M' for mapping
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		// Scan for all keys with this prefix and remove them
		let prefix_key = EncodedKey::new(prefix);
		let entries = operator.state_scan_prefix(ctx, &prefix_key)?;

		for (key, _) in entries {
			operator.state_remove(ctx, &key)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use reifydb_core::{EncodedKey, Row, interface::FlowNodeId};
	use reifydb_type::{RowNumber, Value};

	use crate::{
		FFIOperator, FFIOperatorMetadata, FlowChange,
		context::OperatorContext,
		error::Result,
		stateful::{FFIRawStatefulOperator, RowNumberProvider},
		testing::{TestHarnessBuilder, helpers::encode_key},
	};

	/// Test operator for RowNumberProvider tests
	struct RowNumberTestOperator;

	impl FFIOperatorMetadata for RowNumberTestOperator {
		const NAME: &'static str = "row_number_test";
		const VERSION: u32 = 1;
	}

	impl FFIOperator for RowNumberTestOperator {
		fn new(_operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
			Ok(input)
		}

		fn get_rows(
			&mut self,
			_ctx: &mut OperatorContext,
			_row_numbers: &[RowNumber],
		) -> Result<Vec<Option<Row>>> {
			Ok(vec![])
		}
	}

	impl FFIRawStatefulOperator for RowNumberTestOperator {}

	#[test]
	fn test_first_row_number_starts_at_one() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let key = encode_key("test_key");
		let mut ctx = harness.create_operator_context();
		let (row_num, is_new) = ctx.get_or_create_row_number(harness.operator(), &key).unwrap();

		assert_eq!(row_num.0, 1);
		assert!(is_new);
	}

	#[test]
	fn test_duplicate_key_returns_same_row_number() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let key = encode_key("test_key");

		let mut ctx = harness.create_operator_context();
		let (row_num1, is_new1) = ctx.get_or_create_row_number(harness.operator(), &key).unwrap();

		let mut ctx = harness.create_operator_context();
		let (row_num2, is_new2) = ctx.get_or_create_row_number(harness.operator(), &key).unwrap();

		assert_eq!(row_num1.0, row_num2.0);
		assert!(is_new1);
		assert!(!is_new2);
	}

	#[test]
	fn test_sequential_numbering() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let key1 = encode_key("key1");
		let key2 = encode_key("key2");
		let key3 = encode_key("key3");

		let mut ctx = harness.create_operator_context();
		let (row_num1, _) = ctx.get_or_create_row_number(harness.operator(), &key1).unwrap();

		let mut ctx = harness.create_operator_context();
		let (row_num2, _) = ctx.get_or_create_row_number(harness.operator(), &key2).unwrap();

		let mut ctx = harness.create_operator_context();
		let (row_num3, _) = ctx.get_or_create_row_number(harness.operator(), &key3).unwrap();

		assert_eq!(row_num1.0, 1);
		assert_eq!(row_num2.0, 2);
		assert_eq!(row_num3.0, 3);
	}

	#[test]
	fn test_operator_isolation() {
		// Two harnesses with different node IDs share state store but have isolated namespaces
		let mut harness1 = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness1");

		let mut harness2 = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(2))
			.build()
			.expect("Failed to build harness2");

		let key = encode_key("same_key");

		let mut ctx1 = harness1.create_operator_context();
		let (row_num1, is_new1) = ctx1.get_or_create_row_number(harness1.operator(), &key).unwrap();

		let mut ctx2 = harness2.create_operator_context();
		let (row_num2, is_new2) = ctx2.get_or_create_row_number(harness2.operator(), &key).unwrap();

		// Both should be new because they're from different operators
		assert!(is_new1);
		assert!(is_new2);
		// Both should start at 1
		assert_eq!(row_num1.0, 1);
		assert_eq!(row_num2.0, 1);
	}

	#[test]
	fn test_persistence_across_calls() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Create first few row numbers
		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(harness.operator(), &key1).unwrap();

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(harness.operator(), &key2).unwrap();

		// New key should continue from where we left off
		let key3 = encode_key("key3");
		let mut ctx = harness.create_operator_context();
		let (row_num3, is_new3) = ctx.get_or_create_row_number(harness.operator(), &key3).unwrap();

		assert!(is_new3);
		assert_eq!(row_num3.0, 3);

		// Old keys should still return their original row numbers
		let mut ctx = harness.create_operator_context();
		let (row_num1, is_new1) = ctx.get_or_create_row_number(harness.operator(), &key1).unwrap();
		assert!(!is_new1);
		assert_eq!(row_num1.0, 1);
	}

	#[test]
	fn test_large_scale_row_numbers() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Create 1000 unique keys
		for i in 0..1000 {
			let key = encode_key(format!("key_{}", i));
			let mut ctx = harness.create_operator_context();
			let (row_num, is_new) = ctx.get_or_create_row_number(harness.operator(), &key).unwrap();
			assert!(is_new);
			assert_eq!(row_num.0, i + 1);
		}

		// Verify a random sample still works correctly
		let key_500 = encode_key("key_500");
		let mut ctx = harness.create_operator_context();
		let (row_num, is_new) = ctx.get_or_create_row_number(harness.operator(), &key_500).unwrap();
		assert!(!is_new);
		assert_eq!(row_num.0, 501);
	}

	#[test]
	fn test_remove_by_prefix() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Create keys with different prefixes
		let key_a1 = encode_key("prefix_a_1");
		let key_a2 = encode_key("prefix_a_2");
		let key_b1 = encode_key("prefix_b_1");

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(harness.operator(), &key_a1).unwrap();

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(harness.operator(), &key_a2).unwrap();

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(harness.operator(), &key_b1).unwrap();

		// Remove all keys with prefix "prefix_a"
		let provider = RowNumberProvider::new(FlowNodeId(1));
		let mut ctx = harness.create_operator_context();
		provider.remove_by_prefix(&mut ctx, harness.operator(), b"prefix_a").unwrap();

		// Keys with prefix_a should be new again
		let mut ctx = harness.create_operator_context();
		let (_, is_new_a1) = ctx.get_or_create_row_number(harness.operator(), &key_a1).unwrap();

		let mut ctx = harness.create_operator_context();
		let (_, is_new_a2) = ctx.get_or_create_row_number(harness.operator(), &key_a2).unwrap();

		// But they'll get new row numbers (continuing from counter)
		assert!(is_new_a1);
		assert!(is_new_a2);

		// Key with prefix_b should still be known
		let mut ctx = harness.create_operator_context();
		let (_, is_new_b1) = ctx.get_or_create_row_number(harness.operator(), &key_b1).unwrap();
		assert!(!is_new_b1);
	}

	#[test]
	fn test_empty_key() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let empty_key = encode_key("");

		let mut ctx = harness.create_operator_context();
		let (row_num, is_new) = ctx.get_or_create_row_number(harness.operator(), &empty_key).unwrap();
		assert!(is_new);
		assert_eq!(row_num.0, 1);

		// Should work for duplicate empty keys too
		let mut ctx = harness.create_operator_context();
		let (row_num2, is_new2) = ctx.get_or_create_row_number(harness.operator(), &empty_key).unwrap();
		assert!(!is_new2);
		assert_eq!(row_num2.0, 1);
	}

	#[test]
	fn test_binary_key_data() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Test with binary data including null bytes
		let binary_key = EncodedKey::new(vec![0x00, 0xFF, 0x00, 0xAB, 0xCD]);

		let mut ctx = harness.create_operator_context();
		let (row_num, is_new) = ctx.get_or_create_row_number(harness.operator(), &binary_key).unwrap();
		assert!(is_new);
		assert_eq!(row_num.0, 1);

		let mut ctx = harness.create_operator_context();
		let (row_num2, is_new2) = ctx.get_or_create_row_number(harness.operator(), &binary_key).unwrap();
		assert!(!is_new2);
		assert_eq!(row_num2.0, 1);
	}

	#[test]
	fn test_interleaved_operations() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		// First access key1
		let mut ctx = harness.create_operator_context();
		let (row_num1_first, _) = ctx.get_or_create_row_number(harness.operator(), &key1).unwrap();
		assert_eq!(row_num1_first.0, 1);

		// First access key2
		let mut ctx = harness.create_operator_context();
		let (row_num2_first, _) = ctx.get_or_create_row_number(harness.operator(), &key2).unwrap();
		assert_eq!(row_num2_first.0, 2);

		// Second access key1 - should return same number
		let mut ctx = harness.create_operator_context();
		let (row_num1_second, is_new1) = ctx.get_or_create_row_number(harness.operator(), &key1).unwrap();
		assert!(!is_new1);
		assert_eq!(row_num1_second.0, 1);

		// Second access key2 - should return same number
		let mut ctx = harness.create_operator_context();
		let (row_num2_second, is_new2) = ctx.get_or_create_row_number(harness.operator(), &key2).unwrap();
		assert!(!is_new2);
		assert_eq!(row_num2_second.0, 2);
	}

	#[test]
	fn test_counter_key_uniqueness_per_node() {
		// Counter keys for different nodes should be different
		let provider1 = RowNumberProvider::new(FlowNodeId(1));
		let provider2 = RowNumberProvider::new(FlowNodeId(2));

		let key1 = provider1.make_counter_key();
		let key2 = provider2.make_counter_key();

		assert!(!key1.is_empty());
		assert!(!key2.is_empty());
		assert_ne!(key1, key2);
	}

	#[test]
	fn test_map_key_uniqueness() {
		let provider = RowNumberProvider::new(FlowNodeId(42));
		let original_key1 = encode_key("test1");
		let original_key2 = encode_key("test2");

		let map_key1 = provider.make_map_key(&original_key1);
		let map_key2 = provider.make_map_key(&original_key2);

		// Map keys should be non-empty and different for different inputs
		assert!(!map_key1.is_empty());
		assert!(!map_key2.is_empty());
		assert_ne!(map_key1, map_key2);

		// Same original key should produce same map key
		let map_key1_again = provider.make_map_key(&original_key1);
		assert_eq!(map_key1, map_key1_again);
	}

	#[test]
	fn test_counter_key_vs_map_key_separation() {
		// Counter key and map key should never collide
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let counter_key = provider.make_counter_key();
		let map_key = provider.make_map_key(&EncodedKey::new(Vec::new()));

		// Even with an empty original key, they should be different
		assert_ne!(counter_key, map_key);
	}
}
