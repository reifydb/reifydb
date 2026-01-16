// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Row number provider for stable row numbering in stateful operators

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	util::encoding::keycode::serializer::KeySerializer,
};
use reifydb_type::{util::cowvec::CowVec, value::row_number::RowNumber};

use crate::{error::Result, operator::context::OperatorContext};

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

	/// Get or create RowNumbers for a batch of keys
	/// Returns Vec<(RowNumber, is_new)> in the same order as input keys
	/// where is_new indicates if the row number was newly created
	pub fn get_or_create_row_numbers_batch<'a, I>(
		&self,
		ctx: &mut OperatorContext,
		keys: I,
	) -> Result<Vec<(RowNumber, bool)>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let mut results = Vec::new();
		let mut counter = self.load_counter(ctx)?;
		let initial_counter = counter;

		for key in keys {
			// Check if we already have a row number for this key
			let map_key = self.make_map_key(key);
			let internal_key = FlowNodeInternalStateKey::new(self.node, map_key.as_ref().to_vec());

			if let Some(existing_row) = ctx.state().get(&internal_key.encode())? {
				// Key exists, return existing row number
				let bytes = existing_row.as_ref();
				if bytes.len() >= 8 {
					let row_num = u64::from_be_bytes([
						bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
						bytes[7],
					]);
					results.push((RowNumber(row_num), false));
					continue;
				}
			}

			// Key doesn't exist, allocate a new row number
			let new_row_number = RowNumber(counter);

			// Save the mapping from key to row number
			let row_num_bytes = counter.to_be_bytes().to_vec();
			ctx.state().set(&internal_key.encode(), &EncodedValues(CowVec::new(row_num_bytes)))?;

			results.push((new_row_number, true));
			counter += 1;
		}

		// Save the updated counter if we allocated any new row numbers
		if counter != initial_counter {
			self.save_counter(ctx, counter)?;
		}

		Ok(results)
	}

	/// Get or create a RowNumber for a given key
	/// Returns (RowNumber, is_new) where is_new indicates if it was newly created
	pub fn get_or_create_row_number(
		&self,
		ctx: &mut OperatorContext,
		key: &EncodedKey,
	) -> reifydb_type::Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers_batch(ctx, std::iter::once(key))?.into_iter().next().unwrap())
	}

	/// Load the current counter value
	fn load_counter(&self, ctx: &mut OperatorContext) -> reifydb_type::Result<u64> {
		let key = self.make_counter_key();
		let internal_key = FlowNodeInternalStateKey::new(self.node, key.as_ref().to_vec());
		match ctx.state().get(&internal_key.encode())? {
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
	fn save_counter(&self, ctx: &mut OperatorContext, counter: u64) -> Result<()> {
		let key = self.make_counter_key();
		let internal_key = FlowNodeInternalStateKey::new(self.node, key.as_ref().to_vec());
		let value = EncodedValues(CowVec::new(counter.to_be_bytes().to_vec()));
		ctx.state().set(&internal_key.encode(), &value)?;
		Ok(())
	}

	/// Create a key for the counter (node_id added by FlowNodeInternalStateKey wrapper)
	fn make_counter_key(&self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'C'); // 'C' for counter
		EncodedKey::new(serializer.finish())
	}

	/// Create a mapping key for a given encoded key (node_id added by FlowNodeInternalStateKey wrapper)
	fn make_map_key(&self, key: &EncodedKey) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'M'); // 'M' for mapping
		serializer.extend_bytes(key.as_ref());
		EncodedKey::new(serializer.finish())
	}

	/// Remove all row number mappings with the given prefix
	/// This is useful for cleaning up all join results from a specific left row
	pub fn remove_by_prefix(&self, ctx: &mut OperatorContext, key_prefix: &[u8]) -> Result<()> {
		// Create the prefix for scanning (node_id added by FlowNodeInternalStateKey wrapper)
		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'M'); // 'M' for mapping
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		// Wrap with FlowNodeInternalStateKey and scan for all keys with this prefix
		let internal_prefix = FlowNodeInternalStateKey::new(self.node, prefix);
		let prefix_key = internal_prefix.encode();
		let entries = ctx.state().scan_prefix(&prefix_key)?;

		for (key, _) in entries {
			ctx.state().remove(&key)?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use std::collections::HashMap;

	use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
	use reifydb_core::{
		encoded::key::EncodedKey,
		interface::catalog::flow::FlowNodeId,
		key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
		value::column::columns::Columns,
	};
	use reifydb_type::value::{Value, row_number::RowNumber};

	use crate::{
		error::Result,
		flow::FlowChange,
		operator::{FFIOperator, FFIOperatorMetadata, column::OperatorColumnDef, context::OperatorContext},
		state::{FFIRawStatefulOperator, row::RowNumberProvider},
		testing::{harness::TestHarnessBuilder, helpers::encode_key},
	};

	/// Test operator for RowNumberProvider tests
	struct RowNumberTestOperator;

	impl FFIOperatorMetadata for RowNumberTestOperator {
		const NAME: &'static str = "row_number_test";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Test operator for row number provider";
		const INPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}

	impl FFIOperator for RowNumberTestOperator {
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

	impl FFIRawStatefulOperator for RowNumberTestOperator {}

	#[test]
	fn test_first_row_number_starts_at_one() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let key = encode_key("test_key");
		let mut ctx = harness.create_operator_context();
		let (row_num, is_new) = ctx.get_or_create_row_number(&key).unwrap();

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
		let (row_num1, is_new1) = ctx.get_or_create_row_number(&key).unwrap();

		let mut ctx = harness.create_operator_context();
		let (row_num2, is_new2) = ctx.get_or_create_row_number(&key).unwrap();

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
		let (row_num1, _) = ctx.get_or_create_row_number(&key1).unwrap();

		let mut ctx = harness.create_operator_context();
		let (row_num2, _) = ctx.get_or_create_row_number(&key2).unwrap();

		let mut ctx = harness.create_operator_context();
		let (row_num3, _) = ctx.get_or_create_row_number(&key3).unwrap();

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
		let (row_num1, is_new1) = ctx1.get_or_create_row_number(&key).unwrap();

		let mut ctx2 = harness2.create_operator_context();
		let (row_num2, is_new2) = ctx2.get_or_create_row_number(&key).unwrap();

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
		ctx.get_or_create_row_number(&key1).unwrap();

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(&key2).unwrap();

		// New key should continue from where we left off
		let key3 = encode_key("key3");
		let mut ctx = harness.create_operator_context();
		let (row_num3, is_new3) = ctx.get_or_create_row_number(&key3).unwrap();

		assert!(is_new3);
		assert_eq!(row_num3.0, 3);

		// Old keys should still return their original row numbers
		let mut ctx = harness.create_operator_context();
		let (row_num1, is_new1) = ctx.get_or_create_row_number(&key1).unwrap();
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
			let (row_num, is_new) = ctx.get_or_create_row_number(&key).unwrap();
			assert!(is_new);
			assert_eq!(row_num.0, i + 1);
		}

		// Verify a random sample still works correctly
		let key_500 = encode_key("key_500");
		let mut ctx = harness.create_operator_context();
		let (row_num, is_new) = ctx.get_or_create_row_number(&key_500).unwrap();
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
		ctx.get_or_create_row_number(&key_a1).unwrap();

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(&key_a2).unwrap();

		let mut ctx = harness.create_operator_context();
		ctx.get_or_create_row_number(&key_b1).unwrap();

		// Remove all keys with prefix "prefix_a"
		let provider = RowNumberProvider::new(FlowNodeId(1));
		let mut ctx = harness.create_operator_context();
		provider.remove_by_prefix(&mut ctx, b"prefix_a").unwrap();

		// Keys with prefix_a should be new again
		let mut ctx = harness.create_operator_context();
		let (_, is_new_a1) = ctx.get_or_create_row_number(&key_a1).unwrap();

		let mut ctx = harness.create_operator_context();
		let (_, is_new_a2) = ctx.get_or_create_row_number(&key_a2).unwrap();

		// But they'll get new row numbers (continuing from counter)
		assert!(is_new_a1);
		assert!(is_new_a2);

		// Key with prefix_b should still be known
		let mut ctx = harness.create_operator_context();
		let (_, is_new_b1) = ctx.get_or_create_row_number(&key_b1).unwrap();
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
		let (row_num, is_new) = ctx.get_or_create_row_number(&empty_key).unwrap();
		assert!(is_new);
		assert_eq!(row_num.0, 1);

		// Should work for duplicate empty keys too
		let mut ctx = harness.create_operator_context();
		let (row_num2, is_new2) = ctx.get_or_create_row_number(&empty_key).unwrap();
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
		let (row_num, is_new) = ctx.get_or_create_row_number(&binary_key).unwrap();
		assert!(is_new);
		assert_eq!(row_num.0, 1);

		let mut ctx = harness.create_operator_context();
		let (row_num2, is_new2) = ctx.get_or_create_row_number(&binary_key).unwrap();
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
		let (row_num1_first, _) = ctx.get_or_create_row_number(&key1).unwrap();
		assert_eq!(row_num1_first.0, 1);

		// First access key2
		let mut ctx = harness.create_operator_context();
		let (row_num2_first, _) = ctx.get_or_create_row_number(&key2).unwrap();
		assert_eq!(row_num2_first.0, 2);

		// Second access key1 - should return same number
		let mut ctx = harness.create_operator_context();
		let (row_num1_second, is_new1) = ctx.get_or_create_row_number(&key1).unwrap();
		assert!(!is_new1);
		assert_eq!(row_num1_second.0, 1);

		// Second access key2 - should return same number
		let mut ctx = harness.create_operator_context();
		let (row_num2_second, is_new2) = ctx.get_or_create_row_number(&key2).unwrap();
		assert!(!is_new2);
		assert_eq!(row_num2_second.0, 2);
	}

	#[test]
	fn test_counter_key_uniqueness_per_node() {
		// Counter keys for different nodes should be different after wrapping with FlowNodeInternalStateKey
		let provider1 = RowNumberProvider::new(FlowNodeId(1));
		let provider2 = RowNumberProvider::new(FlowNodeId(2));

		let internal_key1 = provider1.make_counter_key();
		let internal_key2 = provider2.make_counter_key();

		// Internal keys are the same (node_id is added by wrapper)
		assert_eq!(internal_key1, internal_key2);

		// But after wrapping with FlowNodeInternalStateKey, they should be different
		let final_key1 =
			FlowNodeInternalStateKey::new(provider1.node, internal_key1.as_ref().to_vec()).encode();
		let final_key2 =
			FlowNodeInternalStateKey::new(provider2.node, internal_key2.as_ref().to_vec()).encode();

		assert!(!final_key1.is_empty());
		assert!(!final_key2.is_empty());
		assert_ne!(final_key1, final_key2);
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

	#[test]
	fn test_batch_mixed_existing_and_new_keys() {
		let mut harness = TestHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create 3 initial keys to establish existing row numbers
		let key1 = encode_key("batch_key_1");
		let key2 = encode_key("batch_key_2");
		let key3 = encode_key("batch_key_3");

		let mut ctx = harness.create_operator_context();
		let (rn1, _) = provider.get_or_create_row_number(&mut ctx, &key1).unwrap();
		assert_eq!(rn1.0, 1);

		let mut ctx = harness.create_operator_context();
		let (rn2, _) = provider.get_or_create_row_number(&mut ctx, &key2).unwrap();
		assert_eq!(rn2.0, 2);

		let mut ctx = harness.create_operator_context();
		let (rn3, _) = provider.get_or_create_row_number(&mut ctx, &key3).unwrap();
		assert_eq!(rn3.0, 3);

		// Now test batch with mix of existing and new keys
		let key4 = encode_key("batch_key_4");
		let key5 = encode_key("batch_key_5");

		// Batch: [existing key2, new key4, existing key1, new key5, existing key3]
		let batch_keys = vec![&key2, &key4, &key1, &key5, &key3];

		let mut ctx = harness.create_operator_context();
		let results = provider.get_or_create_row_numbers_batch(&mut ctx, batch_keys.into_iter()).unwrap();

		// Verify results are in correct order and have correct values
		assert_eq!(results.len(), 5);

		// key2 (existing) -> row number 2, not new
		assert_eq!(results[0].0.0, 2);
		assert!(!results[0].1);

		// key4 (new) -> row number 4, is new
		assert_eq!(results[1].0.0, 4);
		assert!(results[1].1);

		// key1 (existing) -> row number 1, not new
		assert_eq!(results[2].0.0, 1);
		assert!(!results[2].1);

		// key5 (new) -> row number 5, is new
		assert_eq!(results[3].0.0, 5);
		assert!(results[3].1);

		// key3 (existing) -> row number 3, not new
		assert_eq!(results[4].0.0, 3);
		assert!(!results[4].1);

		// Verify that counter was only incremented by 2 (for key4 and key5)
		// by checking that the next new key gets row number 6
		let key6 = encode_key("batch_key_6");
		let mut ctx = harness.create_operator_context();
		let (rn6, is_new6) = provider.get_or_create_row_number(&mut ctx, &key6).unwrap();
		assert_eq!(rn6.0, 6);
		assert!(is_new6);

		// Verify all mappings are still correct by retrieving them individually
		let mut ctx = harness.create_operator_context();
		let (check_rn4, is_new4) = provider.get_or_create_row_number(&mut ctx, &key4).unwrap();
		assert_eq!(check_rn4.0, 4);
		assert!(!is_new4);

		let mut ctx = harness.create_operator_context();
		let (check_rn5, is_new5) = provider.get_or_create_row_number(&mut ctx, &key5).unwrap();
		assert_eq!(check_rn5.0, 5);
		assert!(!is_new5);
	}
}
