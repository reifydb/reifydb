// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	iter,
	ops::Bound,
};

use reifydb_codec::key::{
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_core::{interface::catalog::flow::FlowNodeId, key::flow_node_internal_state::FlowNodeInternalStateKey};
use reifydb_value::value::row_number::RowNumber;

use crate::{
	error::Result,
	operator::context::{InternalStateApi, OperatorContext},
};

pub struct RowNumberProvider {
	_node: FlowNodeId,
}

impl RowNumberProvider {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			_node: node,
		}
	}

	pub fn get_or_create_row_numbers_batch<'a, O, I>(&self, ctx: &mut O, keys: I) -> Result<Vec<(RowNumber, bool)>>
	where
		O: OperatorContext,
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let map_keys: Vec<EncodedKey> = keys.into_iter().map(|key| self.make_map_key(key)).collect();

		let mut existing: HashMap<Vec<u8>, u64> = HashMap::with_capacity(map_keys.len());
		ctx.internal_state().get_many_visit::<u64>(&map_keys, &mut |map_key, row_num| {
			existing.insert(map_key.as_bytes().to_vec(), row_num);
			Ok(())
		})?;

		let mut distinct_new: HashSet<Vec<u8>> = HashSet::new();
		for map_key in &map_keys {
			let bytes = map_key.as_bytes();
			if !existing.contains_key(bytes) {
				distinct_new.insert(bytes.to_vec());
			}
		}
		let mut next = if distinct_new.is_empty() {
			0
		} else {
			ctx.allocate_row_numbers(distinct_new.len() as u64)?.0
		};

		let mut newly_assigned: HashMap<Vec<u8>, u64> = HashMap::new();
		let mut results = Vec::with_capacity(map_keys.len());
		for map_key in &map_keys {
			let bytes = map_key.as_bytes();
			if let Some(&row_num) = existing.get(bytes).or_else(|| newly_assigned.get(bytes)) {
				results.push((RowNumber(row_num), false));
				continue;
			}

			let row_num = next;
			next += 1;
			ctx.internal_state().set::<u64>(map_key, &row_num)?;
			newly_assigned.insert(bytes.to_vec(), row_num);
			results.push((RowNumber(row_num), true));
		}

		Ok(results)
	}

	pub fn get_or_create_row_number<O: OperatorContext>(
		&self,
		ctx: &mut O,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers_batch(ctx, iter::once(key))?.into_iter().next().unwrap())
	}

	pub fn drop<O: OperatorContext>(&self, ctx: &mut O, key: &EncodedKey) -> Result<()> {
		let map_key = self.make_map_key(key);
		ctx.internal_state().drop(&map_key)
	}

	pub fn drop_below<O: OperatorContext>(&self, ctx: &mut O, upper: &EncodedKey) -> Result<Vec<RowNumber>> {
		let boundary = self.make_map_key(upper);
		let tag_range = EncodedKeyRange::prefix(self.map_prefix().as_ref());
		let end = match &tag_range.end {
			Bound::Included(k) => Bound::Included(k),
			Bound::Excluded(k) => Bound::Excluded(k),
			Bound::Unbounded => Bound::Unbounded,
		};
		let entries = ctx.internal_state().range::<u64>(Bound::Excluded(&boundary), end)?;
		let mut dropped = Vec::with_capacity(entries.len());
		for (map_key, row_number) in entries {
			ctx.internal_state().drop(&map_key)?;
			dropped.push(RowNumber(row_number));
		}
		Ok(dropped)
	}

	fn map_prefix(&self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		serializer.finish()
	}

	fn make_map_key(&self, key: &EncodedKey) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::interface::catalog::flow::FlowNodeId;

	use crate::{
		config::Config,
		error::Result,
		operator::{
			FFIOperator, OperatorMetadata, change::BorrowedChange, column::operator::OperatorColumn,
			context::ffi::FFIOperatorContext,
		},
		state::{RawStatefulOperator, row::RowNumberProvider},
		testing::{harness::FFIOperatorHarnessBuilder, helpers::encode_key},
	};

	struct RowNumberTestOperator;

	impl OperatorMetadata for RowNumberTestOperator {
		const NAME: &'static str = "row_number_test";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Test operator for row number provider";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl FFIOperator for RowNumberTestOperator {
		fn new(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, _ctx: &mut FFIOperatorContext, _input: BorrowedChange<'_>) -> Result<()> {
			Ok(())
		}
	}

	impl RawStatefulOperator for RowNumberTestOperator {}

	#[test]
	fn test_first_row_number_starts_at_one() {
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness1 = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness1");

		let mut harness2 = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
	fn test_empty_key() {
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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
	fn test_batch_mixed_existing_and_new_keys() {
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
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

	#[test]
	fn drop_removes_mapping_but_never_reuses_row_number() {
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = encode_key("victim");
		let mut ctx = harness.create_operator_context();
		let (first, is_new) = provider.get_or_create_row_number(&mut ctx, &key).unwrap();
		assert!(is_new);

		let mut ctx = harness.create_operator_context();
		provider.drop(&mut ctx, &key).unwrap();

		// After the drop the mapping is gone, so the key is minted fresh - and the monotonic
		// counter must keep advancing rather than handing back the dropped number, or a
		// downstream consumer tracking rows by number would conflate two distinct rows.
		let mut ctx = harness.create_operator_context();
		let (second, is_new_again) = provider.get_or_create_row_number(&mut ctx, &key).unwrap();
		assert!(is_new_again, "dropped mapping must be recreated fresh");
		assert_ne!(first.0, second.0, "the dropped row number must not be reused");
	}

	#[test]
	fn drop_below_reclaims_only_keys_under_the_bound() {
		let mut harness = FFIOperatorHarnessBuilder::<RowNumberTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Slot-leading keys, the shape the block operators use: (slot, base, quote).
		let key = |slot: u64| EncodedKey::builder().u64(slot).u32(1u32).u32(2u32).build();
		let mut ctx = harness.create_operator_context();
		let (rn10, _) = provider.get_or_create_row_number(&mut ctx, &key(10)).unwrap();
		let mut ctx = harness.create_operator_context();
		let (rn20, _) = provider.get_or_create_row_number(&mut ctx, &key(20)).unwrap();
		let mut ctx = harness.create_operator_context();
		let (rn30, _) = provider.get_or_create_row_number(&mut ctx, &key(30)).unwrap();

		// Reclaim everything below slot 25: slots 10 and 20, never slot 30.
		let upper = EncodedKey::builder().u64(25u64).u32(0u32).u32(0u32).build();
		let mut ctx = harness.create_operator_context();
		let mut dropped = provider.drop_below(&mut ctx, &upper).unwrap();
		dropped.sort_by_key(|rn| rn.0);
		assert_eq!(dropped, vec![rn10, rn20], "exactly the below-bound mappings are reclaimed");

		// Slot 30 sat above the bound: its mapping survives with the same row number.
		let mut ctx = harness.create_operator_context();
		let (rn30_again, is_new30) = provider.get_or_create_row_number(&mut ctx, &key(30)).unwrap();
		assert!(!is_new30, "slot 30 was above the horizon and must remain mapped");
		assert_eq!(rn30, rn30_again);

		// Slot 10 was reclaimed: re-lookup mints a fresh, non-reused number.
		let mut ctx = harness.create_operator_context();
		let (rn10_again, is_new10) = provider.get_or_create_row_number(&mut ctx, &key(10)).unwrap();
		assert!(is_new10, "reclaimed slot 10 must be recreated fresh");
		assert_ne!(rn10.0, rn10_again.0);
	}
}
