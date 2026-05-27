// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB
use std::iter::once;

use reifydb_core::{
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::catalog::flow::FlowNodeId,
	key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	util::encoding::keycode::serializer::KeySerializer,
};
use reifydb_sdk::state::{decode_payload, encode_payload};
use reifydb_type::{Result, value::row_number::RowNumber};

use crate::{
	operator::stateful::{
		counter::{Counter, CounterDirection},
		utils::{internal_state_drop, internal_state_get, internal_state_set},
	},
	transaction::FlowTransaction,
};

pub struct RowNumberProvider {
	node: FlowNodeId,
	counter: Counter,
}

impl RowNumberProvider {
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
			counter: Counter::with_prefix(
				node,
				FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG,
				CounterDirection::Ascending,
			),
		}
	}

	pub fn get_or_create_row_numbers<'a, I>(
		&self,
		txn: &mut FlowTransaction,
		keys: I,
	) -> Result<Vec<(RowNumber, bool)>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let now = txn.clock().now_nanos();
		let mut results = Vec::new();

		for key in keys {
			let map_key = self.make_map_key(key);

			if let Some(existing_row) = internal_state_get(self.node, txn, &map_key)? {
				results.push((RowNumber(decode_payload::<u64>(&existing_row)?), false));
				continue;
			}

			let new_row_number = self.counter.next(txn)?;

			internal_state_set(self.node, txn, &map_key, encode_payload(&new_row_number.0, now)?)?;

			results.push((new_row_number, true));
		}

		Ok(results)
	}

	pub fn get_or_create_row_number(
		&self,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(txn, once(key))?.into_iter().next().unwrap())
	}

	pub fn get_row_number(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<Option<RowNumber>> {
		let map_key = self.make_map_key(key);
		match internal_state_get(self.node, txn, &map_key)? {
			Some(existing_row) => Ok(Some(RowNumber(decode_payload::<u64>(&existing_row)?))),
			None => Ok(None),
		}
	}

	pub fn remove_for_key(&self, txn: &mut FlowTransaction, key: &EncodedKey) -> Result<bool> {
		let map_key = self.make_map_key(key);
		if internal_state_get(self.node, txn, &map_key)?.is_none() {
			return Ok(false);
		}
		internal_state_drop(self.node, txn, &map_key)?;
		Ok(true)
	}

	fn make_map_key(&self, key: &EncodedKey) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}

	pub fn remove_by_prefix(&self, txn: &mut FlowTransaction, key_prefix: &[u8]) -> Result<()> {
		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG);
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		let state_prefix = FlowNodeInternalStateKey::new(self.node, prefix.clone());
		let full_range = EncodedKeyRange::prefix(&state_prefix.encode());

		let keys_to_remove = {
			let stream = txn.range(full_range, 1024);
			let mut keys = Vec::new();
			for result in stream {
				let multi = result?;
				keys.push(multi.key);
			}
			keys
		};

		for key in keys_to_remove {
			txn.remove(&key)?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::common::CommitVersion;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	#[test]
	fn test_first_row_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("first");
		let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

		assert_eq!(row_num.0, 1);
		assert!(is_new);
	}

	#[test]
	fn test_duplicate_key_same_row_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("duplicate");

		// First call - should create new
		let (row_num1, is_new1) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(row_num1.0, 1);
		assert!(is_new1);

		// Second call with same key - should return existing
		let (row_num2, is_new2) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(row_num2.0, 1);
		assert!(!is_new2);

		// Row numbers should be the same
		assert_eq!(row_num1, row_num2);
	}

	#[test]
	fn test_sequential_row_numbers() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create multiple unique keys
		for i in 1..=5 {
			let key = test_key(&format!("key_{}", i));
			let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

			assert_eq!(row_num.0, i as u64);
			assert!(is_new);
		}
	}

	#[test]
	fn test_mixed_new_and_existing() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some keys
		let key1 = test_key("mixed_1");
		let key2 = test_key("mixed_2");
		let key3 = test_key("mixed_3");

		// First round - all new
		let (rn1, new1) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();
		let (rn2, new2) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		let (rn3, new3) = provider.get_or_create_row_number(&mut txn, &key3).unwrap();

		assert_eq!(rn1.0, 1);
		assert!(new1);
		assert_eq!(rn2.0, 2);
		assert!(new2);
		assert_eq!(rn3.0, 3);
		assert!(new3);

		// Second round - mixed
		let key4 = test_key("mixed_4");
		let (rn2_again, new2_again) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		let (rn4, new4) = provider.get_or_create_row_number(&mut txn, &key4).unwrap();
		let (rn1_again, new1_again) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();

		assert_eq!(rn2_again.0, 2);
		assert!(!new2_again);
		assert_eq!(rn4.0, 4); // Next sequential number
		assert!(new4);
		assert_eq!(rn1_again.0, 1);
		assert!(!new1_again);
	}

	#[test]
	fn test_multiple_providers_isolated() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider1 = RowNumberProvider::new(FlowNodeId(1));
		let provider2 = RowNumberProvider::new(FlowNodeId(2));

		let key = test_key("shared_key");

		// Same key in different providers should get different encoded numbers
		let (rn1, _) = provider1.get_or_create_row_number(&mut txn, &key).unwrap();
		let (rn2, _) = provider2.get_or_create_row_number(&mut txn, &key).unwrap();

		assert_eq!(rn1.0, 1);
		assert_eq!(rn2.0, 1);

		// Add more keys to provider1
		let key2 = test_key("key2");
		let (rn1_2, _) = provider1.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn1_2.0, 2);

		// Provider2 should still be at 1 for new keys
		let (rn2_2, _) = provider2.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn2_2.0, 2);
	}

	#[test]
	fn test_counter_persistence() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some encoded numbers
		for i in 1..=3 {
			let key = test_key(&format!("persist_{}", i));
			let (rn, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert_eq!(rn.0, i as u64);
		}

		// Simulate loading counter again (internally happens in get_or_create)
		let new_key = test_key("persist_new");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &new_key).unwrap();

		// Should continue from where we left off
		assert_eq!(rn.0, 4);
		assert!(is_new);
	}

	#[test]
	fn test_large_row_numbers() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create many encoded numbers
		for i in 1..=1000 {
			let key = test_key(&format!("large_{}", i));
			let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert_eq!(rn.0, i as u64);
			assert!(is_new);
		}

		// Verify we can still retrieve early ones
		let key = test_key("large_1");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(rn.0, 1);
		assert!(!is_new);

		// And continue adding new ones
		let key = test_key("large_1001");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(rn.0, 1001);
		assert!(is_new);
	}

	#[test]
	fn test_mixed_existing_and_new_keys() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create 3 initial keys to establish existing row numbers
		let key1 = test_key("key_1");
		let key2 = test_key("key_2");
		let key3 = test_key("key_3");

		let (rn1, _) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();
		assert_eq!(rn1.0, 1);

		let (rn2, _) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn2.0, 2);

		let (rn3, _) = provider.get_or_create_row_number(&mut txn, &key3).unwrap();
		assert_eq!(rn3.0, 3);

		// Now test batch with mix of existing and new keys
		let key4 = test_key("key_4");
		let key5 = test_key("key_5");

		// Batch: [existing key2, new key4, existing key1, new key5, existing key3]
		let keys = vec![&key2, &key4, &key1, &key5, &key3];

		let results = provider.get_or_create_row_numbers(&mut txn, keys.into_iter()).unwrap();

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
		let key6 = test_key("key_6");
		let (rn6, is_new6) = provider.get_or_create_row_number(&mut txn, &key6).unwrap();
		assert_eq!(rn6.0, 6);
		assert!(is_new6);

		// Verify all mappings are still correct by retrieving them individually
		let (check_rn4, is_new4) = provider.get_or_create_row_number(&mut txn, &key4).unwrap();
		assert_eq!(check_rn4.0, 4);
		assert!(!is_new4);

		let (check_rn5, is_new5) = provider.get_or_create_row_number(&mut txn, &key5).unwrap();
		assert_eq!(check_rn5.0, 5);
		assert!(!is_new5);
	}

	#[test]
	fn test_get_row_number_returns_none_for_unknown() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("never_seen");
		assert_eq!(provider.get_row_number(&mut txn, &key).unwrap(), None);
	}

	#[test]
	fn test_get_row_number_returns_existing_without_creating() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("lookup_hit");
		let (created, was_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(was_new);

		let looked_up = provider.get_row_number(&mut txn, &key).unwrap();
		assert_eq!(looked_up, Some(created));

		let another = test_key("another_missing");
		assert_eq!(provider.get_row_number(&mut txn, &another).unwrap(), None);
		let (after, was_new_after) = provider.get_or_create_row_number(&mut txn, &another).unwrap();
		assert!(was_new_after);
		assert_ne!(after, created);
	}

	#[test]
	fn test_remove_for_key_clears_mapping() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("to_remove");
		let (_assigned, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(provider.get_row_number(&mut txn, &key).unwrap().is_some());

		let removed = provider.remove_for_key(&mut txn, &key).unwrap();
		assert!(removed);

		assert_eq!(provider.get_row_number(&mut txn, &key).unwrap(), None);
	}

	#[test]
	fn test_remove_for_key_is_idempotent() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("absent");
		assert!(!provider.remove_for_key(&mut txn, &key).unwrap());

		let (_assigned, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(provider.remove_for_key(&mut txn, &key).unwrap());
		assert!(!provider.remove_for_key(&mut txn, &key).unwrap());
	}

	#[test]
	fn test_remove_for_key_then_recreate_assigns_new_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("recycled");
		let (first, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(provider.remove_for_key(&mut txn, &key).unwrap());

		let (second, was_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert!(was_new, "after removal the next mapping should be created fresh");
		assert_ne!(first, second, "counter must keep advancing, not recycle old row numbers");
	}

	#[test]
	fn internal_state_tags_are_pairwise_distinct() {
		// The row-number counter/forward-map keys share the per-node
		// FlowNodeInternalState namespace with window-meta and gate-visibility keys.
		// Every tag must be pairwise distinct, or an operator that mixes them (e.g. a
		// windowed operator that also assigns row numbers) would overwrite another's
		// state in the same node range.
		let tags = [
			FlowNodeInternalStateKey::ROW_NUMBER_COUNTER_TAG,
			FlowNodeInternalStateKey::ROW_NUMBER_MAPPING_TAG,
			FlowNodeInternalStateKey::WINDOW_META_TAG,
			FlowNodeInternalStateKey::GATE_VISIBILITY_TAG,
		];
		for i in 0..tags.len() {
			for j in (i + 1)..tags.len() {
				assert_ne!(tags[i], tags[j], "internal-state tag collision at {:#04x}", tags[i]);
			}
		}
	}

	#[test]
	fn mapping_values_are_postcard_encoded() {
		// The forward map value must be encoded via postcard (encode_payload), not raw
		// big-endian / raw bytes. This pins it: the forward map value decodes as a u64
		// via decode_payload. RED on the old raw-be encoding.
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("encoded");
		let (rn, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

		let forward =
			internal_state_get(FlowNodeId(1), &mut txn, &provider.make_map_key(&key)).unwrap().unwrap();
		assert_eq!(decode_payload::<u64>(&forward).unwrap(), rn.0);
	}

	#[test]
	fn test_counter_survives_full_mapping_eviction() {
		// Regression: purging EVERY per-key mapping (full eviction of the provider's
		// state) must not delete the monotonic counter. If it did, a fresh key would
		// reuse a previously issued row number and corrupt any downstream consumer that
		// tracks rows by number.
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::deferred(
			&mut txn,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			Clock::Mock(MockClock::from_millis(1000)),
		);
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let keys = [test_key("a"), test_key("b"), test_key("c")];
		let mut issued = Vec::new();
		for key in &keys {
			let (n, was_new) = provider.get_or_create_row_number(&mut txn, key).unwrap();
			assert!(was_new);
			issued.push(n);
		}

		for key in &keys {
			assert!(provider.remove_for_key(&mut txn, key).unwrap());
		}

		let (fresh, was_new) = provider.get_or_create_row_number(&mut txn, &test_key("d")).unwrap();
		assert!(was_new, "a brand-new key after full eviction must be assigned fresh");
		for prev in &issued {
			assert_ne!(&fresh, prev, "row number {:?} was reused after full eviction", prev);
		}
		assert!(
			issued.iter().all(|prev| fresh.0 > prev.0),
			"counter must keep advancing past every previously issued number, got {:?} after {:?}",
			fresh,
			issued
		);
	}
}
