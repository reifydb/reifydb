// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use reifydb_codec::{
	encoded::{
		row::{EncodedRow, EncodedRowBuilder},
		shape::RowShape,
	},
	key::encoded::EncodedKeyRange,
};
use reifydb_core::key::{EncodableKey, operator_group_state::GroupStateKey, operator_state::OperatorStateKey};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_transaction::multi::RangeScope;
use reifydb_value::Result;

use super::utils;
use crate::operator::stateful::raw::RawStatefulOperator;

pub trait WindowStateful: RawStatefulOperator {
	fn layout(&self) -> RowShape;

	fn create_state(&self) -> EncodedRowBuilder {
		let layout = self.layout();
		layout.allocate()
	}

	fn load_state(&self, txn: &mut FlowTransaction, window_key: &GroupStateKey) -> Result<EncodedRow> {
		utils::load_or_create_row(self.id(), txn, window_key, &self.layout())
	}

	fn save_state(&self, txn: &mut FlowTransaction, window_key: &GroupStateKey, row: EncodedRow) -> Result<()> {
		utils::save_row(self.id(), txn, window_key, row)
	}

	fn expire_range(&self, txn: &mut FlowTransaction, range: EncodedKeyRange) -> Result<u32> {
		let prefixed_range = range.with_prefix(OperatorStateKey::new(self.id(), vec![]).encode());

		let keys_to_remove = {
			let stream = txn.range(prefixed_range, RangeScope::All, 1024);
			let mut keys = Vec::new();
			for result in stream {
				let multi = result?;
				keys.push(multi.key);
			}
			keys
		};

		let mut count = 0;
		for key in keys_to_remove {
			txn.remove(&key)?;
			count += 1;
		}

		Ok(count as u32)
	}
}

#[cfg(test)]
pub mod tests {
	use std::ops::Bound::{Excluded, Unbounded};

	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_group_state::Keyspace};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	fn test_window_key(window_id: u64) -> GroupStateKey {
		// Inverted encoding: a smaller window id produces a larger key.
		let mut serializer = KeySerializer::with_capacity(16);
		serializer.extend_bytes(b"w:");
		serializer.extend_u64(window_id);
		GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, serializer.finish().as_ref().to_vec())
	}

	impl WindowStateful for TestOperator {
		fn layout(&self) -> RowShape {
			self.layout.clone()
		}
	}

	#[test]
	fn test_window_key_encoding() {
		let key1 = test_window_key(1);
		let key2 = test_window_key(2);
		let key100 = test_window_key(100);

		assert_ne!(key1.as_slice(), key2.as_slice());
		assert_ne!(key1.as_slice(), key100.as_slice());

		// Inverted encoding, so a smaller window id sorts later.
		assert!(key1 > key2);
		assert!(key2 > key100);
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::simple(OperatorId(1));
		let state = operator.create_state();

		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_window_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));
		let window_key = test_window_key(42);

		let state1 = operator.load_state(&mut txn, &window_key).unwrap();

		let mut modified = state1.clone().thaw();
		let layout = operator.layout();
		layout.set::<i64>(&mut modified, 0, 0xAB);
		operator.save_state(&mut txn, &window_key, modified.clone().freeze()).unwrap();

		let state2 = operator.load_state(&mut txn, &window_key).unwrap();
		assert_eq!(layout.get::<i64>(&state2, 0), 0xAB);
	}

	#[test]
	fn test_multiple_windows() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let window_keys: Vec<_> = (0..5).map(|i| test_window_key(i)).collect();
		let layout = operator.layout();
		for (i, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			layout.set::<i64>(&mut state, 0, i as i64);
			operator.save_state(&mut txn, window_key, state.freeze()).unwrap();
		}

		for (i, window_key) in window_keys.iter().enumerate() {
			let state = operator.load_state(&mut txn, window_key).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), i as i64);
		}
	}

	#[test]
	fn test_expire_before() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let window_keys: Vec<_> = (0..10).map(|i| test_window_key(i)).collect();
		let layout = operator.layout();
		for (i, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			layout.set::<i64>(&mut state, 0, i as i64);
			operator.save_state(&mut txn, window_key, state.freeze()).unwrap();
		}

		// Inverted encoding, so expiring windows below 5 means the range above key(5).
		let before_key = test_window_key(5);
		let range = EncodedKeyRange::new(Excluded(before_key.into_encoded()), Unbounded);
		let expired = operator.expire_range(&mut txn, range).unwrap();
		assert_eq!(expired, 5);

		for i in 0..5 {
			let state = operator.load_state(&mut txn, &window_keys[i]).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), 0); // Should be newly created (default)
		}

		for i in 5..10 {
			let state = operator.load_state(&mut txn, &window_keys[i]).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), i as i64);
		}
	}

	#[test]
	fn test_expire_empty_range() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let window_keys: Vec<_> = (5..10).map(|i| test_window_key(i)).collect();
		let layout = operator.layout();
		for (idx, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			layout.set::<i64>(&mut state, 0, (idx + 5) as i64);
			operator.save_state(&mut txn, window_key, state.freeze()).unwrap();
		}

		// Every window is at or above 5, so a cutoff of 3 must find nothing.
		let before_key = test_window_key(3);
		let range = EncodedKeyRange::new(Excluded(before_key.into_encoded()), Unbounded);
		let expired = operator.expire_range(&mut txn, range).unwrap();
		assert_eq!(expired, 0);

		for (idx, window_key) in window_keys.iter().enumerate() {
			let state = operator.load_state(&mut txn, window_key).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), (idx + 5) as i64);
		}
	}

	#[test]
	fn test_expire_all() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let window_keys: Vec<_> = (0..5).map(|i| test_window_key(i)).collect();
		let layout = operator.layout();
		for (i, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			layout.set::<i64>(&mut state, 0, i as i64);
			operator.save_state(&mut txn, window_key, state.freeze()).unwrap();
		}

		let before_key = test_window_key(100);
		let range = EncodedKeyRange::new(Excluded(before_key.into_encoded()), Unbounded);
		let expired = operator.expire_range(&mut txn, range).unwrap();
		assert_eq!(expired, 5);

		for window_key in &window_keys {
			let state = operator.load_state(&mut txn, window_key).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), 0); // Should be newly created (default)
		}
	}

	#[test]
	fn test_sliding_window_simulation() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::new(OperatorId(1));

		// A sliding window that keeps only the last three.
		let window_size = 3;
		let mut all_window_keys = Vec::new();
		let layout = operator.layout();

		for current_window in 0..10 {
			let window_key = test_window_key(current_window);
			all_window_keys.push(window_key.clone());
			let mut state = operator.create_state();
			layout.set::<i64>(&mut state, 0, current_window as i64);
			operator.save_state(&mut txn, &window_key, state.freeze()).unwrap();

			if current_window >= window_size {
				let expire_before = current_window - window_size + 1;
				let before_key = test_window_key(expire_before);
				let range = EncodedKeyRange::new(Excluded(before_key.into_encoded()), Unbounded);
				operator.expire_range(&mut txn, range).unwrap();
			}
		}

		for i in 0..7 {
			let state = operator.load_state(&mut txn, &all_window_keys[i]).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), 0); // Should be default (expired)
		}

		for i in 7..10 {
			let state = operator.load_state(&mut txn, &all_window_keys[i]).unwrap();
			assert_eq!(layout.get::<i64>(&state, 0), i as i64); // Should have saved data
		}
	}
}
