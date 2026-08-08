// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	encoded::{
		bytes::{EncodedBytes, EncodedRowBuilder},
		shape::RowShape,
	},
	key::serializer::KeySerializer,
};
use reifydb_core::key::operator_group_state::{GroupStateKey, Keyspace};
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};

use super::utils;
use crate::operator::stateful::raw::RawStatefulOperator;

pub trait KeyedStateful: RawStatefulOperator {
	fn layout(&self) -> RowShape;

	fn key_types(&self) -> &[ValueType];

	fn encode_state_key(&self, key_values: &[Value]) -> GroupStateKey {
		let mut serializer = KeySerializer::new();

		for value in key_values.iter() {
			serializer.extend_value(value);
		}

		GroupStateKey::node_scoped(Keyspace::FIRST_CUSTOM, serializer.finish().as_ref())
	}

	fn create_state(&self) -> EncodedRowBuilder {
		let layout = self.layout();
		layout.allocate()
	}

	fn load_state(&self, txn: &mut FlowTransaction, key_values: &[Value]) -> Result<EncodedBytes> {
		let key = self.encode_state_key(key_values);
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	fn save_state(&self, txn: &mut FlowTransaction, key_values: &[Value], bytes: EncodedBytes) -> Result<()> {
		let key = self.encode_state_key(key_values);
		utils::save_row(self.id(), txn, &key, bytes)
	}

	fn update_state<F>(&self, txn: &mut FlowTransaction, key_values: &[Value], f: F) -> Result<EncodedBytes>
	where
		F: FnOnce(&RowShape, &mut EncodedRowBuilder) -> Result<()>,
	{
		let shape = self.layout();
		let mut row = self.load_state(txn, key_values)?.thaw();
		f(&shape, &mut row)?;
		let row = row.freeze();
		self.save_state(txn, key_values, row.clone())?;
		Ok(row)
	}

	fn remove_state(&self, txn: &mut FlowTransaction, key_values: &[Value]) -> Result<()> {
		let key = self.encode_state_key(key_values);
		utils::state_remove(self.id(), txn, &key)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;
	#[cfg(test)]
	use crate::operator::stateful::test_utils::test::*;

	impl KeyedStateful for TestOperator {
		fn layout(&self) -> RowShape {
			self.layout.clone()
		}

		fn key_types(&self) -> &[ValueType] {
			&self.key_types
		}
	}

	#[test]
	fn test_encode_key() {
		let operator = TestOperator::with_key_types(OperatorId(1), vec![ValueType::Int4, ValueType::Utf8]);

		let key1 = vec![Value::Int4(42), Value::Utf8("test".to_string())];
		let encoded1 = operator.encode_state_key(&key1);

		let key2 = vec![Value::Int4(42), Value::Utf8("test2".to_string())];
		let encoded2 = operator.encode_state_key(&key2);

		assert_ne!(encoded1.as_slice(), encoded2.as_slice());

		let encoded1_again = operator.encode_state_key(&key1);
		assert_eq!(encoded1.as_slice(), encoded1_again.as_slice());
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::new(OperatorId(1));
		let state = operator.create_state();

		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::with_key_types(OperatorId(1), vec![ValueType::Int4, ValueType::Utf8]);
		let key = vec![Value::Int4(100), Value::Utf8("key1".to_string())];

		let state1 = operator.load_state(&mut txn, &key).unwrap();

		// with_key_types lays state out as [Blob, Int4], so field 1 is the Int4.
		let mut modified = state1.clone().thaw();
		let layout = operator.layout();
		layout.set::<i32>(&mut modified, 1, 0x42);
		operator.save_state(&mut txn, &key, modified.clone().freeze()).unwrap();

		let state2 = operator.load_state(&mut txn, &key).unwrap();
		assert_eq!(layout.get::<i32>(&state2, 1), 0x42);
	}

	#[test]
	fn test_update_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::with_key_types(OperatorId(1), vec![ValueType::Int4, ValueType::Utf8]);
		let key = vec![Value::Int4(200), Value::Utf8("update_key".to_string())];

		let result = operator
			.update_state(&mut txn, &key, |shape, row| {
				shape.set::<i32>(row, 1, 0x55);
				Ok(())
			})
			.unwrap();

		let layout = operator.layout();
		assert_eq!(layout.get::<i32>(&result, 1), 0x55);

		let loaded = operator.load_state(&mut txn, &key).unwrap();
		assert_eq!(layout.get::<i32>(&loaded, 1), 0x55);
	}

	#[test]
	fn test_remove_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::with_key_types(OperatorId(1), vec![ValueType::Int4, ValueType::Utf8]);
		let key = vec![Value::Int4(300), Value::Utf8("remove_key".to_string())];

		let state = operator.create_state();
		operator.save_state(&mut txn, &key, state.freeze()).unwrap();

		operator.remove_state(&mut txn, &key).unwrap();

		// A load after removal creates fresh, default-initialized state.
		let new_state = operator.load_state(&mut txn, &key).unwrap();
		let layout = operator.layout();
		assert_eq!(layout.get::<i32>(&new_state, 1), 0);
	}

	#[test]
	fn test_multiple_keys() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::with_key_types(OperatorId(1), vec![ValueType::Int4, ValueType::Utf8]);

		for i in 0..5 {
			let key = vec![Value::Int4(i), Value::Utf8(format!("key_{}", i))];
			operator.update_state(&mut txn, &key, |shape, row| {
				shape.set::<i32>(row, 1, i);
				Ok(())
			})
			.unwrap();
		}

		let layout = operator.layout();
		for i in 0..5 {
			let key = vec![Value::Int4(i), Value::Utf8(format!("key_{}", i))];
			let state = operator.load_state(&mut txn, &key).unwrap();
			assert_eq!(layout.get::<i32>(&state, 1), i);
		}
	}

	#[test]
	fn test_key_ordering() {
		let operator = TestOperator::with_key_types(OperatorId(1), vec![ValueType::Int4, ValueType::Utf8]);

		let key1 = vec![Value::Int4(1), Value::Utf8("a".to_string())];
		let key2 = vec![Value::Int4(1), Value::Utf8("b".to_string())];
		let key3 = vec![Value::Int4(2), Value::Utf8("a".to_string())];

		let encoded1 = operator.encode_state_key(&key1);
		let encoded2 = operator.encode_state_key(&key2);
		let encoded3 = operator.encode_state_key(&key3);

		// Integers encode inverted, so a smaller value sorts later; strings keep normal order.
		assert!(encoded1 < encoded2);
		assert!(encoded3 < encoded1);
	}
}
