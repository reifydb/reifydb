// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod test {
	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::{
			operator::{EncodedOperatorRow, EncodedOperatorRowBuilder},
			shape::{RowFamily, RowShape},
		},
	};
	use reifydb_core::interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::{
		Result,
		value::{identity::IdentityId, value_type::ValueType},
	};

	use crate::flow::{operator::Operator, transaction::FlowTransaction};

	pub struct TestOperator {
		pub id: OperatorId,
		pub layout: RowShape,
		pub key_types: Vec<ValueType>,
	}

	impl TestOperator {
		pub fn new(id: OperatorId) -> Self {
			Self {
				id,
				layout: RowShape::testing(
					RowFamily::Operator,
					&[ValueType::Int8, ValueType::Float8, ValueType::Utf8],
				),
				key_types: vec![ValueType::Utf8, ValueType::Int4],
			}
		}

		pub fn simple(id: OperatorId) -> Self {
			Self {
				id,
				layout: RowShape::testing(RowFamily::Operator, &[ValueType::Int8]),
				key_types: vec![],
			}
		}

		pub fn with_key_types(id: OperatorId, key_types: Vec<ValueType>) -> Self {
			Self {
				id,
				layout: RowShape::testing(RowFamily::Operator, &[ValueType::Blob, ValueType::Int4]),
				key_types,
			}
		}
	}

	impl Operator for TestOperator {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&self, _txn: &mut FlowTransaction, _change: Change) -> Result<Change> {
			todo!()
		}
	}

	pub fn set_i32(shape: &RowShape, row: &mut EncodedOperatorRow, index: usize, value: i32) {
		set_field(shape, row, |shape, builder| shape.set::<i32>(builder, index, value));
	}

	pub fn set_i64(shape: &RowShape, row: &mut EncodedOperatorRow, index: usize, value: i64) {
		set_field(shape, row, |shape, builder| shape.set::<i64>(builder, index, value));
	}

	pub fn get_i32(shape: &RowShape, row: &EncodedOperatorRow, index: usize) -> i32 {
		shape.get::<i32>(row.bytes(), index)
	}

	pub fn get_i64(shape: &RowShape, row: &EncodedOperatorRow, index: usize) -> i64 {
		shape.get::<i64>(row.bytes(), index)
	}

	fn set_field<F>(shape: &RowShape, row: &mut EncodedOperatorRow, f: F)
	where
		F: FnOnce(&RowShape, &mut EncodedOperatorRowBuilder),
	{
		let mut builder = std::mem::replace(row, EncodedOperatorRow::timeless(&[])).thaw();
		f(shape, &mut builder);
		*row = builder.freeze();
	}

	pub fn test_row() -> EncodedOperatorRow {
		EncodedOperatorRow::timeless(&[1, 2, 3, 4, 5])
	}

	pub fn test_key(suffix: &str) -> EncodedKey {
		EncodedKey::new(format!("test_{}", suffix).into_bytes())
	}

	pub fn assert_row_eq(actual: &EncodedOperatorRow, expected: &EncodedOperatorRow) {
		assert_eq!(actual, expected, "Rows do not match");
	}

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
