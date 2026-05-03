// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod test {
	use reifydb_core::{
		encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
		interface::{catalog::flow::FlowNodeId, change::Change},
		value::column::columns::Columns,
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_type::{
		Result,
		util::cowvec::CowVec,
		value::{identity::IdentityId, row_number::RowNumber, r#type::Type},
	};

	use crate::{operator::Operator, transaction::FlowTransaction};

	pub struct TestOperator {
		pub id: FlowNodeId,
		pub layout: RowShape,
		pub key_types: Vec<Type>,
	}

	impl TestOperator {
		pub fn new(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: RowShape::testing(&[Type::Int8, Type::Float8, Type::Utf8]),
				key_types: vec![Type::Utf8, Type::Int4],
			}
		}

		pub fn simple(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: RowShape::testing(&[Type::Int8]),
				key_types: vec![],
			}
		}

		pub fn with_key_types(id: FlowNodeId, key_types: Vec<Type>) -> Self {
			Self {
				id,
				layout: RowShape::testing(&[Type::Blob, Type::Int4]),
				key_types,
			}
		}
	}

	impl Operator for TestOperator {
		fn id(&self) -> FlowNodeId {
			self.id
		}

		fn apply(&self, _txn: &mut FlowTransaction, _change: Change) -> Result<Change> {
			todo!()
		}

		fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> Result<Columns> {
			unimplemented!()
		}
	}

	pub fn test_row() -> EncodedRow {
		EncodedRow(CowVec::new(vec![1, 2, 3, 4, 5]))
	}

	pub fn test_key(suffix: &str) -> EncodedKey {
		EncodedKey::new(format!("test_{}", suffix).into_bytes())
	}

	pub fn assert_row_eq(actual: &EncodedRow, expected: &EncodedRow) {
		assert_eq!(actual.to_vec(), expected.to_vec(), "Rows do not match");
	}

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
