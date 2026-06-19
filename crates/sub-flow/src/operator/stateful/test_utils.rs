// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod test {
	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_core::{
		encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
		interface::{catalog::flow::FlowNodeId, change::Change},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::{
		Result,
		util::cowvec::CowVec,
		value::{identity::IdentityId, value_type::ValueType},
	};

	use crate::{operator::Operator, transaction::FlowTransaction};

	pub struct TestOperator {
		pub id: FlowNodeId,
		pub layout: RowShape,
		pub key_types: Vec<ValueType>,
	}

	impl TestOperator {
		pub fn new(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: RowShape::testing(&[ValueType::Int8, ValueType::Float8, ValueType::Utf8]),
				key_types: vec![ValueType::Utf8, ValueType::Int4],
			}
		}

		pub fn simple(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: RowShape::testing(&[ValueType::Int8]),
				key_types: vec![],
			}
		}

		pub fn with_key_types(id: FlowNodeId, key_types: Vec<ValueType>) -> Self {
			Self {
				id,
				layout: RowShape::testing(&[ValueType::Blob, ValueType::Int4]),
				key_types,
			}
		}
	}

	impl Operator for TestOperator {
		fn id(&self) -> FlowNodeId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&self, _txn: &mut FlowTransaction, _change: Change) -> Result<Change> {
			todo!()
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
