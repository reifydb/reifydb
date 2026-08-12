// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod test {
	use reifydb_codec::row::{
		operator::EncodedOperatorRow,
		shape::{RowFamily, RowShape},
	};
	use reifydb_core::{
		interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
		key::operator_state::{GroupStateKey, Keyspace},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::{
		Result,
		value::{identity::IdentityId, value_type::ValueType},
	};

	use crate::{operator::Operator, transaction::FlowTransaction};

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
					RowFamily::Pod,
					&[ValueType::Int8, ValueType::Float8, ValueType::Utf8],
				),
				key_types: vec![ValueType::Utf8, ValueType::Int4],
			}
		}

		pub fn simple(id: OperatorId) -> Self {
			Self {
				id,
				layout: RowShape::testing(RowFamily::Pod, &[ValueType::Int8]),
				key_types: vec![],
			}
		}

		pub fn with_key_types(id: OperatorId, key_types: Vec<ValueType>) -> Self {
			Self {
				id,
				layout: RowShape::testing(RowFamily::Pod, &[ValueType::Blob, ValueType::Int4]),
				key_types,
			}
		}
	}

	impl<T: FlowTransaction> Operator<T> for TestOperator {
		fn id(&self) -> OperatorId {
			self.id
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _txn: &mut T, _change: Change) -> Result<Change> {
			todo!()
		}
	}

	pub fn test_row() -> EncodedOperatorRow {
		EncodedOperatorRow::timeless(&[1, 2, 3, 4, 5])
	}

	pub fn test_key(suffix: &str) -> GroupStateKey {
		GroupStateKey::root(Keyspace::CUSTOM, format!("test_{}", suffix).into_bytes())
	}

	pub fn assert_row_eq(actual: &EncodedOperatorRow, expected: &EncodedOperatorRow) {
		assert_eq!(actual, expected, "Rows do not match");
	}

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
