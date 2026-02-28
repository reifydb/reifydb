// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[cfg(test)]
pub mod test {
	use reifydb_core::{
		encoded::{encoded::EncodedValues, key::EncodedKey, schema::Schema},
		interface::{catalog::flow::FlowNodeId, change::Change},
		value::column::columns::Columns,
	};
	use reifydb_engine::test_utils::create_test_engine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_type::{
		Result,
		util::cowvec::CowVec,
		value::{row_number::RowNumber, r#type::Type},
	};

	use crate::{operator::Operator, transaction::FlowTransaction};

	/// Test operator implementation for stateful traits
	pub struct TestOperator {
		pub id: FlowNodeId,
		pub layout: Schema,
		pub key_types: Vec<Type>,
	}

	impl TestOperator {
		/// Create a new test operator with a complex schema
		pub fn new(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: Schema::testing(&[Type::Int8, Type::Float8, Type::Utf8]),
				key_types: vec![Type::Utf8, Type::Int4],
			}
		}

		/// Create a simple test operator with a single column
		pub fn simple(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: Schema::testing(&[Type::Int8]),
				key_types: vec![],
			}
		}

		/// Create a test operator with custom key types
		pub fn with_key_types(id: FlowNodeId, key_types: Vec<Type>) -> Self {
			Self {
				id,
				layout: Schema::testing(&[Type::Blob, Type::Int4]),
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

	/// Helper to create test encoded
	pub fn test_row() -> EncodedValues {
		EncodedValues(CowVec::new(vec![1, 2, 3, 4, 5]))
	}

	/// Helper to create test key with suffix
	pub fn test_key(suffix: &str) -> EncodedKey {
		EncodedKey::new(format!("test_{}", suffix).into_bytes())
	}

	/// Helper to verify encoded equality
	pub fn assert_row_eq(actual: &EncodedValues, expected: &EncodedValues) {
		assert_eq!(actual.as_ref().to_vec(), expected.as_ref().to_vec(), "Rows do not match");
	}

	/// Helper to create a test transaction
	pub fn create_test_transaction() -> AdminTransaction {
		let engine = create_test_engine();
		engine.begin_admin().unwrap()
	}
}
