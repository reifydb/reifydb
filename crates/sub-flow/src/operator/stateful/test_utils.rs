// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(test)]
pub mod test {
	use reifydb_catalog::MaterializedCatalog;
	use reifydb_core::{
		EncodedKey, Row,
		event::EventBus,
		interceptor::StandardInterceptorFactory,
		interface::{Engine, FlowNodeId},
		util::CowVec,
		value::encoded::{EncodedValues, EncodedValuesLayout},
	};
	use reifydb_engine::{StandardCommandTransaction, StandardEngine, StandardRowEvaluator};
	use reifydb_flow_operator_sdk::FlowChange;
	use reifydb_store_transaction::TransactionStore;
	use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMulti, single::TransactionSingle};
	use reifydb_type::{RowNumber, Type, Value};

	use crate::{
		operator::{Operator, transform::TransformOperator},
		transaction::FlowTransaction,
	};

	/// Create a test engine with memory storage
	pub fn create_test_engine() -> StandardEngine {
		let store = TransactionStore::testing_memory();
		let eventbus = EventBus::new();
		let single = TransactionSingle::svl(store.clone(), eventbus.clone());
		let cdc = TransactionCdc::new(store.clone());
		let multi = TransactionMulti::new(store, single.clone(), eventbus.clone());

		StandardEngine::new(
			multi,
			single,
			cdc,
			eventbus,
			Box::new(StandardInterceptorFactory::default()),
			MaterializedCatalog::new(),
		)
	}

	/// Test operator implementation for stateful traits
	pub struct TestOperator {
		pub id: FlowNodeId,
		pub layout: EncodedValuesLayout,
		pub key_types: Vec<Type>,
	}

	impl TestOperator {
		/// Create a new test operator with a complex schema
		pub fn new(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: EncodedValuesLayout::new(&[Type::Int8, Type::Float8, Type::Utf8]),
				key_types: vec![Type::Utf8, Type::Int4],
			}
		}

		/// Create a simple test operator with a single column
		pub fn simple(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: EncodedValuesLayout::new(&[Type::Int8]),
				key_types: vec![],
			}
		}

		/// Create a test operator with custom key types
		pub fn with_key_types(id: FlowNodeId, key_types: Vec<Type>) -> Self {
			Self {
				id,
				layout: EncodedValuesLayout::new(&[Type::Blob, Type::Int4]),
				key_types,
			}
		}
	}

	impl Operator for TestOperator {
		fn id(&self) -> FlowNodeId {
			self.id
		}

		fn apply(
			&self,
			txn: &mut FlowTransaction,
			change: FlowChange,
			evaluator: &StandardRowEvaluator,
		) -> reifydb_core::Result<FlowChange> {
			todo!()
		}

		fn get_rows(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Vec<Option<Row>>> {
			unimplemented!()
		}
	}

	impl TransformOperator for TestOperator {}

	/// Helper to create test values
	pub fn test_values() -> Vec<Value> {
		vec![Value::Utf8("test_key".to_string()), Value::Int4(42)]
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
	pub async fn create_test_transaction().await -> StandardCommandTransaction {
		let engine = create_test_engine();
		engine.begin_command().await.unwrap()
	}
}
