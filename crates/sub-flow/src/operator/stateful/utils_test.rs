// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(test)]
pub mod test {
	use reifydb_catalog::MaterializedCatalog;
	use reifydb_core::{
		EncodedKey,
		event::EventBus,
		interceptor::StandardInterceptorFactory,
		interface::{Engine, FlowNodeId},
		util::CowVec,
		value::encoded::{EncodedValues, EncodedValuesLayout},
	};
	use reifydb_engine::{
		EngineTransaction, StandardCdcTransaction, StandardCommandTransaction, StandardEngine,
		StandardRowEvaluator,
	};
	use reifydb_store_transaction::StandardTransactionStore;
	use reifydb_transaction::{mvcc::transaction::optimistic::Optimistic, svl::SingleVersionLock};
	use reifydb_type::{Type, Value};

	use crate::{
		flow::FlowChange,
		operator::{Operator, transform::TransformOperator},
	};

	/// Test transaction type using optimistic concurrency control and memory storage
	pub type TestTransaction = EngineTransaction<
		Optimistic<StandardTransactionStore, SingleVersionLock<StandardTransactionStore>>,
		SingleVersionLock<StandardTransactionStore>,
		StandardCdcTransaction<StandardTransactionStore>,
	>;

	/// Create a test engine with memory storage and optimistic transactions
	pub fn create_test_engine() -> StandardEngine<TestTransaction> {
		let store = StandardTransactionStore::testing_memory();
		let eventbus = EventBus::new();
		let single = SingleVersionLock::new(store.clone(), eventbus.clone());
		let cdc = StandardCdcTransaction::new(store.clone());
		let multi = Optimistic::new(store, single.clone(), eventbus.clone());

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

	impl Operator<TestTransaction> for TestOperator {
		fn id(&self) -> FlowNodeId {
			self.id
		}

		fn apply(
			&self,
			txn: &mut StandardCommandTransaction<TestTransaction>,
			change: FlowChange,
			evaluator: &StandardRowEvaluator,
		) -> reifydb_core::Result<FlowChange> {
			todo!()
		}
	}

	impl TransformOperator<TestTransaction> for TestOperator {}

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
	pub fn create_test_transaction() -> StandardCommandTransaction<TestTransaction> {
		let engine = create_test_engine();
		engine.begin_command().unwrap()
	}
}
