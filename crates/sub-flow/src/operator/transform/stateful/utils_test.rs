// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(test)]
pub mod test {
	use reifydb_catalog::MaterializedCatalog;
	use reifydb_core::{
		EncodedKey,
		event::EventBus,
		flow::FlowChange,
		interceptor::StandardInterceptorFactory,
		interface::{Engine, FlowNodeId},
		row::{EncodedRow, EncodedRowLayout},
		util::CowVec,
	};
	use reifydb_engine::{
		EngineTransaction, StandardCdcTransaction, StandardCommandTransaction, StandardEngine,
		StandardEvaluator,
	};
	use reifydb_storage::memory::Memory;
	use reifydb_transaction::{mvcc::transaction::optimistic::Optimistic, svl::SingleVersionLock};
	use reifydb_type::{Type, Value};

	use crate::operator::{Operator, transform::TransformOperator};

	/// Test transaction type using optimistic concurrency control and memory storage
	pub type TestTransaction = EngineTransaction<
		Optimistic<Memory, SingleVersionLock<Memory>>,
		SingleVersionLock<Memory>,
		StandardCdcTransaction<Memory>,
	>;

	/// Create a test engine with memory storage and optimistic transactions
	pub fn create_test_engine() -> StandardEngine<TestTransaction> {
		let memory = Memory::new();
		let eventbus = EventBus::new();
		let unversioned = SingleVersionLock::new(memory.clone(), eventbus.clone());
		let cdc = StandardCdcTransaction::new(memory.clone());
		let versioned = Optimistic::new(memory, unversioned.clone(), eventbus.clone());

		StandardEngine::new(
			versioned,
			unversioned,
			cdc,
			eventbus,
			Box::new(StandardInterceptorFactory::default()),
			MaterializedCatalog::new(),
		)
	}

	/// Test operator implementation for stateful traits
	pub struct TestOperator {
		pub id: FlowNodeId,
		pub layout: EncodedRowLayout,
		pub key_types: Vec<Type>,
	}

	impl TestOperator {
		/// Create a new test operator with a complex schema
		pub fn new(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: EncodedRowLayout::new(&[Type::Int8, Type::Float8, Type::Utf8]),
				key_types: vec![Type::Utf8, Type::Int4],
			}
		}

		/// Create a simple test operator with a single column
		pub fn simple(id: FlowNodeId) -> Self {
			Self {
				id,
				layout: EncodedRowLayout::new(&[Type::Int8]),
				key_types: vec![],
			}
		}

		/// Create a test operator with custom key types
		pub fn with_key_types(id: FlowNodeId, key_types: Vec<Type>) -> Self {
			Self {
				id,
				layout: EncodedRowLayout::new(&[Type::Blob, Type::Int4]),
				key_types,
			}
		}
	}

	impl Operator<TestTransaction> for TestOperator {
		fn apply(
			&self,
			txn: &mut StandardCommandTransaction<TestTransaction>,
			change: FlowChange,
			evaluator: &StandardEvaluator,
		) -> reifydb_core::Result<FlowChange> {
			todo!()
		}
	}

	impl TransformOperator<TestTransaction> for TestOperator {
		fn id(&self) -> FlowNodeId {
			self.id
		}
	}

	/// Helper to create test values
	pub fn test_values() -> Vec<Value> {
		vec![Value::Utf8("test_key".to_string()), Value::Int4(42)]
	}

	/// Helper to create test row
	pub fn test_row() -> EncodedRow {
		EncodedRow(CowVec::new(vec![1, 2, 3, 4, 5]))
	}

	/// Helper to create test key with suffix
	pub fn test_key(suffix: &str) -> EncodedKey {
		EncodedKey::new(format!("test_{}", suffix).into_bytes())
	}

	/// Helper to verify row equality
	pub fn assert_row_eq(actual: &EncodedRow, expected: &EncodedRow) {
		assert_eq!(actual.as_ref().to_vec(), expected.as_ref().to_vec(), "Rows do not match");
	}

	/// Helper to create a test transaction
	pub fn create_test_transaction() -> StandardCommandTransaction<TestTransaction> {
		let engine = create_test_engine();
		engine.begin_command().unwrap()
	}
}
