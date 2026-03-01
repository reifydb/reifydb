// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ffi::c_void, marker::PhantomData, ptr};

use ptr::null;
use reifydb_abi::context::context::ContextFFI;
use reifydb_core::{
	common::CommitVersion,
	encoded::{encoded::EncodedValues, key::EncodedKey, schema::Schema},
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::EncodableKey,
	value::column::columns::Columns,
};
use reifydb_type::{
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};

use crate::{
	error::Result,
	operator::{FFIOperator, FFIOperatorMetadata, context::OperatorContext},
	testing::{callbacks::create_test_callbacks, context::TestContext, state::TestStateStore},
};

/// Test harness for FFI operators
///
/// This harness provides a complete testing environment for FFI operators with:
/// - Mock FFI context with test-specific callbacks
/// - State management via TestContext
/// - Version tracking
/// - Log capture (to stderr for now)
/// - Full support for apply() and pull()
pub struct OperatorTestHarness<T: FFIOperator> {
	operator: T,
	context: Box<TestContext>, // Boxed for stable address (pointed to by ffi_context)
	ffi_context: Box<ContextFFI>,
	config: HashMap<String, Value>,
	node_id: FlowNodeId,
}

impl<T: FFIOperator> OperatorTestHarness<T> {
	/// Create a new test harness builder
	pub fn builder() -> TestHarnessBuilder<T> {
		TestHarnessBuilder::new()
	}

	/// Apply a flow change to the operator
	pub fn apply(&mut self, input: Change) -> Result<Change> {
		let mut ctx = self.create_operator_context();
		self.operator.apply(&mut ctx, input)
	}

	/// Pull rows by their row numbers
	pub fn pull(&mut self, row_numbers: &[RowNumber]) -> Result<Columns> {
		let mut ctx = self.create_operator_context();
		self.operator.pull(&mut ctx, row_numbers)
	}

	/// Get the current version
	pub fn version(&self) -> CommitVersion {
		(*self.context).version()
	}

	/// Set the current version
	pub fn set_version(&mut self, version: CommitVersion) {
		(*self.context).set_version(version);
	}

	/// Get access to the state store for assertions
	pub fn state(&self) -> TestStateStore {
		let store = self.context.state_store();
		let data = store.lock().unwrap();
		let mut result = TestStateStore::new();
		for (k, v) in data.iter() {
			result.set(k.clone(), v.clone());
		}
		result
	}

	/// Assert that a state key exists with the given value
	pub fn assert_state<K>(&self, key: K, expected: Value)
	where
		K: EncodableKey,
	{
		let encoded_key = key.encode();
		let store = self.state();
		let schema = Schema::testing(&[expected.get_type()]);

		store.assert_value(&encoded_key, &[expected], &schema);
	}

	/// Get captured log messages
	pub fn logs(&self) -> Vec<String> {
		(*self.context).logs()
	}

	/// Clear captured log messages
	pub fn clear_logs(&self) {
		(*self.context).clear_logs()
	}

	/// Take a snapshot of the current state
	pub fn snapshot_state(&self) -> HashMap<EncodedKey, EncodedValues> {
		self.state().snapshot()
	}

	/// Restore state from a snapshot
	pub fn restore_state(&mut self, snapshot: HashMap<EncodedKey, EncodedValues>) {
		(*self.context).clear_state();
		for (k, v) in snapshot {
			(*self.context).set_state(k, v.0.to_vec());
		}
	}

	/// Reset the harness to initial state
	pub fn reset(&mut self) -> Result<()> {
		(*self.context).clear_state();
		(*self.context).clear_logs();
		(*self.context).set_version(CommitVersion(1));

		// Recreate the operator
		self.operator = T::new(self.node_id, &self.config)?;
		Ok(())
	}

	/// Create an operator context for direct access
	///
	/// This is useful for testing components that need an OperatorContext
	/// without going through the apply() or pull() methods.
	///
	/// # Example
	///
	/// ```ignore
	/// let mut harness = TestHarnessBuilder::<MyOperator>::new().build()?;
	/// let mut ctx = harness.create_operator_context();
	/// let (row_num, is_new) = ctx.get_or_create_row_number(harness.operator(), &key)?;
	/// ```
	pub fn create_operator_context(&mut self) -> OperatorContext {
		OperatorContext::new(&mut *self.ffi_context as *mut ContextFFI)
	}

	/// Get a reference to the operator
	pub fn operator(&self) -> &T {
		&self.operator
	}

	/// Get a mutable reference to the operator
	pub fn operator_mut(&mut self) -> &mut T {
		&mut self.operator
	}

	/// Get the node ID
	pub fn node_id(&self) -> FlowNodeId {
		self.node_id
	}
}

/// Builder for OperatorTestHarness
pub struct TestHarnessBuilder<T: FFIOperator> {
	config: HashMap<String, Value>,
	node_id: FlowNodeId,
	version: CommitVersion,
	initial_state: HashMap<EncodedKey, EncodedValues>,
	_phantom: PhantomData<T>,
}

impl<T: FFIOperator> TestHarnessBuilder<T> {
	/// Create a new builder
	pub fn new() -> Self {
		Self {
			config: HashMap::new(),
			node_id: FlowNodeId(1),
			version: CommitVersion(1),
			initial_state: HashMap::new(),
			_phantom: PhantomData,
		}
	}

	/// Set the operator configuration
	pub fn with_config<I, K>(mut self, config: I) -> Self
	where
		I: IntoIterator<Item = (K, Value)>,
		K: Into<String>,
	{
		self.config = config.into_iter().map(|(k, v)| (k.into(), v)).collect();
		self
	}

	/// Add a single config value
	pub fn add_config(mut self, key: impl Into<String>, value: Value) -> Self {
		self.config.insert(key.into(), value);
		self
	}

	/// Set the node ID
	pub fn with_node_id(mut self, node_id: FlowNodeId) -> Self {
		self.node_id = node_id;
		self
	}

	/// Set the initial version
	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = version;
		self
	}

	/// Set initial state
	pub fn with_initial_state<K>(mut self, key: K, value: Vec<u8>) -> Self
	where
		K: EncodableKey,
	{
		self.initial_state.insert(key.encode(), EncodedValues(CowVec::new(value)));
		self
	}

	/// Build the test harness
	pub fn build(self) -> Result<OperatorTestHarness<T>> {
		// Create TestContext in a Box for stable address
		let context = Box::new(TestContext::new(self.version));

		// Set initial state
		for (k, v) in self.initial_state {
			context.set_state(k, v.0.to_vec());
		}

		// Create FFI context with test callbacks
		// The txn_ptr points to the TestContext
		let ffi_context = Box::new(ContextFFI {
			txn_ptr: &*context as *const TestContext as *mut c_void,
			executor_ptr: null(),
			operator_id: self.node_id.0,
			callbacks: create_test_callbacks(),
		});

		// Create the operator
		let operator = T::new(self.node_id, &self.config)?;

		Ok(OperatorTestHarness {
			operator,
			context,
			ffi_context,
			config: self.config,
			node_id: self.node_id,
		})
	}
}

/// Helper for testing operators with metadata
pub struct TestMetadataHarness;

impl TestMetadataHarness {
	/// Assert an operator has the expected name
	pub fn assert_name<T: FFIOperatorMetadata>(expected: &str) {
		assert_eq!(T::NAME, expected, "Operator name mismatch. Expected: {}, Actual: {}", expected, T::NAME);
	}

	/// Assert an operator has the expected API version
	pub fn assert_api<T: FFIOperatorMetadata>(expected: u32) {
		assert_eq!(
			T::API,
			expected,
			"Operator API version mismatch. Expected: {}, Actual: {}",
			expected,
			T::API
		);
	}

	/// Assert an operator has the expected semantic version
	pub fn assert_version<T: FFIOperatorMetadata>(expected: &str) {
		assert_eq!(
			T::VERSION,
			expected,
			"Operator version mismatch. Expected: {}, Actual: {}",
			expected,
			T::VERSION
		);
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
	use reifydb_core::{
		common::CommitVersion,
		encoded::{key::IntoEncodedKey, schema::Schema},
		interface::{
			catalog::flow::FlowNodeId,
			change::{Change, Diff},
		},
		value::column::columns::Columns,
	};
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::{super::helpers::encode_key, *};
	use crate::{
		operator::{FFIOperator, FFIOperatorMetadata, column::OperatorColumnDef, context::OperatorContext},
		testing::builders::TestChangeBuilder,
	};

	// Simple pass-through operator for basic tests
	struct TestOperator {
		_node_id: FlowNodeId,
		_config: HashMap<String, Value>,
	}

	impl FFIOperatorMetadata for TestOperator {
		const NAME: &'static str = "test_operator";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Simple pass-through test operator";
		const INPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}

	impl FFIOperator for TestOperator {
		fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self> {
			Ok(Self {
				_node_id: operator_id,
				_config: config.clone(),
			})
		}

		fn apply(&mut self, _ctx: &mut OperatorContext, input: Change) -> Result<Change> {
			// Simple pass-through for testing
			Ok(input)
		}

		fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<Columns> {
			Ok(Columns::empty())
		}
	}

	// Stateful operator that stores values from flow changes
	struct StatefulTestOperator;

	impl FFIOperatorMetadata for StatefulTestOperator {
		const NAME: &'static str = "stateful_test_operator";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Stateful test operator that stores values";
		const INPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumnDef] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}

	impl FFIOperator for StatefulTestOperator {
		fn new(_operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, ctx: &mut OperatorContext, input: Change) -> Result<Change> {
			let mut state = ctx.state();

			for diff in &input.diffs {
				let post_row = match diff {
					Diff::Insert {
						post,
					} => Some(post),
					Diff::Update {
						post,
						..
					} => Some(post),
					Diff::Remove {
						..
					} => unreachable!(),
				};

				if let Some(columns) = post_row {
					// Convert Columns to Row for processing
					let row = columns.to_single_row();
					let row_key = format!("row_{}", row.number.0);

					let first_value = row.schema.get_value(&row.encoded, 0);

					// Encode the value and store in state
					let schema = Schema::testing(&[Type::Int8]);
					let mut encoded = schema.allocate();
					schema.set_values(&mut encoded, &[first_value]);

					state.set(&row_key.into_encoded_key(), &encoded)?;
				}
			}

			Ok(input)
		}

		fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<Columns> {
			Ok(Columns::empty())
		}
	}

	#[test]
	fn test_operator_metadata() {
		TestMetadataHarness::assert_name::<TestOperator>("test_operator");
		TestMetadataHarness::assert_api::<TestOperator>(1);
		TestMetadataHarness::assert_version::<TestOperator>("1.0.0");
	}

	#[test]
	fn test_harness_builder() {
		let result = TestHarnessBuilder::<TestOperator>::new()
			.with_node_id(FlowNodeId(42))
			.with_version(CommitVersion(10))
			.add_config("key", Value::Utf8("value".into()))
			.build();

		assert!(result.is_ok());

		let harness = result.unwrap();
		assert_eq!(harness.node_id, 42);
		assert_eq!(harness.version(), 10);
	}

	#[test]
	fn test_harness_with_stateful_operator() {
		// Build harness with stateful operator
		let mut harness = TestHarnessBuilder::<StatefulTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// Create a flow change with an insert
		let input = TestChangeBuilder::new().insert_row(1, vec![Value::Int8(42i64)]).build();

		// Apply the flow change - operator should store the value in state
		let output = harness.apply(input).expect("Apply failed");

		// Verify output has the expected diff
		assert_eq!(output.diffs.len(), 1);

		// Verify the operator stored state correctly via FFI callbacks
		let state = harness.state();
		let schema = Schema::testing(&[Type::Int8]);
		let key = encode_key("row_1");

		// Assert the state was set through the FFI bridge
		state.assert_value(&key, &[Value::Int8(42i64)], &schema);
	}

	#[test]
	fn test_harness_multiple_operations() {
		let mut harness =
			TestHarnessBuilder::<StatefulTestOperator>::new().build().expect("Failed to build harness");

		// Insert multiple rows
		let input1 = TestChangeBuilder::new()
			.insert_row(1, vec![Value::Int8(10i64)])
			.insert_row(2, vec![Value::Int8(20i64)])
			.build();

		harness.apply(input1).expect("First apply failed");

		let state = harness.state();
		assert_eq!(state.len(), 2);

		// Insert another row
		let input2 = TestChangeBuilder::new().insert_row(RowNumber(3), vec![Value::Int8(30i64)]).build();

		harness.apply(input2).expect("Second apply failed");

		// Verify all three values were stored
		let state = harness.state();
		let schema = Schema::testing(&[Type::Int8]);

		state.assert_value(&encode_key("row_1"), &[Value::Int8(10i64)], &schema);
		state.assert_value(&encode_key("row_2"), &[Value::Int8(20i64)], &schema);
		state.assert_value(&encode_key("row_3"), &[Value::Int8(30i64)], &schema);

		// Verify total state count
		assert_eq!(state.len(), 3);
	}
}
