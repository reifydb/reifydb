// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ffi::c_void, marker::PhantomData, ops::Index, ptr};

use ptr::null;
use reifydb_abi::context::context::ContextFFI;
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, ChangeOrigin},
	},
	key::EncodableKey,
	row::Row,
	value::column::columns::Columns,
};
use reifydb_type::{
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};

use crate::{
	error::Result,
	ffi::arena::Arena,
	operator::{FFIOperator, FFIOperatorMetadata, change::BorrowedChange, context::OperatorContext},
	testing::{
		builders::TestChangeBuilder,
		callbacks::create_test_callbacks,
		context::TestContext,
		registry::{TestBuilderRegistry, into_diffs, with_registry},
		state::TestStateStore,
	},
};

/// Test harness for FFI operators
///
/// This harness provides a complete testing environment for FFI operators with:
/// - Mock FFI context with test-specific callbacks
/// - State management via TestContext
/// - Version tracking
/// - Log capture (to stderr for now)
/// - Full support for apply() and pull() driven through the zero-copy ABI
pub struct OperatorTestHarness<T: FFIOperator> {
	operator: T,
	context: Box<TestContext>, // Boxed for stable address (pointed to by ffi_context)
	ffi_context: Box<ContextFFI>,
	config: HashMap<String, Value>,
	node_id: FlowNodeId,
	history: Vec<Change>,
	/// Test-side mirror of the host's BuilderRegistry. Captures whatever
	/// the operator emits via `ctx.builder()` during `apply` / `pull` /
	/// `tick` so the harness can synthesise an output `Change` for the
	/// caller to inspect.
	builder_registry: TestBuilderRegistry,
	/// Arena used to marshal input `Change` -> `ChangeFFI` so the
	/// operator can read it as a `BorrowedChange`.
	input_arena: Arena,
}

impl<T: FFIOperator> OperatorTestHarness<T> {
	/// Create a new test harness builder
	pub fn builder() -> TestHarnessBuilder<T> {
		TestHarnessBuilder::new()
	}

	/// Apply a flow change to the operator via the zero-copy ABI.
	///
	/// Marshals the input as a `ChangeFFI` borrow, drives the operator,
	/// and assembles an output `Change` from whatever the operator emitted
	/// via `ctx.builder()`. The result is also appended to the harness
	/// history so it can be inspected via `harness[i]`, `last_change()`,
	/// or `history_len()`.
	pub fn apply(&mut self, input: Change) -> Result<Change> {
		let version = input.version;
		let changed_at = input.changed_at;
		let origin = input.origin.clone();
		// Reset arena for this call (cheap; bumpalo reset).
		self.input_arena.clear();
		let ffi_change = self.input_arena.marshal_change(&input);
		let ffi_ctx_ptr = &mut *self.ffi_context as *mut ContextFFI;

		let result: Result<()> = with_registry(&self.builder_registry, || {
			let mut op_ctx = OperatorContext::new(ffi_ctx_ptr);
			let borrowed = unsafe { BorrowedChange::from_raw(&ffi_change as *const _) };
			self.operator.apply(&mut op_ctx, borrowed)?;
			// Mirror the production txn-commit lifecycle: drain dirty StateCache
			// entries before the call returns. Without this step, operators
			// that buffer state through StateCache silently drop their in-flight
			// state across snapshot/restore cycles.
			self.operator.flush_state(&mut op_ctx)
		});
		// Drop the input arena's outstanding scaffolding before doing
		// anything else (input pointers are now invalid).
		drop(input);
		result?;

		let emitted = self.builder_registry.drain_diffs();
		let diffs = into_diffs(emitted);
		let output = match origin {
			ChangeOrigin::Flow(node) => Change::from_flow(node, version, diffs, changed_at),
			ChangeOrigin::Shape(_) => Change::from_flow(self.node_id, version, diffs, changed_at),
		};
		self.history.push(output.clone());
		Ok(output)
	}

	/// Chainable Insert: applies immediately, records output in history, panics on error.
	///
	/// Use for seeding state and/or inspecting emissions:
	/// ```ignore
	/// harness.insert(row1).insert(row2);
	/// assert_eq!(harness[0].diffs.len(), 1);
	/// ```
	pub fn insert(&mut self, row: Row) -> &mut Self {
		let change = TestChangeBuilder::new().insert(row).build();
		self.apply(change).expect("insert failed");
		self
	}

	/// Chainable Update: applies immediately, records output in history, panics on error.
	pub fn update(&mut self, pre: Row, post: Row) -> &mut Self {
		let change = TestChangeBuilder::new().update(pre, post).build();
		self.apply(change).expect("update failed");
		self
	}

	/// Chainable Remove: applies immediately, records output in history, panics on error.
	pub fn remove(&mut self, row: Row) -> &mut Self {
		let change = TestChangeBuilder::new().remove(row).build();
		self.apply(change).expect("remove failed");
		self
	}

	/// Number of changes recorded in the history so far.
	pub fn history_len(&self) -> usize {
		self.history.len()
	}

	/// Reference to the most recent change, or `None` if the history is empty.
	pub fn last_change(&self) -> Option<&Change> {
		self.history.last()
	}

	/// Clear the recorded history without affecting operator state.
	pub fn clear_history(&mut self) {
		self.history.clear();
	}

	/// Pull rows by their row numbers. The operator emits its result via
	/// `ctx.builder()` as a single Insert-shaped diff; we read its `post`
	/// columns as the return value.
	pub fn pull(&mut self, row_numbers: &[RowNumber]) -> Result<Columns> {
		let ffi_ctx_ptr = &mut *self.ffi_context as *mut ContextFFI;
		let result: Result<()> = with_registry(&self.builder_registry, || {
			let mut op_ctx = OperatorContext::new(ffi_ctx_ptr);
			self.operator.pull(&mut op_ctx, row_numbers)?;
			self.operator.flush_state(&mut op_ctx)
		});
		result?;

		let mut emitted = self.builder_registry.drain_diffs();
		let cols = if let Some(first) = emitted.drain(..).next() {
			first.post.or(first.pre).unwrap_or_else(Columns::empty)
		} else {
			Columns::empty()
		};
		Ok(cols)
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
		let shape = RowShape::testing(&[expected.get_type()]);

		store.assert_value(&encoded_key, &[expected], &shape);
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
	pub fn snapshot_state(&self) -> HashMap<EncodedKey, EncodedRow> {
		self.state().snapshot()
	}

	/// Restore state from a snapshot
	pub fn restore_state(&mut self, snapshot: HashMap<EncodedKey, EncodedRow>) {
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
		self.history.clear();

		// Recreate the operator
		self.operator = T::new(self.node_id, &self.config, None)?;
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

/// Index into the harness history - `harness[i]` returns the i-th recorded `Change`.
///
/// Panics if `i` is out of bounds.
impl<T: FFIOperator> Index<usize> for OperatorTestHarness<T> {
	type Output = Change;

	fn index(&self, index: usize) -> &Self::Output {
		&self.history[index]
	}
}

/// Builder for OperatorTestHarness
pub struct TestHarnessBuilder<T: FFIOperator> {
	config: HashMap<String, Value>,
	node_id: FlowNodeId,
	version: CommitVersion,
	initial_state: HashMap<EncodedKey, EncodedRow>,
	_phantom: PhantomData<T>,
}

impl<T: FFIOperator> Default for TestHarnessBuilder<T> {
	fn default() -> Self {
		Self::new()
	}
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
		self.initial_state.insert(key.encode(), EncodedRow(CowVec::new(value)));
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
			clock_now_nanos: 0,
			callbacks: create_test_callbacks(),
		});

		// Create the operator
		let operator = T::new(self.node_id, &self.config, None)?;

		Ok(OperatorTestHarness {
			operator,
			context,
			ffi_context,
			config: self.config,
			node_id: self.node_id,
			history: Vec::new(),
			builder_registry: TestBuilderRegistry::new(),
			input_arena: Arena::new(),
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
	use reifydb_abi::{
		callbacks::builder::EmitDiffKind, data::column::ColumnTypeCode, flow::diff::DiffType,
		operator::capabilities::CAPABILITY_ALL_STANDARD,
	};
	use reifydb_core::{
		common::CommitVersion,
		encoded::{key::IntoEncodedKey, shape::RowShape},
		interface::catalog::flow::FlowNodeId,
		row::Ttl,
	};
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::{super::helpers::encode_key, *};
	use crate::{
		operator::{
			FFIOperator, FFIOperatorMetadata,
			builder::{ColumnsBuilder, CommittedColumn},
			change::{BorrowedChange, BorrowedColumns},
			column::OperatorColumn,
			context::OperatorContext,
		},
		testing::builders::{TestChangeBuilder, TestRowBuilder},
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
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}

	impl FFIOperator for TestOperator {
		fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>, _ttl: Option<Ttl>) -> Result<Self> {
			Ok(Self {
				_node_id: operator_id,
				_config: config.clone(),
			})
		}

		fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> Result<()> {
			// Pass-through: forward each input diff via the builder.
			forward_diffs_passthrough(ctx, &input)
		}

		fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<()> {
			Ok(())
		}
	}

	// Stateful operator that stores values from flow changes
	struct StatefulTestOperator;

	impl FFIOperatorMetadata for StatefulTestOperator {
		const NAME: &'static str = "stateful_test_operator";
		const API: u32 = 1;
		const VERSION: &'static str = "1.0.0";
		const DESCRIPTION: &'static str = "Stateful test operator that stores values";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
	}

	impl FFIOperator for StatefulTestOperator {
		fn new(_operator_id: FlowNodeId, _config: &HashMap<String, Value>, _ttl: Option<Ttl>) -> Result<Self> {
			Ok(Self)
		}

		fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> Result<()> {
			// Stash the post-row's first int8 value into operator
			// state, keyed by the row number. Then forward the
			// diffs unchanged via the builder so callers can still
			// inspect the apply output.
			for diff in input.diffs() {
				let post = match diff.kind() {
					DiffType::Insert | DiffType::Update => Some(diff.post()),
					DiffType::Remove => None,
				};
				if let Some(columns) = post {
					let row_numbers = columns.row_numbers();
					let first_int8 = columns
						.columns()
						.next()
						.and_then(|c| unsafe { c.as_slice::<i64>() })
						.and_then(|s| s.first().copied());
					if let (Some(&rn), Some(v)) = (row_numbers.first(), first_int8) {
						let row_key = format!("row_{}", rn);
						let shape = RowShape::testing(&[Type::Int8]);
						let mut encoded = shape.allocate();
						shape.set_values(&mut encoded, &[Value::Int8(v)]);
						ctx.state().set(&row_key.into_encoded_key(), &encoded)?;
					}
				}
			}
			forward_diffs_passthrough(ctx, &input)
		}

		fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<()> {
			Ok(())
		}
	}

	/// Helper used by both test operators: read each input diff and emit
	/// it back unchanged via `ctx.builder()`. This keeps the harness's
	/// `apply` returning a `Change` that mirrors the input - same shape
	/// the legacy `Ok(input)` pass-through produced.
	fn forward_diffs_passthrough(ctx: &mut OperatorContext, input: &BorrowedChange<'_>) -> Result<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => {
					let (cols, names) = clone_columns(&mut builder, diff.post())?;
					let post: Vec<CommittedColumn> = cols;
					let post_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
					let row_numbers: Vec<RowNumber> =
						diff.post().row_numbers().iter().copied().map(RowNumber).collect();
					let _ = post; // satisfy borrow checker if unused
					builder.emit_insert(&post, &post_names, &row_numbers)?;
				}
				DiffType::Update => {
					let (pre_cols, pre_names) = clone_columns(&mut builder, diff.pre())?;
					let (post_cols, post_names) = clone_columns(&mut builder, diff.post())?;
					let pre_names: Vec<&str> = pre_names.iter().map(|s| s.as_str()).collect();
					let post_names: Vec<&str> = post_names.iter().map(|s| s.as_str()).collect();
					let pre_row_count = diff.pre().row_count();
					let post_row_count = diff.post().row_count();
					let pre_row_numbers: Vec<RowNumber> =
						diff.pre().row_numbers().iter().copied().map(RowNumber).collect();
					let post_row_numbers: Vec<RowNumber> =
						diff.post().row_numbers().iter().copied().map(RowNumber).collect();
					builder.emit_update(
						&pre_cols,
						&pre_names,
						pre_row_count,
						&pre_row_numbers,
						&post_cols,
						&post_names,
						post_row_count,
						&post_row_numbers,
					)?;
				}
				DiffType::Remove => {
					let (cols, names) = clone_columns(&mut builder, diff.pre())?;
					let names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
					let row_numbers: Vec<RowNumber> =
						diff.pre().row_numbers().iter().copied().map(RowNumber).collect();
					builder.emit_remove(&cols, &names, &row_numbers)?;
				}
			}
		}
		// Suppress emit-kind-not-used warning by silencing the import.
		let _ = EmitDiffKind::Insert;
		Ok(())
	}

	/// Acquire matching builders for each column in `cols`, copy bytes +
	/// offsets across, commit, and return the committed handles + names.
	fn clone_columns(
		builder: &mut ColumnsBuilder<'_>,
		cols: BorrowedColumns<'_>,
	) -> Result<(Vec<CommittedColumn>, Vec<String>)> {
		let row_count = cols.row_count();
		let mut committed: Vec<CommittedColumn> = Vec::new();
		let mut names: Vec<String> = Vec::new();
		for col in cols.columns() {
			let type_code = col.type_code();
			let bytes = col.data_bytes();
			let active = builder.acquire(type_code, row_count.max(1))?;
			active.grow(bytes.len().max(row_count))?;
			let dst = active.data_ptr();
			if !dst.is_null() && !bytes.is_empty() {
				unsafe {
					core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, bytes.len());
				}
			}
			// For var-len types, copy offsets too.
			if matches!(type_code, ColumnTypeCode::Utf8 | ColumnTypeCode::Blob) {
				let off = col.offsets();
				let dst_off = active.offsets_ptr();
				if !dst_off.is_null() && !off.is_empty() {
					unsafe {
						core::ptr::copy_nonoverlapping(off.as_ptr(), dst_off, off.len());
					}
				}
			}
			let c = active.commit(row_count)?;
			committed.push(c);
			names.push(col.name().to_string());
		}
		Ok((committed, names))
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
		let shape = RowShape::testing(&[Type::Int8]);
		let key = encode_key("row_1");

		// Assert the state was set through the FFI bridge
		state.assert_value(&key, &[Value::Int8(42i64)], &shape);
	}

	#[test]
	fn test_harness_history_index() {
		let mut harness = TestHarnessBuilder::<StatefulTestOperator>::new()
			.with_node_id(FlowNodeId(1))
			.build()
			.expect("Failed to build harness");

		// History starts empty
		assert_eq!(harness.history_len(), 0);
		assert!(harness.last_change().is_none());

		// Each apply() call records a Change
		let input_a = TestChangeBuilder::new().insert_row(1, vec![Value::Int8(1i64)]).build();
		harness.apply(input_a).expect("apply a failed");
		assert_eq!(harness.history_len(), 1);

		let input_b = TestChangeBuilder::new().insert_row(2, vec![Value::Int8(2i64)]).build();
		harness.apply(input_b).expect("apply b failed");
		assert_eq!(harness.history_len(), 2);

		// Index returns the i-th recorded Change
		assert_eq!(harness[0].diffs.len(), 1);
		assert_eq!(harness[1].diffs.len(), 1);

		// Chainable insert also records
		harness.insert(TestRowBuilder::new(3).add_value(Value::Int8(3i64)).build());
		assert_eq!(harness.history_len(), 3);

		// last_change returns the most recent
		assert!(harness.last_change().is_some());

		// clear_history resets without affecting state
		let state_count_before = harness.state().len();
		harness.clear_history();
		assert_eq!(harness.history_len(), 0);
		assert!(harness.last_change().is_none());
		assert_eq!(harness.state().len(), state_count_before);
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
		let shape = RowShape::testing(&[Type::Int8]);

		state.assert_value(&encode_key("row_1"), &[Value::Int8(10i64)], &shape);
		state.assert_value(&encode_key("row_2"), &[Value::Int8(20i64)], &shape);
		state.assert_value(&encode_key("row_3"), &[Value::Int8(30i64)], &shape);

		// Verify total state count
		assert_eq!(state.len(), 3);
	}
}
