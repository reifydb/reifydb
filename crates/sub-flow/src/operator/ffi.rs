// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI operator implementation that bridges FFI operators with ReifyDB

use reifydb_abi::{ColumnsFFI, ContextFFI, OperatorDescriptorFFI, OperatorVTableFFI};
use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::{FFIError, FlowChange, ffi::Arena};
use reifydb_type::RowNumber;
use std::cell::RefCell;
use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};
use tracing::{Span, error, instrument};

use crate::{
	ffi::{callbacks::create_host_callbacks, context::new_ffi_context},
	operator::Operator,
	transaction::FlowTransaction,
};

/// FFI operator that wraps an external operator implementation
pub struct FFIOperator {
	/// Operator descriptor from the FFI library
	descriptor: OperatorDescriptorFFI,
	/// Virtual function table for calling FFI functions
	vtable: OperatorVTableFFI,
	/// Pointer to the FFI operator instance
	instance: *mut c_void,
	/// ID for this operator
	operator_id: FlowNodeId,
	/// Arena for type conversions
	arena: RefCell<Arena>,
}

impl FFIOperator {
	/// Create a new FFI operator
	pub fn new(descriptor: OperatorDescriptorFFI, instance: *mut c_void, operator_id: FlowNodeId) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
			vtable,
			instance,
			operator_id,
			arena: RefCell::new(Arena::new()),
		}
	}

	/// Get the operator descriptor
	pub(crate) fn descriptor(&self) -> &OperatorDescriptorFFI {
		&self.descriptor
	}
}

impl Drop for FFIOperator {
	fn drop(&mut self) {
		// Call the destroy function from the vtable to clean up the FFI operator instance
		if !self.instance.is_null() {
			(self.vtable.destroy)(self.instance);
		}
	}
}

/// Marshal a flow change to FFI format
#[inline]
#[instrument(name = "flow::ffi::marshal", level = "trace", skip_all)]
fn marshal_input(arena: &mut Arena, change: &FlowChange) -> reifydb_abi::FlowChangeFFI {
	arena.marshal_flow_change(change)
}

/// Call the FFI vtable apply function
#[inline]
#[instrument(name = "flow::ffi::vtable_call", level = "trace", skip_all, fields(operator_id = operator_id.0))]
fn call_vtable(
	vtable: &OperatorVTableFFI,
	instance: *mut c_void,
	ffi_ctx_ptr: *mut ContextFFI,
	ffi_input: &reifydb_abi::FlowChangeFFI,
	ffi_output: &mut reifydb_abi::FlowChangeFFI,
	operator_id: FlowNodeId,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		(vtable.apply)(instance, ffi_ctx_ptr, ffi_input, ffi_output)
	}));

	match result {
		Ok(code) => code,
		Err(panic_info) => {
			let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
				s.to_string()
			} else if let Some(s) = panic_info.downcast_ref::<String>() {
				s.clone()
			} else {
				"Unknown panic".to_string()
			};
			error!(operator_id = operator_id.0, "FFI operator panicked during apply: {}", msg);
			abort();
		}
	}
}

/// Unmarshal FFI output to FlowChange
#[inline]
#[instrument(name = "flow::ffi::unmarshal", level = "trace", skip_all)]
fn unmarshal_output(arena: &mut Arena, ffi_output: &reifydb_abi::FlowChangeFFI) -> Result<FlowChange, String> {
	arena.unmarshal_flow_change(ffi_output)
}

impl Operator for FFIOperator {
	fn id(&self) -> FlowNodeId {
		self.operator_id
	}

	#[instrument(name = "flow::ffi::apply", level = "debug", skip_all, fields(
		operator_id = self.operator_id.0,
		input_diff_count = change.diffs.len(),
		output_diff_count = tracing::field::Empty
	))]
	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		let mut arena = self.arena.borrow_mut();

		// Phase 1: Marshal the flow change
		let ffi_input = marshal_input(&mut arena, &change);

		// Create output holder
		let mut ffi_output = reifydb_abi::FlowChangeFFI::empty();

		// Create FFI context
		let ffi_ctx = new_ffi_context(txn, self.operator_id, create_host_callbacks());
		let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut ContextFFI;

		// Phase 2: Call FFI vtable
		let result_code = call_vtable(
			&self.vtable,
			self.instance,
			ffi_ctx_ptr,
			&ffi_input,
			&mut ffi_output,
			self.operator_id,
		);

		// Check result code
		if result_code != 0 {
			return Err(
				FFIError::Other(format!("FFI operator apply failed with code: {}", result_code)).into()
			);
		}

		// Phase 3: Unmarshal the output
		let output_change = unmarshal_output(&mut arena, &ffi_output).map_err(|e| FFIError::Other(e))?;

		// Clear the arena after operation
		arena.clear();

		Span::current().record("output_diff_count", output_change.diffs.len());

		Ok(output_change)
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		let mut arena = self.arena.borrow_mut();
		// Convert row numbers to u64 array
		let row_numbers: Vec<u64> = rows.iter().map(|r| (*r).into()).collect();

		// Create output holder
		let mut ffi_output = ColumnsFFI::empty();

		// Create FFI context
		let ffi_ctx = new_ffi_context(txn, self.operator_id, create_host_callbacks());
		let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut ContextFFI;

		// Call FFI pull function
		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.pull)(
				self.instance,
				ffi_ctx_ptr,
				row_numbers.as_ptr(),
				row_numbers.len(),
				&mut ffi_output,
			)
		}));

		// Handle panics from FFI code - abort process on panic
		let result_code = match result {
			Ok(code) => code,
			Err(panic_info) => {
				let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
					s.to_string()
				} else if let Some(s) = panic_info.downcast_ref::<String>() {
					s.clone()
				} else {
					"Unknown panic".to_string()
				};
				error!(operator_id = self.operator_id.0, "FFI operator panicked during pull: {}", msg);
				abort();
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(
				FFIError::Other(format!("FFI operator pull failed with code: {}", result_code)).into()
			);
		}

		// Unmarshal the columns
		let columns = arena.unmarshal_columns(&ffi_output);

		// Clear the arena's arena after operation
		arena.clear();

		Ok(columns)
	}
}
