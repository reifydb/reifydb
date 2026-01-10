// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI operator implementation that bridges FFI operators with ReifyDB

use parking_lot::Mutex;
use reifydb_abi::{ColumnsFFI, OperatorDescriptorFFI, OperatorVTableFFI};
use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::{FFIError, FlowChange, ffi::Arena};
use reifydb_type::RowNumber;
use std::cell::RefCell;
use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
};
use tracing::{Span, debug_span, instrument};

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

impl Operator for FFIOperator {
	fn id(&self) -> FlowNodeId {
		self.operator_id
	}

	#[instrument(name = "flow::ffi::apply", level = "debug", skip_all, fields(
		operator_id = self.operator_id.0,
		input_diff_count = change.diffs.len(),
		output_diff_count = tracing::field::Empty,
		marshal_time_us = tracing::field::Empty,
		ffi_call_time_us = tracing::field::Empty,
		unmarshal_time_us = tracing::field::Empty,
		total_time_ms = tracing::field::Empty
	))]
	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		_evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<FlowChange> {
		let total_start = std::time::Instant::now();

		let mut arena = self.arena.borrow_mut();

		// Phase 1: Marshal the flow change
		let marshal_span = debug_span!("flow::ffi::marshal");
		let _marshal_guard = marshal_span.enter();
		let marshal_start = std::time::Instant::now();
		let ffi_input = arena.marshal_flow_change(&change);
		let marshal_us = marshal_start.elapsed().as_micros() as u64;
		drop(_marshal_guard);

		// Create output holder
		let mut ffi_output = reifydb_abi::FlowChangeFFI::empty();

		// Create FFI context
		let ffi_ctx = new_ffi_context(txn, self.operator_id, create_host_callbacks());
		let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut reifydb_abi::ContextFFI;

		// Phase 2: Call FFI vtable
		let ffi_span = debug_span!("flow::ffi::vtable_call");
		let _ffi_guard = ffi_span.enter();
		let ffi_start = std::time::Instant::now();

		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.apply)(self.instance, ffi_ctx_ptr, &ffi_input, &mut ffi_output)
		}));
		let ffi_us = ffi_start.elapsed().as_micros() as u64;
		drop(_ffi_guard);

		// Handle panics from FFI code
		let result_code = match result {
			Ok(code) => code,
			Err(_) => {
				return Err(FFIError::Other("FFI operator panicked during apply".to_string()).into());
			}
		};

		// Check result code
		if result_code != 0 {
			return Err(
				FFIError::Other(format!("FFI operator apply failed with code: {}", result_code)).into()
			);
		}

		// Phase 3: Unmarshal the output
		let unmarshal_span = debug_span!("flow::ffi::unmarshal");
		let _unmarshal_guard = unmarshal_span.enter();
		let unmarshal_start = std::time::Instant::now();
		let output_change = arena.unmarshal_flow_change(&ffi_output).map_err(|e| FFIError::Other(e))?;
		let unmarshal_us = unmarshal_start.elapsed().as_micros() as u64;
		drop(_unmarshal_guard);

		// Clear the arena's arena after operation
		arena.clear();

		Span::current().record("output_diff_count", output_change.diffs.len());
		Span::current().record("marshal_time_us", marshal_us);
		Span::current().record("ffi_call_time_us", ffi_us);
		Span::current().record("unmarshal_time_us", unmarshal_us);
		Span::current().record("total_time_ms", total_start.elapsed().as_millis() as u64);

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
		let ffi_ctx_ptr = &ffi_ctx as *const _ as *mut reifydb_abi::ContextFFI;

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

		// Handle panics from FFI code
		let result_code = match result {
			Ok(code) => code,
			Err(_) => {
				return Err(FFIError::Other("FFI operator panicked during pull".to_string()).into());
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
