// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust operators to FFI interface.
//!
//! FFI function return codes:
//! - `< 0`: Unrecoverable error - process will abort immediately
//! - `0`: Success
//! - `> 0`: Recoverable error (reserved for future use)

use std::{
	cell::RefCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_abi::{
	context::context::ContextFFI, data::column::ColumnsFFI, flow::change::ChangeFFI,
	operator::vtable::OperatorVTableFFI,
};
use reifydb_core::interface::change::Change;
use reifydb_type::value::row_number::RowNumber;
use tracing::{Span, error, instrument, warn};

use crate::{
	ffi::Arena,
	operator::{FFIOperator, context::OperatorContext},
};

/// Wrapper that adapts a Rust operator to the FFI interface
pub struct OperatorWrapper<O: FFIOperator> {
	operator: O,
	arena: RefCell<Arena>,
}

impl<O: FFIOperator> OperatorWrapper<O> {
	/// Create a new operator wrapper
	pub fn new(operator: O) -> Self {
		Self {
			operator,
			arena: RefCell::new(Arena::new()),
		}
	}

	/// Get a pointer to this wrapper as c_void
	pub fn as_ptr(&mut self) -> *mut c_void {
		self as *mut _ as *mut c_void
	}

	/// Create from a raw pointer
	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// Unmarshal FFI input to Change
#[inline]
#[instrument(name = "unmarshal", level = "trace", skip_all)]
fn unmarshal_input<O: FFIOperator>(arena: &mut Arena, input: *const ChangeFFI) -> Result<Change, i32> {
	unsafe {
		match arena.unmarshal_change(&*input) {
			Ok(change) => Ok(change),
			Err(e) => {
				warn!(?e, "Unmarshal failed");
				Err(-3)
			}
		}
	}
}

/// Apply the operator
#[inline]
#[instrument(name = "operator_apply", level = "trace", skip_all)]
fn apply_operator<O: FFIOperator>(
	operator: &mut O,
	ctx: *mut ContextFFI,
	input_change: Change,
) -> Result<Change, i32> {
	let mut op_ctx = OperatorContext::new(ctx);
	match operator.apply(&mut op_ctx, input_change) {
		Ok(change) => Ok(change),
		Err(e) => {
			warn!(?e, "Apply failed");
			Err(-2)
		}
	}
}

/// Marshal Change to FFI output
#[inline]
#[instrument(name = "marshal", level = "trace", skip_all)]
fn marshal_output(arena: &mut Arena, output_change: &Change, output: *mut ChangeFFI) {
	unsafe {
		*output = arena.marshal_change(output_change);
	}
}

#[instrument(name = "flow::operator::ffi::apply", level = "debug", skip_all, fields(
	operator_type = std::any::type_name::<O>(),
	input_diffs,
	output_diffs
))]
pub extern "C" fn ffi_apply<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	input: *const ChangeFFI,
	output: *mut ChangeFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = OperatorWrapper::<O>::from_ptr(instance);

		let mut arena = wrapper.arena.borrow_mut();
		arena.clear();

		// Unmarshal input
		let input_change = match unmarshal_input::<O>(&mut arena, input) {
			Ok(change) => {
				Span::current().record("input_diffs", change.diffs.len());
				change
			}
			Err(code) => return code,
		};

		// Apply operator
		let output_change = match apply_operator::<O>(&mut wrapper.operator, ctx, input_change) {
			Ok(change) => {
				Span::current().record("output_diffs", change.diffs.len());
				change
			}
			Err(code) => return code,
		};

		// Marshal output
		marshal_output(&mut arena, &output_change, output);

		0 // Success
	}));

	let code = result.unwrap_or_else(|e| {
		error!(?e, "Panic in ffi_apply");
		-99
	});
	if code < 0 {
		error!(code, "ffi_apply failed - aborting");
		abort();
	}
	code
}

#[instrument(name = "flow::operator::ffi::pull", level = "debug", skip_all, fields(
	operator_type = std::any::type_name::<O>(),
	row_count = count,
	rows_returned
))]
pub extern "C" fn ffi_pull<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	row_numbers: *const u64,
	count: usize,
	output: *mut ColumnsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		unsafe {
			let wrapper = OperatorWrapper::<O>::from_ptr(instance);

			let mut arena = wrapper.arena.borrow_mut();
			arena.clear();

			// Convert row numbers
			let numbers: Vec<RowNumber> = if !row_numbers.is_null() && count > 0 {
				std::slice::from_raw_parts(row_numbers, count)
					.iter()
					.map(|&n| RowNumber::from(n))
					.collect()
			} else {
				Vec::new()
			};

			// Create context
			let mut op_ctx = OperatorContext::new(ctx);

			// Call the operator
			let columns = match wrapper.operator.pull(&mut op_ctx, &numbers) {
				Ok(cols) => {
					Span::current().record("rows_returned", cols.row_count());
					cols
				}
				Err(e) => {
					warn!(?e, "pull failed");
					return -2;
				}
			};

			*output = arena.marshal_columns(&columns);

			0 // Success
		}
	}));

	let code = result.unwrap_or_else(|e| {
		error!(?e, "Panic in ffi_pull");
		-99
	});
	if code < 0 {
		error!(code, "ffi_pull failed - aborting");
		abort();
	}
	code
}

pub extern "C" fn ffi_destroy<O: FFIOperator>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		// Reconstruct the Box from the raw pointer and let it drop
		let _wrapper = Box::from_raw(instance as *mut OperatorWrapper<O>);
		// Wrapper will be dropped here, cleaning up the operator
	}));

	if let Err(e) = result {
		error!(?e, "Panic in ffi_destroy - aborting");
		abort();
	}
}

/// Create the vtable for an operator type
pub fn create_vtable<O: FFIOperator>() -> OperatorVTableFFI {
	OperatorVTableFFI {
		apply: ffi_apply::<O>,
		pull: ffi_pull::<O>,
		destroy: ffi_destroy::<O>,
	}
}
