//! Wrapper that bridges Rust operators to FFI interface

use std::{
	cell::RefCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	sync::Mutex,
};

use reifydb_flow_operator_abi::*;
use reifydb_type::RowNumber;
use tracing::{Span, debug_span, instrument, warn};

use crate::{FFIOperator, context::OperatorContext, marshal::Marshaller};

/// Wrapper that adapts a Rust operator to the FFI interface
pub struct OperatorWrapper<O: FFIOperator> {
	operator: Mutex<O>,
	marshaller: RefCell<Marshaller>,
}

impl<O: FFIOperator> OperatorWrapper<O> {
	/// Create a new operator wrapper
	pub fn new(operator: O) -> Self {
		Self {
			operator: Mutex::new(operator),
			marshaller: RefCell::new(Marshaller::new()),
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

#[instrument(name = "flow::operator::ffi::apply", level = "debug", skip_all, fields(
	operator_type = std::any::type_name::<O>(),
	input_diffs,
	output_diffs
))]
pub extern "C" fn ffi_apply<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut FFIContext,
	input: *const FlowChangeFFI,
	output: *mut FlowChangeFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		unsafe {
			let wrapper = OperatorWrapper::<O>::from_ptr(instance);
			let mut operator = match wrapper.operator.lock() {
				Ok(op) => op,
				Err(_) => {
					warn!("Failed to lock operator");
					return -1;
				}
			};

			let mut marshaller = wrapper.marshaller.borrow_mut();
			marshaller.clear();

			// Unmarshal input using the marshaller
			let unmarshal_span = debug_span!("unmarshal");
			let _guard = unmarshal_span.enter();
			let input_change = match marshaller.unmarshal_flow_change(&*input) {
				Ok(change) => {
					Span::current().record("input_diffs", change.diffs.len());
					change
				}
				Err(e) => {
					warn!(?e, "Unmarshal failed");
					return -3;
				}
			};
			drop(_guard);

			// Create context and apply operator
			let apply_span = debug_span!("operator_apply");
			let _guard = apply_span.enter();
			let mut op_ctx = OperatorContext::new(ctx);
			let output_change = match operator.apply(&mut op_ctx, input_change) {
				Ok(change) => {
					Span::current().record("output_diffs", change.diffs.len());
					change
				}
				Err(e) => {
					warn!(?e, "Apply failed");
					return -2;
				}
			};
			drop(_guard);

			// Marshal output
			let marshal_span = debug_span!("marshal");
			let _guard = marshal_span.enter();
			*output = marshaller.marshal_flow_change(&output_change);
			drop(_guard);

			0 // Success
		}
	}));

	result.unwrap_or_else(|e| {
		warn!(?e, "Panic in ffi_apply");
		-99
	})
}

#[instrument(name = "flow::operator::ffi::pull", level = "debug", skip_all, fields(
	operator_type = std::any::type_name::<O>(),
	row_count = count,
	rows_returned
))]
pub extern "C" fn ffi_pull<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut FFIContext,
	row_numbers: *const u64,
	count: usize,
	output: *mut ColumnsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		unsafe {
			let wrapper = OperatorWrapper::<O>::from_ptr(instance);
			let mut operator = match wrapper.operator.lock() {
				Ok(op) => op,
				Err(_) => {
					warn!("Failed to lock operator");
					return -1;
				}
			};

			let mut marshaller = wrapper.marshaller.borrow_mut();
			marshaller.clear();

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
			let columns = match operator.pull(&mut op_ctx, &numbers) {
				Ok(cols) => {
					Span::current().record("rows_returned", cols.row_count());
					cols
				}
				Err(e) => {
					warn!(?e, "pull failed");
					return -2;
				}
			};

			*output = marshaller.marshal_columns(&columns);

			0 // Success
		}
	}));

	result.unwrap_or_else(|e| {
		warn!(?e, "Panic in ffi_pull");
		-99
	})
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

	if result.is_err() {
		eprintln!("FFI operator panicked during destroy");
	}
}

/// Create the vtable for an operator type
pub fn create_vtable<O: FFIOperator>() -> FFIOperatorVTable {
	FFIOperatorVTable {
		apply: ffi_apply::<O>,
		pull: ffi_pull::<O>,
		destroy: ffi_destroy::<O>,
	}
}
