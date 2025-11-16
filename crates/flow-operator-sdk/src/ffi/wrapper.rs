//! Wrapper that bridges Rust operators to FFI interface

use std::{
	cell::RefCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	sync::Mutex,
};

use reifydb_flow_operator_abi::*;
use reifydb_type::RowNumber;

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
				Err(_) => return -1,
			};

			let mut marshaller = wrapper.marshaller.borrow_mut();
			marshaller.clear();

			// Unmarshal input using the marshaller
			let input_change = match marshaller.unmarshal_flow_change(&*input) {
				Ok(change) => change,
				Err(_) => return -3,
			};

			// Create context and apply operator
			let mut op_ctx = OperatorContext::new(ctx);
			let output_change = match operator.apply(&mut op_ctx, input_change) {
				Ok(change) => change,
				Err(_) => return -2,
			};

			*output = marshaller.marshal_flow_change(&output_change);
			0 // Success
		}
	}));

	result.unwrap_or(-99)
}

pub extern "C" fn ffi_get_rows<O: FFIOperator>(
	instance: *mut c_void,
	ctx: *mut FFIContext,
	row_numbers: *const u64,
	count: usize,
	output: *mut RowsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		unsafe {
			let wrapper = OperatorWrapper::<O>::from_ptr(instance);
			let mut operator = match wrapper.operator.lock() {
				Ok(op) => op,
				Err(_) => return -1,
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
			let rows = match operator.get_rows(&mut op_ctx, &numbers) {
				Ok(rows) => rows,
				Err(_) => return -2,
			};

			*output = marshaller.marshal_rows(&rows);

			0 // Success
		}
	}));

	result.unwrap_or(-99)
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
		get_rows: ffi_get_rows::<O>,
		destroy: ffi_destroy::<O>,
	}
}
