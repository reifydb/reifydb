//! Wrapper that bridges Rust operators to FFI interface

use crate::context::OperatorContext;
use crate::operator::{FFIOperator, FlowChange};
use super::marshaller::FFIMarshaller;
use reifydb_core::interface::FlowNodeId;
use reifydb_operator_abi::*;
use reifydb_type::RowNumber;
use std::cell::RefCell;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

/// Wrapper that adapts a Rust operator to the FFI interface
pub struct OperatorWrapper<O: FFIOperator> {
	operator: Mutex<O>,
	node_id: FlowNodeId,
	marshaller: RefCell<FFIMarshaller>,
}

impl<O: FFIOperator> OperatorWrapper<O> {
	/// Create a new operator wrapper
	pub fn new(operator: O, node_id: FlowNodeId) -> Self {
		Self {
			operator: Mutex::new(operator),
			node_id,
			marshaller: RefCell::new(FFIMarshaller::new()),
		}
	}

	/// Get a pointer to this wrapper as c_void
	pub fn as_ptr(&mut self) -> *mut c_void {
		self as *mut _ as *mut c_void
	}

	/// Create from a raw pointer
	pub unsafe fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		&mut *(ptr as *mut Self)
	}
}

// FFI callback implementations

pub extern "C" fn ffi_apply<O: FFIOperator>(
	instance: *mut c_void,
	txn: *mut TransactionHandle,
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

			// Unmarshal input using the marshaller
			let mut marshaller = wrapper.marshaller.borrow_mut();
			let input_change = match marshaller.unmarshal_flow_change(&*input) {
				Ok(change) => change,
				Err(_) => return -3,
			};

			// Create context and apply operator
			let mut ctx = OperatorContext::new(wrapper.node_id, txn);
			let output_change = match operator.apply(&mut ctx, input_change) {
				Ok(change) => change,
				Err(_) => return -2,
			};

			// Marshal output
			*output = marshaller.marshal_flow_change(&output_change);
			0 // Success
		}
	}));

	result.unwrap_or(-99)
}

pub extern "C" fn ffi_get_rows<O: FFIOperator>(
	instance: *mut c_void,
	txn: *mut TransactionHandle,
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
			let mut ctx = OperatorContext::new(wrapper.node_id, txn);

			// Call the operator
			let rows = match operator.get_rows(&mut ctx, &numbers) {
				Ok(rows) => rows,
				Err(_) => return -2,
			};

			// Marshal output using the marshaller
			let mut marshaller = wrapper.marshaller.borrow_mut();
			*output = marshaller.marshal_rows(&rows);

			0 // Success
		}
	}));

	result.unwrap_or(-99)
}

pub extern "C" fn ffi_destroy<O: FFIOperator>(instance: *mut c_void) {
	unsafe {
		if !instance.is_null() {
			let wrapper = Box::from_raw(instance as *mut OperatorWrapper<O>);
			if let Ok(mut operator) = wrapper.operator.into_inner() {
				operator.destroy();
			}
		}
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
