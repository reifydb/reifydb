// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust procedures to FFI interface.

use std::{
	cell::RefCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_abi::{context::context::ContextFFI, data::column::ColumnsFFI, procedure::vtable::ProcedureVTableFFI};
use reifydb_type::params::Params;
use tracing::error;

use crate::{
	ffi::arena::Arena,
	procedure::{FFIProcedure, FFIProcedureContext},
};

/// Wrapper that adapts a Rust procedure to the FFI interface
pub struct ProcedureWrapper<T: FFIProcedure> {
	procedure: T,
	arena: RefCell<Arena>,
}

impl<T: FFIProcedure> ProcedureWrapper<T> {
	/// Create a new procedure wrapper
	pub fn new(procedure: T) -> Self {
		Self {
			procedure,
			arena: RefCell::new(Arena::new()),
		}
	}

	/// Create from a raw pointer
	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// FFI procedure call function
pub extern "C" fn ffi_procedure_call<T: FFIProcedure>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	params_ptr: *const u8,
	params_len: usize,
	output: *mut ColumnsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = ProcedureWrapper::<T>::from_ptr(instance);

		let mut arena = wrapper.arena.borrow_mut();
		arena.clear();

		// Deserialize params from postcard bytes
		let params: Params = if params_ptr.is_null() || params_len == 0 {
			Params::None
		} else {
			let bytes = unsafe { std::slice::from_raw_parts(params_ptr, params_len) };
			match postcard::from_bytes(bytes) {
				Ok(p) => p,
				Err(e) => {
					error!(?e, "Failed to deserialize procedure params");
					return -2;
				}
			}
		};

		// Build context
		let proc_ctx = FFIProcedureContext::new(ctx);

		// Call procedure
		let output_columns = match wrapper.procedure.call(&proc_ctx, params) {
			Ok(cols) => cols,
			Err(e) => {
				error!(?e, "Procedure call failed");
				return -2;
			}
		};

		// Marshal output
		unsafe {
			*output = arena.marshal_columns(&output_columns);
		}

		0 // Success
	}));

	let code = result.unwrap_or_else(|e| {
		error!(?e, "Panic in ffi_procedure_call");
		-99
	});
	if code < 0 {
		error!(code, "ffi_procedure_call failed - aborting");
		abort();
	}
	code
}

/// FFI destroy function - drop the procedure wrapper
pub extern "C" fn ffi_procedure_destroy<T: FFIProcedure>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		let _wrapper = Box::from_raw(instance as *mut ProcedureWrapper<T>);
	}));

	if let Err(e) = result {
		error!(?e, "Panic in ffi_procedure_destroy - aborting");
		abort();
	}
}

/// Create the vtable for a procedure type
pub fn create_procedure_vtable<T: FFIProcedure>() -> ProcedureVTableFFI {
	ProcedureVTableFFI {
		call: ffi_procedure_call::<T>,
		destroy: ffi_procedure_destroy::<T>,
	}
}
