// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust procedures to FFI interface.

use std::{
	cell::UnsafeCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
	slice,
};

use postcard::from_bytes;
use reifydb_abi::{context::context::ContextFFI, procedure::vtable::ProcedureVTableFFI};
use reifydb_type::params::Params;
use tracing::error;

use crate::{
	ffi::arena::Arena,
	procedure::{FFIProcedure, FFIProcedureContext},
};

// One scratch arena per OS thread, shared across `ProcedureWrapper` instances
// active on the same thread. Cleared at the top of each call so scaffolding
// memory is bounded.
thread_local! {
	static GUEST_PROC_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

pub struct ProcedureWrapper<T: FFIProcedure> {
	procedure: T,
}

impl<T: FFIProcedure> ProcedureWrapper<T> {
	pub fn new(procedure: T) -> Self {
		Self {
			procedure,
		}
	}

	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// # Safety
///
/// - `instance` must be a valid pointer to a `ProcedureWrapper<T>`.
/// - `ctx` must point to a valid `ContextFFI` for the duration of the call.
pub unsafe extern "C" fn ffi_procedure_call<T: FFIProcedure>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	params_ptr: *const u8,
	params_len: usize,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = ProcedureWrapper::<T>::from_ptr(instance);

		// SAFETY: single-threaded; no live pointers from a prior call.
		GUEST_PROC_ARENA.with(|cell| unsafe { (*cell.get()).clear() });

		// Deserialize params from postcard bytes
		let params: Params = if params_ptr.is_null() || params_len == 0 {
			Params::None
		} else {
			let bytes = unsafe { slice::from_raw_parts(params_ptr, params_len) };
			match from_bytes(bytes) {
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

		// Marshal output as a zero-copy borrow over guest's Columns
		// memory and hand it to the host's BuilderRegistry.
		let ffi_output =
			GUEST_PROC_ARENA.with(|cell| unsafe { (*cell.get()).marshal_columns(&output_columns) });
		let emit_code = unsafe {
			let cb = (*ctx).callbacks.builder;
			(cb.emit_columns_marshaled)(ctx, &ffi_output)
		};
		// `output_columns` must outlive the callback because `ffi_output`
		// borrows its storage. The host copies what it needs synchronously.
		drop(output_columns);
		if emit_code != 0 {
			return emit_code;
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

/// # Safety
///
/// - `instance` must be a valid pointer to a `ProcedureWrapper<T>`, or null.
pub unsafe extern "C" fn ffi_procedure_destroy<T: FFIProcedure>(instance: *mut c_void) {
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

pub fn create_procedure_vtable<T: FFIProcedure>() -> ProcedureVTableFFI {
	ProcedureVTableFFI {
		call: ffi_procedure_call::<T>,
		destroy: ffi_procedure_destroy::<T>,
	}
}
