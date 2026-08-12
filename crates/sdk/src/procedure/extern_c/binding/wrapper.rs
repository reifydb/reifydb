// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
	slice,
};

use reifydb_codec::value::decode_params;
use reifydb_value::params::Params;
use tracing::error;

use crate::procedure::extern_c::{
	binding::{context::ExternCProcedureContext, procedure::ExternCProcedure},
	wire::{context::ExternCContextRaw, vtable::ExternCProcedureVTable},
};

pub struct ProcedureWrapper<T: ExternCProcedure> {
	procedure: T,
}

impl<T: ExternCProcedure> ProcedureWrapper<T> {
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
/// - `ctx` must point to a valid `ExternCContextRaw` for the duration of the call.
pub unsafe extern "C" fn extern_c_procedure_call<T: ExternCProcedure>(
	instance: *mut c_void,
	ctx: *mut ExternCContextRaw,
	params_ptr: *const u8,
	params_len: usize,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = ProcedureWrapper::<T>::from_ptr(instance);

		let params: Params = if params_ptr.is_null() || params_len == 0 {
			Params::None
		} else {
			// SAFETY: null and zero-length are handled by the other branch; otherwise the host caller
			// keeps params_ptr readable for params_len bytes for the duration of this call.
			let bytes = unsafe { slice::from_raw_parts(params_ptr, params_len) };
			match decode_params(bytes) {
				Ok(p) => p,
				Err(e) => {
					error!(?e, "Failed to deserialize procedure params");
					return -2;
				}
			}
		};

		let mut pctx = ExternCProcedureContext::new(ctx);

		match wrapper.procedure.call(&mut pctx, params) {
			Ok(()) => 0,
			Err(e) => {
				error!(?e, "Procedure call failed");
				-2
			}
		}
	}));

	let code = result.unwrap_or_else(|e| {
		error!(?e, "Panic in extern_c_procedure_call");
		-99
	});
	if code < 0 {
		error!(code, "extern_c_procedure_call failed - aborting");
		abort();
	}
	code
}

/// # Safety
///
/// - `instance` must be a valid pointer to a `ProcedureWrapper<T>`, or null.
pub unsafe extern "C" fn extern_c_procedure_destroy<T: ExternCProcedure>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	// SAFETY: instance was checked non-null above and extern_c_procedure_destroy's contract makes it a Box::new
	// allocated ProcedureWrapper<T>; the host calls destroy once, so ownership is taken exactly once.
	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		let _wrapper = Box::from_raw(instance as *mut ProcedureWrapper<T>);
	}));

	if let Err(e) = result {
		error!(?e, "Panic in extern_c_procedure_destroy - aborting");
		abort();
	}
}

pub fn create_procedure_vtable<T: ExternCProcedure>() -> ExternCProcedureVTable {
	ExternCProcedureVTable {
		call: extern_c_procedure_call::<T>,
		destroy: extern_c_procedure_destroy::<T>,
	}
}
