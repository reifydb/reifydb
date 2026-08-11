// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_abi::{
	context::context::ExternCContext, data::column::ExternCColumns, transform::vtable::ExternCTransformVTable,
};
use tracing::error;

use crate::{
	operator::change::BorrowedColumns,
	transform::{ExternCTransform, context::ExternCTransformContext},
};

pub struct TransformWrapper<T: ExternCTransform> {
	transform: T,
}

impl<T: ExternCTransform> TransformWrapper<T> {
	pub fn new(transform: T) -> Self {
		Self {
			transform,
		}
	}

	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// # Safety
///
/// - `instance` must be a valid pointer to a `TransformWrapper<T>`.
/// - `ctx` must point to a valid `ExternCContext`.
/// - `input` must point to a valid `ExternCColumns`.
pub unsafe extern "C" fn extern_c_transform<T: ExternCTransform>(
	instance: *mut c_void,
	ctx: *mut ExternCContext,
	input: *const ExternCColumns,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = TransformWrapper::<T>::from_ptr(instance);

		// SAFETY: discharges BorrowedColumns::from_extern_c; extern_c_transform's contract makes input a valid
		// ExternCColumns whose buffer pointers stay live for the borrow, which ends with this closure.
		let borrowed_input = unsafe { BorrowedColumns::from_extern_c(input) };
		let mut tctx = ExternCTransformContext::new(ctx);

		match wrapper.transform.transform(&mut tctx, borrowed_input) {
			Ok(()) => 0,
			Err(e) => {
				error!(?e, "Transform failed");
				-2
			}
		}
	}));

	let code = result.unwrap_or_else(|e| {
		error!(?e, "Panic in extern_c_transform");
		-99
	});
	if code < 0 {
		error!(code, "extern_c_transform failed - aborting");
		abort();
	}
	code
}

/// # Safety
///
/// - `instance` must be a valid pointer to a `TransformWrapper<T>`, or null.
pub unsafe extern "C" fn extern_c_transform_destroy<T: ExternCTransform>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	// SAFETY: instance was checked non-null above and extern_c_transform_destroy's contract makes it a Box::new
	// allocated TransformWrapper<T>; the host calls destroy once, so ownership is taken exactly once.
	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		let _wrapper = Box::from_raw(instance as *mut TransformWrapper<T>);
	}));

	if let Err(e) = result {
		error!(?e, "Panic in extern_c_transform_destroy - aborting");
		abort();
	}
}

pub fn create_transform_vtable<T: ExternCTransform>() -> ExternCTransformVTable {
	ExternCTransformVTable {
		transform: extern_c_transform::<T>,
		destroy: extern_c_transform_destroy::<T>,
	}
}
