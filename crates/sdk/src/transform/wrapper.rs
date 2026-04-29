// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust transforms to FFI interface.
//!
//! Zero-copy ABI: input arrives as `BorrowedColumns<'_>` over native column
//! storage; output is emitted via `ctx.builder()` directly into host-pool
//! buffers. Nothing crosses the FFI boundary as a guest-allocated `Columns`.
//!
//! FFI function return codes:
//! - `< 0`: Unrecoverable error - process will abort immediately
//! - `0`: Success
//! - `> 0`: Recoverable error (reserved for future use)

use std::{
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_abi::{context::context::ContextFFI, data::column::ColumnsFFI, transform::vtable::TransformVTableFFI};
use tracing::error;

use crate::{
	operator::change::BorrowedColumns,
	transform::{FFITransform, context::FFITransformContext},
};

pub struct TransformWrapper<T: FFITransform> {
	transform: T,
}

impl<T: FFITransform> TransformWrapper<T> {
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
/// - `ctx` must point to a valid `ContextFFI`.
/// - `input` must point to a valid `ColumnsFFI`.
pub unsafe extern "C" fn ffi_transform<T: FFITransform>(
	instance: *mut c_void,
	ctx: *mut ContextFFI,
	input: *const ColumnsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = TransformWrapper::<T>::from_ptr(instance);

		// Zero-copy: borrow input columns directly from the FFI struct.
		let borrowed_input = unsafe { BorrowedColumns::from_ffi(input) };
		let mut tctx = FFITransformContext::new(ctx);

		match wrapper.transform.transform(&mut tctx, borrowed_input) {
			Ok(()) => 0,
			Err(e) => {
				error!(?e, "Transform failed");
				-2
			}
		}
	}));

	let code = result.unwrap_or_else(|e| {
		error!(?e, "Panic in ffi_transform");
		-99
	});
	if code < 0 {
		error!(code, "ffi_transform failed - aborting");
		abort();
	}
	code
}

/// # Safety
///
/// - `instance` must be a valid pointer to a `TransformWrapper<T>`, or null.
pub unsafe extern "C" fn ffi_transform_destroy<T: FFITransform>(instance: *mut c_void) {
	if instance.is_null() {
		return;
	}

	let result = catch_unwind(AssertUnwindSafe(|| unsafe {
		let _wrapper = Box::from_raw(instance as *mut TransformWrapper<T>);
	}));

	if let Err(e) = result {
		error!(?e, "Panic in ffi_transform_destroy - aborting");
		abort();
	}
}

pub fn create_transform_vtable<T: FFITransform>() -> TransformVTableFFI {
	TransformVTableFFI {
		transform: ffi_transform::<T>,
		destroy: ffi_transform_destroy::<T>,
	}
}
