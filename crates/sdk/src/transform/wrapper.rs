// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust transforms to FFI interface.
//!
//! FFI function return codes:
//! - `< 0`: Unrecoverable error - process will abort immediately
//! - `0`: Success
//! - `> 0`: Recoverable error (reserved for future use)

use std::{
	cell::UnsafeCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_abi::{context::context::ContextFFI, data::column::ColumnsFFI, transform::vtable::TransformVTableFFI};
use tracing::error;

use crate::{ffi::arena::Arena, transform::FFITransform};

// One scratch arena per OS thread, shared across `TransformWrapper` instances
// active on the same thread. Cleared at the top of each call so scaffolding
// memory is bounded.
thread_local! {
	static GUEST_TRANSFORM_ARENA: UnsafeCell<Arena> = UnsafeCell::new(Arena::new());
}

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

		// Reset the per-thread arena and unmarshal input within it.
		// SAFETY: single-threaded; no live pointers from a prior call.
		GUEST_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).clear() });
		let input_columns =
			GUEST_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).unmarshal_columns(&*input) });

		// Apply transform
		let output_columns = match wrapper.transform.transform(input_columns) {
			Ok(cols) => cols,
			Err(e) => {
				error!(?e, "Transform failed");
				return -2;
			}
		};

		// Marshal output as a zero-copy borrow over the guest's Columns
		// memory and hand it to the host's BuilderRegistry. The host
		// adopts it as a single Insert-shaped diff.
		let ffi_output =
			GUEST_TRANSFORM_ARENA.with(|cell| unsafe { (*cell.get()).marshal_columns(&output_columns) });

		let emit_code = unsafe {
			let cb = (*ctx).callbacks.builder;
			(cb.emit_columns_marshaled)(ctx, &ffi_output)
		};
		// `output_columns` must outlive the callback because `ffi_output`
		// borrows its storage. The host copies what it needs synchronously
		// before returning; `output_columns` then drops at end of scope.
		drop(output_columns);
		if emit_code != 0 {
			return emit_code;
		}

		0 // Success
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
