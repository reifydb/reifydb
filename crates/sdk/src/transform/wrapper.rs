// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Wrapper that bridges Rust transforms to FFI interface.
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

use reifydb_abi::{data::column::ColumnsFFI, transform::vtable::TransformVTableFFI};
use tracing::error;

use crate::{ffi::arena::Arena, transform::FFITransform};

/// Wrapper that adapts a Rust transform to the FFI interface
pub struct TransformWrapper<T: FFITransform> {
	transform: T,
	arena: RefCell<Arena>,
}

impl<T: FFITransform> TransformWrapper<T> {
	/// Create a new transform wrapper
	pub fn new(transform: T) -> Self {
		Self {
			transform,
			arena: RefCell::new(Arena::new()),
		}
	}

	/// Create from a raw pointer
	pub fn from_ptr(ptr: *mut c_void) -> &'static mut Self {
		unsafe { &mut *(ptr as *mut Self) }
	}
}

/// FFI transform function - unmarshal input, call transform, marshal output
pub extern "C" fn ffi_transform<T: FFITransform>(
	instance: *mut c_void,
	input: *const ColumnsFFI,
	output: *mut ColumnsFFI,
) -> i32 {
	let result = catch_unwind(AssertUnwindSafe(|| {
		let wrapper = TransformWrapper::<T>::from_ptr(instance);

		let mut arena = wrapper.arena.borrow_mut();
		arena.clear();

		// Unmarshal input
		let input_columns = unsafe { arena.unmarshal_columns(&*input) };

		// Apply transform
		let output_columns = match wrapper.transform.transform(input_columns) {
			Ok(cols) => cols,
			Err(e) => {
				error!(?e, "Transform failed");
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
		error!(?e, "Panic in ffi_transform");
		-99
	});
	if code < 0 {
		error!(code, "ffi_transform failed - aborting");
		abort();
	}
	code
}

/// FFI destroy function - drop the transform wrapper
pub extern "C" fn ffi_transform_destroy<T: FFITransform>(instance: *mut c_void) {
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

/// Create the vtable for a transform type
pub fn create_transform_vtable<T: FFITransform>() -> TransformVTableFFI {
	TransformVTableFFI {
		transform: ffi_transform::<T>,
		destroy: ffi_transform_destroy::<T>,
	}
}
