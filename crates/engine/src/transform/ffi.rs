#![cfg(reifydb_target = "native")]
// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI transform implementation that bridges native shared-library transforms with ReifyDB

use std::{
	cell::RefCell,
	ffi::c_void,
	panic::{AssertUnwindSafe, catch_unwind},
	process::abort,
};

use reifydb_abi::{
	data::column::ColumnsFFI,
	transform::{descriptor::TransformDescriptorFFI, vtable::TransformVTableFFI},
};
use reifydb_core::value::column::columns::Columns;
use reifydb_sdk::ffi::arena::Arena;
use reifydb_type;
use tracing::{error, instrument};

use super::{Transform, TransformContext};

/// FFI transform that wraps an external transform implementation
pub struct NativeTransformFFI {
	/// Transform descriptor from the FFI library
	#[allow(dead_code)]
	descriptor: TransformDescriptorFFI,
	/// Virtual function table for calling FFI functions
	vtable: TransformVTableFFI,
	/// Pointer to the FFI transform instance
	instance: *mut c_void,
	/// Arena for type conversions
	arena: RefCell<Arena>,
}

impl NativeTransformFFI {
	/// Create a new FFI transform
	pub fn new(descriptor: TransformDescriptorFFI, instance: *mut c_void) -> Self {
		let vtable = descriptor.vtable;

		Self {
			descriptor,
			vtable,
			instance,
			arena: RefCell::new(Arena::new()),
		}
	}

	/// Get the transform descriptor
	#[allow(dead_code)]
	pub(crate) fn descriptor(&self) -> &TransformDescriptorFFI {
		&self.descriptor
	}
}

// SAFETY: NativeTransformFFI is only accessed from a single context at a time.
// The raw pointer and RefCell<Arena> are not shared across threads.
unsafe impl Send for NativeTransformFFI {}
unsafe impl Sync for NativeTransformFFI {}

impl Drop for NativeTransformFFI {
	fn drop(&mut self) {
		if !self.instance.is_null() {
			(self.vtable.destroy)(self.instance);
		}
	}
}

impl Transform for NativeTransformFFI {
	#[instrument(name = "transform::ffi::apply", level = "debug", skip_all)]
	fn apply(&self, _ctx: &TransformContext, input: Columns) -> reifydb_type::Result<Columns> {
		let mut arena = self.arena.borrow_mut();

		let ffi_input = arena.marshal_columns(&input);
		let mut ffi_output = ColumnsFFI::empty();

		let result = catch_unwind(AssertUnwindSafe(|| {
			(self.vtable.transform)(self.instance, &ffi_input, &mut ffi_output)
		}));

		let result_code = match result {
			Ok(code) => code,
			Err(panic_info) => {
				let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
					s.to_string()
				} else if let Some(s) = panic_info.downcast_ref::<String>() {
					s.clone()
				} else {
					"Unknown panic".to_string()
				};
				error!("FFI transform panicked during apply: {}", msg);
				abort();
			}
		};

		if result_code != 0 {
			arena.clear();
			return Err(reifydb_sdk::error::FFIError::Other(format!(
				"FFI transform apply failed with code: {}",
				result_code
			))
			.into());
		}

		let columns = arena.unmarshal_columns(&ffi_output);

		arena.clear();

		Ok(columns)
	}
}
