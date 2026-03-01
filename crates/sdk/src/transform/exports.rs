// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FFI exports for transform libraries

use std::{collections::HashMap, ffi::c_void, ptr, slice};

use postcard::from_bytes;
use reifydb_abi::{
	constants::CURRENT_API,
	data::buffer::BufferFFI,
	transform::{descriptor::TransformDescriptorFFI, types::TRANSFORM_MAGIC},
};
use reifydb_type::value::Value;

use crate::transform::{
	FFITransformWithMetadata,
	wrapper::{TransformWrapper, create_transform_vtable},
};

/// Convert a static string to a BufferFFI
fn str_to_buffer(s: &'static str) -> BufferFFI {
	BufferFFI {
		ptr: s.as_ptr(),
		len: s.len(),
		cap: s.len(),
	}
}

/// Create a transform descriptor from a transform type's metadata
pub fn create_transform_descriptor<T: FFITransformWithMetadata>() -> TransformDescriptorFFI {
	TransformDescriptorFFI {
		api: CURRENT_API,
		name: str_to_buffer(T::NAME),
		version: str_to_buffer(T::VERSION),
		description: str_to_buffer(T::DESCRIPTION),
		vtable: create_transform_vtable::<T>(),
	}
}

/// Create a transform instance from FFI parameters
///
/// # Safety
/// - config_ptr must be valid for config_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_transform_instance<T: FFITransformWithMetadata>(
	config_ptr: *const u8,
	config_len: usize,
) -> *mut c_void {
	let config = if config_ptr.is_null() || config_len == 0 {
		HashMap::new()
	} else {
		let config_bytes = unsafe { slice::from_raw_parts(config_ptr, config_len) };

		match from_bytes::<HashMap<String, Value>>(config_bytes) {
			Ok(decoded_config) => decoded_config,
			Err(e) => {
				panic!("Failed to deserialize transform config: {}", e);
			}
		}
	};

	let transform = match T::new(&config) {
		Ok(t) => t,
		Err(e) => {
			eprintln!("Failed to create transform: {}", e);
			return ptr::null_mut();
		}
	};

	let wrapper = Box::new(TransformWrapper::new(transform));
	Box::into_raw(wrapper) as *mut c_void
}

/// Returns the transform magic number
///
/// FFI transform libraries must export this function as `ffi_transform_magic`
/// to be recognized as valid transforms by the loader.
pub extern "C" fn transform_magic() -> u32 {
	TRANSFORM_MAGIC
}
