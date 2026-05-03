// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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

fn str_to_buffer(s: &'static str) -> BufferFFI {
	BufferFFI {
		ptr: s.as_ptr(),
		len: s.len(),
		cap: s.len(),
	}
}

pub fn create_transform_descriptor<T: FFITransformWithMetadata>() -> TransformDescriptorFFI {
	TransformDescriptorFFI {
		api: CURRENT_API,
		name: str_to_buffer(T::NAME),
		version: str_to_buffer(T::VERSION),
		description: str_to_buffer(T::DESCRIPTION),
		vtable: create_transform_vtable::<T>(),
	}
}

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

pub extern "C" fn transform_magic() -> u32 {
	TRANSFORM_MAGIC
}
