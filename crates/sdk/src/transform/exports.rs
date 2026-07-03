// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ffi::c_void, ptr, slice, sync::Arc};

use reifydb_abi::{
	constants::CURRENT_API,
	data::buffer::BufferFFI,
	transform::{descriptor::TransformDescriptorFFI, types::TRANSFORM_MAGIC},
};
use reifydb_codec::value::decode_params;
use reifydb_value::params::Params;

use crate::{
	config::Config,
	transform::{
		FFITransformWithMetadata,
		wrapper::{TransformWrapper, create_transform_vtable},
	},
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

/// # Safety
///
/// - `config_ptr` must either be null or point to `config_len` valid bytes of codec-encoded named params.
pub unsafe extern "C" fn create_transform_instance<T: FFITransformWithMetadata>(
	config_ptr: *const u8,
	config_len: usize,
) -> *mut c_void {
	let config = if config_ptr.is_null() || config_len == 0 {
		HashMap::new()
	} else {
		let config_bytes = unsafe { slice::from_raw_parts(config_ptr, config_len) };

		match decode_params(config_bytes) {
			Ok(Params::Named(map)) => Arc::try_unwrap(map).unwrap_or_else(|map| (*map).clone()),
			Ok(Params::None) => HashMap::new(),
			Ok(Params::Positional(_)) => {
				panic!("Failed to deserialize transform config: expected named params");
			}
			Err(e) => {
				panic!("Failed to deserialize transform config: {}", e);
			}
		}
	};

	let config = Config::new(T::NAME, config.into_iter().collect());
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
