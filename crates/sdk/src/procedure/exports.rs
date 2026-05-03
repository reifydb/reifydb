// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ffi::c_void, ptr, slice};

use postcard::from_bytes;
use reifydb_abi::{
	constants::CURRENT_API,
	data::buffer::BufferFFI,
	procedure::{descriptor::ProcedureDescriptorFFI, types::PROCEDURE_MAGIC},
};
use reifydb_type::value::Value;

use crate::procedure::{
	FFIProcedureWithMetadata,
	wrapper::{ProcedureWrapper, create_procedure_vtable},
};

fn str_to_buffer(s: &'static str) -> BufferFFI {
	BufferFFI {
		ptr: s.as_ptr(),
		len: s.len(),
		cap: s.len(),
	}
}

pub fn create_procedure_descriptor<T: FFIProcedureWithMetadata>() -> ProcedureDescriptorFFI {
	ProcedureDescriptorFFI {
		api: CURRENT_API,
		name: str_to_buffer(T::NAME),
		version: str_to_buffer(T::VERSION),
		description: str_to_buffer(T::DESCRIPTION),
		vtable: create_procedure_vtable::<T>(),
	}
}

/// Create a procedure instance from FFI parameters
///
/// # Safety
/// - config_ptr must be valid for config_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_procedure_instance<T: FFIProcedureWithMetadata>(
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
				panic!("Failed to deserialize procedure config: {}", e);
			}
		}
	};

	let procedure = match T::new(&config) {
		Ok(p) => p,
		Err(e) => {
			eprintln!("Failed to create procedure: {}", e);
			return ptr::null_mut();
		}
	};

	let wrapper = Box::new(ProcedureWrapper::new(procedure));
	Box::into_raw(wrapper) as *mut c_void
}

pub extern "C" fn procedure_magic() -> u32 {
	PROCEDURE_MAGIC
}
