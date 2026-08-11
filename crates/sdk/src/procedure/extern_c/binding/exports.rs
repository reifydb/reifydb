// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ffi::c_void, ptr, slice, sync::Arc};

use reifydb_codec::value::decode_params;
use reifydb_value::{config::Config, params::Params};

use crate::{
	common::extern_c::wire::buffer::ExternCBuffer,
	procedure::extern_c::{
		binding::{
			procedure::ExternCProcedureWithMetadata,
			wrapper::{ProcedureWrapper, create_procedure_vtable},
		},
		wire::{
			descriptor::ExternCProcedureDescriptor,
			types::{PROCEDURE_ABI_TAG, PROCEDURE_MAGIC},
		},
	},
};

fn str_to_buffer(s: &'static str) -> ExternCBuffer {
	ExternCBuffer {
		ptr: s.as_ptr(),
		len: s.len(),
		cap: s.len(),
	}
}

pub fn create_procedure_descriptor<T: ExternCProcedureWithMetadata>() -> ExternCProcedureDescriptor {
	ExternCProcedureDescriptor {
		abi_tag: PROCEDURE_ABI_TAG,
		name: str_to_buffer(T::NAME),
		version: str_to_buffer(T::VERSION),
		description: str_to_buffer(T::DESCRIPTION),
		vtable: create_procedure_vtable::<T>(),
	}
}

/// # Safety
/// - config_ptr must be valid for config_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_procedure_instance<T: ExternCProcedureWithMetadata>(
	config_ptr: *const u8,
	config_len: usize,
) -> *mut c_void {
	let config = if config_ptr.is_null() || config_len == 0 {
		HashMap::new()
	} else {
		// SAFETY: the null and zero-length cases are handled above, and the caller guarantees config_ptr is
		// valid for config_len initialised bytes for the duration of this call.
		let config_bytes = unsafe { slice::from_raw_parts(config_ptr, config_len) };

		match decode_params(config_bytes) {
			Ok(Params::Named(map)) => Arc::try_unwrap(map).unwrap_or_else(|map| (*map).clone()),
			Ok(Params::None) => HashMap::new(),
			Ok(Params::Positional(_)) => {
				panic!("Failed to deserialize procedure config: expected named params");
			}
			Err(e) => {
				panic!("Failed to deserialize procedure config: {}", e);
			}
		}
	};

	let config = Config::new(T::NAME, config.into_iter().collect());
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
