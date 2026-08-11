// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ffi::c_void, ptr, slice, sync::Arc};

use reifydb_abi::{
	constants::{CURRENT_API, OPERATOR_ABI_TAG},
	data::buffer::ExternCBuffer,
	operator::{
		capabilities::to_bitmask,
		column::{ExternCOperatorColumn, ExternCOperatorColumns},
		descriptor::ExternCOperatorDescriptor,
		types::OPERATOR_MAGIC,
	},
};
use reifydb_codec::{constraint::type_constraint_to_extern_c, value::decode_params};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::params::Params;

use crate::{
	config::Config,
	extern_c::wrapper::{OperatorWrapper, create_vtable},
	operator::{ExternCOperator, OperatorMetadata, column::operator::OperatorColumn},
};

fn str_to_buffer(s: &'static str) -> ExternCBuffer {
	ExternCBuffer {
		ptr: s.as_ptr(),
		len: s.len(),
		cap: s.len(),
	}
}

fn columns_to_extern_c(columns: &'static [OperatorColumn]) -> ExternCOperatorColumns {
	if columns.is_empty() {
		return ExternCOperatorColumns::empty();
	}

	let extern_c_columns: Vec<ExternCOperatorColumn> = columns
		.iter()
		.map(|c| {
			let extern_c_type = type_constraint_to_extern_c(&c.type_constraint)
				.expect("constraint exceeds tag capacity");
			ExternCOperatorColumn {
				name: str_to_buffer(c.name),
				base_type: extern_c_type.base_type,
				constraint_type: extern_c_type.constraint_type,
				constraint_param1: extern_c_type.constraint_param1,
				constraint_param2: extern_c_type.constraint_param2,
				description: str_to_buffer(c.description),
			}
		})
		.collect();

	let column_count = extern_c_columns.len();
	let columns_ptr = Box::leak(extern_c_columns.into_boxed_slice()).as_ptr();

	ExternCOperatorColumns {
		columns: columns_ptr,
		column_count,
	}
}

pub fn create_descriptor<O: ExternCOperator + OperatorMetadata>() -> ExternCOperatorDescriptor {
	ExternCOperatorDescriptor {
		api: CURRENT_API,
		abi_tag: OPERATOR_ABI_TAG,
		operator: str_to_buffer(O::NAME),
		version: str_to_buffer(O::VERSION),
		description: str_to_buffer(O::DESCRIPTION),
		input_columns: columns_to_extern_c(O::INPUT_COLUMNS),
		output_columns: columns_to_extern_c(O::OUTPUT_COLUMNS),
		capabilities: to_bitmask(O::CAPABILITIES),
		vtable: create_vtable::<O>(),
	}
}

/// # Safety
/// - config_ptr must be valid for config_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_operator_instance<O: ExternCOperator + OperatorMetadata>(
	config_ptr: *const u8,
	config_len: usize,
	operator_id: u64,
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
				panic!(
					"Failed to deserialize operator config for operator {}: expected named params",
					operator_id
				);
			}
			Err(e) => {
				panic!(
					"Failed to deserialize operator config for operator {}: {}. Using empty config.",
					operator_id, e
				);
			}
		}
	};

	let config = Config::new(O::NAME, config.into_iter().collect());
	let operator = match O::new(OperatorId(operator_id), &config) {
		Ok(op) => op,
		Err(e) => {
			eprintln!("Failed to create operator: {}", e);
			return ptr::null_mut();
		}
	};

	let wrapper = Box::new(OperatorWrapper::new(operator));
	Box::into_raw(wrapper) as *mut c_void
}

pub extern "C" fn operator_magic() -> u32 {
	OPERATOR_MAGIC
}
