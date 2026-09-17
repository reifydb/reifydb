// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ffi::c_void, ptr, slice, sync::Arc};

use reifydb_codec::{constraint::encode_type_constraint, value::decode_params};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::to_bitmask},
	operator_with::{ApplyWith, decode_apply_with},
};
use reifydb_value::{config::ExtensionParams, params::Params};

use crate::{
	common::extern_c::wire::buffer::ExternCBuffer,
	flow::{
		extern_c::wire::schema::{ExternCOperatorColumn, ExternCOperatorColumns},
		operator::{
			OperatorMetadata,
			column::operator::OperatorColumn,
			extern_c::{
				binding::{
					operator::ExternCOperator,
					wrapper::{OperatorWrapper, create_vtable},
				},
				wire::{
					descriptor::ExternCOperatorDescriptor,
					types::{OPERATOR_ABI_TAG, OPERATOR_MAGIC},
				},
			},
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

fn columns_to_extern_c(columns: &'static [OperatorColumn]) -> ExternCOperatorColumns {
	if columns.is_empty() {
		return ExternCOperatorColumns::empty();
	}

	let extern_c_columns: Vec<ExternCOperatorColumn> = columns
		.iter()
		.map(|c| {
			let extern_c_type =
				encode_type_constraint(&c.type_constraint).expect("constraint exceeds tag capacity");
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
/// - params_ptr must be valid for params_len bytes or null
/// - with_ptr must be valid for with_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_operator_instance<O: ExternCOperator + OperatorMetadata>(
	params_ptr: *const u8,
	params_len: usize,
	with_ptr: *const u8,
	with_len: usize,
	operator_id: u64,
) -> *mut c_void {
	let params = if params_ptr.is_null() || params_len == 0 {
		HashMap::new()
	} else {
		// SAFETY: the null and zero-length cases are handled above, and the caller guarantees params_ptr is
		// valid for params_len initialised bytes for the duration of this call.
		let params_bytes = unsafe { slice::from_raw_parts(params_ptr, params_len) };

		match decode_params(params_bytes) {
			Ok(Params::Named(map)) => Arc::try_unwrap(map).unwrap_or_else(|map| (*map).clone()),
			Ok(Params::None) => HashMap::new(),
			Ok(Params::Positional(_)) => {
				panic!(
					"Failed to deserialize operator params for operator {}: expected named params",
					operator_id
				);
			}
			Err(e) => {
				panic!(
					"Failed to deserialize operator params for operator {}: {}. Using empty params.",
					operator_id, e
				);
			}
		}
	};

	let with = if with_ptr.is_null() || with_len == 0 {
		ApplyWith::default()
	} else {
		// SAFETY: the null and zero-length cases are handled above, and the caller guarantees with_ptr is
		// valid for with_len initialised bytes for the duration of this call.
		let with_bytes = unsafe { slice::from_raw_parts(with_ptr, with_len) };

		match decode_apply_with(with_bytes) {
			Ok(with) => with,
			Err(e) => {
				panic!("Failed to deserialize operator with for operator {}: {}", operator_id, e);
			}
		}
	};

	let params = ExtensionParams::new(O::NAME, params.into_iter().collect());
	let operator = match O::new(OperatorId(operator_id), &params, &with) {
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
