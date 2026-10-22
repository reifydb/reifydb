// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ffi::c_void, ptr, slice, sync::Arc};

use reifydb_codec::{constraint::encode_type_constraint, value::decode_params};
use reifydb_core::{
	common::WindowRequirements,
	interface::{catalog::flow::OperatorId, flow::to_bitmask},
	operator_with::{ApplyWith, decode_apply_with},
};
use reifydb_value::{config::ExtensionParams, params::Params};

use crate::{
	common::extern_c::wire::buffer::ExternCBuffer,
	flow::{
		extern_c::wire::schema::{ExternCOperatorColumn, ExternCOperatorColumns},
		operator::{
			MountedOperator, OperatorMetadata,
			column::operator::OperatorColumn,
			context::ClassValue,
			extern_c::{
				binding::{
					operator::{ExternCOperator, ExternCOperatorAdapter},
					wrapper::{OperatorWrapper, create_vtable},
				},
				wire::{
					descriptor::{ExternCOperatorDescriptor, ExternCWindowRequirements},
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

fn window_to_extern_c(operator: &str, window: WindowRequirements) -> ExternCWindowRequirements {
	let Some(kinds) = window.kinds_bitmask() else {
		panic!("{}: window kinds {:?} include one the C ABI cannot carry", operator, window.kinds);
	};
	ExternCWindowRequirements {
		takes_window: u8::from(window.takes_window),
		kinds,
		domain: window.domain.to_u8(),
		needs_pane: u8::from(window.needs_pane),
	}
}

pub fn create_descriptor<C: MountedOperator + OperatorMetadata + 'static>() -> ExternCOperatorDescriptor {
	ExternCOperatorDescriptor {
		abi_tag: OPERATOR_ABI_TAG,
		operator: str_to_buffer(C::NAME),
		version: str_to_buffer(C::VERSION),
		description: str_to_buffer(C::DESCRIPTION),
		input_columns: columns_to_extern_c(C::INPUT_COLUMNS),
		output_columns: columns_to_extern_c(C::OUTPUT_COLUMNS),
		capabilities: to_bitmask(C::CAPABILITIES),
		class: <C::Class as ClassValue>::CLASS.to_u8(),
		unmanaged_because: C::UNMANAGED_BECAUSE.map_or(ExternCBuffer::empty(), str_to_buffer),
		window: window_to_extern_c(C::NAME, C::WINDOW),
		vtable: create_vtable::<ExternCOperatorAdapter<C>>(),
	}
}

/// # Safety
/// - params_ptr must be valid for params_len bytes or null
/// - with_ptr must be valid for with_len bytes or null
/// - The returned pointer must be freed by calling the destroy function
pub unsafe extern "C" fn create_operator_instance<C: MountedOperator + OperatorMetadata + 'static>(
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

	let params = ExtensionParams::new(C::NAME, params.into_iter().collect());
	let operator = match ExternCOperatorAdapter::<C>::new(OperatorId(operator_id), &params, &with) {
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

#[cfg(test)]
mod tests {
	use reifydb_core::common::{WindowRequirements, WindowSizeDomain};

	use super::window_to_extern_c;

	#[test]
	fn every_window_field_reaches_the_c_descriptor() {
		// A field pinned to its default passes every time-domain driver and breaks only slot or pane ones.
		let window = window_to_extern_c(
			"probe",
			WindowRequirements {
				takes_window: true,
				kinds: &["rolling"],
				domain: WindowSizeDomain::Slots,
				needs_pane: true,
			},
		);
		assert_eq!((window.takes_window, window.kinds, window.needs_pane), (1, 0b1000, 1));
		assert_eq!(WindowSizeDomain::from_u8(window.domain), Some(WindowSizeDomain::Slots));
	}

	#[test]
	#[should_panic(expected = "probe: window kinds")]
	fn a_kind_the_c_abi_cannot_carry_fails_the_export() {
		// An unknown kind dropped from the mask would publish a window the guest never agreed to.
		window_to_extern_c(
			"probe",
			WindowRequirements {
				takes_window: true,
				kinds: &["hopping"],
				domain: WindowSizeDomain::Time,
				needs_pane: false,
			},
		);
	}
}
