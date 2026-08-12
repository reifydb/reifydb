// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice::from_raw_parts, str::from_utf8};

use reifydb_codec::{
	tag::type_tag_byte,
	value::{decode_value, encode_value},
};
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		status::{
			EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_INVALID_UTF8, EXTERN_C_ERROR_NULL_PTR,
			EXTERN_C_NOT_FOUND, EXTERN_C_OK,
		},
	},
	flow::operator::extern_c::wire::context::ExternCContextRaw,
};
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
};

use super::{context::get_host_mut, marshal::write_buffer};

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_dictionary_id_by_name(
	ctx: *mut ExternCContextRaw,
	name_ptr: *const u8,
	name_len: usize,
	out_id: *mut u64,
	found: *mut u8,
) -> i32 {
	if ctx.is_null() || name_ptr.is_null() || out_id.is_null() || found.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: all four pointers are null-checked above; the guest must pass back the ExternCContextRaw the
	// host handed it for this call (discharging get_host_mut), a `name_ptr` valid for reads of
	// `name_len` bytes, and `out_id`/`found` valid and aligned for one u64 and one u8 write.
	unsafe {
		let name = match from_utf8(from_raw_parts(name_ptr, name_len)) {
			Ok(name) => name,
			Err(_) => return EXTERN_C_ERROR_INVALID_UTF8,
		};

		let host = get_host_mut(&mut *ctx);
		match host.dictionary_id_by_name(name) {
			Ok(Some(id)) => {
				*out_id = id.0;
				*found = 1;
				EXTERN_C_OK
			}
			Ok(None) => {
				*found = 0;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_dictionary_find(
	ctx: *mut ExternCContextRaw,
	dictionary_id: u64,
	value_ptr: *const u8,
	value_len: usize,
	out_id: *mut u128,
	out_id_type: *mut u8,
	found: *mut u8,
) -> i32 {
	if ctx.is_null() || value_ptr.is_null() || out_id.is_null() || out_id_type.is_null() || found.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: all five pointers are null-checked above; the guest must pass back the ExternCContextRaw the
	// host handed it for this call, a `value_ptr` valid for reads of `value_len` bytes, and
	// `out_id`/`out_id_type`/`found` valid and aligned for one u128, u8 and u8 write.
	unsafe {
		let value: Value = match decode_value(from_raw_parts(value_ptr, value_len)) {
			Ok(value) => value,
			Err(_) => return EXTERN_C_ERROR_INTERNAL,
		};

		let host = get_host_mut(&mut *ctx);
		match host.dictionary_find(DictionaryId(dictionary_id), &value) {
			Ok(Some(id)) => {
				*out_id = id.to_u128();
				*out_id_type = type_tag_byte(&id.id_type());
				*found = 1;
				EXTERN_C_OK
			}
			Ok(None) => {
				*found = 0;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_dictionary_get(
	ctx: *mut ExternCContextRaw,
	dictionary_id: u64,
	id: u128,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `output` are null-checked above; the guest must pass back the ExternCContextRaw the
	// host handed it for this call (discharging get_host_mut) and an `output` valid and aligned
	// for one ExternCBuffer write whose buffer it then releases via memory.free.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let dictionary = DictionaryId(dictionary_id);
		let Some(id_type) = host.dictionary_id_type(dictionary) else {
			return EXTERN_C_NOT_FOUND;
		};

		let entry_id = match DictionaryEntryId::from_u128(id, id_type) {
			Ok(entry_id) => entry_id,
			Err(_) => return EXTERN_C_ERROR_INTERNAL,
		};

		match host.dictionary_get(dictionary, entry_id) {
			Ok(Some(value)) => match encode_value(&value) {
				Ok(bytes) => write_buffer(output, &bytes),
				Err(_) => EXTERN_C_ERROR_INTERNAL,
			},
			Ok(None) => EXTERN_C_NOT_FOUND,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}
