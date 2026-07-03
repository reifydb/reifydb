// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{slice::from_raw_parts, str::from_utf8};

use reifydb_abi::{
	constants::{FFI_ERROR_INTERNAL, FFI_ERROR_INVALID_UTF8, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK},
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_codec::{
	tag::type_tag_byte,
	value::{decode_value, encode_value},
};
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
};

use super::marshal::write_buffer;
use crate::ffi::context::get_transaction_mut;

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_dictionary_id_by_name(
	ctx: *mut ContextFFI,
	name_ptr: *const u8,
	name_len: usize,
	out_id: *mut u64,
	found: *mut u8,
) -> i32 {
	if ctx.is_null() || name_ptr.is_null() || out_id.is_null() || found.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let name = match from_utf8(from_raw_parts(name_ptr, name_len)) {
			Ok(name) => name,
			Err(_) => return FFI_ERROR_INVALID_UTF8,
		};

		let flow_txn = get_transaction_mut(&mut *ctx);
		match flow_txn.find_dictionary_by_name(name) {
			Some(dictionary) => {
				*out_id = dictionary.id.0;
				*found = 1;
			}
			None => *found = 0,
		}
		FFI_OK
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_dictionary_find(
	ctx: *mut ContextFFI,
	dictionary_id: u64,
	value_ptr: *const u8,
	value_len: usize,
	out_id: *mut u128,
	out_id_type: *mut u8,
	found: *mut u8,
) -> i32 {
	if ctx.is_null() || value_ptr.is_null() || out_id.is_null() || out_id_type.is_null() || found.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let value: Value = match decode_value(from_raw_parts(value_ptr, value_len)) {
			Ok(value) => value,
			Err(_) => return FFI_ERROR_INTERNAL,
		};

		let flow_txn = get_transaction_mut(&mut *ctx);
		let Some(dictionary) = flow_txn.find_dictionary(DictionaryId(dictionary_id)) else {
			*found = 0;
			return FFI_OK;
		};

		match flow_txn.find_in_dictionary(&dictionary, &value) {
			Ok(Some(id)) => {
				*out_id = id.to_u128();
				*out_id_type = type_tag_byte(&id.id_type());
				*found = 1;
				FFI_OK
			}
			Ok(None) => {
				*found = 0;
				FFI_OK
			}
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}

#[unsafe(no_mangle)]
pub(super) extern "C" fn host_dictionary_get(
	ctx: *mut ContextFFI,
	dictionary_id: u64,
	id: u128,
	output: *mut BufferFFI,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let Some(dictionary) = flow_txn.find_dictionary(DictionaryId(dictionary_id)) else {
			return FFI_NOT_FOUND;
		};

		let entry_id = match DictionaryEntryId::from_u128(id, dictionary.id_type.clone()) {
			Ok(entry_id) => entry_id,
			Err(_) => return FFI_ERROR_INTERNAL,
		};

		match flow_txn.get_from_dictionary(&dictionary, entry_id) {
			Ok(Some(value)) => match encode_value(&value) {
				Ok(bytes) => write_buffer(output, &bytes),
				Err(_) => FFI_ERROR_INTERNAL,
			},
			Ok(None) => FFI_NOT_FOUND,
			Err(_) => FFI_ERROR_INTERNAL,
		}
	}
}
