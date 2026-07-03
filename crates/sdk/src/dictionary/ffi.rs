// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr::null_mut, slice::from_raw_parts};

use reifydb_abi::{
	constants::{FFI_NOT_FOUND, FFI_OK},
	data::buffer::BufferFFI,
};
use reifydb_codec::{
	tag::value_type_from_tag_byte,
	value::{decode_value, encode_value},
};
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
};

use crate::{
	error::{Result, SdkError},
	operator::context::ffi::FFIOperatorContext,
};

pub(super) fn raw_id_by_name(ctx: &FFIOperatorContext, name: &str) -> Result<Option<DictionaryId>> {
	let name_bytes = name.as_bytes();
	let mut out_id: u64 = 0;
	let mut found: u8 = 0;

	unsafe {
		let result = ((*ctx.ctx).callbacks.dictionary.id_by_name)(
			ctx.ctx,
			name_bytes.as_ptr(),
			name_bytes.len(),
			&mut out_id,
			&mut found,
		);

		if result == FFI_OK {
			if found == 0 {
				Ok(None)
			} else {
				Ok(Some(DictionaryId(out_id)))
			}
		} else {
			Err(SdkError::Other(format!("host_dictionary_id_by_name failed with code {}", result)))
		}
	}
}

pub(super) fn raw_find(
	ctx: &FFIOperatorContext,
	dictionary: DictionaryId,
	value: &Value,
) -> Result<Option<DictionaryEntryId>> {
	let value_bytes =
		encode_value(value).map_err(|e| SdkError::Other(format!("failed to serialize value: {}", e)))?;
	let mut out_id: u128 = 0;
	let mut out_id_type: u8 = 0;
	let mut found: u8 = 0;

	unsafe {
		let result = ((*ctx.ctx).callbacks.dictionary.find)(
			ctx.ctx,
			dictionary.0,
			value_bytes.as_ptr(),
			value_bytes.len(),
			&mut out_id,
			&mut out_id_type,
			&mut found,
		);

		if result == FFI_OK {
			if found == 0 {
				Ok(None)
			} else {
				let id = DictionaryEntryId::from_u128(out_id, value_type_from_tag_byte(out_id_type))
					.map_err(|e| SdkError::Other(e.to_string()))?;
				Ok(Some(id))
			}
		} else {
			Err(SdkError::Other(format!("host_dictionary_find failed with code {}", result)))
		}
	}
}

pub(super) fn raw_get(
	ctx: &FFIOperatorContext,
	dictionary: DictionaryId,
	id: DictionaryEntryId,
) -> Result<Option<Value>> {
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	unsafe {
		let result = ((*ctx.ctx).callbacks.dictionary.get)(ctx.ctx, dictionary.0, id.to_u128(), &mut output);

		if result == FFI_OK {
			if output.ptr.is_null() || output.len == 0 {
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();
				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				let value: Value = decode_value(&value_bytes)
					.map_err(|e| SdkError::Other(format!("failed to deserialize value: {}", e)))?;
				Ok(Some(value))
			}
		} else if result == FFI_NOT_FOUND {
			Ok(None)
		} else {
			Err(SdkError::Other(format!("host_dictionary_get failed with code {}", result)))
		}
	}
}
