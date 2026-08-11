// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ptr::null_mut, slice::from_raw_parts};

use reifydb_abi::{
	constants::{EXTERN_C_NOT_FOUND, EXTERN_C_OK},
	data::buffer::ExternCBuffer,
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
	operator::context::extern_c::ExternCOperatorContext,
};

pub(super) fn raw_id_by_name(ctx: &ExternCOperatorContext, name: &str) -> Result<Option<DictionaryId>> {
	let name_bytes = name.as_bytes();
	let mut out_id: u64 = 0;
	let mut found: u8 = 0;

	// SAFETY: ExternCOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContext valid for the
	// whole guest call; name_bytes outlives the callback, and out_id and found are live local slots the host fills.
	unsafe {
		let result = ((*ctx.ctx).callbacks.dictionary.id_by_name)(
			ctx.ctx,
			name_bytes.as_ptr(),
			name_bytes.len(),
			&mut out_id,
			&mut found,
		);

		if result == EXTERN_C_OK {
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
	ctx: &ExternCOperatorContext,
	dictionary: DictionaryId,
	value: &Value,
) -> Result<Option<DictionaryEntryId>> {
	let value_bytes =
		encode_value(value).map_err(|e| SdkError::Other(format!("failed to serialize value: {}", e)))?;
	let mut out_id: u128 = 0;
	let mut out_id_type: u8 = 0;
	let mut found: u8 = 0;

	// SAFETY: ExternCOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContext valid for the
	// whole guest call; value_bytes outlives the callback, and out_id, out_id_type and found are live local slots
	// the host writes.
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

		if result == EXTERN_C_OK {
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
	ctx: &ExternCOperatorContext,
	dictionary: DictionaryId,
	id: DictionaryEntryId,
) -> Result<Option<Value>> {
	let mut output = ExternCBuffer {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	// SAFETY: ExternCOperatorContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContext valid for the
	// whole guest call. On EXTERN_C_OK the host writes a buffer of output.len initialised bytes, copied out before
	// memory.free releases it with the length it was allocated with.
	unsafe {
		let result = ((*ctx.ctx).callbacks.dictionary.get)(ctx.ctx, dictionary.0, id.to_u128(), &mut output);

		if result == EXTERN_C_OK {
			if output.ptr.is_null() || output.len == 0 {
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();
				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				let value: Value = decode_value(&value_bytes)
					.map_err(|e| SdkError::Other(format!("failed to deserialize value: {}", e)))?;
				Ok(Some(value))
			}
		} else if result == EXTERN_C_NOT_FOUND {
			Ok(None)
		} else {
			Err(SdkError::Other(format!("host_dictionary_get failed with code {}", result)))
		}
	}
}
