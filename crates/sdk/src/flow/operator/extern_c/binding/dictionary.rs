// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_raw_parts;

use reifydb_codec::value::decode_value;
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
};

use crate::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		status::{EXTERN_C_NOT_FOUND, EXTERN_C_OK},
	},
	error::{Result, SdkError},
	flow::operator::extern_c::binding::context::ExternCContext,
};

pub(crate) fn raw_get_many(
	ctx: &ExternCContext,
	dictionary: DictionaryId,
	ids: &[DictionaryEntryId],
) -> Result<Vec<Option<Value>>> {
	let raw_ids: Vec<u128> = ids.iter().map(DictionaryEntryId::to_u128).collect();
	let mut values = vec![ExternCBuffer::empty(); ids.len()];
	let mut present = vec![0u8; ids.len()];

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; raw_ids, values and present each hold ids.len() live slots. On EXTERN_C_OK
	// every present slot owns a host buffer of its len, copied out before memory.free releases it.
	unsafe {
		let result = ((*ctx.ctx).callbacks.dictionary_get_many)(
			ctx.ctx,
			dictionary.0,
			raw_ids.as_ptr(),
			raw_ids.len(),
			values.as_mut_ptr(),
			present.as_mut_ptr(),
		);
		if result == EXTERN_C_NOT_FOUND {
			return Ok(vec![None; ids.len()]);
		}
		if result != EXTERN_C_OK {
			return Err(SdkError::Other(format!("host_dictionary_get_many failed with code {}", result)));
		}
		let copied: Vec<Option<Vec<u8>>> = values
			.iter()
			.zip(&present)
			.map(|(buffer, present)| {
				if *present == 0 || buffer.ptr.is_null() {
					return None;
				}
				let value_bytes = from_raw_parts(buffer.ptr, buffer.len).to_vec();
				((*ctx.ctx).callbacks.memory.free)(buffer.ptr as *mut u8, buffer.len);
				Some(value_bytes)
			})
			.collect();
		let decoded = copied
			.into_iter()
			.map(|bytes| {
				bytes.map(|bytes| decode_value(&bytes))
					.transpose()
					.map_err(|e| SdkError::Other(format!("failed to deserialize value: {}", e)))
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(decoded)
	}
}
