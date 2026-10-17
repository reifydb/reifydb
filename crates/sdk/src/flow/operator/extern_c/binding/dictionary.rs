// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_raw_parts;

use reifydb_codec::value::{decode_value, encode_value};
use reifydb_value::value::{
	Value,
	dictionary::{DictionaryEntryId, DictionaryId},
	value_type::ValueType,
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

pub(crate) fn raw_find_many(
	ctx: &ExternCContext,
	dictionary: DictionaryId,
	values: &[Value],
) -> Result<Vec<Option<DictionaryEntryId>>> {
	let encoded = values
		.iter()
		.map(|value| {
			encode_value(value).map_err(|e| SdkError::Other(format!("failed to serialize value: {}", e)))
		})
		.collect::<Result<Vec<_>>>()?;
	let buffers: Vec<ExternCBuffer> = encoded
		.iter()
		.map(|bytes| ExternCBuffer {
			ptr: bytes.as_ptr(),
			len: bytes.len(),
			cap: bytes.len(),
		})
		.collect();
	let mut ids = vec![0u128; values.len()];
	let mut present = vec![0u8; values.len()];
	let mut id_width = 0u8;

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; buffers borrow `encoded`, which outlives the call, ids / present each hold
	// values.len() live slots and id_width one, all only written by the host.
	let result = unsafe {
		((*ctx.ctx).callbacks.dictionary_find_many)(
			ctx.ctx,
			dictionary.0,
			buffers.as_ptr(),
			buffers.len(),
			ids.as_mut_ptr(),
			present.as_mut_ptr(),
			&mut id_width,
		)
	};
	if result == EXTERN_C_NOT_FOUND {
		return Ok(vec![None; values.len()]);
	}
	if result != EXTERN_C_OK {
		return Err(SdkError::Other(format!("host_dictionary_find_many failed with code {}", result)));
	}
	let id_type = match id_width {
		1 => ValueType::Uint1,
		2 => ValueType::Uint2,
		4 => ValueType::Uint4,
		8 => ValueType::Uint8,
		16 => ValueType::Uint16,
		width => return Err(SdkError::Other(format!("host returned dictionary id width {}", width))),
	};
	ids.iter()
		.zip(&present)
		.map(|(&id, &present)| {
			if present == 0 {
				return Ok(None);
			}
			DictionaryEntryId::from_u128(id, id_type.clone())
				.map(Some)
				.map_err(|e| SdkError::Other(format!("host returned an invalid dictionary id: {}", e)))
		})
		.collect()
}

pub(crate) fn raw_intern_many(
	ctx: &ExternCContext,
	dictionary: DictionaryId,
	values: &[Value],
) -> Result<Vec<DictionaryEntryId>> {
	let encoded = values
		.iter()
		.map(|value| {
			encode_value(value).map_err(|e| SdkError::Other(format!("failed to serialize value: {}", e)))
		})
		.collect::<Result<Vec<_>>>()?;
	let buffers: Vec<ExternCBuffer> = encoded
		.iter()
		.map(|bytes| ExternCBuffer {
			ptr: bytes.as_ptr(),
			len: bytes.len(),
			cap: bytes.len(),
		})
		.collect();
	let mut ids = vec![0u128; values.len()];
	let mut id_width = 0u8;

	// SAFETY: ExternCContext::new asserts ctx.ctx is non-null and the host keeps the ExternCContextRaw valid
	// for the whole guest call; buffers borrow `encoded`, which outlives the call, ids holds values.len()
	// live slots and id_width one, all only written by the host.
	let result = unsafe {
		((*ctx.ctx).callbacks.dictionary_intern_many)(
			ctx.ctx,
			dictionary.0,
			buffers.as_ptr(),
			buffers.len(),
			ids.as_mut_ptr(),
			&mut id_width,
		)
	};
	if result == EXTERN_C_NOT_FOUND {
		return Err(SdkError::Other(format!("dictionary {} not found", dictionary.0)));
	}
	if result != EXTERN_C_OK {
		return Err(SdkError::Other(format!("host_dictionary_intern_many failed with code {}", result)));
	}
	let id_type = match id_width {
		1 => ValueType::Uint1,
		2 => ValueType::Uint2,
		4 => ValueType::Uint4,
		8 => ValueType::Uint8,
		16 => ValueType::Uint16,
		width => return Err(SdkError::Other(format!("host returned dictionary id width {}", width))),
	};
	ids.into_iter()
		.map(|id| {
			DictionaryEntryId::from_u128(id, id_type.clone())
				.map_err(|e| SdkError::Other(format!("host returned an invalid dictionary id: {}", e)))
		})
		.collect()
}
