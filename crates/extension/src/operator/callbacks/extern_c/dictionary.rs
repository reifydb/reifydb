// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::{from_raw_parts, from_raw_parts_mut};

use reifydb_codec::value::{decode_value, encode_value};
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		status::{EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_NULL_PTR, EXTERN_C_NOT_FOUND, EXTERN_C_OK},
	},
	flow::operator::extern_c::wire::context::ExternCContextRaw,
};
use reifydb_value::value::{
	dictionary::{DictionaryEntryId, DictionaryId},
	value_type::ValueType,
};

use super::{context::get_host_mut, marshal::write_buffer, state::free_rows};

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_dictionary_get_many(
	ctx: *mut ExternCContextRaw,
	dictionary_id: u64,
	ids: *const u128,
	n: usize,
	values_out: *mut ExternCBuffer,
	present_out: *mut u8,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if n > 0 && (ids.is_null() || values_out.is_null() || present_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `ids`, `values_out` and `present_out` are null-checked above; the guest must pass back the
	// ExternCContextRaw the host handed it for this call (discharging get_host_mut), `ids` valid for `n` reads,
	// and `values_out` / `present_out` valid and aligned for `n` writes each. Every buffer written is handed
	// over to the guest on EXTERN_C_OK and freed here on any other status.
	unsafe {
		let (ids, values_out, present_out): (&[u128], &mut [ExternCBuffer], &mut [u8]) = if n == 0 {
			(&[], &mut [], &mut [])
		} else {
			(from_raw_parts(ids, n), from_raw_parts_mut(values_out, n), from_raw_parts_mut(present_out, n))
		};
		values_out.fill(ExternCBuffer::empty());
		present_out.fill(0);

		let host = get_host_mut(&mut *ctx);
		let dictionary = DictionaryId(dictionary_id);
		let Some(id_type) = host.dictionary_id_type(dictionary) else {
			return EXTERN_C_NOT_FOUND;
		};
		let Ok(entry_ids) = ids
			.iter()
			.map(|&id| DictionaryEntryId::from_u128(id, id_type.clone()))
			.collect::<Result<Vec<_>, _>>()
		else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Ok(values) = host.dictionary_get_many(dictionary, &entry_ids) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		if values.len() != ids.len() {
			return EXTERN_C_ERROR_INTERNAL;
		}
		for (slot, value) in values.into_iter().enumerate() {
			let Some(value) = value else {
				continue;
			};
			let Ok(bytes) = encode_value(&value) else {
				free_rows(values_out.as_mut_ptr(), present_out.as_mut_ptr(), slot);
				return EXTERN_C_ERROR_INTERNAL;
			};
			let status = write_buffer(&mut values_out[slot], &bytes);
			if status != EXTERN_C_OK {
				free_rows(values_out.as_mut_ptr(), present_out.as_mut_ptr(), slot);
				return status;
			}
			present_out[slot] = 1;
		}
		EXTERN_C_OK
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_dictionary_find_many(
	ctx: *mut ExternCContextRaw,
	dictionary_id: u64,
	values: *const ExternCBuffer,
	n: usize,
	ids_out: *mut u128,
	present_out: *mut u8,
	id_width_out: *mut u8,
) -> i32 {
	if ctx.is_null() || id_width_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if n > 0 && (values.is_null() || ids_out.is_null() || present_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: every pointer is null-checked above; the guest must pass back the ExternCContextRaw the host
	// handed it for this call (discharging get_host_mut), `values` valid for `n` reads each naming `len`
	// readable bytes, `ids_out` / `present_out` valid and aligned for `n` writes and `id_width_out` for one.
	unsafe {
		let (values, ids_out, present_out): (&[ExternCBuffer], &mut [u128], &mut [u8]) = if n == 0 {
			(&[], &mut [], &mut [])
		} else {
			(from_raw_parts(values, n), from_raw_parts_mut(ids_out, n), from_raw_parts_mut(present_out, n))
		};
		ids_out.fill(0);
		present_out.fill(0);

		let host = get_host_mut(&mut *ctx);
		let dictionary = DictionaryId(dictionary_id);
		let Some(id_type) = host.dictionary_id_type(dictionary) else {
			return EXTERN_C_NOT_FOUND;
		};
		let Some(width) = id_width(&id_type) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Ok(decoded) = values
			.iter()
			.map(|buffer| {
				if buffer.ptr.is_null() {
					return decode_value(&[]);
				}
				decode_value(from_raw_parts(buffer.ptr, buffer.len))
			})
			.collect::<Result<Vec<_>, _>>()
		else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Ok(found) = host.dictionary_find_many(dictionary, &decoded) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		if found.len() != n {
			return EXTERN_C_ERROR_INTERNAL;
		}
		for (slot, id) in found.into_iter().enumerate() {
			if let Some(id) = id {
				ids_out[slot] = id.to_u128();
				present_out[slot] = 1;
			}
		}
		*id_width_out = width;
		EXTERN_C_OK
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_dictionary_intern_many(
	ctx: *mut ExternCContextRaw,
	dictionary_id: u64,
	values: *const ExternCBuffer,
	n: usize,
	ids_out: *mut u128,
	id_width_out: *mut u8,
) -> i32 {
	if ctx.is_null() || id_width_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if n > 0 && (values.is_null() || ids_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: every pointer is null-checked above; the guest must pass back the ExternCContextRaw the host
	// handed it for this call (discharging get_host_mut), `values` valid for `n` reads each naming `len`
	// readable bytes, `ids_out` valid and aligned for `n` writes and `id_width_out` for one.
	unsafe {
		let (values, ids_out): (&[ExternCBuffer], &mut [u128]) = if n == 0 {
			(&[], &mut [])
		} else {
			(from_raw_parts(values, n), from_raw_parts_mut(ids_out, n))
		};
		ids_out.fill(0);

		let host = get_host_mut(&mut *ctx);
		let Ok(dictionary) = host.dictionary(DictionaryId(dictionary_id)) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Some(dictionary) = dictionary else {
			return EXTERN_C_NOT_FOUND;
		};
		let Some(width) = id_width(&dictionary.id_type) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Ok(decoded) = values
			.iter()
			.map(|buffer| {
				if buffer.ptr.is_null() {
					return decode_value(&[]);
				}
				decode_value(from_raw_parts(buffer.ptr, buffer.len))
			})
			.collect::<Result<Vec<_>, _>>()
		else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Ok(interned) = host.dictionary_intern_many(&dictionary, &decoded) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		if interned.len() != n {
			return EXTERN_C_ERROR_INTERNAL;
		}
		for (slot, id) in interned.into_iter().enumerate() {
			ids_out[slot] = id.to_u128();
		}
		*id_width_out = width;
		EXTERN_C_OK
	}
}

fn id_width(id_type: &ValueType) -> Option<u8> {
	match id_type {
		ValueType::Uint1 => Some(1),
		ValueType::Uint2 => Some(2),
		ValueType::Uint4 => Some(4),
		ValueType::Uint8 => Some(8),
		ValueType::Uint16 => Some(16),
		_ => None,
	}
}
