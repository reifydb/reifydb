// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::{from_raw_parts, from_raw_parts_mut};

use reifydb_codec::value::encode_value;
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		status::{EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_NULL_PTR, EXTERN_C_NOT_FOUND, EXTERN_C_OK},
	},
	flow::operator::extern_c::wire::context::ExternCContextRaw,
};
use reifydb_value::value::dictionary::{DictionaryEntryId, DictionaryId};

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
