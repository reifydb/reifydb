// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, ops::Bound, ptr, slice::from_raw_parts};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey},
	state::store::TimerKind,
};
use reifydb_flow::timer::Timer;
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		key_ref::ExternCKeyRef,
		status::{
			EXTERN_C_END_OF_ITERATION, EXTERN_C_ERROR_ALLOC, EXTERN_C_ERROR_INTERNAL,
			EXTERN_C_ERROR_NULL_PTR, EXTERN_C_NOT_FOUND, EXTERN_C_OK,
		},
	},
	flow::operator::extern_c::wire::{
		callbacks::state::GROUP_ABSENT,
		context::ExternCContext,
		iterators::ExternCStateIterator,
		state::{ExternCStateEntry, ExternCStateSlice},
	},
};
use reifydb_value::value::datetime::DateTime;

use super::{
	context::get_transaction_mut,
	marshal::{encoded_bytes, encoded_key, encoded_keys, state_key, write_buffer},
	state_iterator::{self, StateIteratorHandle},
};
use crate::procedure::callbacks::extern_c::memory::{host_alloc, host_free};

#[repr(C)]
struct StateIteratorInternal {
	handle: StateIteratorHandle,
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_get(
	operator_id: u64,
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `key_ptr` and `output` are null-checked above; the guest must pass back the
	// ExternCContext the host handed it for this call (discharging get_transaction_mut and state_key), and
	// an `output` valid and aligned for one ExternCBuffer write that it then frees via memory.free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let Some(key) = state_key(key_ptr, key_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		let result = flow_txn.state_get(OperatorId(operator_id), &key);

		match result {
			Ok(Some(row)) => write_buffer(output, row.bytes().as_slice()),
			Ok(None) => EXTERN_C_NOT_FOUND,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_set(
	operator_id: u64,
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
	value_ptr: *const u8,
	value_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || value_ptr.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `key_ptr` and `value_ptr` are null-checked above; the guest must pass back the
	// ExternCContext the host handed it for this call, a `key_ptr` valid for `key_len` reads (discharging
	// state_key) and a `value_ptr` valid for `value_len` reads (discharging encoded_bytes).
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let Some(key) = state_key(key_ptr, key_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		let Ok(row) = EncodedOperatorRow::try_from(encoded_bytes(value_ptr, value_len)) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		match flow_txn.state_set(OperatorId(operator_id), &key, row) {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_remove(
	operator_id: u64,
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `key_ptr` are null-checked above; the guest must pass back the ExternCContext the
	// host handed it for this call (discharging get_transaction_mut) and a `key_ptr` valid for reads
	// of `key_len` bytes (discharging state_key).
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);

		let Some(key) = state_key(key_ptr, key_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		match flow_txn.state_remove(OperatorId(operator_id), &key) {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_clear(operator_id: u64, ctx: *mut ExternCContext) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above and the guest must pass back the ExternCContext the host handed
	// it for this call, which discharges get_transaction_mut.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);
		let operator_id = OperatorId(operator_id);

		let result = flow_txn.state_clear(operator_id);

		match result {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_prefix(
	operator_id: u64,
	ctx: *mut ExternCContext,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut ExternCStateIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `iterator_out` are null-checked above; the guest must pass back the ExternCContext
	// the host handed it for this call, a `prefix_ptr` that is null or valid for `prefix_len` reads,
	// and an `iterator_out` valid for one pointer write; the handle is freed via state.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);
		let operator_id = OperatorId(operator_id);

		let prefix_bytes = if prefix_ptr.is_null() {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};

		let result = if prefix_bytes.is_empty() {
			flow_txn.state_scan_all(operator_id)
		} else {
			let range = EncodedKeyRange::prefix(&prefix_bytes);
			flow_txn.state_range_all(operator_id, range)
		};

		match result {
			Ok(batch) => {
				let handle = state_iterator::create_iterator(batch);

				let iter_ptr = host_alloc(mem::size_of::<StateIteratorInternal>())
					as *mut StateIteratorInternal;
				if iter_ptr.is_null() {
					state_iterator::free_iterator(handle);
					return EXTERN_C_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StateIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut ExternCStateIterator;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_get_many(
	operator_id: u64,
	ctx: *mut ExternCContext,
	keys: *const ExternCKeyRef,
	keys_len: usize,
	iterator_out: *mut *mut ExternCStateIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if keys_len > 0 && keys.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `iterator_out` and (for a non-zero `keys_len`) `keys` are null-checked above; the
	// guest must pass back the ExternCContext the host handed it for this call, `keys` valid for reads of
	// `keys_len` ExternCKeyRef whose non-empty entries are valid for their own `len`, and an
	// `iterator_out` valid for one pointer write; the handle is freed via state.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);
		let operator_id = OperatorId(operator_id);

		let key_refs = if keys_len == 0 {
			&[]
		} else {
			from_raw_parts(keys, keys_len)
		};

		let mut encoded_keys: Vec<GroupStateKey> = Vec::with_capacity(key_refs.len());
		for key_ref in key_refs {
			if key_ref.len > 0 && key_ref.ptr.is_null() {
				return EXTERN_C_ERROR_NULL_PTR;
			}
			let bytes = if key_ref.len == 0 {
				Vec::new()
			} else {
				from_raw_parts(key_ref.ptr, key_ref.len).to_vec()
			};
			let Some(framed) = GroupStateKey::from_framed(EncodedKey::new(bytes)) else {
				return EXTERN_C_ERROR_INTERNAL;
			};
			encoded_keys.push(framed);
		}

		match flow_txn.state_get_many(operator_id, &encoded_keys) {
			Ok(batch) => {
				let handle = state_iterator::create_iterator(batch);

				let iter_ptr = host_alloc(mem::size_of::<StateIteratorInternal>())
					as *mut StateIteratorInternal;
				if iter_ptr.is_null() {
					state_iterator::free_iterator(handle);
					return EXTERN_C_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StateIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut ExternCStateIterator;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_range(
	operator_id: u64,
	ctx: *mut ExternCContext,
	start_ptr: *const u8,
	start_len: usize,
	start_bound_type: u8,
	end_ptr: *const u8,
	end_len: usize,
	end_bound_type: u8,
	iterator_out: *mut *mut ExternCStateIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `iterator_out` are null-checked above, and each bound pointer is null-checked
	// on the arm that reads it; the guest must pass back the ExternCContext the host handed it for this
	// call, bound pointers valid for their stated lengths, and an `iterator_out` valid for one pointer
	// write; the handle is freed via state.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let flow_txn = get_transaction_mut(ctx_handle);
		let operator_id = OperatorId(operator_id);

		let start_bound = match start_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if start_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Included(EncodedKey::new(bytes))
			}
			BOUND_EXCLUDED => {
				if start_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let bytes = from_raw_parts(start_ptr, start_len).to_vec();
				Bound::Excluded(EncodedKey::new(bytes))
			}
			_ => return EXTERN_C_ERROR_INTERNAL,
		};

		let end_bound = match end_bound_type {
			BOUND_UNBOUNDED => Bound::Unbounded,
			BOUND_INCLUDED => {
				if end_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Included(EncodedKey::new(bytes))
			}
			BOUND_EXCLUDED => {
				if end_ptr.is_null() {
					return EXTERN_C_ERROR_NULL_PTR;
				}
				let bytes = from_raw_parts(end_ptr, end_len).to_vec();
				Bound::Excluded(EncodedKey::new(bytes))
			}
			_ => return EXTERN_C_ERROR_INTERNAL,
		};

		let range = EncodedKeyRange::new(start_bound, end_bound);
		let result = flow_txn.state_range_all(operator_id, range);

		match result {
			Ok(batch) => {
				let handle = state_iterator::create_iterator(batch);

				let iter_ptr = host_alloc(mem::size_of::<StateIteratorInternal>())
					as *mut StateIteratorInternal;
				if iter_ptr.is_null() {
					state_iterator::free_iterator(handle);
					return EXTERN_C_ERROR_ALLOC;
				}

				ptr::write(
					iter_ptr,
					StateIteratorInternal {
						handle,
					},
				);

				*iterator_out = iter_ptr as *mut ExternCStateIterator;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_iterator_next(
	iterator: *mut ExternCStateIterator,
	out: *mut ExternCStateEntry,
	cap: usize,
	out_len: *mut usize,
) -> i32 {
	if iterator.is_null() || out.is_null() || out_len.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: null-checked above, and the caller must pass an iterator this host handed out as a
	// live StateIteratorInternal, so the cast target is valid and correctly aligned.
	unsafe {
		let iter_internal = iterator as *mut StateIteratorInternal;
		let iter_handle = (*iter_internal).handle;

		match state_iterator::next_iterator_batch(iter_handle, cap) {
			Some((entries, len)) => {
				for i in 0..len {
					let (key, value) = &*entries.add(i);
					*out.add(i) = ExternCStateEntry {
						key: ExternCStateSlice {
							ptr: key.as_ptr(),
							len: key.len(),
						},
						value: ExternCStateSlice {
							ptr: value.as_ptr(),
							len: value.len(),
						},
					};
				}
				*out_len = len;
				if len == 0 {
					EXTERN_C_END_OF_ITERATION
				} else {
					EXTERN_C_OK
				}
			}
			None => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_iterator_free(iterator: *mut ExternCStateIterator) {
	if iterator.is_null() {
		return;
	}

	// SAFETY: `iterator` is null-checked above and must be an unfreed handle this host handed out, so
	// it is a host_alloc block of exactly `size_of::<StateIteratorInternal>()` bytes at align 8 >=
	// align_of::<StateIteratorInternal>(), holding an initialised handle (discharges host_free).
	unsafe {
		let iter_internal = iterator as *mut StateIteratorInternal;

		let handle = (*iter_internal).handle;
		state_iterator::free_iterator(handle);

		host_free(iter_internal as *mut u8, mem::size_of::<StateIteratorInternal>());
	}
}

pub(super) extern "C" fn host_get_or_create_row_numbers(
	operator_id: u64,
	ctx: *mut ExternCContext,
	group: u64,
	keys: *const ExternCKeyRef,
	keys_len: usize,
	row_numbers_out: *mut u64,
	is_new_out: *mut u8,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if keys_len > 0 && (keys.is_null() || row_numbers_out.is_null() || is_new_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above, and for a non-zero `keys_len` so are `keys`,
	// `row_numbers_out` and `is_new_out`; the guest must pass back the ExternCContext the host handed it
	// for this call, `keys` satisfying encoded_keys, and both out arrays valid and aligned for
	// `keys_len` writes - get_or_create_row_numbers returns exactly one result per key.
	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let Some(encoded_keys) = encoded_keys(keys, keys_len) else {
			return EXTERN_C_ERROR_NULL_PTR;
		};
		match flow_txn.get_or_create_row_numbers(OperatorId(operator_id), GroupId(group), &encoded_keys) {
			Ok(results) => {
				for (i, (row_number, is_new)) in results.iter().enumerate() {
					*row_numbers_out.add(i) = row_number.0;
					*is_new_out.add(i) = *is_new as u8;
				}
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_remove_row_number(
	operator_id: u64,
	ctx: *mut ExternCContext,
	group: u64,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || (key_len > 0 && key_ptr.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let key = encoded_key(key_ptr, key_len);
		match flow_txn.remove_row_number(OperatorId(operator_id), GroupId(group), &key) {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_remove_row_numbers_below(
	operator_id: u64,
	ctx: *mut ExternCContext,
	group: u64,
	upper_ptr: *const u8,
	upper_len: usize,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || output.is_null() || (upper_len > 0 && upper_ptr.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let upper = encoded_key(upper_ptr, upper_len);
		match flow_txn.remove_row_numbers_below(OperatorId(operator_id), GroupId(group), &upper) {
			Ok(dropped) => {
				if dropped.is_empty() {
					(*output).ptr = ptr::null_mut();
					(*output).len = 0;
					(*output).cap = 0;
					return EXTERN_C_OK;
				}
				let mut packed = Vec::with_capacity(dropped.len() * 8);
				for row_number in dropped {
					packed.extend_from_slice(&row_number.0.to_le_bytes());
				}
				write_buffer(output, &packed)
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_arm_timer(
	operator_id: u64,
	ctx: *mut ExternCContext,
	at_millis: u64,
	kind: u8,
	key: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if key_len > 0 && key.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	let Some(kind) = TimerKind::from_u8(kind) else {
		return EXTERN_C_ERROR_INTERNAL;
	};

	// SAFETY: `ctx` is null-checked above, as is `key` whenever `key_len` is non-zero; the guest must
	// pass back the ExternCContext the host handed it for this call (discharging get_transaction_mut) and
	// a `key` valid for reads of `key_len` bytes, which the zero-length arm never touches.
	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let key = if key_len == 0 {
			EncodedKey::new(Vec::new())
		} else {
			EncodedKey::new(from_raw_parts(key, key_len))
		};
		let timer = Timer {
			at: DateTime::from_millis(at_millis),
			kind,
			key,
		};
		match flow_txn.arm_timer(OperatorId(operator_id), &timer) {
			Ok(()) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_flow_watermark(
	_operator_id: u64,
	ctx: *mut ExternCContext,
	millis_out: *mut u64,
	present_out: *mut u8,
) -> i32 {
	if ctx.is_null() || millis_out.is_null() || present_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: null-checked above; `ctx` must be the context this host handed to the guest for the
	// duration of the call, and the out pointers must be valid for writes.
	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		match flow_txn.flow_watermark() {
			Some(watermark) => {
				*millis_out = watermark.to_millis();
				*present_out = 1;
			}
			None => {
				*millis_out = 0;
				*present_out = 0;
			}
		}
		EXTERN_C_OK
	}
}

pub(super) extern "C" fn host_disarm_timer(
	operator_id: u64,
	ctx: *mut ExternCContext,
	at_millis: u64,
	kind: u8,
	key: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if key_len > 0 && key.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	let Some(kind) = TimerKind::from_u8(kind) else {
		return EXTERN_C_ERROR_INTERNAL;
	};

	// SAFETY: `ctx` is null-checked above, as is `key` whenever `key_len` is non-zero; the guest must
	// pass back the ExternCContext the host handed it for this call (discharging get_transaction_mut) and
	// a `key` valid for reads of `key_len` bytes, which the zero-length arm never touches.
	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let key = if key_len == 0 {
			EncodedKey::new(Vec::new())
		} else {
			EncodedKey::new(from_raw_parts(key, key_len))
		};
		let timer = Timer {
			at: DateTime::from_millis(at_millis),
			kind,
			key,
		};
		match flow_txn.disarm_timer(OperatorId(operator_id), &timer) {
			Ok(()) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_intern_groups(
	operator_id: u64,
	ctx: *mut ExternCContext,
	groups: *const ExternCKeyRef,
	groups_len: usize,
	ids_out: *mut u64,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if groups_len > 0 && (groups.is_null() || ids_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above, and for a non-zero `groups_len` so are `groups` and
	// `ids_out`; the guest must pass back the ExternCContext the host handed it for this call, `groups`
	// satisfying encoded_keys, and an `ids_out` valid and aligned for `groups_len` u64 writes -
	// intern_groups returns exactly one id per group.
	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let Some(keys) = encoded_keys(groups, groups_len) else {
			return EXTERN_C_ERROR_NULL_PTR;
		};
		match flow_txn.intern_groups(OperatorId(operator_id), &keys) {
			Ok(interned) => {
				for (index, (group, _)) in interned.iter().enumerate() {
					*ids_out.add(index) = group.0;
				}
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_lookup_groups(
	operator_id: u64,
	ctx: *mut ExternCContext,
	groups: *const ExternCKeyRef,
	groups_len: usize,
	ids_out: *mut u64,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if groups_len > 0 && (groups.is_null() || ids_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above, and for a non-zero `groups_len` so are `groups` and
	// `ids_out`; the guest must pass back the ExternCContext the host handed it for this call, `groups`
	// satisfying encoded_keys, and an `ids_out` valid and aligned for `groups_len` u64 writes - the
	// loop below indexes `keys`, which encoded_keys builds one entry per group.
	unsafe {
		let flow_txn = get_transaction_mut(&mut *ctx);
		let Some(keys) = encoded_keys(groups, groups_len) else {
			return EXTERN_C_ERROR_NULL_PTR;
		};
		for (index, key) in keys.iter().enumerate() {
			match flow_txn.lookup_group(OperatorId(operator_id), key) {
				Ok(found) => *ids_out.add(index) = found.map_or(GROUP_ABSENT, |group| group.0),
				Err(_) => return EXTERN_C_ERROR_INTERNAL,
			}
		}
		EXTERN_C_OK
	}
}
