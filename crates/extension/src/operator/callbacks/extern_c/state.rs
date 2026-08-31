// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, ops::Bound, ptr, slice::from_raw_parts};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, keyspace_inner_range_in},
	state::timer::TimerKind,
};
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
		context::ExternCContextRaw,
		iterators::ExternCStateIterator,
		state::{ExternCStateEntry, ExternCStateSlice},
	},
};
use reifydb_value::value::datetime::DateTime;

use super::{
	context::get_host_mut,
	marshal::{encoded_bytes, encoded_key, encoded_keys, identity_keys, state_key, write_buffer},
	state_iterator::{self, StateIteratorHandle},
};
use crate::procedure::callbacks::extern_c::memory::{host_alloc, host_free};

#[repr(C)]
struct StateIteratorInternal {
	handle: StateIteratorHandle,
}

fn iterator_entries(entries: Vec<(GroupStateKey, EncodedPodRow)>) -> Vec<(GroupStateKey, EncodedBytes)> {
	entries.into_iter().map(|(key, row)| (key, row.bytes().clone())).collect()
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_get(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `key_ptr` and `output` are null-checked above; the guest must pass back the
	// ExternCContextRaw the host handed it for this call (discharging get_host_mut and state_key), and
	// an `output` valid and aligned for one ExternCBuffer write that it then frees via memory.free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let host = get_host_mut(ctx_handle);

		let Some(key) = state_key(key_ptr, key_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		let result = host.state_get(&key);

		match result {
			Ok(Some(row)) => write_buffer(output, row.bytes().as_slice()),
			Ok(None) => EXTERN_C_NOT_FOUND,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_set(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	key_ptr: *const u8,
	key_len: usize,
	value_ptr: *const u8,
	value_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || value_ptr.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx`, `key_ptr` and `value_ptr` are null-checked above; the guest must pass back the
	// ExternCContextRaw the host handed it for this call, a `key_ptr` valid for `key_len` reads (discharging
	// state_key) and a `value_ptr` valid for `value_len` reads (discharging encoded_bytes).
	unsafe {
		let ctx_handle = &mut *ctx;
		let host = get_host_mut(ctx_handle);

		let Some(key) = state_key(key_ptr, key_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		match host.state_set(&key, EncodedPodRow::from(encoded_bytes(value_ptr, value_len))) {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_remove(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `key_ptr` are null-checked above; the guest must pass back the ExternCContextRaw the
	// host handed it for this call (discharging get_host_mut) and a `key_ptr` valid for reads
	// of `key_len` bytes (discharging state_key).
	unsafe {
		let ctx_handle = &mut *ctx;
		let host = get_host_mut(ctx_handle);

		let Some(key) = state_key(key_ptr, key_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		match host.state_remove(&key) {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_clear(_operator_id: u64, ctx: *mut ExternCContextRaw) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above and the guest must pass back the ExternCContextRaw the host handed
	// it for this call, which discharges get_host_mut.
	unsafe {
		let ctx_handle = &mut *ctx;
		let host = get_host_mut(ctx_handle);

		let result = host.state_clear();

		match result {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_prefix(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	prefix_ptr: *const u8,
	prefix_len: usize,
	limit: usize,
	iterator_out: *mut *mut ExternCStateIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `iterator_out` are null-checked above; the guest must pass back the ExternCContextRaw
	// the host handed it for this call, a `prefix_ptr` that is null or valid for `prefix_len` reads,
	// and an `iterator_out` valid for one pointer write; the handle is freed via state.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let host = get_host_mut(ctx_handle);

		if prefix_ptr.is_null() || prefix_len == 0 {
			return EXTERN_C_ERROR_INTERNAL;
		}

		let Some(prefix) = state_key(prefix_ptr, prefix_len) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		let range = EncodedKeyRange::prefix(prefix.as_slice());

		match host.state_range_limited(range, (limit != usize::MAX).then_some(limit)) {
			Ok(entries) => {
				let handle = state_iterator::create_iterator(iterator_entries(entries));

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
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
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
	// guest must pass back the ExternCContextRaw the host handed it for this call, `keys` valid for reads of
	// `keys_len` ExternCKeyRef whose non-empty entries are valid for their own `len`, and an
	// `iterator_out` valid for one pointer write; the handle is freed via state.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let host = get_host_mut(ctx_handle);

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
			let Some(framed) = GroupStateKey::from_guest_framed(EncodedKey::new(bytes)) else {
				return EXTERN_C_ERROR_INTERNAL;
			};
			encoded_keys.push(framed);
		}

		match host.state_get_many(&encoded_keys) {
			Ok(entries) => {
				let handle = state_iterator::create_iterator(iterator_entries(entries));

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

fn guest_may_address(_operator: OperatorId, keyspace: KeyspaceId) -> bool {
	keyspace.is_guest_owned()
}

/// # Safety
/// `ptr` must be null, or valid for reads of `len` bytes.
unsafe fn suffix_bound(ptr: *const u8, len: usize, bound_type: u8) -> Option<Bound<Vec<u8>>> {
	let suffix = || match len {
		0 => Some(Vec::new()),
		// SAFETY: forwards this function's own contract; `len` is non-zero here, so a null `ptr`
		// cannot be valid and is refused rather than read.
		_ if !ptr.is_null() => Some(unsafe { from_raw_parts(ptr, len) }.to_vec()),
		_ => None,
	};
	match bound_type {
		BOUND_UNBOUNDED => Some(Bound::Unbounded),
		BOUND_INCLUDED => suffix().map(Bound::Included),
		BOUND_EXCLUDED => suffix().map(Bound::Excluded),
		_ => None,
	}
}

fn bound_as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
	match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(suffix) => Bound::Included(suffix),
		Bound::Excluded(suffix) => Bound::Excluded(suffix),
	}
}

#[cfg_attr(not(test), unsafe(no_mangle))]
pub(super) extern "C" fn host_state_range(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	group: u128,
	keyspace: u8,
	start_ptr: *const u8,
	start_len: usize,
	start_bound_type: u8,
	end_ptr: *const u8,
	end_len: usize,
	end_bound_type: u8,
	limit: usize,
	iterator_out: *mut *mut ExternCStateIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` and `iterator_out` are null-checked above, and each bound pointer is read only by
	// suffix_bound, which null-checks it on the arm that reads it; the guest must pass back the
	// ExternCContextRaw the host handed it for this call, bound pointers valid for their stated lengths,
	// and an `iterator_out` valid for one pointer write; the handle is freed via state.iterator_free.
	unsafe {
		let ctx_handle = &mut *ctx;
		let keyspace = KeyspaceId(keyspace);
		if !guest_may_address(OperatorId(ctx_handle.operator_id), keyspace) {
			return EXTERN_C_ERROR_INTERNAL;
		}
		let host = get_host_mut(ctx_handle);

		let Some(start_bound) = suffix_bound(start_ptr, start_len, start_bound_type) else {
			return EXTERN_C_ERROR_INTERNAL;
		};
		let Some(end_bound) = suffix_bound(end_ptr, end_len, end_bound_type) else {
			return EXTERN_C_ERROR_INTERNAL;
		};

		let range = keyspace_inner_range_in(
			GroupId(group),
			keyspace,
			bound_as_slice(&start_bound),
			bound_as_slice(&end_bound),
		);
		let result = host.state_range_limited(range, (limit != usize::MAX).then_some(limit));

		match result {
			Ok(entries) => {
				let handle = state_iterator::create_iterator(iterator_entries(entries));

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
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	group: u128,
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
	// `row_numbers_out` and `is_new_out`; the guest must pass back the ExternCContextRaw the host handed it
	// for this call, `keys` satisfying encoded_keys, and both out arrays valid and aligned for
	// `keys_len` writes - get_or_create_row_numbers returns exactly one result per key.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let Some(encoded_keys) = encoded_keys(keys, keys_len) else {
			return EXTERN_C_ERROR_NULL_PTR;
		};
		match host.get_or_create_row_numbers(GroupId(group), &encoded_keys) {
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
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	group: u128,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || (key_len > 0 && key_ptr.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above, as is `key_ptr` whenever `key_len` is non-zero; the guest must
	// pass back the ExternCContextRaw the host handed it for this call (discharging get_host_mut) and a
	// `key_ptr` valid for reads of `key_len` bytes (discharging encoded_key).
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let key = encoded_key(key_ptr, key_len);
		let removed = if key.is_empty() {
			host.remove_row_number_for_group(GroupId(group))
		} else {
			host.remove_row_number(GroupId(group), &key)
		};
		match removed {
			Ok(_) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_arm_timer(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	due_bits: u64,
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
	// pass back the ExternCContextRaw the host handed it for this call (discharging get_host_mut) and
	// a `key` valid for reads of `key_len` bytes, which the zero-length arm never touches.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let key = if key_len == 0 {
			EncodedKey::new(Vec::new())
		} else {
			EncodedKey::new(from_raw_parts(key, key_len))
		};
		match host.arm_timer(DateTime::from_bits(due_bits), kind, &key) {
			Ok(()) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_reclaim_group_identity(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	group: u128,
	limit: usize,
	removed_out: *mut usize,
	more_out: *mut u8,
) -> i32 {
	if ctx.is_null() || removed_out.is_null() || more_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: null-checked above; `ctx` must be the context this host handed to the guest for the
	// duration of the call, and the out pointers must be valid for writes.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		match host.reclaim_group_identity(GroupId(group), limit) {
			Ok(outcome) => {
				*removed_out = outcome.removed.as_u64() as usize;
				*more_out = outcome.more as u8;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_reclaim_group_identity_keys(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	group: u128,
	keys: *const ExternCKeyRef,
	keys_len: usize,
	removed_out: *mut usize,
	more_out: *mut u8,
) -> i32 {
	if ctx.is_null() || removed_out.is_null() || more_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if keys_len > 0 && keys.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above, as are the out pointers, and so is `keys` whenever `keys_len`
	// is non-zero; the guest must pass back the ExternCContextRaw the host handed it for this call
	// (discharging get_host_mut) and `keys` satisfying identity_keys.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let Some(keys) = identity_keys(keys, keys_len) else {
			return EXTERN_C_ERROR_NULL_PTR;
		};
		match host.reclaim_group_identity_keys(GroupId(group), &keys) {
			Ok(outcome) => {
				*removed_out = outcome.removed.as_u64() as usize;
				*more_out = outcome.more as u8;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_flow_watermark(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	bits_out: *mut u64,
	present_out: *mut u8,
) -> i32 {
	if ctx.is_null() || bits_out.is_null() || present_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: null-checked above; `ctx` must be the context this host handed to the guest for the
	// duration of the call, and the out pointers must be valid for writes.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		match host.flow_watermark() {
			Ok(Some(watermark)) => {
				*bits_out = watermark.to_bits();
				*present_out = 1;
				EXTERN_C_OK
			}
			Ok(None) => {
				*bits_out = 0;
				*present_out = 0;
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_disarm_timer(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	due_bits: u64,
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
	// pass back the ExternCContextRaw the host handed it for this call (discharging get_host_mut) and
	// a `key` valid for reads of `key_len` bytes, which the zero-length arm never touches.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let key = if key_len == 0 {
			EncodedKey::new(Vec::new())
		} else {
			EncodedKey::new(from_raw_parts(key, key_len))
		};
		match host.disarm_timer(DateTime::from_bits(due_bits), kind, &key) {
			Ok(()) => EXTERN_C_OK,
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

pub(super) extern "C" fn host_get_or_create_row_numbers_for_pairs(
	_operator_id: u64,
	ctx: *mut ExternCContextRaw,
	groups: *const u128,
	keys: *const ExternCKeyRef,
	pairs_len: usize,
	row_numbers_out: *mut u64,
	is_new_out: *mut u8,
) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	if pairs_len > 0 && (groups.is_null() || keys.is_null() || row_numbers_out.is_null() || is_new_out.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: `ctx` is null-checked above, and for a non-zero `pairs_len` so are `groups`, `keys`,
	// `row_numbers_out` and `is_new_out`; the guest must pass back the ExternCContextRaw the host handed it
	// for this call, `groups` valid and aligned for `pairs_len` u64 reads, `keys` satisfying encoded_keys for
	// the same length, and both out arrays valid and aligned for `pairs_len` writes -
	// get_or_create_row_numbers_for_pairs returns exactly one result per pair.
	unsafe {
		let host = get_host_mut(&mut *ctx);
		let Some(encoded) = encoded_keys(keys, pairs_len) else {
			return EXTERN_C_ERROR_NULL_PTR;
		};
		let group_ids: Vec<GroupId> = (0..encoded.len()).map(|index| GroupId(*groups.add(index))).collect();
		match host.get_or_create_row_numbers_for_groups(&group_ids) {
			Ok(results) => {
				for (index, (row_number, is_new)) in results.iter().enumerate() {
					*row_numbers_out.add(index) = row_number.0;
					*is_new_out.add(index) = *is_new as u8;
				}
				EXTERN_C_OK
			}
			Err(_) => EXTERN_C_ERROR_INTERNAL,
		}
	}
}

#[cfg(test)]
mod join_row_expiry_guard_tests {
	use std::{
		cell::{Cell, RefCell},
		iter::empty,
		rc::Rc,
	};

	use reifydb_codec::key::encoded::EncodedKeyRange;
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::{config::ConfigKey, flow::OperatorId},
			store::MultiVersionRow,
		},
		key::operator::{
			keyspace::{join::JoinRowMappingKey, suffix_width_of},
			state::{
				GroupId, KeyspaceId, OperatorStateKey, keyspace_inner_range, keyspace_inner_range_split,
			},
		},
		state::timer::{StateStore, TimerStore},
	};
	use reifydb_flow::{
		operator::{
			host::HostContext,
			state::{iter::StateIterator, reaper::IdentityReclaim, reclaim::ReclaimOutcome},
		},
		transaction::join_expiry::JoinDuePage,
	};
	use reifydb_value::{
		Result,
		count::Count,
		value::{
			Value,
			dictionary::{DictionaryEntryId, DictionaryId},
			row_number::RowNumber,
			value_type::ValueType,
		},
	};

	use super::*;
	use crate::operator::callbacks::extern_c::{
		context::{ExternCHostContext, new_extern_c_context},
		create_host_callbacks,
	};

	/// Records whether a write reached the host at all; the guard has to reject before that, not after.
	struct RecordingHost {
		reached: Rc<Cell<bool>>,
		range: Rc<RefCell<Option<EncodedKeyRange>>>,
	}

	impl TimerStore for RecordingHost {
		fn arm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			Ok(())
		}

		fn disarm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			Ok(())
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(None)
		}
	}

	impl StateStore for RecordingHost {
		fn state_get(&mut self, _key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
			Ok(None)
		}

		fn state_get_many_visit(
			&mut self,
			_keys: &[GroupStateKey],
			_visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
		) -> Result<()> {
			Ok(())
		}

		fn state_set(&mut self, _key: &GroupStateKey, _payload: EncodedPodRow) -> Result<()> {
			self.reached.set(true);
			Ok(())
		}

		fn state_remove(&mut self, _key: &GroupStateKey) -> Result<()> {
			self.reached.set(true);
			Ok(())
		}

		fn state_page_inner(
			&mut self,
			_range: EncodedKeyRange,
			_limit: Option<usize>,
		) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
			Ok(Vec::new())
		}

		fn state_last(&mut self, _range: EncodedKeyRange) -> Result<Option<(GroupStateKey, EncodedPodRow)>> {
			Ok(None)
		}

		fn get_or_create_row_numbers(
			&mut self,
			_group: GroupId,
			_keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(Vec::new())
		}

		fn get_or_create_row_numbers_for_groups(
			&mut self,
			_groups: &[GroupId],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(Vec::new())
		}

		fn remove_row_number(&mut self, _group: GroupId, _key: &EncodedKey) -> Result<()> {
			Ok(())
		}

		fn remove_row_number_for_group(&mut self, _group: GroupId) -> Result<()> {
			Ok(())
		}

		fn written_at(&self) -> DateTime {
			DateTime::EPOCH
		}
	}

	impl IdentityReclaim for RecordingHost {
		fn reclaim_identity(&mut self, _group: GroupId, _limit: usize) -> Result<ReclaimOutcome> {
			Ok(ReclaimOutcome::NOTHING)
		}

		fn reclaim_identity_keys(
			&mut self,
			_group: GroupId,
			_keys: &[GroupStateKey],
		) -> Result<ReclaimOutcome> {
			Ok(ReclaimOutcome::NOTHING)
		}
	}

	impl HostContext for RecordingHost {
		fn version(&self) -> CommitVersion {
			CommitVersion(0)
		}

		fn disarm_timer_by_key(&mut self, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			Ok(())
		}

		fn join_expiry_at(
			&mut self,
			_group: GroupId,
			_side: u8,
			_row_number: RowNumber,
		) -> Result<Option<DateTime>> {
			Ok(None)
		}

		fn join_expiry_min(&mut self, _group: GroupId) -> Result<Option<DateTime>> {
			Ok(None)
		}

		fn join_due_page(&mut self, _group: GroupId, _at: DateTime, _budget: usize) -> Result<JoinDuePage> {
			Ok(JoinDuePage {
				due: Vec::new(),
				next: None,
				more: false,
			})
		}

		fn config_uint8(&self, _key: ConfigKey) -> u64 {
			0
		}

		fn state_get_many(&mut self, _keys: &[GroupStateKey]) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
			Ok(Vec::new())
		}

		fn state_range(&mut self, _range: EncodedKeyRange) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
			Ok(Vec::new())
		}

		fn state_range_limited(
			&mut self,
			range: EncodedKeyRange,
			_limit: Option<usize>,
		) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
			self.reached.set(true);
			*self.range.borrow_mut() = Some(range);
			Ok(Vec::new())
		}

		fn state_range_iter(&mut self, _range: EncodedKeyRange) -> StateIterator<'_> {
			StateIterator::new(Box::new(empty::<Result<MultiVersionRow>>()))
		}

		fn state_clear(&mut self) -> Result<()> {
			Ok(())
		}

		fn reclaim_group_identity(&mut self, _group: GroupId, _limit: usize) -> Result<ReclaimOutcome> {
			Ok(ReclaimOutcome::NOTHING)
		}

		fn reclaim_group_identity_keys(
			&mut self,
			_group: GroupId,
			keys: &[GroupStateKey],
		) -> Result<ReclaimOutcome> {
			Ok(ReclaimOutcome {
				removed: Count::new(keys.len() as u64),
				more: false,
			})
		}

		fn get_row_numbers(&mut self, _group: GroupId, _keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
			Ok(Vec::new())
		}

		fn get_row_numbers_for_groups(&mut self, _groups: &[GroupId]) -> Result<Vec<Option<RowNumber>>> {
			Ok(Vec::new())
		}

		fn get_join_row_numbers(&mut self, _keys: &[JoinRowMappingKey]) -> Result<Vec<Option<RowNumber>>> {
			Ok(Vec::new())
		}

		fn get_or_create_join_row_numbers(
			&mut self,
			_keys: &[JoinRowMappingKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(Vec::new())
		}

		fn remove_join_row_numbers(&mut self, _keys: &[JoinRowMappingKey]) -> Result<()> {
			Ok(())
		}

		fn remove_join_row_numbers_for_left(&mut self, _tag: u8, _left: u64) -> Result<()> {
			Ok(())
		}

		fn dictionary_id_by_name(&mut self, _name: &str) -> Result<Option<DictionaryId>> {
			Ok(None)
		}

		fn dictionary_value_type(&mut self, _dictionary: DictionaryId) -> Option<ValueType> {
			None
		}

		fn dictionary_id_type(&mut self, _dictionary: DictionaryId) -> Option<ValueType> {
			None
		}

		fn dictionary_find(
			&mut self,
			_dictionary: DictionaryId,
			_value: &Value,
		) -> Result<Option<DictionaryEntryId>> {
			Ok(None)
		}

		fn dictionary_get(
			&mut self,
			_dictionary: DictionaryId,
			_id: DictionaryEntryId,
		) -> Result<Option<Value>> {
			Ok(None)
		}
	}

	fn framed(keyspace: KeyspaceId) -> Vec<u8> {
		let width = suffix_width_of(keyspace).expect("a fixture keyspace must appear in the catalogue");
		OperatorStateKey::inner_encoded(GroupId(7), keyspace, vec![0u8; width]).as_slice().to_vec()
	}

	fn with_context(call: impl FnOnce(*mut ExternCContextRaw) -> i32) -> (i32, bool) {
		let (status, reached, _) = with_recording_context(call);
		(status, reached)
	}

	fn with_recording_context(
		call: impl FnOnce(*mut ExternCContextRaw) -> i32,
	) -> (i32, bool, Option<EncodedKeyRange>) {
		let reached = Rc::new(Cell::new(false));
		let range = Rc::new(RefCell::new(None));
		let mut recording = RecordingHost {
			reached: Rc::clone(&reached),
			range: Rc::clone(&range),
		};
		let mut host = ExternCHostContext::new(&mut recording);
		let mut ctx = new_extern_c_context(&mut host, OperatorId(1), create_host_callbacks());
		let status = call(&mut ctx as *mut ExternCContextRaw);
		let seen = range.borrow().clone();
		(status, reached.get(), seen)
	}

	fn guest_range(
		ctx: *mut ExternCContextRaw,
		keyspace: KeyspaceId,
		start: Option<&[u8]>,
		end: Option<&[u8]>,
	) -> i32 {
		let mut iterator: *mut ExternCStateIterator = ptr::null_mut();
		let (start_ptr, start_len, start_type) = match start {
			None => (ptr::null(), 0, BOUND_UNBOUNDED),
			Some(suffix) => (suffix.as_ptr(), suffix.len(), BOUND_INCLUDED),
		};
		let (end_ptr, end_len, end_type) = match end {
			None => (ptr::null(), 0, BOUND_UNBOUNDED),
			Some(suffix) => (suffix.as_ptr(), suffix.len(), BOUND_EXCLUDED),
		};
		host_state_range(
			1,
			ctx,
			GROUP.0,
			keyspace.0,
			start_ptr,
			start_len,
			start_type,
			end_ptr,
			end_len,
			end_type,
			usize::MAX,
			&mut iterator,
		)
	}

	const GROUP: GroupId = GroupId(7);

	#[test]
	fn a_guest_range_naming_a_host_keyspace_is_refused() {
		// The guard that only checked the two bound keys let a guest name its own keyspace at each end and
		// still be served every host keyspace lying between them, because keys sort by group before
		// keyspace. The keyspace is now a parameter, so it is the one thing the check has to look at.
		let (status, reached, seen) =
			with_recording_context(|ctx| guest_range(ctx, KeyspaceId::TIMER_WHEEL, None, None));

		assert_eq!(status, EXTERN_C_ERROR_INTERNAL, "a guest must not be able to scan the timer wheel");
		assert!(!reached, "and the refusal must land before the host is touched");
		assert!(seen.is_none());
	}

	#[test]
	fn a_guest_range_in_its_own_keyspace_reaches_the_host() {
		// A guard keyed on anything wider than the one keyspace would silently break every guest operator.
		let (status, reached, seen) =
			with_recording_context(|ctx| guest_range(ctx, KeyspaceId::CUSTOM_NOT_CACHED, None, None));

		assert_eq!(status, EXTERN_C_OK);
		assert!(reached, "a guest keyspace must still reach the host");
		assert!(seen.is_some());
	}

	#[test]
	fn a_guest_range_cannot_widen_past_the_keyspace_it_names() {
		// The invariant the leak violated: with both ends unbounded, the widest range a guest can ask for
		// is still one keyspace of one group. Bounds no longer decide the span, so no choice of bounds can
		// reach a neighbouring keyspace or a neighbouring group.
		let (_, _, seen) =
			with_recording_context(|ctx| guest_range(ctx, KeyspaceId::CUSTOM_NOT_CACHED, None, None));
		let range = seen.expect("an allowed guest range must reach the host");

		let whole = keyspace_inner_range(GROUP, KeyspaceId::CUSTOM_NOT_CACHED);
		assert_eq!(range.start, whole.start, "an unbounded start is the keyspace's own first key");
		assert_eq!(range.end, whole.end, "and an unbounded end is the keyspace's own last key");

		let (group, keyspace, _, _) =
			keyspace_inner_range_split(&range).expect("a guest range must confine to one keyspace");
		assert_eq!(group, GROUP);
		assert_eq!(keyspace, KeyspaceId::CUSTOM_NOT_CACHED);
	}

	#[test]
	fn a_guest_range_narrows_inside_the_keyspace_it_names() {
		// The companion to the widening test: suffix bounds still narrow, they just cannot escape. A guest
		// that could not narrow would have to scan a whole keyspace to read one row.
		let (status, _, seen) = with_recording_context(|ctx| {
			guest_range(ctx, KeyspaceId::CUSTOM_NOT_CACHED, Some(&[1u8; 4]), Some(&[9u8; 4]))
		});
		let range = seen.expect("an allowed guest range must reach the host");

		assert_eq!(status, EXTERN_C_OK);
		let (group, keyspace, start, end) =
			keyspace_inner_range_split(&range).expect("a guest range must confine to one keyspace");
		assert_eq!(group, GROUP);
		assert_eq!(keyspace, KeyspaceId::CUSTOM_NOT_CACHED);
		assert_eq!(start, Bound::Included(vec![1u8; 4]));
		assert_eq!(end, Bound::Excluded(vec![9u8; 4]));
	}

	#[test]
	fn a_guest_write_to_the_join_row_expiry_keyspace_is_refused() {
		// A guest write reaches the commit path, where routing decodes its body and panics the committer.
		let key = framed(KeyspaceId::JOIN_ROW_EXPIRY);
		let value = EncodedPodRow::new(&[0u8; 4]);

		let (set, set_reached) = with_context(|ctx| {
			host_state_set(1, ctx, key.as_ptr(), key.len(), value.bytes().as_ptr(), value.bytes().len())
		});
		let (removed, remove_reached) = with_context(|ctx| host_state_remove(1, ctx, key.as_ptr(), key.len()));

		assert_eq!(set, EXTERN_C_ERROR_INTERNAL, "a guest must not be able to write a join row expiry");
		assert_eq!(removed, EXTERN_C_ERROR_INTERNAL, "nor remove one");
		assert!(!set_reached, "and the refusal must land before the host is touched");
		assert!(!remove_reached);
	}

	#[test]
	fn a_guest_write_to_its_own_keyspace_still_reaches_the_host() {
		// A guard keyed on anything wider than the one keyspace would silently break every guest operator.
		let key = framed(KeyspaceId::CUSTOM_NOT_CACHED);
		let value = EncodedPodRow::new(&[0u8; 4]);

		let (set, set_reached) = with_context(|ctx| {
			host_state_set(1, ctx, key.as_ptr(), key.len(), value.bytes().as_ptr(), value.bytes().len())
		});
		let (removed, remove_reached) = with_context(|ctx| host_state_remove(1, ctx, key.as_ptr(), key.len()));

		assert_eq!(set, EXTERN_C_OK);
		assert_eq!(removed, EXTERN_C_OK);
		assert!(set_reached, "a guest keyspace must still reach the host");
		assert!(remove_reached);
	}
}

#[cfg(test)]
mod empty_value_boundary_tests {
	use std::ptr::null_mut;

	use reifydb_sdk::common::extern_c::wire::{
		buffer::ExternCBuffer,
		status::{EXTERN_C_ERROR_ALLOC, EXTERN_C_OK},
	};

	use crate::{
		operator::callbacks::extern_c::marshal::write_buffer, procedure::callbacks::extern_c::memory::host_free,
	};

	fn empty_buffer() -> ExternCBuffer {
		ExternCBuffer {
			ptr: null_mut(),
			len: 0,
			cap: 0,
		}
	}

	#[test]
	fn a_zero_length_row_cannot_cross_the_guest_boundary_as_a_present_value() {
		// host_alloc(0) returns null, so the only status a present-but-empty row can produce here is an
		// allocation failure; and even on OK the guest binding reads len == 0 as absent. A zero-length
		// value is therefore not representable across this boundary, so no state a guest operator reads
		// may rely on one.
		let mut output = empty_buffer();

		// SAFETY: output is a live, aligned ExternCBuffer this test owns for the whole call.
		let status = unsafe { write_buffer(&mut output as *mut ExternCBuffer, &[]) };

		assert_eq!(status, EXTERN_C_ERROR_ALLOC, "an empty payload has no allocation to hand over");
		assert!(output.ptr.is_null(), "and nothing may be written into the guest buffer");
		assert_eq!(output.len, 0);
	}

	#[test]
	fn a_one_byte_row_does_cross_the_guest_boundary() {
		// The companion case: one byte is the smallest payload that survives, which is why a marker that
		// must reach a guest carries a flags byte instead of an empty body.
		let mut output = empty_buffer();

		// SAFETY: output is a live, aligned ExternCBuffer this test owns for the whole call.
		let status = unsafe { write_buffer(&mut output as *mut ExternCBuffer, &[0u8]) };

		assert_eq!(status, EXTERN_C_OK);
		assert!(!output.ptr.is_null());
		assert_eq!(output.len, 1);

		// SAFETY: write_buffer returned OK, so ptr owns exactly output.len host-allocated bytes.
		unsafe {
			host_free(output.ptr as *mut u8, output.len);
		}
	}
}
