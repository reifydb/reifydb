// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	alloc::{Layout, alloc, dealloc},
	slice::from_raw_parts,
	str::from_utf8,
};

#[unsafe(no_mangle)]
extern "C" fn test_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return ptr::null_mut();
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return ptr::null_mut(),
	};

	// SAFETY: size is non-zero and from_size_align accepted the layout, so it is valid.
	unsafe { alloc(layout) }
}

/// # Safety
///
/// `ptr` must come from `test_alloc` and `size` must be the size it was
/// allocated with, since the layout is reconstructed from `size` rather than recorded.
#[unsafe(no_mangle)]
unsafe extern "C" fn test_free(ptr: *mut u8, size: usize) {
	if ptr.is_null() || size == 0 {
		return;
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};

	// SAFETY: the caller guarantees ptr came from this allocator with exactly this size.
	unsafe { dealloc(ptr, layout) }
}

/// # Safety
///
/// `ctx` must be non-null and its `txn_ptr` must point at a live `TestContext` that outlives
/// the returned reference; the `'static` lifetime is forged and is not checked.
unsafe fn get_test_context(ctx: *mut ExternCContext) -> &'static TestContext {
	// SAFETY: the caller guarantees ctx is valid and its txn_ptr is a live TestContext.
	unsafe {
		let txn_ptr = (*ctx).txn_ptr;
		&*(txn_ptr as *const TestContext)
	}
}

fn test_state_envelope(operator_id: u64, group: GroupId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> EncodedKey {
	OperatorStateKey::encoded(OperatorId(operator_id), group, keyspace, suffix.into())
}

#[unsafe(no_mangle)]
extern "C" fn test_state_get(
	_operator_id: u64,
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: pointers null-checked above; caller owns (key_ptr, key_len); value_ptr checked too.
	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey::new(key_bytes);

		match test_ctx.get_state(&key) {
			Some(value_bytes) => {
				let value_ptr = test_alloc(value_bytes.len());
				if value_ptr.is_null() {
					return -2;
				}

				ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_bytes.len());

				(*output).ptr = value_ptr;
				(*output).len = value_bytes.len();
				(*output).cap = value_bytes.len();

				EXTERN_C_OK
			}
			None => EXTERN_C_NOT_FOUND,
		}
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_set(
	_operator_id: u64,
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
	value_ptr: *const u8,
	value_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || value_ptr.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: pointers null-checked above; caller owns both (key_ptr, key_len) and (value_ptr, value_len).
	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey::new(key_bytes);

		let value_bytes = from_raw_parts(value_ptr, value_len);

		test_ctx.set_state(key, value_bytes.to_vec());

		EXTERN_C_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_remove(
	_operator_id: u64,
	ctx: *mut ExternCContext,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: both pointers null-checked above; caller owns (key_ptr, key_len) as a readable region.
	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey::new(key_bytes);

		test_ctx.remove_state(&key);

		EXTERN_C_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_clear(_operator_id: u64, ctx: *mut ExternCContext) -> i32 {
	if ctx.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: ctx is null-checked above and points at a live ExternCContext for this call.
	unsafe {
		let test_ctx = get_test_context(ctx);
		test_ctx.clear_state();
		EXTERN_C_OK
	}
}

#[repr(C)]
struct TestStateIterator {
	items: Vec<(Vec<u8>, Vec<u8>)>,

	position: usize,
}

#[unsafe(no_mangle)]
extern "C" fn test_state_get_many(
	_operator_id: u64,
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

	// SAFETY: ctx and iterator_out checked; keys checked when keys_len > 0; each ExternCKeyRef when its len > 0.
	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_refs = if keys_len == 0 {
			&[]
		} else {
			from_raw_parts(keys, keys_len)
		};

		let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		for key_ref in key_refs {
			let key_bytes = if key_ref.len == 0 {
				Vec::new()
			} else {
				from_raw_parts(key_ref.ptr, key_ref.len).to_vec()
			};
			let key = EncodedKey::new(key_bytes.clone());
			if let Some(value_bytes) = test_ctx.get_state(&key) {
				items.push((key_bytes, value_bytes.to_vec()));
			}
		}

		let iter = Box::new(TestStateIterator {
			items,
			position: 0,
		});

		*iterator_out = Box::into_raw(iter) as *mut ExternCStateIterator;

		EXTERN_C_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_prefix(
	_operator_id: u64,
	ctx: *mut ExternCContext,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut ExternCStateIterator,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: ctx and iterator_out null-checked; prefix_ptr read only when non-null with a non-zero len.
	unsafe {
		let test_ctx = get_test_context(ctx);

		let prefix_bytes = if prefix_ptr.is_null() || prefix_len == 0 {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};

		let state_store = test_ctx.state_store();
		let state = state_store.lock();

		let mut items: Vec<(Vec<u8>, Vec<u8>)> = state
			.iter()
			.filter(|(key, _)| {
				if prefix_bytes.is_empty() {
					true
				} else {
					key.starts_with(&prefix_bytes)
				}
			})
			.map(|(key, value)| (key.to_vec(), value.0.to_vec()))
			.collect();

		items.sort_by(|a, b| a.0.cmp(&b.0));

		let iter = Box::new(TestStateIterator {
			items,
			position: 0,
		});

		*iterator_out = Box::into_raw(iter) as *mut ExternCStateIterator;

		EXTERN_C_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_iterator_next(
	iterator: *mut ExternCStateIterator,
	out: *mut ExternCStateEntry,
	cap: usize,
	out_len: *mut usize,
) -> i32 {
	if iterator.is_null() || out.is_null() || out_len.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: all three pointers null-checked; iterator is one this module minted; writes stay under cap.
	unsafe {
		let iter = &mut *(iterator as *mut TestStateIterator);

		let mut written = 0usize;
		while written < cap && iter.position < iter.items.len() {
			let (key, value) = &iter.items[iter.position];
			*out.add(written) = ExternCStateEntry {
				key: ExternCStateSlice {
					ptr: key.as_ptr(),
					len: key.len(),
				},
				value: ExternCStateSlice {
					ptr: value.as_ptr(),
					len: value.len(),
				},
			};
			iter.position += 1;
			written += 1;
		}
		*out_len = written;

		if written == 0 {
			return EXTERN_C_END_OF_ITERATION;
		}
		EXTERN_C_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_iterator_free(iterator: *mut ExternCStateIterator) {
	if iterator.is_null() {
		return;
	}

	// SAFETY: iterator null-checked, was minted by this module, and per contract is freed exactly once.
	unsafe {
		let _ = Box::from_raw(iterator as *mut TestStateIterator);
	}
}

const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

#[unsafe(no_mangle)]
extern "C" fn test_state_range(
	_operator_id: u64,
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

	// SAFETY: ctx and iterator_out null-checked; a bound pointer is read only when bounded and non-null.
	unsafe {
		let test_ctx = get_test_context(ctx);

		let start_key = if start_bound_type == BOUND_UNBOUNDED || start_ptr.is_null() {
			None
		} else {
			Some(from_raw_parts(start_ptr, start_len).to_vec())
		};

		let end_key = if end_bound_type == BOUND_UNBOUNDED || end_ptr.is_null() {
			None
		} else {
			Some(from_raw_parts(end_ptr, end_len).to_vec())
		};

		let state_store = test_ctx.state_store();
		let state = state_store.lock();

		let mut items: Vec<(Vec<u8>, Vec<u8>)> = state
			.iter()
			.filter(|(key, _)| {
				let key_bytes = key.as_slice();

				let start_ok = match (&start_key, start_bound_type) {
					(None, _) => true,
					(Some(start), BOUND_INCLUDED) => key_bytes >= start.as_slice(),
					(Some(start), BOUND_EXCLUDED) => key_bytes > start.as_slice(),
					_ => true,
				};

				let end_ok = match (&end_key, end_bound_type) {
					(None, _) => true,
					(Some(end), BOUND_INCLUDED) => key_bytes <= end.as_slice(),
					(Some(end), BOUND_EXCLUDED) => key_bytes < end.as_slice(),
					_ => true,
				};

				start_ok && end_ok
			})
			.map(|(key, value)| (key.to_vec(), value.0.to_vec()))
			.collect();

		items.sort_by(|a, b| a.0.cmp(&b.0));

		let iter = Box::new(TestStateIterator {
			items,
			position: 0,
		});

		*iterator_out = Box::into_raw(iter) as *mut ExternCStateIterator;

		EXTERN_C_OK
	}
}

use std::ptr;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	state::store::TimerKind,
};
use reifydb_sdk::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		callbacks::{builder::BuilderCallbacks, memory::MemoryCallbacks},
		key_ref::ExternCKeyRef,
		status::{
			EXTERN_C_END_OF_ITERATION, EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_NULL_PTR,
			EXTERN_C_NOT_FOUND, EXTERN_C_OK,
		},
	},
	flow::operator::extern_c::wire::{
		callbacks::{
			OperatorCallbacks,
			dictionary::DictionaryCallbacks,
			state::{GROUP_ABSENT, StateCallbacks},
		},
		context::ExternCContext,
		iterators::ExternCStateIterator,
		state::{ExternCStateEntry, ExternCStateSlice},
	},
};
use reifydb_value::value::datetime::DateTime;

use crate::{
	context::{ArmedTimer, TestContext},
	registry::{
		test_acquire, test_bitvec_ptr, test_commit, test_data_ptr, test_emit_diff, test_grow, test_offsets_ptr,
		test_release,
	},
};

extern "C" fn test_intern_groups(
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

	// SAFETY: ctx checked; groups and ids_out checked when groups_len > 0; writes stay under groups_len.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let counter_key = test_state_envelope(
			operator_id,
			GroupId::ROOT,
			Keyspace::NODE_COUNTER,
			b"__group_alloc__".to_vec(),
		);
		let group_refs = if groups_len == 0 {
			&[]
		} else {
			from_raw_parts(groups, groups_len)
		};
		for (i, group_ref) in group_refs.iter().enumerate() {
			let group_bytes = if group_ref.len == 0 {
				&[][..]
			} else {
				from_raw_parts(group_ref.ptr, group_ref.len)
			};
			let dictionary_key = test_state_envelope(
				operator_id,
				GroupId::ROOT,
				Keyspace::GROUP_DICTIONARY,
				group_bytes.to_vec(),
			);
			match test_ctx.get_state(&dictionary_key) {
				Some(bytes) if bytes.len() >= 8 => {
					*ids_out.add(i) = u64::from_le_bytes(bytes[..8].try_into().unwrap());
				}
				_ => {
					let current = test_ctx
						.get_state(&counter_key)
						.and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
						.map(u64::from_le_bytes)
						.unwrap_or(GroupId::FIRST.0);
					test_ctx.set_state(counter_key.clone(), (current + 1).to_le_bytes().to_vec());
					test_ctx.set_state(dictionary_key, current.to_le_bytes().to_vec());
					*ids_out.add(i) = current;
				}
			}
		}
		EXTERN_C_OK
	}
}

extern "C" fn test_arm_timer(
	_operator_id: u64,
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

	// SAFETY: ctx null-checked and key null-checked when key_len > 0, so the region is readable.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let key = if key_len == 0 {
			Vec::new()
		} else {
			from_raw_parts(key, key_len).to_vec()
		};
		test_ctx.arm_timer(ArmedTimer {
			at: DateTime::from_millis(at_millis),
			kind,
			key,
		});
	}

	EXTERN_C_OK
}

extern "C" fn test_disarm_timer(
	_operator_id: u64,
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

	// SAFETY: ctx null-checked and key null-checked when key_len > 0, so the region is readable.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let key = if key_len == 0 {
			Vec::new()
		} else {
			from_raw_parts(key, key_len).to_vec()
		};
		test_ctx.disarm_timer(&ArmedTimer {
			at: DateTime::from_millis(at_millis),
			kind,
			key,
		});
	}

	EXTERN_C_OK
}

extern "C" fn test_flow_watermark(
	_operator_id: u64,
	ctx: *mut ExternCContext,
	millis_out: *mut u64,
	present_out: *mut u8,
) -> i32 {
	if ctx.is_null() || millis_out.is_null() || present_out.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: all three pointers null-checked; both out-params are caller-owned cells, written not read.
	unsafe {
		let test_ctx = get_test_context(ctx);
		match test_ctx.flow_watermark() {
			Some(watermark) => {
				*millis_out = watermark.to_millis();
				*present_out = 1;
			}
			None => {
				*millis_out = 0;
				*present_out = 0;
			}
		}
	}

	EXTERN_C_OK
}

extern "C" fn test_lookup_groups(
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

	// SAFETY: ctx checked; groups and ids_out checked when groups_len > 0; writes stay under groups_len.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let group_refs = if groups_len == 0 {
			&[]
		} else {
			from_raw_parts(groups, groups_len)
		};
		for (i, group_ref) in group_refs.iter().enumerate() {
			let group_bytes = if group_ref.len == 0 {
				&[][..]
			} else {
				from_raw_parts(group_ref.ptr, group_ref.len)
			};
			let dictionary_key = test_state_envelope(
				operator_id,
				GroupId::ROOT,
				Keyspace::GROUP_DICTIONARY,
				group_bytes.to_vec(),
			);
			*ids_out.add(i) = match test_ctx.get_state(&dictionary_key) {
				Some(bytes) if bytes.len() >= 8 => u64::from_le_bytes(bytes[..8].try_into().unwrap()),
				_ => GROUP_ABSENT,
			};
		}
		EXTERN_C_OK
	}
}

extern "C" fn test_get_or_create_row_numbers(
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

	// SAFETY: ctx checked; keys and both out arrays checked when keys_len > 0; writes stay under it.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let counter_key = test_state_envelope(
			operator_id,
			GroupId::ROOT,
			Keyspace::NODE_COUNTER,
			b"__row_number_alloc__".to_vec(),
		);
		let key_refs = if keys_len == 0 {
			&[]
		} else {
			from_raw_parts(keys, keys_len)
		};
		for (i, key_ref) in key_refs.iter().enumerate() {
			let key_bytes = if key_ref.len == 0 {
				&[][..]
			} else {
				from_raw_parts(key_ref.ptr, key_ref.len)
			};
			let map_key = test_state_envelope(
				operator_id,
				GroupId(group),
				Keyspace::ROW_NUMBER_MAPPING,
				key_bytes.to_vec(),
			);
			match test_ctx.get_state(&map_key) {
				Some(bytes) if bytes.len() >= 8 => {
					*row_numbers_out.add(i) = u64::from_le_bytes(bytes[..8].try_into().unwrap());
					*is_new_out.add(i) = 0;
				}
				_ => {
					let current = test_ctx
						.get_state(&counter_key)
						.and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
						.map(u64::from_le_bytes)
						.unwrap_or(1);
					test_ctx.set_state(counter_key.clone(), (current + 1).to_le_bytes().to_vec());
					test_ctx.set_state(map_key, current.to_le_bytes().to_vec());
					*row_numbers_out.add(i) = current;
					*is_new_out.add(i) = 1;
				}
			}
		}
		EXTERN_C_OK
	}
}

extern "C" fn test_remove_row_number(
	operator_id: u64,
	ctx: *mut ExternCContext,
	group: u64,
	key_ptr: *const u8,
	key_len: usize,
) -> i32 {
	if ctx.is_null() || (key_len > 0 && key_ptr.is_null()) {
		return EXTERN_C_ERROR_NULL_PTR;
	}
	// SAFETY: ctx is null-checked above and key_ptr is null-checked whenever key_len is non-zero,
	// so (key_ptr, key_len) is a readable region the caller owns for the duration of the call.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let key_bytes = if key_len == 0 {
			&[][..]
		} else {
			from_raw_parts(key_ptr, key_len)
		};
		let map_key = test_state_envelope(
			operator_id,
			GroupId(group),
			Keyspace::ROW_NUMBER_MAPPING,
			key_bytes.to_vec(),
		);
		test_ctx.remove_state(&map_key);
		EXTERN_C_OK
	}
}

extern "C" fn test_remove_row_numbers_below(
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
	// SAFETY: ctx and output are null-checked above and upper_ptr is null-checked whenever
	// upper_len is non-zero. The buffer written into output is allocated here and its pointer is
	// null-checked before the copy.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let upper_bytes = if upper_len == 0 {
			&[][..]
		} else {
			from_raw_parts(upper_ptr, upper_len)
		};
		let boundary = test_state_envelope(
			operator_id,
			GroupId(group),
			Keyspace::ROW_NUMBER_MAPPING,
			upper_bytes.to_vec(),
		)
		.as_slice()
		.to_vec();
		let prefix = test_state_envelope(
			operator_id,
			GroupId(group),
			Keyspace::ROW_NUMBER_MAPPING,
			Vec::<u8>::new(),
		)
		.as_slice()
		.to_vec();

		let mut dropped: Vec<u64> = Vec::new();
		let mut to_remove: Vec<EncodedKey> = Vec::new();
		for key in test_ctx.state_keys() {
			let bytes = key.as_slice();
			if bytes.starts_with(&prefix) && bytes > boundary.as_slice() {
				if let Some(value) = test_ctx.get_state(&key)
					&& value.len() >= 8
				{
					dropped.push(u64::from_le_bytes(value[..8].try_into().unwrap()));
				}
				to_remove.push(key);
			}
		}
		for key in to_remove {
			test_ctx.remove_state(&key);
		}

		let mut packed = Vec::with_capacity(dropped.len() * 8);
		for row_number in dropped {
			packed.extend_from_slice(&row_number.to_le_bytes());
		}
		if packed.is_empty() {
			(*output).ptr = ptr::null_mut();
			(*output).len = 0;
			(*output).cap = 0;
		} else {
			let out_ptr = test_alloc(packed.len());
			if out_ptr.is_null() {
				return EXTERN_C_ERROR_INTERNAL;
			}
			ptr::copy_nonoverlapping(packed.as_ptr(), out_ptr, packed.len());
			(*output).ptr = out_ptr;
			(*output).len = packed.len();
			(*output).cap = packed.len();
		}
		EXTERN_C_OK
	}
}

extern "C" fn test_dictionary_id_by_name(
	ctx: *mut ExternCContext,
	name_ptr: *const u8,
	name_len: usize,
	out_id: *mut u64,
	found: *mut u8,
) -> i32 {
	if ctx.is_null() || name_ptr.is_null() || out_id.is_null() || found.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: every pointer is null-checked above; the caller owns (name_ptr, name_len) as a
	// readable region, and out_id and found as single writable cells that are written, never read.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let name = match from_utf8(from_raw_parts(name_ptr, name_len)) {
			Ok(name) => name,
			Err(_) => return EXTERN_C_ERROR_INTERNAL,
		};
		match test_ctx.dictionary_id_by_name(name) {
			Some(id) => {
				*out_id = id;
				*found = 1;
			}
			None => *found = 0,
		}
		EXTERN_C_OK
	}
}

extern "C" fn test_dictionary_find(
	ctx: *mut ExternCContext,
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

	// SAFETY: every pointer is null-checked above; the caller owns (value_ptr, value_len) as a
	// readable region, and out_id, out_id_type and found as single writable cells.
	unsafe {
		let test_ctx = get_test_context(ctx);
		let value_bytes = from_raw_parts(value_ptr, value_len);
		match test_ctx.dictionary_find(dictionary_id, value_bytes) {
			Some((id, id_type)) => {
				*out_id = id;
				*out_id_type = id_type;
				*found = 1;
			}
			None => *found = 0,
		}
		EXTERN_C_OK
	}
}

extern "C" fn test_dictionary_get(
	ctx: *mut ExternCContext,
	dictionary_id: u64,
	id: u128,
	output: *mut ExternCBuffer,
) -> i32 {
	if ctx.is_null() || output.is_null() {
		return EXTERN_C_ERROR_NULL_PTR;
	}

	// SAFETY: ctx and output are null-checked above, and the freshly allocated value_ptr is
	// null-checked before the copy that fills output; source and destination cannot alias.
	unsafe {
		let test_ctx = get_test_context(ctx);
		match test_ctx.dictionary_get(dictionary_id, id) {
			Some(value_bytes) => {
				let value_ptr = test_alloc(value_bytes.len());
				if value_ptr.is_null() {
					return -2;
				}
				ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_bytes.len());
				(*output).ptr = value_ptr;
				(*output).len = value_bytes.len();
				(*output).cap = value_bytes.len();
				EXTERN_C_OK
			}
			None => EXTERN_C_NOT_FOUND,
		}
	}
}

pub fn create_test_callbacks() -> OperatorCallbacks {
	OperatorCallbacks {
		memory: MemoryCallbacks {
			alloc: test_alloc,
			free: test_free,
		},
		state: StateCallbacks {
			get: test_state_get,
			set: test_state_set,
			remove: test_state_remove,
			clear: test_state_clear,
			prefix: test_state_prefix,
			range: test_state_range,
			iterator_next: test_state_iterator_next,
			iterator_free: test_state_iterator_free,
			get_many: test_state_get_many,
			get_or_create_row_numbers: test_get_or_create_row_numbers,
			remove_row_number: test_remove_row_number,
			remove_row_numbers_below: test_remove_row_numbers_below,
			intern_groups: test_intern_groups,
			lookup_groups: test_lookup_groups,
			arm_timer: test_arm_timer,
			disarm_timer: test_disarm_timer,
			flow_watermark: test_flow_watermark,
		},
		dictionary: DictionaryCallbacks {
			id_by_name: test_dictionary_id_by_name,
			find: test_dictionary_find,
			get: test_dictionary_get,
		},
		builder: BuilderCallbacks {
			acquire: test_acquire,
			data_ptr: test_data_ptr,
			offsets_ptr: test_offsets_ptr,
			bitvec_ptr: test_bitvec_ptr,
			grow: test_grow,
			commit: test_commit,
			release: test_release,
			emit_diff: test_emit_diff,
		},
	}
}
