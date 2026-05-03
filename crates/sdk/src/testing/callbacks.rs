// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	alloc::{Layout, alloc, dealloc, realloc as system_realloc},
	slice::from_raw_parts,
};

use reifydb_type::util::cowvec::CowVec;

#[unsafe(no_mangle)]
extern "C" fn test_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return ptr::null_mut();
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return ptr::null_mut(),
	};

	unsafe { alloc(layout) }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn test_free(ptr: *mut u8, size: usize) {
	if ptr.is_null() || size == 0 {
		return;
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};

	unsafe { dealloc(ptr, layout) }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn test_realloc(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
	if ptr.is_null() {
		return test_alloc(new_size);
	}

	if new_size == 0 {
		unsafe { test_free(ptr, old_size) };
		return ptr::null_mut();
	}

	let old_layout = match Layout::from_size_align(old_size, 8) {
		Ok(layout) => layout,
		Err(_) => return ptr::null_mut(),
	};

	let new_layout = match Layout::from_size_align(new_size, 8) {
		Ok(layout) => layout,
		Err(_) => return ptr::null_mut(),
	};

	unsafe { system_realloc(ptr, old_layout, new_layout.size()) }
}

unsafe fn get_test_context(ctx: *mut ContextFFI) -> &'static TestContext {
	unsafe {
		let txn_ptr = (*ctx).txn_ptr;
		&*(txn_ptr as *const TestContext)
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_get(
	_operator_id: u64,
	ctx: *mut ContextFFI,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut BufferFFI,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

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

				FFI_OK
			}
			None => FFI_NOT_FOUND,
		}
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_set(
	_operator_id: u64,
	ctx: *mut ContextFFI,
	key_ptr: *const u8,
	key_len: usize,
	value_ptr: *const u8,
	value_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || value_ptr.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		let value_bytes = from_raw_parts(value_ptr, value_len);

		test_ctx.set_state(key, value_bytes.to_vec());

		FFI_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_remove(_operator_id: u64, ctx: *mut ContextFFI, key_ptr: *const u8, key_len: usize) -> i32 {
	if ctx.is_null() || key_ptr.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		test_ctx.remove_state(&key);

		FFI_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_clear(_operator_id: u64, ctx: *mut ContextFFI) -> i32 {
	if ctx.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);
		test_ctx.clear_state();
		FFI_OK
	}
}

#[repr(C)]
struct TestStateIterator {
	items: Vec<(Vec<u8>, Vec<u8>)>,

	position: usize,
}

#[unsafe(no_mangle)]
extern "C" fn test_state_prefix(
	_operator_id: u64,
	ctx: *mut ContextFFI,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		let prefix_bytes = if prefix_ptr.is_null() || prefix_len == 0 {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};

		let state_store = test_ctx.state_store();
		let state = state_store.lock().unwrap();

		let mut items: Vec<(Vec<u8>, Vec<u8>)> = state
			.iter()
			.filter(|(key, _)| {
				if prefix_bytes.is_empty() {
					true
				} else {
					key.0.starts_with(&prefix_bytes)
				}
			})
			.map(|(key, value)| (key.0.to_vec(), value.0.to_vec()))
			.collect();

		items.sort_by(|a, b| a.0.cmp(&b.0));

		let iter = Box::new(TestStateIterator {
			items,
			position: 0,
		});

		*iterator_out = Box::into_raw(iter) as *mut StateIteratorFFI;

		FFI_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_iterator_next(
	iterator: *mut StateIteratorFFI,
	key_out: *mut BufferFFI,
	value_out: *mut BufferFFI,
) -> i32 {
	if iterator.is_null() || key_out.is_null() || value_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

	unsafe {
		let iter = &mut *(iterator as *mut TestStateIterator);

		if iter.position >= iter.items.len() {
			return FFI_END_OF_ITERATION;
		}

		let (key, value) = &iter.items[iter.position];
		iter.position += 1;

		let key_ptr = test_alloc(key.len());
		if key_ptr.is_null() {
			return -2;
		}
		ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());
		(*key_out).ptr = key_ptr;
		(*key_out).len = key.len();
		(*key_out).cap = key.len();

		let value_ptr = test_alloc(value.len());
		if value_ptr.is_null() {
			test_free(key_ptr, key.len());
			return -2;
		}
		ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
		(*value_out).ptr = value_ptr;
		(*value_out).len = value.len();
		(*value_out).cap = value.len();

		FFI_OK
	}
}

#[unsafe(no_mangle)]
extern "C" fn test_state_iterator_free(iterator: *mut StateIteratorFFI) {
	if iterator.is_null() {
		return;
	}

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
	ctx: *mut ContextFFI,
	start_ptr: *const u8,
	start_len: usize,
	start_bound_type: u8,
	end_ptr: *const u8,
	end_len: usize,
	end_bound_type: u8,
	iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return FFI_ERROR_NULL_PTR;
	}

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
		let state = state_store.lock().unwrap();

		let mut items: Vec<(Vec<u8>, Vec<u8>)> = state
			.iter()
			.filter(|(key, _)| {
				let key_bytes = key.0.as_slice();

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
			.map(|(key, value)| (key.0.to_vec(), value.0.to_vec()))
			.collect();

		items.sort_by(|a, b| a.0.cmp(&b.0));

		let iter = Box::new(TestStateIterator {
			items,
			position: 0,
		});

		*iterator_out = Box::into_raw(iter) as *mut StateIteratorFFI;

		FFI_OK
	}
}

#[unsafe(no_mangle)]
unsafe extern "C" fn test_log_message(_operator_id: u64, _level: u32, _message: *const u8, _message_len: usize) {
	unimplemented!()
}

extern "C" fn test_store_get(_ctx: *mut ContextFFI, _key: *const u8, _key_len: usize, _output: *mut BufferFFI) -> i32 {
	unimplemented!()
}

extern "C" fn test_store_contains_key(
	_ctx: *mut ContextFFI,
	_key: *const u8,
	_key_len: usize,
	_result: *mut u8,
) -> i32 {
	unimplemented!()
}

extern "C" fn test_store_prefix(
	_ctx: *mut ContextFFI,
	_prefix: *const u8,
	_prefix_len: usize,
	_iterator_out: *mut *mut StoreIteratorFFI,
) -> i32 {
	unimplemented!()
}

extern "C" fn test_store_range(
	_ctx: *mut ContextFFI,
	_start: *const u8,
	_start_len: usize,
	_start_bound_type: u8,
	_end: *const u8,
	_end_len: usize,
	_end_bound_type: u8,
	_iterator_out: *mut *mut StoreIteratorFFI,
) -> i32 {
	unimplemented!()
}

extern "C" fn test_store_iterator_next(
	_iterator: *mut StoreIteratorFFI,
	_key_out: *mut BufferFFI,
	_value_out: *mut BufferFFI,
) -> i32 {
	unimplemented!()
}

extern "C" fn test_store_iterator_free(_iterator: *mut StoreIteratorFFI) {
	unimplemented!()
}

use std::ptr;

use reifydb_abi::{
	callbacks::{
		builder::BuilderCallbacks, catalog::CatalogCallbacks, host::HostCallbacks, log::LogCallbacks,
		memory::MemoryCallbacks, rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
	},
	catalog::{namespace::NamespaceFFI, table::TableFFI},
	constants::{FFI_END_OF_ITERATION, FFI_ERROR_INTERNAL, FFI_ERROR_NULL_PTR, FFI_NOT_FOUND, FFI_OK},
	context::{
		context::ContextFFI,
		iterators::{StateIteratorFFI, StoreIteratorFFI},
	},
	data::buffer::BufferFFI,
};
use reifydb_core::encoded::key::EncodedKey;

use crate::testing::{
	context::TestContext,
	registry::{
		test_acquire, test_bitvec_ptr, test_commit, test_data_ptr, test_emit_diff, test_grow, test_offsets_ptr,
		test_release,
	},
};

extern "C" fn test_catalog_find_namespace(
	_ctx: *mut ContextFFI,
	_namespace_id: u64,
	_version: u64,
	_output: *mut NamespaceFFI,
) -> i32 {
	1
}

extern "C" fn test_catalog_find_namespace_by_name(
	_ctx: *mut ContextFFI,
	_name_ptr: *const u8,
	_name_len: usize,
	_version: u64,
	_output: *mut NamespaceFFI,
) -> i32 {
	1
}

extern "C" fn test_catalog_find_table(
	_ctx: *mut ContextFFI,
	_table_id: u64,
	_version: u64,
	_output: *mut TableFFI,
) -> i32 {
	1
}

extern "C" fn test_catalog_find_table_by_name(
	_ctx: *mut ContextFFI,
	_namespace_id: u64,
	_name_ptr: *const u8,
	_name_len: usize,
	_version: u64,
	_output: *mut TableFFI,
) -> i32 {
	1
}

extern "C" fn test_catalog_free_namespace(_namespace: *mut NamespaceFFI) {}

extern "C" fn test_catalog_free_table(_table: *mut TableFFI) {}

unsafe extern "C" fn test_rql(
	_ctx: *mut ContextFFI,
	_rql_ptr: *const u8,
	_rql_len: usize,
	_params_ptr: *const u8,
	_params_len: usize,
	_result_out: *mut BufferFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}

pub fn create_test_callbacks() -> HostCallbacks {
	HostCallbacks {
		memory: MemoryCallbacks {
			alloc: test_alloc,
			free: test_free,
			realloc: test_realloc,
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
		},
		log: LogCallbacks {
			message: test_log_message,
		},
		store: StoreCallbacks {
			get: test_store_get,
			contains_key: test_store_contains_key,
			prefix: test_store_prefix,
			range: test_store_range,
			iterator_next: test_store_iterator_next,
			iterator_free: test_store_iterator_free,
		},
		catalog: CatalogCallbacks {
			find_namespace: test_catalog_find_namespace,
			find_namespace_by_name: test_catalog_find_namespace_by_name,
			find_table: test_catalog_find_table,
			find_table_by_name: test_catalog_find_table_by_name,
			free_namespace: test_catalog_free_namespace,
			free_table: test_catalog_free_table,
		},
		rql: RqlCallbacks {
			rql: test_rql,
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
