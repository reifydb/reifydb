// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! FFI callback implementations for testing operators
//!
//! This module provides test-specific implementations of FFI callbacks that bridge
//! OperatorContext with TestContext, enabling operators to be tested in isolation.
//!
//! Unlike the production implementation:
//! - Memory uses system allocator directly (no arena)
//! - State operations work with TestContext's HashMap instead of FlowTransaction
//! - Logs are captured to TestContext.logs instead of actual logging
//! - Iterators are simplified in-memory implementations

use std::{
	alloc::{Layout, alloc, dealloc, realloc as system_realloc},
	slice::from_raw_parts,
};

use reifydb_core::{CowVec, value::encoded::EncodedKey};
use reifydb_flow_operator_abi::{
	BufferFFI, FFIContext, HostCallbacks, LogCallbacks, MemoryCallbacks, StateCallbacks, StateIteratorFFI,
};

use super::TestContext;

// ============================================================================
// Memory Callbacks (System Allocator)
// ============================================================================

/// Allocate memory using system allocator
#[unsafe(no_mangle)]
extern "C" fn test_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return std::ptr::null_mut();
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return std::ptr::null_mut(),
	};

	unsafe { alloc(layout) }
}

/// Free memory allocated by test_alloc
#[unsafe(no_mangle)]
extern "C" fn test_free(ptr: *mut u8, size: usize) {
	if ptr.is_null() || size == 0 {
		return;
	}

	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};

	unsafe { dealloc(ptr, layout) }
}

/// Reallocate memory
#[unsafe(no_mangle)]
extern "C" fn test_realloc(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
	if ptr.is_null() {
		return test_alloc(new_size);
	}

	if new_size == 0 {
		test_free(ptr, old_size);
		return std::ptr::null_mut();
	}

	let old_layout = match Layout::from_size_align(old_size, 8) {
		Ok(layout) => layout,
		Err(_) => return std::ptr::null_mut(),
	};

	let new_layout = match Layout::from_size_align(new_size, 8) {
		Ok(layout) => layout,
		Err(_) => return std::ptr::null_mut(),
	};

	unsafe { system_realloc(ptr, old_layout, new_layout.size()) }
}

// ============================================================================
// State Callbacks (TestContext HashMap)
// ============================================================================

/// Helper to get TestContext from FFI context
unsafe fn get_test_context(ctx: *mut FFIContext) -> &'static TestContext {
	unsafe {
		let txn_ptr = (*ctx).txn_ptr;
		&*(txn_ptr as *const TestContext)
	}
}

/// Get state value from TestContext
#[unsafe(no_mangle)]
extern "C" fn test_state_get(
	_operator_id: u64,
	ctx: *mut FFIContext,
	key_ptr: *const u8,
	key_len: usize,
	output: *mut BufferFFI,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || output.is_null() {
		return -1;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Get from TestContext state store
		match test_ctx.get_state(&key) {
			Some(value_bytes) => {
				// Allocate and copy value
				let value_ptr = test_alloc(value_bytes.len());
				if value_ptr.is_null() {
					return -2; // Allocation failed
				}

				std::ptr::copy_nonoverlapping(value_bytes.as_ptr(), value_ptr, value_bytes.len());

				(*output).ptr = value_ptr;
				(*output).len = value_bytes.len();
				(*output).cap = value_bytes.len();

				0 // Success, value found
			}
			None => 1, // Key not found
		}
	}
}

/// Set state value in TestContext
#[unsafe(no_mangle)]
extern "C" fn test_state_set(
	_operator_id: u64,
	ctx: *mut FFIContext,
	key_ptr: *const u8,
	key_len: usize,
	value_ptr: *const u8,
	value_len: usize,
) -> i32 {
	if ctx.is_null() || key_ptr.is_null() || value_ptr.is_null() {
		return -1;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Convert raw bytes to value
		let value_bytes = from_raw_parts(value_ptr, value_len);

		// Set in TestContext
		test_ctx.set_state(key, value_bytes.to_vec());

		0 // Success
	}
}

/// Remove state value from TestContext
#[unsafe(no_mangle)]
extern "C" fn test_state_remove(_operator_id: u64, ctx: *mut FFIContext, key_ptr: *const u8, key_len: usize) -> i32 {
	if ctx.is_null() || key_ptr.is_null() {
		return -1;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		// Convert raw bytes to EncodedKey
		let key_bytes = from_raw_parts(key_ptr, key_len);
		let key = EncodedKey(CowVec::new(key_bytes.to_vec()));

		// Remove from TestContext
		test_ctx.remove_state(&key);

		0 // Success
	}
}

/// Clear all state in TestContext
#[unsafe(no_mangle)]
extern "C" fn test_state_clear(_operator_id: u64, ctx: *mut FFIContext) -> i32 {
	if ctx.is_null() {
		return -1;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);
		test_ctx.clear_state();
		0 // Success
	}
}

// ============================================================================
// State Iterator Support
// ============================================================================

/// Internal structure for state iterators
#[repr(C)]
struct TestStateIterator {
	/// Collected key-value pairs (snapshot at creation time)
	items: Vec<(Vec<u8>, Vec<u8>)>,
	/// Current position in iteration
	position: usize,
}

/// Create an iterator for state with a specific prefix
#[unsafe(no_mangle)]
extern "C" fn test_state_prefix(
	_operator_id: u64,
	ctx: *mut FFIContext,
	prefix_ptr: *const u8,
	prefix_len: usize,
	iterator_out: *mut *mut StateIteratorFFI,
) -> i32 {
	if ctx.is_null() || iterator_out.is_null() {
		return -1;
	}

	unsafe {
		let test_ctx = get_test_context(ctx);

		// Get prefix bytes (can be empty for full scan)
		let prefix_bytes = if prefix_ptr.is_null() || prefix_len == 0 {
			vec![]
		} else {
			from_raw_parts(prefix_ptr, prefix_len).to_vec()
		};

		// Collect all matching key-value pairs from TestContext
		let state_store = test_ctx.state_store();
		let state = state_store.lock().unwrap();

		let mut items: Vec<(Vec<u8>, Vec<u8>)> = state
			.iter()
			.filter(|(key, _)| {
				if prefix_bytes.is_empty() {
					true // Full scan
				} else {
					key.0.starts_with(&prefix_bytes) // Prefix match
				}
			})
			.map(|(key, value)| (key.0.to_vec(), value.0.to_vec()))
			.collect();

		// Sort by key for deterministic iteration order
		items.sort_by(|a, b| a.0.cmp(&b.0));

		// Create iterator structure
		let iter = Box::new(TestStateIterator {
			items,
			position: 0,
		});

		// Leak the box and cast to opaque pointer
		*iterator_out = Box::into_raw(iter) as *mut StateIteratorFFI;

		0 // Success
	}
}

/// Get the next key-value pair from a state iterator
#[unsafe(no_mangle)]
extern "C" fn test_state_iterator_next(
	iterator: *mut StateIteratorFFI,
	key_out: *mut BufferFFI,
	value_out: *mut BufferFFI,
) -> i32 {
	if iterator.is_null() || key_out.is_null() || value_out.is_null() {
		return -1;
	}

	unsafe {
		// Cast opaque pointer back to TestStateIterator
		let iter = &mut *(iterator as *mut TestStateIterator);

		// Check if we have more items
		if iter.position >= iter.items.len() {
			return 1; // End of iteration
		}

		let (key, value) = &iter.items[iter.position];
		iter.position += 1;

		// Allocate and copy key
		let key_ptr = test_alloc(key.len());
		if key_ptr.is_null() {
			return -2; // Allocation failed
		}
		std::ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());
		(*key_out).ptr = key_ptr;
		(*key_out).len = key.len();
		(*key_out).cap = key.len();

		// Allocate and copy value
		let value_ptr = test_alloc(value.len());
		if value_ptr.is_null() {
			// Free the key we just allocated
			test_free(key_ptr, key.len());
			return -2; // Allocation failed
		}
		std::ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
		(*value_out).ptr = value_ptr;
		(*value_out).len = value.len();
		(*value_out).cap = value.len();

		0 // Success, has next
	}
}

/// Free a state iterator
#[unsafe(no_mangle)]
extern "C" fn test_state_iterator_free(iterator: *mut StateIteratorFFI) {
	if iterator.is_null() {
		return;
	}

	unsafe {
		// Cast back to TestStateIterator and drop
		let _ = Box::from_raw(iterator as *mut TestStateIterator);
	}
}

// ============================================================================
// Log Callback (Capture to TestContext)
// ============================================================================

/// Capture log message to TestContext
#[unsafe(no_mangle)]
extern "C" fn test_log_message(_operator_id: u64, level: u32, message: *const u8, message_len: usize) {
	if message.is_null() {
		return;
	}

	// Convert message to string
	let msg_str = unsafe {
		let bytes = from_raw_parts(message, message_len);
		String::from_utf8_lossy(bytes).to_string()
	};

	// Format with log level prefix
	let formatted = match level {
		0 => format!("[TRACE] {}", msg_str),
		1 => format!("[DEBUG] {}", msg_str),
		2 => format!("[INFO] {}", msg_str),
		3 => format!("[WARN] {}", msg_str),
		4 => format!("[ERROR] {}", msg_str),
		_ => format!("[UNKNOWN] {}", msg_str),
	};

	// Note: We can't access TestContext here without passing it through
	// For now, log messages are effectively discarded in tests
	// TODO: Find a way to capture logs to TestContext
	eprintln!("{}", formatted);
}

// ============================================================================
// Public API
// ============================================================================

/// Create the complete host callbacks structure for testing
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
			iterator_next: test_state_iterator_next,
			iterator_free: test_state_iterator_free,
		},
		log: LogCallbacks {
			message: test_log_message,
		},
	}
}
