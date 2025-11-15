//! Host callback implementations for FFI operators

use std::{
	alloc::{Layout, alloc, dealloc, realloc as system_realloc},
	ffi::c_void,
};

use reifydb_flow_operator_abi::*;

use crate::ffi::Arena;

/// Thread-local storage for the current arena
/// All allocations during an FFI operation will use this arena
thread_local! {
    static CURRENT_ARENA: std::cell::RefCell<Option<*mut Arena>> = std::cell::RefCell::new(None);
}

/// Set the current arena for this thread
pub fn set_current_arena(arena: *mut Arena) {
	CURRENT_ARENA.with(|a| {
		*a.borrow_mut() = Some(arena);
	});
}

/// Clear the current arena for this thread
pub fn clear_current_arena() {
	CURRENT_ARENA.with(|a| {
		*a.borrow_mut() = None;
	});
}

/// Create host callbacks structure
pub fn create_host_callbacks() -> HostCallbacks {
	HostCallbacks {
		alloc: host_alloc,
		dealloc: host_dealloc,
		realloc: host_realloc,
		eval_expression: host_eval_expression,
		create_row: host_create_row,
		clone_row: host_clone_row,
		free_row: host_free_row,
		encode_values_as_key: host_encode_values_as_key,
		free_value: host_free_value,
		state_iterator_next: host_state_iterator_next,
		state_iterator_free: host_state_iterator_free,
		log_message: host_log_message,
	}
}

// ==================== Memory Management ====================

extern "C" fn host_alloc(size: usize) -> *mut u8 {
	if size == 0 {
		return std::ptr::null_mut();
	}

	// Try to use the thread-local arena first
	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			unsafe { (*arena_ptr).alloc(size) }
		} else {
			// Fallback to system allocator if no arena set
			let layout = match Layout::from_size_align(size, 8) {
				Ok(layout) => layout,
				Err(_) => return std::ptr::null_mut(),
			};
			unsafe { alloc(layout) }
		}
	})
}

extern "C" fn host_dealloc(ptr: *mut u8, size: usize) {
	if ptr.is_null() || size == 0 {
		return;
	}

	// Check if this is arena-allocated memory
	CURRENT_ARENA.with(|a| {
		if (*a.borrow()).is_some() {
			// Arena memory is freed automatically - do nothing
			return;
		}
	});

	// Otherwise use system deallocator
	let layout = match Layout::from_size_align(size, 8) {
		Ok(layout) => layout,
		Err(_) => return,
	};
	unsafe { dealloc(ptr, layout) }
}

extern "C" fn host_realloc(ptr: *mut u8, old_size: usize, new_size: usize) -> *mut u8 {
	// For arena allocations, we can't realloc in place, so alloc new and copy
	if ptr.is_null() {
		return host_alloc(new_size);
	}

	if new_size == 0 {
		host_dealloc(ptr, old_size);
		return std::ptr::null_mut();
	}

	// Check if using arena
	CURRENT_ARENA.with(|a| {
		if let Some(arena_ptr) = *a.borrow() {
			// Arena can't realloc, so alloc new and copy
			let new_ptr = unsafe { (*arena_ptr).alloc(new_size) };
			if !new_ptr.is_null() {
				let copy_size = old_size.min(new_size);
				unsafe {
					std::ptr::copy_nonoverlapping(ptr, new_ptr, copy_size);
				}
			}
			// Note: old arena memory will be freed when arena is cleared
			new_ptr
		} else {
			// Use system realloc
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
	})
}

// ==================== Expression Evaluation ====================

extern "C" fn host_eval_expression(expr: *const ExpressionHandle, row: *const RowFFI) -> ValueFFI {
	if expr.is_null() || row.is_null() {
		return ValueFFI::undefined();
	}

	// TODO: Implement actual expression evaluation
	// For now, return undefined as placeholder
	ValueFFI::undefined()
}

// ==================== Row Operations ====================

extern "C" fn host_create_row(
	row_number: u64,
	encoded: *const u8,
	encoded_len: usize,
	layout_handle: *const c_void,
) -> *mut RowFFI {
	// Allocate a RowFFI structure
	let row_size = std::mem::size_of::<RowFFI>();
	let row_ptr = host_alloc(row_size) as *mut RowFFI;

	if row_ptr.is_null() {
		return std::ptr::null_mut();
	}

	// Copy encoded data
	let encoded_copy = if !encoded.is_null() && encoded_len > 0 {
		let data_ptr = host_alloc(encoded_len);
		if !data_ptr.is_null() {
			unsafe {
				std::ptr::copy_nonoverlapping(encoded, data_ptr, encoded_len);
			}
		}
		BufferFFI {
			ptr: data_ptr,
			len: encoded_len,
			cap: encoded_len,
		}
	} else {
		BufferFFI {
			ptr: std::ptr::null_mut(),
			len: 0,
			cap: 0,
		}
	};

	// Initialize the row
	unsafe {
		*row_ptr = RowFFI {
			number: row_number,
			encoded: encoded_copy,
			layout_handle,
		};
	}

	row_ptr
}

extern "C" fn host_clone_row(row: *const RowFFI) -> *mut RowFFI {
	if row.is_null() {
		return std::ptr::null_mut();
	}

	unsafe {
		let src_row = &*row;
		host_create_row(src_row.number, src_row.encoded.ptr, src_row.encoded.len, src_row.layout_handle)
	}
}

extern "C" fn host_free_row(row: *mut RowFFI) {
	if row.is_null() {
		return;
	}

	unsafe {
		let row_data = &*row;

		// Free encoded data if present
		if !row_data.encoded.ptr.is_null() {
			host_dealloc(row_data.encoded.ptr as *mut u8, row_data.encoded.len);
		}

		// Free the row structure itself
		host_dealloc(row as *mut u8, std::mem::size_of::<RowFFI>());
	}
}

// ==================== Value Operations ====================

extern "C" fn host_encode_values_as_key(values: *const ValueFFI, value_count: usize, output: *mut BufferFFI) -> i32 {
	if values.is_null() || output.is_null() || value_count == 0 {
		return -1;
	}

	// TODO: Implement actual value encoding
	// For now, create a simple concatenated key

	// Estimate size needed (very rough estimate)
	let estimated_size = value_count * 32;
	let key_ptr = host_alloc(estimated_size);

	if key_ptr.is_null() {
		return -2; // Allocation failed
	}

	// Simple encoding: just concatenate value representations
	let mut offset = 0;
	unsafe {
		let values_slice = std::slice::from_raw_parts(values, value_count);

		for value in values_slice {
			// Write value type tag
			if offset < estimated_size {
				*key_ptr.add(offset) = get_value_type_tag(value);
				offset += 1;
			}

			// TODO: Write actual value data based on type
			// For now, just add a separator
			if offset < estimated_size {
				*key_ptr.add(offset) = 0xFF; // Separator
				offset += 1;
			}
		}

		(*output).ptr = key_ptr;
		(*output).len = offset;
	}

	0 // Success
}

extern "C" fn host_free_value(value: *mut ValueFFI) {
	if value.is_null() {
		return;
	}

	// Free the value structure
	host_dealloc(value as *mut u8, std::mem::size_of::<ValueFFI>());
}

// ==================== Iterator Operations ====================

extern "C" fn host_state_iterator_next(
	iterator: *mut StateIteratorFFI,
	key_out: *mut BufferFFI,
	value_out: *mut BufferFFI,
) -> i32 {
	if iterator.is_null() || value_out.is_null() {
		return -1;
	}

	// TODO: Implement actual state iteration
	// For now, return end of iteration
	1 // End of iteration
}

extern "C" fn host_state_iterator_free(iterator: *mut StateIteratorFFI) {
	if iterator.is_null() {
		return;
	}

	// TODO: Free iterator resources
	host_dealloc(iterator as *mut u8, std::mem::size_of::<StateIteratorFFI>());
}

// ==================== Logging ====================

extern "C" fn host_log_message(level: u32, message: *const u8) {
	if message.is_null() {
		return;
	}

	// Convert message to string
	let msg_str = unsafe {
		let len = {
			let mut len = 0;
			while *message.add(len) != 0 {
				len += 1;
			}
			len
		};

		let bytes = std::slice::from_raw_parts(message, len);
		String::from_utf8_lossy(bytes)
	};

	// Log based on level
	use reifydb_core::{log_debug, log_error, log_info, log_trace, log_warn};

	match level {
		0 => log_trace!("FFI Operator: {}", msg_str),
		1 => log_debug!("FFI Operator: {}", msg_str),
		2 => log_info!("FFI Operator: {}", msg_str),
		3 => log_warn!("FFI Operator: {}", msg_str),
		4 => log_error!("FFI Operator: {}", msg_str),
		_ => log_info!("FFI Operator: {}", msg_str),
	}
}

// Helper function for getting value type tag
fn get_value_type_tag(value: &ValueFFI) -> u8 {
	value.value_type as u8
}
