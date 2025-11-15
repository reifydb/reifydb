//! Raw FFI state operations
//!
//! Low-level state operations that directly call host FFI callbacks.
//! These functions should not be used directly - use the State API instead.

use std::{ptr::null_mut, slice::from_raw_parts};

use reifydb_flow_operator_abi::{BufferFFI, StateIteratorFFI};

use crate::{
	context::OperatorContext,
	error::{FFIError, Result},
};

/// Get a value from state by key
pub(crate) fn raw_state_get(ctx: &OperatorContext, key: &str) -> Result<Option<Vec<u8>>> {
	let key_bytes = key.as_bytes();
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.get)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			&mut output,
		);

		if result == 0 {
			// Success - value found
			if output.ptr.is_null() || output.len == 0 {
				Ok(None)
			} else {
				let value = from_raw_parts(output.ptr, output.len).to_vec();
				// TODO: Free the buffer using host dealloc
				Ok(Some(value))
			}
		} else if result == 1 {
			// Key not found
			Ok(None)
		} else {
			Err(FFIError::Other(format!("host_state_get failed with code {}", result)))
		}
	}
}

/// Set a value in state by key
pub(crate) fn raw_state_set(ctx: &mut OperatorContext, key: &str, value: &[u8]) -> Result<()> {
	let key_bytes = key.as_bytes();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.set)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			value.as_ptr(),
			value.len(),
		);

		if result == 0 {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_set failed with code {}", result)))
		}
	}
}

/// Remove a value from state by key
pub(crate) fn raw_state_remove(ctx: &mut OperatorContext, key: &str) -> Result<()> {
	let key_bytes = key.as_bytes();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.remove)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
		);

		if result == 0 {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_remove failed with code {}", result)))
		}
	}
}

/// Scan all keys with a given prefix
pub(crate) fn raw_state_prefix(ctx: &OperatorContext, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
	let prefix_bytes = prefix.as_bytes();
	let mut iterator: *mut StateIteratorFFI = null_mut();

	unsafe {
		let result = ((*ctx.ctx).callbacks.state.prefix)(
			(*ctx.ctx).operator_id,
			ctx.ctx,
			prefix_bytes.as_ptr(),
			prefix_bytes.len(),
			&mut iterator,
		);

		if result != 0 {
			return Err(FFIError::Other(format!("host_state_prefix failed with code {}", result)));
		}

		if iterator.is_null() {
			return Ok(Vec::new());
		}

		let mut results = Vec::new();

		loop {
			let mut key_buf = BufferFFI {
				ptr: null_mut(),
				len: 0,
				cap: 0,
			};
			let mut value_buf = BufferFFI {
				ptr: null_mut(),
				len: 0,
				cap: 0,
			};

			let next_result =
				((*ctx.ctx).callbacks.state.iterator_next)(iterator, &mut key_buf, &mut value_buf);

			if next_result == 1 {
				// End of iteration
				break;
			} else if next_result != 0 {
				((*ctx.ctx).callbacks.state.iterator_free)(iterator);
				return Err(FFIError::Other(format!(
					"host_state_iterator_next failed with code {}",
					next_result
				)));
			}

			// Convert buffers to owned data
			if !key_buf.ptr.is_null() && key_buf.len > 0 {
				let key_slice = from_raw_parts(key_buf.ptr, key_buf.len);
				let key = String::from_utf8_lossy(key_slice).to_string();

				let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
					from_raw_parts(value_buf.ptr, value_buf.len).to_vec()
				} else {
					Vec::new()
				};

				results.push((key, value));
				// TODO: Free key_buf and value_buf using host dealloc
			}
		}

		((*ctx.ctx).callbacks.state.iterator_free)(iterator);
		Ok(results)
	}
}

/// Clear all state for this operator
pub(crate) fn raw_state_clear(ctx: &mut OperatorContext) -> Result<()> {
	unsafe {
		let result = ((*ctx.ctx).callbacks.state.clear)((*ctx.ctx).operator_id, ctx.ctx);

		if result == 0 {
			Ok(())
		} else {
			Err(FFIError::Other(format!("host_state_clear failed with code {}", result)))
		}
	}
}
