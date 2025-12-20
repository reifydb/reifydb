//! Store access for FFI operators
//!
//! Provides read-only access to the underlying store,
//! allowing operators to query data beyond their own state.

use std::{ops::Bound, ptr::null_mut, slice::from_raw_parts};

use reifydb_core::{
	CowVec,
	value::encoded::{EncodedKey, EncodedValues},
};
use reifydb_flow_operator_abi::{BufferFFI, FFI_END_OF_ITERATION, FFI_NOT_FOUND, FFI_OK, StoreIteratorFFI};
use tracing::instrument;

use crate::{
	context::OperatorContext,
	error::{FFIError, Result},
};

/// Store accessor providing read-only access to the underlying store
pub struct Store<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> Store<'a> {
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	#[instrument(name = "flow::operator::store::get", level = "trace", skip(self), fields(
		key_len = key.as_bytes().len(),
		found
	))]
	pub fn get(&self, key: &EncodedKey) -> Result<Option<EncodedValues>> {
		let result = raw_store_get(self.ctx, key)?;
		tracing::Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::operator::store::contains_key", level = "trace", skip(self), fields(
		key_len = key.as_bytes().len()
	))]
	pub fn contains_key(&self, key: &EncodedKey) -> Result<bool> {
		raw_store_contains_key(self.ctx, key)
	}

	#[instrument(name = "flow::operator::store::prefix", level = "trace", skip(self), fields(
		prefix_len = prefix.as_bytes().len(),
		result_count
	))]
	pub fn prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		let results = raw_store_prefix(self.ctx, prefix)?;
		tracing::Span::current().record("result_count", results.len());
		Ok(results)
	}

	#[instrument(
		name = "flow::operator::store::range",
		level = "trace",
		skip(self, start, end),
		fields(result_count)
	)]
	pub fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		let results = raw_store_range(self.ctx, start, end)?;
		tracing::Span::current().record("result_count", results.len());
		Ok(results)
	}
}

/// Get a value from store by key
#[instrument(name = "flow::operator::store::raw::get", level = "trace", skip(ctx), fields(
	key_len = key.as_bytes().len()
))]
fn raw_store_get(ctx: &OperatorContext, key: &EncodedKey) -> Result<Option<EncodedValues>> {
	let key_bytes = key.as_bytes();
	let mut output = BufferFFI {
		ptr: null_mut(),
		len: 0,
		cap: 0,
	};

	unsafe {
		let result =
			((*ctx.ctx).callbacks.store.get)(ctx.ctx, key_bytes.as_ptr(), key_bytes.len(), &mut output);

		if result == FFI_OK {
			// Success - value found
			if output.ptr.is_null() || output.len == 0 {
				Ok(None)
			} else {
				let value_bytes = from_raw_parts(output.ptr, output.len).to_vec();
				// Free the buffer allocated by host
				((*ctx.ctx).callbacks.memory.free)(output.ptr as *mut u8, output.len);
				Ok(Some(EncodedValues(CowVec::new(value_bytes))))
			}
		} else if result == FFI_NOT_FOUND {
			// Key not found
			Ok(None)
		} else {
			Err(FFIError::Other(format!("host_store_get failed with code {}", result)))
		}
	}
}

/// Check if a key exists in store
#[instrument(name = "flow::operator::store::raw::contains_key", level = "trace", skip(ctx), fields(
	key_len = key.as_bytes().len()
))]
fn raw_store_contains_key(ctx: &OperatorContext, key: &EncodedKey) -> Result<bool> {
	let key_bytes = key.as_bytes();
	let mut result_byte: u8 = 0;

	unsafe {
		let result = ((*ctx.ctx).callbacks.store.contains_key)(
			ctx.ctx,
			key_bytes.as_ptr(),
			key_bytes.len(),
			&mut result_byte,
		);

		if result == FFI_OK {
			Ok(result_byte != 0)
		} else {
			Err(FFIError::Other(format!("host_store_contains_key failed with code {}", result)))
		}
	}
}

/// Scan all keys with a given prefix
#[instrument(name = "flow::operator::store::raw::prefix", level = "trace", skip(ctx), fields(
	prefix_len = prefix.as_bytes().len()
))]
fn raw_store_prefix(ctx: &OperatorContext, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedValues)>> {
	let prefix_bytes = prefix.as_bytes();
	let mut iterator: *mut StoreIteratorFFI = null_mut();

	unsafe {
		let result = ((*ctx.ctx).callbacks.store.prefix)(
			ctx.ctx,
			prefix_bytes.as_ptr(),
			prefix_bytes.len(),
			&mut iterator,
		);

		if result < 0 {
			return Err(FFIError::Other(format!("host_store_prefix failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

/// Bound type constants for FFI
const BOUND_UNBOUNDED: u8 = 0;
const BOUND_INCLUDED: u8 = 1;
const BOUND_EXCLUDED: u8 = 2;

/// Scan all keys within a range
#[instrument(name = "flow::operator::store::raw::range", level = "trace", skip(ctx, start, end))]
fn raw_store_range(
	ctx: &OperatorContext,
	start: Bound<&EncodedKey>,
	end: Bound<&EncodedKey>,
) -> Result<Vec<(EncodedKey, EncodedValues)>> {
	let mut iterator: *mut StoreIteratorFFI = null_mut();

	unsafe {
		let (start_ptr, start_len, start_bound_type) = match start {
			Bound::Unbounded => (std::ptr::null(), 0, BOUND_UNBOUNDED),
			Bound::Included(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_INCLUDED),
			Bound::Excluded(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_EXCLUDED),
		};

		let (end_ptr, end_len, end_bound_type) = match end {
			Bound::Unbounded => (std::ptr::null(), 0, BOUND_UNBOUNDED),
			Bound::Included(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_INCLUDED),
			Bound::Excluded(key) => (key.as_bytes().as_ptr(), key.as_bytes().len(), BOUND_EXCLUDED),
		};

		let result = ((*ctx.ctx).callbacks.store.range)(
			ctx.ctx,
			start_ptr,
			start_len,
			start_bound_type,
			end_ptr,
			end_len,
			end_bound_type,
			&mut iterator,
		);

		if result < 0 {
			return Err(FFIError::Other(format!("host_store_range failed with code {}", result)));
		}

		collect_iterator_results(ctx, iterator)
	}
}

/// Helper to collect all results from a store iterator
///
/// # Safety
/// - iterator must be a valid pointer returned by a store prefix/range call
/// - ctx must have valid callbacks
#[instrument(
	name = "flow::operator::store::collect_iterator",
	level = "trace",
	skip(ctx, iterator),
	fields(result_count)
)]
unsafe fn collect_iterator_results(
	ctx: &OperatorContext,
	iterator: *mut StoreIteratorFFI,
) -> Result<Vec<(EncodedKey, EncodedValues)>> {
	if iterator.is_null() {
		tracing::Span::current().record("result_count", 0);
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
			unsafe { ((*ctx.ctx).callbacks.store.iterator_next)(iterator, &mut key_buf, &mut value_buf) };

		if next_result == FFI_END_OF_ITERATION {
			// End of iteration
			break;
		} else if next_result != FFI_OK {
			unsafe { ((*ctx.ctx).callbacks.store.iterator_free)(iterator) };
			return Err(FFIError::Other(format!(
				"host_store_iterator_next failed with code {}",
				next_result
			)));
		}

		// Convert buffers to owned data
		if !key_buf.ptr.is_null() && key_buf.len > 0 {
			let key_bytes = unsafe { from_raw_parts(key_buf.ptr, key_buf.len) }.to_vec();
			let key = EncodedKey(CowVec::new(key_bytes));

			let value = if !value_buf.ptr.is_null() && value_buf.len > 0 {
				let value_bytes = unsafe { from_raw_parts(value_buf.ptr, value_buf.len) }.to_vec();
				EncodedValues(CowVec::new(value_bytes))
			} else {
				EncodedValues(CowVec::new(Vec::new()))
			};

			// Free buffers allocated by host
			unsafe { ((*ctx.ctx).callbacks.memory.free)(key_buf.ptr as *mut u8, key_buf.len) };
			if !value_buf.ptr.is_null() && value_buf.len > 0 {
				unsafe { ((*ctx.ctx).callbacks.memory.free)(value_buf.ptr as *mut u8, value_buf.len) };
			}

			results.push((key, value));
		}
	}

	unsafe { ((*ctx.ctx).callbacks.store.iterator_free)(iterator) };
	tracing::Span::current().record("result_count", results.len());
	Ok(results)
}
