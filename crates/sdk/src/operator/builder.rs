// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ptr;

use reifydb_abi::{
	callbacks::builder::{ColumnBufferHandle, EmitDiffKind},
	context::context::ContextFFI,
	data::column::ColumnTypeCode,
};
use reifydb_type::value::row_number::RowNumber;

use crate::{error::FFIError, operator::context::OperatorContext};

/// A handle to an active (mutable, growable) column buffer being filled by
/// the guest. Drop without `commit` releases the buffer back to the pool.
pub struct ColumnBuilder<'a> {
	ctx: *mut ContextFFI,
	handle: *mut ColumnBufferHandle,
	type_code: ColumnTypeCode,
	committed: bool,
	_phantom: core::marker::PhantomData<&'a ()>,
}

/// A handle to a finalised (committed) column buffer ready to be emitted via
/// `emit_diff`.
#[derive(Clone, Copy)]
pub struct CommittedColumn {
	handle: *mut ColumnBufferHandle,
	row_count: usize,
}

impl<'a> ColumnBuilder<'a> {
	/// Get a writable byte pointer into the column's data region.
	/// May be invalidated by a subsequent call to `grow`.
	pub fn data_ptr(&self) -> *mut u8 {
		unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.data_ptr)(self.handle)
		}
	}

	/// Get a writable u64 pointer into the offsets region (var-len only).
	/// Returns null for fixed-size types.
	pub fn offsets_ptr(&self) -> *mut u64 {
		unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.offsets_ptr)(self.handle)
		}
	}

	/// Get a writable byte pointer into the lazily-allocated bitvec.
	pub fn bitvec_ptr(&self) -> *mut u8 {
		unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.bitvec_ptr)(self.handle)
		}
	}

	/// Grow the column buffer by `additional` elements (or bytes for
	/// var-len data). Pointers from `data_ptr`/`offsets_ptr`/`bitvec_ptr`
	/// must be re-fetched after this call.
	pub fn grow(&self, additional: usize) -> Result<(), FFIError> {
		let code = unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.grow)(self.handle, additional)
		};
		if code != 0 {
			return Err(FFIError::Other(format!("ColumnBuilder::grow failed: {}", code)));
		}
		Ok(())
	}

	/// Commit the buffer with the given final element count. The host
	/// adopts the buffer as a native ColumnBuffer; the returned
	/// `CommittedColumn` is what gets passed to `emit_diff`.
	pub fn commit(mut self, written_count: usize) -> Result<CommittedColumn, FFIError> {
		let code = unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.commit)(self.handle, written_count)
		};
		self.committed = true;
		if code != 0 {
			return Err(FFIError::Other(format!("ColumnBuilder::commit failed: {}", code)));
		}
		Ok(CommittedColumn {
			handle: self.handle,
			row_count: written_count,
		})
	}

	pub fn type_code(&self) -> ColumnTypeCode {
		self.type_code
	}

	pub fn write_bool(self, values: &[bool]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Bool, "write_bool requires a Bool ColumnBuilder");
		let byte_count = values.len().div_ceil(8);
		let mut packed = vec![0u8; byte_count];
		for (i, &b) in values.iter().enumerate() {
			if b {
				packed[i / 8] |= 1 << (i % 8);
			}
		}
		if byte_count > 0 {
			unsafe {
				core::ptr::copy_nonoverlapping(packed.as_ptr(), self.data_ptr(), byte_count);
			}
		}
		self.commit(values.len())
	}

	pub fn write_f32(self, values: &[f32]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Float4);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_f64(self, values: &[f64]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Float8);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i8(self, values: &[i8]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Int1);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i16(self, values: &[i16]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Int2);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i32(self, values: &[i32]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Int4);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i64(self, values: &[i64]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Int8);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i128(self, values: &[i128]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Int16);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u8(self, values: &[u8]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Uint1);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u16(self, values: &[u16]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Uint2);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u32(self, values: &[u32]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Uint4);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u64(self, values: &[u64]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Uint8);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u128(self, values: &[u128]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Uint16);
		unsafe { write_scalar(self, values) }
	}

	pub fn write_utf8<S: AsRef<str>>(self, values: &[S]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Utf8, "write_utf8 requires a Utf8 ColumnBuilder");
		write_var_len(self, values.iter().map(|s| s.as_ref().as_bytes()))
	}

	pub fn write_blob<B: AsRef<[u8]>>(self, values: &[B]) -> Result<CommittedColumn, FFIError> {
		debug_assert_eq!(self.type_code, ColumnTypeCode::Blob, "write_blob requires a Blob ColumnBuilder");
		write_var_len(self, values.iter().map(|b| b.as_ref()))
	}
}

unsafe fn write_scalar<T: Copy>(col: ColumnBuilder<'_>, values: &[T]) -> Result<CommittedColumn, FFIError> {
	let bytes = core::mem::size_of_val(values);
	if bytes > 0 {
		unsafe {
			core::ptr::copy_nonoverlapping(values.as_ptr() as *const u8, col.data_ptr(), bytes);
		}
	}
	col.commit(values.len())
}

fn write_var_len<'b, I>(col: ColumnBuilder<'_>, items: I) -> Result<CommittedColumn, FFIError>
where
	I: IntoIterator<Item = &'b [u8]>,
{
	let items: Vec<&[u8]> = items.into_iter().collect();
	let total: usize = items.iter().map(|b| b.len()).sum();
	let needed = total.max(items.len());
	if needed > 0 {
		col.grow(needed)?;
	}
	let mut cursor = 0usize;
	unsafe {
		let data = col.data_ptr();
		let offsets = col.offsets_ptr();
		core::ptr::write(offsets, 0u64);
		for (i, bytes) in items.iter().enumerate() {
			if !bytes.is_empty() {
				core::ptr::copy_nonoverlapping(bytes.as_ptr(), data.add(cursor), bytes.len());
			}
			cursor += bytes.len();
			core::ptr::write(offsets.add(i + 1), cursor as u64);
		}
	}
	col.commit(items.len())
}

impl<'a> Drop for ColumnBuilder<'a> {
	fn drop(&mut self) {
		if !self.committed {
			unsafe {
				let cb = (*self.ctx).callbacks.builder;
				(cb.release)(self.handle);
			}
		}
	}
}

/// Top-level builder. Acquires column builders from the host pool and emits
/// diffs via the host's accumulator.
pub struct ColumnsBuilder<'a> {
	ctx: *mut ContextFFI,
	_phantom: core::marker::PhantomData<&'a mut ()>,
}

impl<'a> ColumnsBuilder<'a> {
	/// Create a builder bound to the given operator context. Lives only
	/// for the duration of the current vtable call.
	pub fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx: ctx.ctx,
			_phantom: core::marker::PhantomData,
		}
	}

	/// Create a builder from a raw `*mut ContextFFI`. Used by transform /
	/// procedure contexts that aren't `OperatorContext`. The caller is
	/// responsible for ensuring the pointer outlives `'a`.
	pub fn from_raw_ctx(ctx: *mut ContextFFI) -> Self {
		Self {
			ctx,
			_phantom: core::marker::PhantomData,
		}
	}

	/// Acquire a fresh column builder of the given type with at least
	/// `capacity` elements (bytes for var-len data buffer).
	pub fn acquire(&mut self, type_code: ColumnTypeCode, capacity: usize) -> Result<ColumnBuilder<'_>, FFIError> {
		let handle = unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.acquire)(self.ctx, type_code, capacity)
		};
		if handle.is_null() {
			return Err(FFIError::Other(format!(
				"ColumnsBuilder::acquire failed for type {:?}",
				type_code
			)));
		}
		Ok(ColumnBuilder {
			ctx: self.ctx,
			handle,
			type_code,
			committed: false,
			_phantom: core::marker::PhantomData,
		})
	}

	/// Emit an Insert diff with the given committed columns, names, and row
	/// numbers.
	///
	/// `row_numbers.len()` must equal the row count of the committed
	/// columns. Operators that re-emit the same key on a later batch should
	/// pass the same `RowNumber` so the materialiser upserts the existing
	/// row in place. Stable per-key numbers come from
	/// `OperatorContext::get_or_create_row_numbers`. Stateless operators
	/// can pass any contiguous range; the host treats them as fresh inserts.
	pub fn emit_insert(
		&mut self,
		post: &[CommittedColumn],
		names: &[&str],
		row_numbers: &[RowNumber],
	) -> Result<(), FFIError> {
		assert_eq!(post.len(), names.len(), "emit_insert: post columns and names must have matching length");
		let row_count = post.first().map(|c| c.row_count).unwrap_or(0);
		assert_eq!(row_numbers.len(), row_count, "emit_insert: row_numbers length must equal post row count");
		self.emit_internal(EmitDiffKind::Insert, &[], &[], 0, &[], post, names, row_count, row_numbers)
	}

	/// Emit an Update diff. `pre_row_numbers` and `post_row_numbers` must
	/// match `pre_row_count` and `post_row_count` respectively.
	#[allow(clippy::too_many_arguments)]
	pub fn emit_update(
		&mut self,
		pre: &[CommittedColumn],
		pre_names: &[&str],
		pre_row_count: usize,
		pre_row_numbers: &[RowNumber],
		post: &[CommittedColumn],
		post_names: &[&str],
		post_row_count: usize,
		post_row_numbers: &[RowNumber],
	) -> Result<(), FFIError> {
		assert_eq!(pre.len(), pre_names.len(), "emit_update: pre columns/names mismatch");
		assert_eq!(post.len(), post_names.len(), "emit_update: post columns/names mismatch");
		assert_eq!(pre_row_numbers.len(), pre_row_count, "emit_update: pre_row_numbers length mismatch");
		assert_eq!(post_row_numbers.len(), post_row_count, "emit_update: post_row_numbers length mismatch");
		self.emit_internal(
			EmitDiffKind::Update,
			pre,
			pre_names,
			pre_row_count,
			pre_row_numbers,
			post,
			post_names,
			post_row_count,
			post_row_numbers,
		)
	}

	/// Emit a Remove diff. `row_numbers.len()` must equal the row count of
	/// the committed `pre` columns.
	pub fn emit_remove(
		&mut self,
		pre: &[CommittedColumn],
		names: &[&str],
		row_numbers: &[RowNumber],
	) -> Result<(), FFIError> {
		assert_eq!(pre.len(), names.len(), "emit_remove: pre columns and names must have matching length");
		let row_count = pre.first().map(|c| c.row_count).unwrap_or(0);
		assert_eq!(row_numbers.len(), row_count, "emit_remove: row_numbers length must equal pre row count");
		self.emit_internal(EmitDiffKind::Remove, pre, names, row_count, row_numbers, &[], &[], 0, &[])
	}

	#[allow(clippy::too_many_arguments)]
	fn emit_internal(
		&mut self,
		kind: EmitDiffKind,
		pre: &[CommittedColumn],
		pre_names: &[&str],
		pre_row_count: usize,
		pre_row_numbers: &[RowNumber],
		post: &[CommittedColumn],
		post_names: &[&str],
		post_row_count: usize,
		post_row_numbers: &[RowNumber],
	) -> Result<(), FFIError> {
		let pre_handles: Vec<*mut ColumnBufferHandle> = pre.iter().map(|c| c.handle).collect();
		let pre_name_ptrs: Vec<*const u8> = pre_names.iter().map(|n| n.as_ptr()).collect();
		let pre_name_lens: Vec<usize> = pre_names.iter().map(|n| n.len()).collect();
		let pre_row_nums: Vec<u64> = pre_row_numbers.iter().map(|r| r.0).collect();
		let post_handles: Vec<*mut ColumnBufferHandle> = post.iter().map(|c| c.handle).collect();
		let post_name_ptrs: Vec<*const u8> = post_names.iter().map(|n| n.as_ptr()).collect();
		let post_name_lens: Vec<usize> = post_names.iter().map(|n| n.len()).collect();
		let post_row_nums: Vec<u64> = post_row_numbers.iter().map(|r| r.0).collect();

		let code = unsafe {
			let cb = (*self.ctx).callbacks.builder;
			(cb.emit_diff)(
				self.ctx,
				kind,
				if pre_handles.is_empty() {
					ptr::null()
				} else {
					pre_handles.as_ptr()
				},
				if pre_name_ptrs.is_empty() {
					ptr::null()
				} else {
					pre_name_ptrs.as_ptr()
				},
				if pre_name_lens.is_empty() {
					ptr::null()
				} else {
					pre_name_lens.as_ptr()
				},
				pre_handles.len(),
				pre_row_count,
				if pre_row_nums.is_empty() {
					ptr::null()
				} else {
					pre_row_nums.as_ptr()
				},
				pre_row_nums.len(),
				if post_handles.is_empty() {
					ptr::null()
				} else {
					post_handles.as_ptr()
				},
				if post_name_ptrs.is_empty() {
					ptr::null()
				} else {
					post_name_ptrs.as_ptr()
				},
				if post_name_lens.is_empty() {
					ptr::null()
				} else {
					post_name_lens.as_ptr()
				},
				post_handles.len(),
				post_row_count,
				if post_row_nums.is_empty() {
					ptr::null()
				} else {
					post_row_nums.as_ptr()
				},
				post_row_nums.len(),
			)
		};
		if code != 0 {
			return Err(FFIError::Other(format!("emit_diff failed: {}", code)));
		}
		Ok(())
	}
}
