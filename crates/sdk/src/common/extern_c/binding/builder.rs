// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::{ffi::c_void, ptr};

use reifydb_codec::tag::ValueKind;
use reifydb_value::{reifydb_assertions, value::row_number::RowNumber};

use crate::{
	common::extern_c::wire::callbacks::builder::{BuilderCallbacks, ColumnBufferHandle, EmitDiffKind},
	error::SdkError,
};

pub struct ColumnBuilder<'a> {
	callbacks: BuilderCallbacks,
	handle: *mut ColumnBufferHandle,
	type_code: ValueKind,
	committed: bool,
	_phantom: core::marker::PhantomData<&'a ()>,
}

#[derive(Clone, Copy)]
pub struct CommittedColumn {
	handle: *mut ColumnBufferHandle,
	row_count: usize,
}

impl<'a> ColumnBuilder<'a> {
	pub fn data_ptr(&self) -> *mut u8 {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `self.handle` is the handle `acquire` returned and is released only once, in `Drop`.
		unsafe { (self.callbacks.data_ptr)(self.handle) }
	}

	pub fn offsets_ptr(&self) -> *mut u64 {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `self.handle` is the handle `acquire` returned and is released only once, in `Drop`.
		unsafe { (self.callbacks.offsets_ptr)(self.handle) }
	}

	pub fn bitvec_ptr(&self) -> *mut u8 {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `self.handle` is the handle `acquire` returned and is released only once, in `Drop`.
		unsafe { (self.callbacks.bitvec_ptr)(self.handle) }
	}

	pub fn grow(&self, additional: usize) -> Result<(), SdkError> {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `self.handle` is the handle `acquire` returned and is released only once, in `Drop`.
		let code = unsafe { (self.callbacks.grow)(self.handle, additional) };
		if code != 0 {
			return Err(SdkError::Other(format!("ColumnBuilder::grow failed: {}", code)));
		}
		Ok(())
	}

	pub fn commit(mut self, written_count: usize) -> Result<CommittedColumn, SdkError> {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `self.handle` is the handle `acquire` returned, still uncommitted because `commit` consumes the
		// builder.
		let code = unsafe { (self.callbacks.commit)(self.handle, written_count) };
		self.committed = true;
		if code != 0 {
			return Err(SdkError::Other(format!("ColumnBuilder::commit failed: {}", code)));
		}
		Ok(CommittedColumn {
			handle: self.handle,
			row_count: written_count,
		})
	}

	pub fn type_code(&self) -> ValueKind {
		self.type_code
	}

	pub fn write_bool(self, values: &[bool]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Boolean, "write_bool requires a Bool ColumnBuilder");
		}

		let byte_count = values.len().div_ceil(8);
		let buffer_byte_len = values.len();
		let mut packed = vec![0u8; buffer_byte_len.max(byte_count)];
		for (i, &b) in values.iter().enumerate() {
			if b {
				packed[i / 8] |= 1 << (i % 8);
			}
		}
		if !packed.is_empty() {
			// SAFETY: `data_ptr` is the base of this builder's host buffer and `packed` is a
			// separate live allocation; a Bool element is one byte, so the copy stays inside the
			// buffer as long as the caller acquired capacity for at least `values.len()` elements.
			unsafe {
				core::ptr::copy_nonoverlapping(packed.as_ptr(), self.data_ptr(), packed.len());
			}
		}
		self.commit(values.len())
	}

	pub fn write_f32(self, values: &[f32]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Float4);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_f64(self, values: &[f64]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Float8);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i8(self, values: &[i8]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Int1);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i16(self, values: &[i16]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Int2);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i32(self, values: &[i32]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Int4);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i64(self, values: &[i64]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Int8);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_i128(self, values: &[i128]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Int16);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u8(self, values: &[u8]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Uint1);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u16(self, values: &[u16]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Uint2);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u32(self, values: &[u32]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Uint4);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u64(self, values: &[u64]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Uint8);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_u128(self, values: &[u128]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Uint16);
		}
		// SAFETY: discharges `write_scalar` - the slice element type is the in-memory encoding of the
		// ValueKind this method requires, and the caller must have acquired the builder with
		// capacity for at least `values.len()` elements.
		unsafe { write_scalar(self, values) }
	}

	pub fn write_utf8<S: AsRef<str>>(self, values: &[S]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Utf8, "write_utf8 requires a Utf8 ColumnBuilder");
		}
		write_var_len(self, values.iter().map(|s| s.as_ref().as_bytes()))
	}

	pub fn write_blob<B: AsRef<[u8]>>(self, values: &[B]) -> Result<CommittedColumn, SdkError> {
		reifydb_assertions! {
			assert_eq!(self.type_code, ValueKind::Blob, "write_blob requires a Blob ColumnBuilder");
		}
		write_var_len(self, values.iter().map(|b| b.as_ref()))
	}

	pub fn set_defined(&self, defined: &[bool]) {
		let bytes = defined.len().div_ceil(8);
		if bytes == 0 {
			return;
		}
		let mut packed = vec![0u8; bytes];
		for (i, &b) in defined.iter().enumerate() {
			if b {
				packed[i / 8] |= 1 << (i % 8);
			}
		}
		// SAFETY: `bitvec_ptr` returns a host bitvec sized from the acquired element capacity, so the
		// `defined.len().div_ceil(8)` bytes copied out of the separate `packed` allocation fit as long
		// as the caller acquired capacity for at least `defined.len()` elements.
		unsafe {
			core::ptr::copy_nonoverlapping(packed.as_ptr(), self.bitvec_ptr(), bytes);
		}
	}
}

/// # Safety
///
/// `col` must have been acquired with a capacity of at least
/// `size_of_val(values)` bytes, since this writes without growing, and `T` must
/// be the Rust type whose in-memory representation is the element encoding of
/// `col.type_code`.
unsafe fn write_scalar<T: Copy>(col: ColumnBuilder<'_>, values: &[T]) -> Result<CommittedColumn, SdkError> {
	let bytes = core::mem::size_of_val(values);
	if bytes > 0 {
		// SAFETY: this fn's contract puts a capacity of at least `bytes` on the caller; `data_ptr` is
		// the base of that host buffer and `values` is a distinct live slice, and both sides are
		// copied as untyped bytes so neither needs alignment for `T`.
		unsafe {
			core::ptr::copy_nonoverlapping(values.as_ptr() as *const u8, col.data_ptr(), bytes);
		}
	}
	col.commit(values.len())
}

fn write_var_len<'b, I>(col: ColumnBuilder<'_>, items: I) -> Result<CommittedColumn, SdkError>
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
	// SAFETY: `col` is a var-len builder, so `offsets_ptr` is non-null and starts out with room for
	// one entry; `grow(needed)` above reserved `total` further data bytes and `items.len()` further
	// offset slots, so every write through `data` and `offsets` here is inside those reservations.
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
			// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is
			// live; the `committed` check makes this the only release of `self.handle`.
			unsafe {
				(self.callbacks.release)(self.handle);
			}
		}
	}
}

pub struct ColumnsBuilder<'a> {
	ctx: *mut c_void,
	callbacks: BuilderCallbacks,
	written_at_nanos: u64,
	_phantom: core::marker::PhantomData<&'a mut ()>,
}

impl<'a> ColumnsBuilder<'a> {
	pub fn new(ctx: *mut c_void, callbacks: BuilderCallbacks, written_at_nanos: u64) -> Self {
		assert!(!ctx.is_null(), "context pointer must not be null");
		Self {
			ctx,
			callbacks,
			written_at_nanos,
			_phantom: core::marker::PhantomData,
		}
	}

	pub fn acquire(&mut self, type_code: ValueKind, capacity: usize) -> Result<ColumnBuilder<'_>, SdkError> {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `acquire` accepts any type_code and capacity and signals failure by returning null, which is
		// checked below.
		let handle = unsafe { (self.callbacks.acquire)(self.ctx, type_code, capacity) };
		if handle.is_null() {
			return Err(SdkError::Other(format!(
				"ColumnsBuilder::acquire failed for type {:?}",
				type_code
			)));
		}
		Ok(ColumnBuilder {
			callbacks: self.callbacks,
			handle,
			type_code,
			committed: false,
			_phantom: core::marker::PhantomData,
		})
	}

	pub fn emit_insert(
		&mut self,
		post: &[CommittedColumn],
		names: &[&str],
		row_numbers: &[RowNumber],
	) -> Result<(), SdkError> {
		assert_eq!(post.len(), names.len(), "emit_insert: post columns and names must have matching length");
		let row_count = post.first().map(|c| c.row_count).unwrap_or(0);
		assert_eq!(row_numbers.len(), row_count, "emit_insert: row_numbers length must equal post row count");
		self.emit_internal(EmitDiffKind::Insert, &[], &[], 0, &[], post, names, row_count, row_numbers)
	}

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
	) -> Result<(), SdkError> {
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

	pub fn emit_remove(
		&mut self,
		pre: &[CommittedColumn],
		names: &[&str],
		row_numbers: &[RowNumber],
	) -> Result<(), SdkError> {
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
	) -> Result<(), SdkError> {
		let pre_handles: Vec<*mut ColumnBufferHandle> = pre.iter().map(|c| c.handle).collect();
		let pre_name_ptrs: Vec<*const u8> = pre_names.iter().map(|n| n.as_ptr()).collect();
		let pre_name_lens: Vec<usize> = pre_names.iter().map(|n| n.len()).collect();
		let pre_row_nums: Vec<u64> = pre_row_numbers.iter().map(|r| r.0).collect();
		let post_handles: Vec<*mut ColumnBufferHandle> = post.iter().map(|c| c.handle).collect();
		let post_name_ptrs: Vec<*const u8> = post_names.iter().map(|n| n.as_ptr()).collect();
		let post_name_lens: Vec<usize> = post_names.iter().map(|n| n.len()).collect();
		let post_row_nums: Vec<u64> = post_row_numbers.iter().map(|r| r.0).collect();

		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live; every
		// array argument is either the base of one of the local `Vec`s above, passed with that `Vec`'s own
		// length, or null when the `Vec` is empty.
		let code = unsafe {
			(self.callbacks.emit_diff)(
				self.written_at_nanos,
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
			return Err(SdkError::Other(format!("emit_diff failed: {}", code)));
		}
		Ok(())
	}
}
