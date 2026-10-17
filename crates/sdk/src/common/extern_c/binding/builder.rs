// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::{ffi::c_void, iter, ptr};

use reifydb_codec::tag::ValueKind;
use reifydb_value::{reifydb_assertions, value::row_number::RowNumber};

use crate::{
	common::extern_c::wire::callbacks::builder::{BuilderCallbacks, ColumnBufferHandle, EmitDiffKind},
	error::SdkError,
	flow::operator::context::check_emit_count,
};

pub struct ColumnBuilder<'a> {
	callbacks: BuilderCallbacks,
	handle: *mut ColumnBufferHandle,
	type_code: ValueKind,
	precision: u8,
	scale: u8,
	committed: bool,
	_phantom: core::marker::PhantomData<&'a ()>,
}

#[derive(Clone, Copy)]
pub struct CommittedColumn {
	handle: *mut ColumnBufferHandle,
	row_count: usize,
}

impl CommittedColumn {
	pub(crate) fn passed(handle: *mut ColumnBufferHandle, row_count: usize) -> Self {
		Self {
			handle,
			row_count,
		}
	}

	pub(crate) fn row_count(&self) -> usize {
		self.row_count
	}

	pub(crate) fn handle(&self) -> *mut ColumnBufferHandle {
		self.handle
	}
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

	pub fn precision(&self) -> u8 {
		self.precision
	}

	pub fn scale(&self) -> u8 {
		self.scale
	}

	pub(crate) fn write_bool(self, values: &[bool]) -> Result<CommittedColumn, SdkError> {
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

	pub(crate) fn set_defined(&self, defined: &[bool]) {
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

fn emitted_rows(columns: &[CommittedColumn], row_numbers: &[RowNumber]) -> Result<usize, SdkError> {
	let rows = columns.first().map_or(0, CommittedColumn::row_count);
	check_emit_count(rows, row_numbers.len())?;
	for column in columns {
		check_emit_count(column.row_count, row_numbers.len())?;
	}
	Ok(rows)
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
	written_at_nanos: i64,
	_phantom: core::marker::PhantomData<&'a mut ()>,
}

impl<'a> ColumnsBuilder<'a> {
	pub fn new(ctx: *mut c_void, callbacks: BuilderCallbacks, written_at_nanos: i64) -> Self {
		assert!(!ctx.is_null(), "context pointer must not be null");
		Self {
			ctx,
			callbacks,
			written_at_nanos,
			_phantom: core::marker::PhantomData,
		}
	}

	pub fn acquire(&mut self, type_code: ValueKind, capacity: usize) -> Result<ColumnBuilder<'_>, SdkError> {
		self.acquire_with_params(type_code, 0, 0, capacity)
	}

	pub fn acquire_with_params(
		&mut self,
		type_code: ValueKind,
		precision: u8,
		scale: u8,
		capacity: usize,
	) -> Result<ColumnBuilder<'_>, SdkError> {
		// SAFETY: `self.callbacks` was copied out of the context borrowed for `'a`, so the table is live;
		// `acquire` accepts any type_code, precision, scale and capacity and signals failure by returning
		// null, which is checked below.
		let handle = unsafe { (self.callbacks.acquire)(self.ctx, type_code, precision, scale, capacity) };
		if handle.is_null() {
			return Err(SdkError::Other(format!(
				"ColumnsBuilder::acquire failed for type {:?} with precision {} and scale {}",
				type_code, precision, scale
			)));
		}
		Ok(ColumnBuilder {
			callbacks: self.callbacks,
			handle,
			type_code,
			precision,
			scale,
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
		self.insert_named(post, names.iter().copied(), row_numbers)
	}

	pub(crate) fn insert_named<'n>(
		&mut self,
		post: &[CommittedColumn],
		names: impl ExactSizeIterator<Item = &'n str> + Clone,
		row_numbers: &[RowNumber],
	) -> Result<(), SdkError> {
		assert_eq!(post.len(), names.len(), "emit_insert: post columns and names must have matching length");
		let row_count = emitted_rows(post, row_numbers)?;
		self.emit_internal(
			EmitDiffKind::Insert,
			&[],
			iter::empty(),
			0,
			&[],
			post,
			names,
			row_count,
			row_numbers,
		)
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
		self.update_named(
			pre,
			pre_names.iter().copied(),
			pre_row_count,
			pre_row_numbers,
			post,
			post_names.iter().copied(),
			post_row_count,
			post_row_numbers,
		)
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn update_named<'n>(
		&mut self,
		pre: &[CommittedColumn],
		pre_names: impl ExactSizeIterator<Item = &'n str> + Clone,
		pre_row_count: usize,
		pre_row_numbers: &[RowNumber],
		post: &[CommittedColumn],
		post_names: impl ExactSizeIterator<Item = &'n str> + Clone,
		post_row_count: usize,
		post_row_numbers: &[RowNumber],
	) -> Result<(), SdkError> {
		assert_eq!(pre.len(), pre_names.len(), "emit_update: pre columns/names mismatch");
		assert_eq!(post.len(), post_names.len(), "emit_update: post columns/names mismatch");
		check_emit_count(pre_row_count, pre_row_numbers.len())?;
		check_emit_count(post_row_count, post_row_numbers.len())?;
		for column in pre {
			check_emit_count(column.row_count, pre_row_numbers.len())?;
		}
		for column in post {
			check_emit_count(column.row_count, post_row_numbers.len())?;
		}
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
		self.remove_named(pre, names.iter().copied(), row_numbers)
	}

	pub(crate) fn remove_named<'n>(
		&mut self,
		pre: &[CommittedColumn],
		names: impl ExactSizeIterator<Item = &'n str> + Clone,
		row_numbers: &[RowNumber],
	) -> Result<(), SdkError> {
		assert_eq!(pre.len(), names.len(), "emit_remove: pre columns and names must have matching length");
		let row_count = emitted_rows(pre, row_numbers)?;
		self.emit_internal(EmitDiffKind::Remove, pre, names, row_count, row_numbers, &[], iter::empty(), 0, &[])
	}

	#[allow(clippy::too_many_arguments)]
	fn emit_internal<'p, 'q>(
		&mut self,
		kind: EmitDiffKind,
		pre: &[CommittedColumn],
		pre_names: impl Iterator<Item = &'p str> + Clone,
		pre_row_count: usize,
		pre_row_numbers: &[RowNumber],
		post: &[CommittedColumn],
		post_names: impl Iterator<Item = &'q str> + Clone,
		post_row_count: usize,
		post_row_numbers: &[RowNumber],
	) -> Result<(), SdkError> {
		let pre_handles: Vec<*mut ColumnBufferHandle> = pre.iter().map(|c| c.handle).collect();
		let pre_name_ptrs: Vec<*const u8> = pre_names.clone().map(str::as_ptr).collect();
		let pre_name_lens: Vec<usize> = pre_names.map(str::len).collect();
		let pre_row_nums: Vec<u64> = pre_row_numbers.iter().map(|r| r.0).collect();
		let post_handles: Vec<*mut ColumnBufferHandle> = post.iter().map(|c| c.handle).collect();
		let post_name_ptrs: Vec<*const u8> = post_names.clone().map(str::as_ptr).collect();
		let post_name_lens: Vec<usize> = post_names.map(str::len).collect();
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
