// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::{slice, str};

use reifydb_codec::tag::ValueKind;
use reifydb_value::{reifydb_assertions, value::diff_type::DiffType};

use crate::{
	common::{
		extern_c::wire::{
			buffer::ExternCBuffer,
			columns::{ExternCColumn, ExternCColumns},
		},
		family::{FamilyValue, cell_width, family_params},
	},
	error::SdkError,
	flow::extern_c::wire::change::{ExternCChange, ExternCDiff, ExternCOrigin},
};

#[derive(Clone, Copy)]
pub struct BorrowedChange<'a> {
	extern_c: &'a ExternCChange,
}

impl<'a> BorrowedChange<'a> {
	/// # Safety
	///
	/// `ptr` must be non-null and point to a valid `ExternCChange` whose backing
	/// buffers remain live for the lifetime `'a`.
	pub unsafe fn from_raw(ptr: *const ExternCChange) -> Self {
		reifydb_assertions! {
			assert!(!ptr.is_null(), "BorrowedChange::from_raw: null pointer");
		}
		Self {
			// SAFETY: the `from_raw` contract above makes `ptr` non-null and a live, initialized
			// `ExternCChange` that outlives `'a`.
			extern_c: unsafe { &*ptr },
		}
	}

	pub fn origin(&self) -> ExternCOrigin {
		self.extern_c.origin
	}

	pub fn version(&self) -> u64 {
		self.extern_c.version
	}

	pub fn changed_at_nanos(&self) -> i64 {
		self.extern_c.changed_at
	}

	pub fn diff_count(&self) -> usize {
		self.extern_c.diff_count
	}

	pub fn diffs(&self) -> impl Iterator<Item = BorrowedDiff<'a>> + 'a {
		let change = *self;
		(0..self.extern_c.diff_count).filter_map(move |i| change.diff_at(i))
	}

	pub(crate) fn diff_at(&self, index: usize) -> Option<BorrowedDiff<'a>> {
		if index >= self.extern_c.diff_count {
			return None;
		}
		// SAFETY: `diffs` is the `diff_count`-element `ExternCDiff` array `marshal_change` wrote and fully
		// initialized; `index < diff_count` keeps the offset inside it.
		let diff: &'a ExternCDiff = unsafe { &*self.extern_c.diffs.add(index) };
		Some(BorrowedDiff {
			extern_c: diff,
		})
	}
}

#[derive(Clone, Copy)]
pub struct BorrowedDiff<'a> {
	extern_c: &'a ExternCDiff,
}

impl<'a> BorrowedDiff<'a> {
	pub fn kind(&self) -> DiffType {
		self.extern_c.diff_type
	}

	pub fn pre(&self) -> BorrowedColumns<'a> {
		BorrowedColumns {
			extern_c: &self.extern_c.pre,
		}
	}

	pub fn post(&self) -> BorrowedColumns<'a> {
		BorrowedColumns {
			extern_c: &self.extern_c.post,
		}
	}
}

#[derive(Clone, Copy)]
pub struct BorrowedColumns<'a> {
	extern_c: &'a ExternCColumns,
}

impl<'a> BorrowedColumns<'a> {
	/// # Safety
	/// - `ptr` must be non-null and point at a `ExternCColumns` whose buffer pointers are valid for at least `'a`.
	pub unsafe fn from_extern_c(ptr: *const ExternCColumns) -> Self {
		reifydb_assertions! {
			assert!(!ptr.is_null(), "BorrowedColumns::from_extern_c: null pointer");
		}
		Self {
			// SAFETY: the `from_extern_c` contract above makes `ptr` non-null and a live, initialized
			// `ExternCColumns` that outlives `'a`.
			extern_c: unsafe { &*ptr },
		}
	}

	pub fn row_count(&self) -> usize {
		self.extern_c.row_count
	}

	pub fn column_count(&self) -> usize {
		self.extern_c.column_count
	}

	pub fn is_empty(&self) -> bool {
		self.extern_c.row_count == 0 && self.extern_c.column_count == 0
	}

	pub fn row_numbers(&self) -> &'a [u64] {
		if self.extern_c.row_numbers.is_null() || self.extern_c.row_count == 0 {
			&[]
		} else {
			// SAFETY: `row_numbers` is non-null here and the host marshal takes it only from the batch's
			// `#rownum` column, which a RecordBatch holds at exactly `row_count` entries; `RowNumber` is
			// `repr(transparent)` over `u64`.
			unsafe { slice::from_raw_parts(self.extern_c.row_numbers, self.extern_c.row_count) }
		}
	}

	pub fn time(&self) -> &'a [i64] {
		if self.extern_c.time.is_null() || self.extern_c.row_count == 0 {
			&[]
		} else {
			// SAFETY: `time` is non-null here and the host marshal takes it only from the batch's `#time`
			// column with no nones, which a RecordBatch holds at exactly `row_count` entries; `DateTime`
			// is `repr(transparent)` over `i64`.
			unsafe { slice::from_raw_parts(self.extern_c.time, self.extern_c.row_count) }
		}
	}

	pub fn columns(&self) -> impl Iterator<Item = BorrowedColumn<'a>> + 'a {
		let count = self.extern_c.column_count;
		let base = self.extern_c.columns;
		(0..count).map(move |i| {
			// SAFETY: `base` is the `column_count`-element `ExternCColumn` array `marshal_columns` wrote
			// and fully initialized; `i < count` keeps the offset inside it.
			let col: &'a ExternCColumn = unsafe { &*base.add(i) };
			BorrowedColumn {
				extern_c: col,
			}
		})
	}

	pub fn column(&self, name: &str) -> Option<BorrowedColumn<'a>> {
		self.columns().find(|c| c.name() == name)
	}

	pub fn index_of(&self, name: &str) -> Option<usize> {
		self.columns().position(|c| c.name() == name)
	}
}

#[derive(Clone, Copy)]
pub struct BorrowedColumn<'a> {
	extern_c: &'a ExternCColumn,
}

impl<'a> BorrowedColumn<'a> {
	pub fn name(&self) -> &'a str {
		// SAFETY: `self.extern_c` came from a `BorrowedChange::from_raw` / `BorrowedColumns::from_extern_c`
		// caller, whose contract keeps every buffer this `ExternCColumn` describes live for `'a`.
		unsafe { read_buffer_str(&self.extern_c.name) }
	}

	pub fn type_code(&self) -> ValueKind {
		self.extern_c.data.type_code
	}

	pub fn precision(&self) -> u8 {
		self.extern_c.data.precision
	}

	pub fn scale(&self) -> u8 {
		self.extern_c.data.scale
	}

	pub fn row_count(&self) -> usize {
		self.extern_c.data.row_count
	}

	pub fn data_bytes(&self) -> &'a [u8] {
		// SAFETY: `self.extern_c` came from a `BorrowedChange::from_raw` / `BorrowedColumns::from_extern_c`
		// caller, whose contract keeps every buffer this `ExternCColumn` describes live for `'a`.
		unsafe { read_buffer(&self.extern_c.data.data) }
	}

	pub fn offsets(&self) -> &'a [u64] {
		let buf = &self.extern_c.data.offsets;
		if buf.ptr.is_null() || buf.len == 0 {
			&[]
		} else {
			let count = buf.len / core::mem::size_of::<u64>();
			// SAFETY: `buf.ptr` is non-null here and every offsets buffer is either a marshalled
			// `&[u64]` or an 8-aligned arena block; `count` floors `buf.len / 8`, so that many
			// initialized `u64` fit.
			unsafe { slice::from_raw_parts(buf.ptr as *const u64, count) }
		}
	}

	pub fn defined_bitvec(&self) -> &'a [u8] {
		// SAFETY: `self.extern_c` came from a `BorrowedChange::from_raw` / `BorrowedColumns::from_extern_c`
		// caller, whose contract keeps every buffer this `ExternCColumn` describes live for `'a`.
		unsafe { read_buffer(&self.extern_c.data.defined_bitvec) }
	}

	/// # Safety
	///
	/// The caller must ensure the column's underlying bytes are a valid,
	/// properly aligned array of `T` for the column's row count.
	pub unsafe fn as_slice<T: Copy>(&self) -> Option<&'a [T]> {
		let bytes = self.data_bytes();
		let count = self.row_count();
		let elem = core::mem::size_of::<T>();
		if elem == 0 || count.checked_mul(elem)? != bytes.len() {
			return None;
		}
		// SAFETY: `count * size_of::<T>() == bytes.len()` was checked above, and the caller's contract
		// makes those bytes an aligned, initialized `T` array.
		Some(unsafe { slice::from_raw_parts(bytes.as_ptr() as *const T, count) })
	}

	pub(crate) fn str_at(&self, index: usize) -> Option<&'a str> {
		self.bytes_at(index).map(|bytes| str::from_utf8(bytes).unwrap_or(""))
	}

	pub(crate) fn bytes_at(&self, index: usize) -> Option<&'a [u8]> {
		(index < self.row_count()).then(|| varlen_cell(self.data_bytes(), self.offsets(), index))
	}

	#[inline]
	pub fn is_defined_at(&self, index: usize) -> bool {
		let bv = self.defined_bitvec();
		if bv.is_empty() {
			return true;
		}
		match bv.get(index / 8) {
			Some(b) => (b >> (index % 8)) & 1 == 1,
			None => false,
		}
	}

	pub(crate) fn family_cell_at<T: FamilyValue>(&self, index: usize) -> Result<Option<T>, SdkError> {
		if self.type_code() != T::KIND {
			return Ok(None);
		}
		let (precision, scale) = family_params(T::KIND, self.precision(), self.scale()).ok_or_else(|| {
			SdkError::InvalidInput(format!(
				"column {} has invalid precision {} and scale {}",
				self.name(),
				self.precision(),
				self.scale()
			))
		})?;
		let width = cell_width(precision);
		let Some(cell) = index
			.checked_mul(width)
			.and_then(|start| self.data_bytes().get(start..start.checked_add(width)?))
		else {
			return Ok(None);
		};
		T::decode_cell(cell, scale).map(Some)
	}
}

fn varlen_cell<'a>(data: &'a [u8], offsets: &[u64], index: usize) -> &'a [u8] {
	if index + 1 >= offsets.len() {
		return &[];
	}
	let start = offsets[index] as usize;
	let end = offsets[index + 1] as usize;
	if end > data.len() {
		return &[];
	}
	&data[start..end]
}

/// # Safety
///
/// `buf` must be a host-produced descriptor: either empty, or `buf.ptr` valid for `buf.len`
/// initialized bytes that stay live as long as `buf` itself is borrowed.
unsafe fn read_buffer(buf: &ExternCBuffer) -> &[u8] {
	if buf.ptr.is_null() || buf.len == 0 {
		&[]
	} else {
		// SAFETY: the branch above rules out a null pointer and a zero length; the caller's contract
		// makes the remaining `buf.len` bytes initialized and live for `'a`.
		unsafe { slice::from_raw_parts(buf.ptr, buf.len) }
	}
}

/// # Safety
///
/// Same contract as [`read_buffer`].
unsafe fn read_buffer_str(buf: &ExternCBuffer) -> &str {
	// SAFETY: forwarding this function's own contract, which is `read_buffer`'s.
	let bytes: &[u8] = unsafe { read_buffer(buf) };
	str::from_utf8(bytes).unwrap_or("")
}
