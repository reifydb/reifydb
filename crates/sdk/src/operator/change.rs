// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::{slice, str};

use reifydb_abi::{
	data::{
		buffer::BufferFFI,
		column::{ColumnFFI, ColumnTypeCode, ColumnsFFI},
	},
	flow::{
		change::{ChangeFFI, OriginFFI},
		diff::{DiffFFI, DiffType},
	},
};
use reifydb_type::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

#[derive(Clone, Copy)]
pub struct BorrowedChange<'a> {
	ffi: &'a ChangeFFI,
}

impl<'a> BorrowedChange<'a> {
	/// # Safety
	///
	/// `ptr` must be non-null and point to a valid `ChangeFFI` whose backing
	/// buffers remain live for the lifetime `'a`.
	pub unsafe fn from_raw(ptr: *const ChangeFFI) -> Self {
		debug_assert!(!ptr.is_null(), "BorrowedChange::from_raw: null pointer");
		Self {
			ffi: unsafe { &*ptr },
		}
	}

	pub fn origin(&self) -> OriginFFI {
		self.ffi.origin
	}

	pub fn version(&self) -> u64 {
		self.ffi.version
	}

	pub fn changed_at_nanos(&self) -> u64 {
		self.ffi.changed_at
	}

	pub fn diff_count(&self) -> usize {
		self.ffi.diff_count
	}

	pub fn diffs(&self) -> impl Iterator<Item = BorrowedDiff<'a>> + 'a {
		let count = self.ffi.diff_count;
		let base = self.ffi.diffs;
		(0..count).map(move |i| {
			let diff_ffi: &'a DiffFFI = unsafe { &*base.add(i) };
			BorrowedDiff {
				ffi: diff_ffi,
			}
		})
	}
}

#[derive(Clone, Copy)]
pub struct BorrowedDiff<'a> {
	ffi: &'a DiffFFI,
}

impl<'a> BorrowedDiff<'a> {
	pub fn kind(&self) -> DiffType {
		self.ffi.diff_type
	}

	pub fn pre(&self) -> BorrowedColumns<'a> {
		BorrowedColumns {
			ffi: &self.ffi.pre,
		}
	}

	pub fn post(&self) -> BorrowedColumns<'a> {
		BorrowedColumns {
			ffi: &self.ffi.post,
		}
	}
}

#[derive(Clone, Copy)]
pub struct BorrowedColumns<'a> {
	ffi: &'a ColumnsFFI,
}

impl<'a> BorrowedColumns<'a> {
	/// Wrap a raw `*const ColumnsFFI` for the duration of the FFI call.
	///
	/// # Safety
	/// - `ptr` must be non-null and point at a `ColumnsFFI` whose buffer pointers are valid for at least `'a`.
	pub unsafe fn from_ffi(ptr: *const ColumnsFFI) -> Self {
		debug_assert!(!ptr.is_null(), "BorrowedColumns::from_ffi: null pointer");
		Self {
			ffi: unsafe { &*ptr },
		}
	}

	pub fn row_count(&self) -> usize {
		self.ffi.row_count
	}

	pub fn column_count(&self) -> usize {
		self.ffi.column_count
	}

	pub fn is_empty(&self) -> bool {
		self.ffi.row_count == 0 && self.ffi.column_count == 0
	}

	pub fn row_numbers(&self) -> &'a [u64] {
		if self.ffi.row_numbers.is_null() || self.ffi.row_count == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(self.ffi.row_numbers, self.ffi.row_count) }
		}
	}

	pub fn created_at(&self) -> &'a [u64] {
		if self.ffi.created_at.is_null() || self.ffi.row_count == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(self.ffi.created_at, self.ffi.row_count) }
		}
	}

	pub fn updated_at(&self) -> &'a [u64] {
		if self.ffi.updated_at.is_null() || self.ffi.row_count == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(self.ffi.updated_at, self.ffi.row_count) }
		}
	}

	pub fn columns(&self) -> impl Iterator<Item = BorrowedColumn<'a>> + 'a {
		let count = self.ffi.column_count;
		let base = self.ffi.columns;
		(0..count).map(move |i| {
			let col_ffi: &'a ColumnFFI = unsafe { &*base.add(i) };
			BorrowedColumn {
				ffi: col_ffi,
			}
		})
	}

	pub fn column(&self, name: &str) -> Option<BorrowedColumn<'a>> {
		self.columns().find(|c| c.name() == name)
	}

	pub fn column_at_index(&self, idx: usize) -> Option<BorrowedColumn<'a>> {
		if idx >= self.ffi.column_count {
			return None;
		}
		let col_ffi: &'a ColumnFFI = unsafe { &*self.ffi.columns.add(idx) };
		Some(BorrowedColumn {
			ffi: col_ffi,
		})
	}

	pub fn index_of(&self, name: &str) -> Option<usize> {
		self.columns().position(|c| c.name() == name)
	}
}

#[derive(Clone, Copy)]
pub struct BorrowedColumn<'a> {
	ffi: &'a ColumnFFI,
}

impl<'a> BorrowedColumn<'a> {
	pub fn name(&self) -> &'a str {
		read_buffer_str(&self.ffi.name)
	}

	pub fn type_code(&self) -> ColumnTypeCode {
		self.ffi.data.type_code
	}

	pub fn row_count(&self) -> usize {
		self.ffi.data.row_count
	}

	pub fn data_bytes(&self) -> &'a [u8] {
		read_buffer(&self.ffi.data.data)
	}

	pub fn offsets(&self) -> &'a [u64] {
		let buf = &self.ffi.data.offsets;
		if buf.ptr.is_null() || buf.len == 0 {
			&[]
		} else {
			let count = buf.len / core::mem::size_of::<u64>();
			unsafe { slice::from_raw_parts(buf.ptr as *const u64, count) }
		}
	}

	pub fn defined_bitvec(&self) -> &'a [u8] {
		read_buffer(&self.ffi.data.defined_bitvec)
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
		Some(unsafe { slice::from_raw_parts(bytes.as_ptr() as *const T, count) })
	}

	pub fn iter_str(&self) -> impl Iterator<Item = &'a str> + 'a {
		let data = self.data_bytes();
		let offsets = self.offsets();
		let row_count = self.row_count();
		let offsets_len = offsets.len();
		(0..row_count).map(move |i| {
			if i + 1 >= offsets_len {
				return "";
			}
			let start = offsets[i] as usize;
			let end = offsets[i + 1] as usize;
			if end > data.len() {
				return "";
			}
			str::from_utf8(&data[start..end]).unwrap_or("")
		})
	}

	pub fn iter_bytes(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
		let data = self.data_bytes();
		let offsets = self.offsets();
		let row_count = self.row_count();
		let offsets_len = offsets.len();
		(0..row_count).map(move |i| {
			if i + 1 >= offsets_len {
				return &[][..];
			}
			let start = offsets[i] as usize;
			let end = offsets[i + 1] as usize;
			if end > data.len() {
				return &[][..];
			}
			&data[start..end]
		})
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

	#[inline]
	pub fn utf8_at(&self, index: usize) -> Option<&'a str> {
		if self.type_code() != ColumnTypeCode::Utf8 || !self.is_defined_at(index) {
			return None;
		}
		let offsets = self.offsets();
		if index + 1 >= offsets.len() {
			return None;
		}
		let start = offsets[index] as usize;
		let end = offsets[index + 1] as usize;
		let data = self.data_bytes();
		if end > data.len() || start > end {
			return None;
		}
		str::from_utf8(&data[start..end]).ok()
	}

	#[inline]
	pub fn blob_at(&self, index: usize) -> Option<&'a [u8]> {
		if self.type_code() != ColumnTypeCode::Blob || !self.is_defined_at(index) {
			return None;
		}
		let offsets = self.offsets();
		if index + 1 >= offsets.len() {
			return None;
		}
		let start = offsets[index] as usize;
		let end = offsets[index + 1] as usize;
		let data = self.data_bytes();
		if end > data.len() || start > end {
			return None;
		}
		Some(&data[start..end])
	}

	#[inline]
	pub fn bool_at(&self, index: usize) -> Option<bool> {
		if self.type_code() != ColumnTypeCode::Bool || !self.is_defined_at(index) {
			return None;
		}
		let bytes = self.data_bytes();
		let byte = bytes.get(index / 8).copied()?;
		Some((byte >> (index % 8)) & 1 == 1)
	}

	#[inline]
	pub fn u64_at(&self, index: usize) -> Option<u64> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Uint8 => unsafe { self.as_slice::<u64>()?.get(index).copied() },
			ColumnTypeCode::Uint4 => unsafe { self.as_slice::<u32>()?.get(index).copied().map(u64::from) },
			ColumnTypeCode::Uint2 => unsafe { self.as_slice::<u16>()?.get(index).copied().map(u64::from) },
			ColumnTypeCode::Uint1 => unsafe { self.as_slice::<u8>()?.get(index).copied().map(u64::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn u32_at(&self, index: usize) -> Option<u32> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Uint4 => unsafe { self.as_slice::<u32>()?.get(index).copied() },
			ColumnTypeCode::Uint2 => unsafe { self.as_slice::<u16>()?.get(index).copied().map(u32::from) },
			ColumnTypeCode::Uint1 => unsafe { self.as_slice::<u8>()?.get(index).copied().map(u32::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn u16_at(&self, index: usize) -> Option<u16> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Uint2 => unsafe { self.as_slice::<u16>()?.get(index).copied() },
			ColumnTypeCode::Uint1 => unsafe { self.as_slice::<u8>()?.get(index).copied().map(u16::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn u8_at(&self, index: usize) -> Option<u8> {
		if self.type_code() != ColumnTypeCode::Uint1 || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<u8>()?.get(index).copied() }
	}

	#[inline]
	pub fn i64_at(&self, index: usize) -> Option<i64> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Int8 => unsafe { self.as_slice::<i64>()?.get(index).copied() },
			ColumnTypeCode::Int4 => unsafe { self.as_slice::<i32>()?.get(index).copied().map(i64::from) },
			ColumnTypeCode::Int2 => unsafe { self.as_slice::<i16>()?.get(index).copied().map(i64::from) },
			ColumnTypeCode::Int1 => unsafe { self.as_slice::<i8>()?.get(index).copied().map(i64::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn i32_at(&self, index: usize) -> Option<i32> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Int4 => unsafe { self.as_slice::<i32>()?.get(index).copied() },
			ColumnTypeCode::Int2 => unsafe { self.as_slice::<i16>()?.get(index).copied().map(i32::from) },
			ColumnTypeCode::Int1 => unsafe { self.as_slice::<i8>()?.get(index).copied().map(i32::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn i16_at(&self, index: usize) -> Option<i16> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Int2 => unsafe { self.as_slice::<i16>()?.get(index).copied() },
			ColumnTypeCode::Int1 => unsafe { self.as_slice::<i8>()?.get(index).copied().map(i16::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn i8_at(&self, index: usize) -> Option<i8> {
		if self.type_code() != ColumnTypeCode::Int1 || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<i8>()?.get(index).copied() }
	}

	#[inline]
	pub fn u128_at(&self, index: usize) -> Option<u128> {
		if self.type_code() != ColumnTypeCode::Uint16 || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<u128>()?.get(index).copied() }
	}

	#[inline]
	pub fn i128_at(&self, index: usize) -> Option<i128> {
		if self.type_code() != ColumnTypeCode::Int16 || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<i128>()?.get(index).copied() }
	}

	#[inline]
	pub fn f64_at(&self, index: usize) -> Option<f64> {
		if !self.is_defined_at(index) {
			return None;
		}
		match self.type_code() {
			ColumnTypeCode::Float8 => unsafe { self.as_slice::<f64>()?.get(index).copied() },
			ColumnTypeCode::Float4 => unsafe { self.as_slice::<f32>()?.get(index).copied().map(f64::from) },
			_ => None,
		}
	}

	#[inline]
	pub fn f32_at(&self, index: usize) -> Option<f32> {
		if self.type_code() != ColumnTypeCode::Float4 || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<f32>()?.get(index).copied() }
	}

	#[inline]
	pub fn date_at(&self, index: usize) -> Option<Date> {
		if self.type_code() != ColumnTypeCode::Date || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<Date>()?.get(index).copied() }
	}

	#[inline]
	pub fn datetime_at(&self, index: usize) -> Option<DateTime> {
		if self.type_code() != ColumnTypeCode::DateTime || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<DateTime>()?.get(index).copied() }
	}

	#[inline]
	pub fn time_at(&self, index: usize) -> Option<Time> {
		if self.type_code() != ColumnTypeCode::Time || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<Time>()?.get(index).copied() }
	}

	#[inline]
	pub fn duration_at(&self, index: usize) -> Option<Duration> {
		if self.type_code() != ColumnTypeCode::Duration || !self.is_defined_at(index) {
			return None;
		}
		unsafe { self.as_slice::<Duration>()?.get(index).copied() }
	}
}

fn read_buffer<'a>(buf: &BufferFFI) -> &'a [u8] {
	if buf.ptr.is_null() || buf.len == 0 {
		&[]
	} else {
		unsafe { slice::from_raw_parts(buf.ptr, buf.len) }
	}
}

fn read_buffer_str<'a>(buf: &BufferFFI) -> &'a str {
	let bytes: &'a [u8] = read_buffer(buf);
	str::from_utf8(bytes).unwrap_or("")
}

pub type DiffKind = reifydb_abi::flow::diff::DiffType;
