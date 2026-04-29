// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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

#[derive(Clone, Copy)]
pub struct BorrowedChange<'a> {
	ffi: &'a ChangeFFI,
}

impl<'a> BorrowedChange<'a> {
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
