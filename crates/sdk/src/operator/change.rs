// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Borrowed view types for the zero-copy FFI input ABI.
//!
//! The host hands the guest a `*const ChangeFFI` whose `BufferFFI` fields
//! point directly at native column storage. These wrappers expose that data
//! as safe Rust slices tied to the lifetime of the FFI call frame: the
//! borrow checker prevents the guest from retaining any reference past the
//! return of `apply` / `tick`.
//!
//! Walking the diffs / columns yields `&[T]` / `&str` references the guest
//! can pattern-match on. There is no owned `Change` allocation.

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

/// Borrowed view over an input `ChangeFFI`. Tied to a `'a` lifetime that
/// matches the duration of the FFI call so the guest cannot retain
/// pointers past return.
pub struct BorrowedChange<'a> {
	ffi: &'a ChangeFFI,
}

impl<'a> BorrowedChange<'a> {
	/// Wrap a raw `*const ChangeFFI`. The caller (the SDK's `ffi_apply`
	/// glue) ensures the pointer is non-null and valid for the duration
	/// of the call.
	///
	/// # Safety
	/// - `ptr` must be non-null and point at a `ChangeFFI` whose buffer pointers are valid for at least `'a`.
	pub unsafe fn from_raw(ptr: *const ChangeFFI) -> Self {
		debug_assert!(!ptr.is_null(), "BorrowedChange::from_raw: null pointer");
		Self {
			ffi: unsafe { &*ptr },
		}
	}

	/// Origin discriminant + id (raw FFI form).
	pub fn origin(&self) -> OriginFFI {
		self.ffi.origin
	}

	/// Commit version of the change.
	pub fn version(&self) -> u64 {
		self.ffi.version
	}

	/// Timestamp (nanoseconds since Unix epoch).
	pub fn changed_at_nanos(&self) -> u64 {
		self.ffi.changed_at
	}

	/// Number of diffs in this change.
	pub fn diff_count(&self) -> usize {
		self.ffi.diff_count
	}

	/// Iterate the diffs.
	pub fn diffs(&self) -> impl Iterator<Item = BorrowedDiff<'_>> + '_ {
		let count = self.ffi.diff_count;
		let base = self.ffi.diffs;
		(0..count).map(move |i| {
			// SAFETY: `base` points at an array of `count` DiffFFI
			// for the duration of the FFI call.
			let diff_ffi: &DiffFFI = unsafe { &*base.add(i) };
			BorrowedDiff {
				ffi: diff_ffi,
			}
		})
	}
}

/// Borrowed view over a single `DiffFFI`.
pub struct BorrowedDiff<'a> {
	ffi: &'a DiffFFI,
}

impl<'a> BorrowedDiff<'a> {
	pub fn kind(&self) -> DiffType {
		self.ffi.diff_type
	}

	/// Pre-state columns (`Update` / `Remove` only). For `Insert` this
	/// returns an empty view.
	pub fn pre(&self) -> BorrowedColumns<'_> {
		BorrowedColumns {
			ffi: &self.ffi.pre,
		}
	}

	/// Post-state columns (`Insert` / `Update` only). For `Remove` this
	/// returns an empty view.
	pub fn post(&self) -> BorrowedColumns<'_> {
		BorrowedColumns {
			ffi: &self.ffi.post,
		}
	}
}

/// Borrowed view over a `ColumnsFFI`.
pub struct BorrowedColumns<'a> {
	ffi: &'a ColumnsFFI,
}

impl<'a> BorrowedColumns<'a> {
	pub fn row_count(&self) -> usize {
		self.ffi.row_count
	}

	pub fn column_count(&self) -> usize {
		self.ffi.column_count
	}

	pub fn is_empty(&self) -> bool {
		self.ffi.row_count == 0 && self.ffi.column_count == 0
	}

	/// Borrow the row numbers as `&[u64]`. Empty if the change carries no
	/// row numbers.
	pub fn row_numbers(&self) -> &'a [u64] {
		if self.ffi.row_numbers.is_null() || self.ffi.row_count == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(self.ffi.row_numbers, self.ffi.row_count) }
		}
	}

	/// Borrow `created_at` timestamps (nanoseconds).
	pub fn created_at(&self) -> &'a [u64] {
		if self.ffi.created_at.is_null() || self.ffi.row_count == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(self.ffi.created_at, self.ffi.row_count) }
		}
	}

	/// Borrow `updated_at` timestamps (nanoseconds).
	pub fn updated_at(&self) -> &'a [u64] {
		if self.ffi.updated_at.is_null() || self.ffi.row_count == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(self.ffi.updated_at, self.ffi.row_count) }
		}
	}

	/// Iterate the columns.
	pub fn columns(&self) -> impl Iterator<Item = BorrowedColumn<'_>> + '_ {
		let count = self.ffi.column_count;
		let base = self.ffi.columns;
		(0..count).map(move |i| {
			let col_ffi: &ColumnFFI = unsafe { &*base.add(i) };
			BorrowedColumn {
				ffi: col_ffi,
			}
		})
	}
}

/// Borrowed view over a single `ColumnFFI`.
pub struct BorrowedColumn<'a> {
	ffi: &'a ColumnFFI,
}

impl<'a> BorrowedColumn<'a> {
	/// Borrow the column name as `&str`. Returns `""` if missing.
	pub fn name(&self) -> &'a str {
		read_buffer_str(&self.ffi.name)
	}

	pub fn type_code(&self) -> ColumnTypeCode {
		self.ffi.data.type_code
	}

	pub fn row_count(&self) -> usize {
		self.ffi.data.row_count
	}

	/// Borrow the raw data bytes for this column.
	pub fn data_bytes(&self) -> &'a [u8] {
		read_buffer(&self.ffi.data.data)
	}

	/// Borrow the offsets array (var-len types only). Returns empty for
	/// fixed-size types.
	pub fn offsets(&self) -> &'a [u64] {
		let buf = &self.ffi.data.offsets;
		if buf.ptr.is_null() || buf.len == 0 {
			&[]
		} else {
			let count = buf.len / core::mem::size_of::<u64>();
			unsafe { slice::from_raw_parts(buf.ptr as *const u64, count) }
		}
	}

	/// Borrow the defined-bitvec bytes (LSB-first). Returns empty if the
	/// column is fully defined.
	pub fn defined_bitvec(&self) -> &'a [u8] {
		read_buffer(&self.ffi.data.defined_bitvec)
	}

	/// Typed slice borrow for fixed-size numeric types. Returns `None` if
	/// `T` doesn't match the column's type or the byte length is wrong.
	///
	/// # Safety
	/// - `T` must be a primitive (or `#[repr(transparent)]` over one) that matches the column's `ColumnTypeCode`.
	/// - The host marshal path guarantees that fixed-width column data is laid out as a contiguous `[T]`.
	pub unsafe fn as_slice<T: Copy>(&self) -> Option<&'a [T]> {
		let bytes = self.data_bytes();
		let count = self.row_count();
		let elem = core::mem::size_of::<T>();
		if elem == 0 || count.checked_mul(elem)? != bytes.len() {
			return None;
		}
		Some(unsafe { slice::from_raw_parts(bytes.as_ptr() as *const T, count) })
	}

	/// Iterate UTF-8 elements as `&str` (Utf8 columns only).
	pub fn iter_str(&self) -> impl Iterator<Item = &'a str> + '_ {
		let data = self.data_bytes();
		let offsets = self.offsets();
		let row_count = self.row_count();
		// `offsets` should have length `row_count + 1`. Guard against
		// malformed input by clamping.
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

	/// Iterate Blob elements as `&[u8]` (Blob columns only).
	pub fn iter_bytes(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
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
