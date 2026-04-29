// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, mem::size_of, ptr, slice, str};

use postcard::to_allocvec;
use reifydb_abi::data::{
	buffer::BufferFFI,
	column::{ColumnDataFFI, ColumnFFI, ColumnTypeCode, ColumnsFFI},
};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use serde::Serialize;

use super::util::column_data_to_type_code;
use crate::ffi::arena::Arena;

impl Arena {
	/// Marshal `Columns` into the FFI ABI as a **zero-copy borrow** of the
	/// caller's native storage.
	///
	/// `RowNumber`, `DateTime`, and the per-type column buffers are all laid
	/// out as flat primitive arrays (numerics as `[T]`, temporals as
	/// `[u64]` / `[i32]` via `#[repr(transparent)]`, etc). The marshal path
	/// hands the guest raw pointers into that storage with `cap == 0` as
	/// the borrow sentinel; the only arena allocation is the small
	/// scaffolding for the `ColumnFFI` array.
	///
	/// Borrow contract: every `BufferFFI` produced here is valid for the
	/// duration of the FFI call only. The caller (`FFIOperator::apply` /
	/// `pull` / `tick`) keeps the source `Columns` alive across the call,
	/// so the pointers remain valid until return.
	pub fn marshal_columns(&mut self, columns: &Columns) -> ColumnsFFI {
		let row_count = columns.row_count();
		let column_count = columns.len();

		if row_count == 0 && column_count == 0 {
			return ColumnsFFI::empty();
		}

		// Borrow row_numbers directly: `RowNumber` is `#[repr(transparent)]`
		// over `u64`, so `&[RowNumber]` and `&[u64]` share layout. The
		// FFI ABI receives the raw pointer; cap == 0 signals borrow.
		let row_numbers_ptr = if !columns.row_numbers.is_empty() {
			columns.row_numbers.as_slice().as_ptr() as *const u64
		} else {
			ptr::null()
		};

		// Same for created_at / updated_at: `DateTime` is
		// `#[repr(transparent)]` over `u64` (nanos since epoch).
		let created_at_ptr = if !columns.created_at.is_empty() {
			columns.created_at.as_slice().as_ptr() as *const u64
		} else {
			ptr::null()
		};
		let updated_at_ptr = if !columns.updated_at.is_empty() {
			columns.updated_at.as_slice().as_ptr() as *const u64
		} else {
			ptr::null()
		};

		// The only thing that still needs arena scaffolding: the
		// per-column `ColumnFFI` array. Each entry's `data` and `name`
		// are themselves borrows (cap == 0).
		let columns_size = column_count * size_of::<ColumnFFI>();
		let columns_ptr = self.alloc(columns_size) as *mut ColumnFFI;

		if !columns_ptr.is_null() {
			unsafe {
				for (i, col) in columns.iter().enumerate() {
					let col_ffi = self.marshal_column_ref(col.name(), col.data());
					*columns_ptr.add(i) = col_ffi;
				}
			}
		}

		ColumnsFFI {
			row_count,
			column_count,
			row_numbers: row_numbers_ptr,
			columns: columns_ptr as *const ColumnFFI,
			created_at: created_at_ptr,
			updated_at: updated_at_ptr,
		}
	}

	pub fn unmarshal_columns(&self, ffi: &ColumnsFFI) -> Columns {
		if ffi.is_empty() || ffi.columns.is_null() {
			return Columns::empty();
		}

		// Unmarshal row numbers
		let row_numbers: Vec<RowNumber> = if !ffi.row_numbers.is_null() && ffi.row_count > 0 {
			unsafe {
				let slice = slice::from_raw_parts(ffi.row_numbers, ffi.row_count);
				slice.iter().map(|&n| RowNumber(n)).collect()
			}
		} else {
			Vec::new()
		};

		// Unmarshal created_at timestamps
		let created_at: Vec<DateTime> = if !ffi.created_at.is_null() && ffi.row_count > 0 {
			unsafe {
				let slice = slice::from_raw_parts(ffi.created_at, ffi.row_count);
				slice.iter().map(|&n| DateTime::from_nanos(n)).collect()
			}
		} else {
			Vec::new()
		};

		// Unmarshal updated_at timestamps
		let updated_at: Vec<DateTime> = if !ffi.updated_at.is_null() && ffi.row_count > 0 {
			unsafe {
				let slice = slice::from_raw_parts(ffi.updated_at, ffi.row_count);
				slice.iter().map(|&n| DateTime::from_nanos(n)).collect()
			}
		} else {
			Vec::new()
		};

		// Unmarshal columns
		let mut columns: Vec<ColumnWithName> = Vec::with_capacity(ffi.column_count);
		unsafe {
			let cols_slice = slice::from_raw_parts(ffi.columns, ffi.column_count);
			for col_ffi in cols_slice {
				columns.push(self.unmarshal_column(col_ffi, ffi.row_count));
			}
		}

		if row_numbers.is_empty() {
			Columns::new(columns)
		} else {
			Columns::with_system_columns(columns, row_numbers, created_at, updated_at)
		}
	}
}

impl Arena {
	pub(super) fn marshal_column_ref(&mut self, name: &Fragment, data: &ColumnBuffer) -> ColumnFFI {
		// Borrow the name's UTF-8 bytes directly. The `Fragment` outlives
		// the FFI call (it's owned by the source `Columns`), so the
		// borrow is valid for the call duration. cap == 0 marks borrow.
		let name_bytes = name.text().as_bytes();
		let name_buf = BufferFFI {
			ptr: name_bytes.as_ptr(),
			len: name_bytes.len(),
			cap: 0,
		};

		let data = self.marshal_column_data(data);

		ColumnFFI {
			name: name_buf,
			data,
		}
	}

	/// Marshal ColumnBuffer to FFI representation
	pub(super) fn marshal_column_data(&mut self, data: &ColumnBuffer) -> ColumnDataFFI {
		let row_count = data.len();

		if row_count == 0 {
			return ColumnDataFFI {
				type_code: column_data_to_type_code(data),
				row_count: 0,
				data: BufferFFI::empty(),
				defined_bitvec: BufferFFI::empty(),
				offsets: BufferFFI::empty(),
			};
		}

		// Unwrap Option to get inner data + optional bitvec
		let (inner_data, bitvec) = data.unwrap_option();
		let type_code = column_data_to_type_code(inner_data);

		// Marshal bitvec if present; empty means "all defined"
		let defined_bitvec = match bitvec {
			Some(bv) => self.marshal_bitvec(bv, row_count),
			None => BufferFFI::empty(),
		};

		// Marshal data and offsets based on inner type
		let (data_buffer, offsets_buffer) = self.marshal_column_data_bytes(inner_data);

		ColumnDataFFI {
			type_code,
			row_count,
			data: data_buffer,
			defined_bitvec,
			offsets: offsets_buffer,
		}
	}

	pub(super) fn unmarshal_column(&self, ffi: &ColumnFFI, row_count: usize) -> ColumnWithName {
		// Unmarshal name
		let name = if !ffi.name.ptr.is_null() && ffi.name.len > 0 {
			unsafe {
				let bytes = slice::from_raw_parts(ffi.name.ptr, ffi.name.len);
				let s = str::from_utf8(bytes).unwrap_or("");
				Fragment::internal(s)
			}
		} else {
			Fragment::internal("")
		};

		// Unmarshal data
		let data = self.unmarshal_column_data(&ffi.data, row_count);

		ColumnWithName::new(name, data)
	}

	pub(super) fn unmarshal_column_data(&self, ffi: &ColumnDataFFI, row_count: usize) -> ColumnBuffer {
		if row_count == 0 {
			return ColumnBuffer::none_typed(Type::Boolean, 0);
		}

		let inner = match ffi.type_code {
			ColumnTypeCode::Bool => {
				let container = self.unmarshal_bool_data(ffi);
				ColumnBuffer::Bool(container)
			}
			ColumnTypeCode::Float4 => {
				let container = self.unmarshal_numeric_data::<f32>(ffi);
				ColumnBuffer::Float4(container)
			}
			ColumnTypeCode::Float8 => {
				let container = self.unmarshal_numeric_data::<f64>(ffi);
				ColumnBuffer::Float8(container)
			}
			ColumnTypeCode::Int1 => {
				let container = self.unmarshal_numeric_data::<i8>(ffi);
				ColumnBuffer::Int1(container)
			}
			ColumnTypeCode::Int2 => {
				let container = self.unmarshal_numeric_data::<i16>(ffi);
				ColumnBuffer::Int2(container)
			}
			ColumnTypeCode::Int4 => {
				let container = self.unmarshal_numeric_data::<i32>(ffi);
				ColumnBuffer::Int4(container)
			}
			ColumnTypeCode::Int8 => {
				let container = self.unmarshal_numeric_data::<i64>(ffi);
				ColumnBuffer::Int8(container)
			}
			ColumnTypeCode::Int16 => {
				let container = self.unmarshal_numeric_data::<i128>(ffi);
				ColumnBuffer::Int16(container)
			}
			ColumnTypeCode::Uint1 => {
				let container = self.unmarshal_numeric_data::<u8>(ffi);
				ColumnBuffer::Uint1(container)
			}
			ColumnTypeCode::Uint2 => {
				let container = self.unmarshal_numeric_data::<u16>(ffi);
				ColumnBuffer::Uint2(container)
			}
			ColumnTypeCode::Uint4 => {
				let container = self.unmarshal_numeric_data::<u32>(ffi);
				ColumnBuffer::Uint4(container)
			}
			ColumnTypeCode::Uint8 => {
				let container = self.unmarshal_numeric_data::<u64>(ffi);
				ColumnBuffer::Uint8(container)
			}
			ColumnTypeCode::Uint16 => {
				let container = self.unmarshal_numeric_data::<u128>(ffi);
				ColumnBuffer::Uint16(container)
			}
			ColumnTypeCode::Utf8 => {
				let container = self.unmarshal_utf8_data(ffi);
				ColumnBuffer::Utf8 {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Date => {
				let container = self.unmarshal_date_data(ffi);
				ColumnBuffer::Date(container)
			}
			ColumnTypeCode::DateTime => {
				let container = self.unmarshal_datetime_data(ffi);
				ColumnBuffer::DateTime(container)
			}
			ColumnTypeCode::Time => {
				let container = self.unmarshal_time_data(ffi);
				ColumnBuffer::Time(container)
			}
			ColumnTypeCode::Duration => {
				let container = self.unmarshal_duration_data(ffi);
				ColumnBuffer::Duration(container)
			}
			ColumnTypeCode::IdentityId => {
				let container = self.unmarshal_identity_id_data(ffi);
				ColumnBuffer::IdentityId(container)
			}
			ColumnTypeCode::Uuid4 => {
				let container = self.unmarshal_uuid4_data(ffi);
				ColumnBuffer::Uuid4(container)
			}
			ColumnTypeCode::Uuid7 => {
				let container = self.unmarshal_uuid7_data(ffi);
				ColumnBuffer::Uuid7(container)
			}
			ColumnTypeCode::Blob => {
				let container = self.unmarshal_blob_data(ffi);
				ColumnBuffer::Blob {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Int => {
				let container = self.unmarshal_serialized_data::<Int>(ffi);
				ColumnBuffer::Int {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Uint => {
				let container = self.unmarshal_serialized_data::<Uint>(ffi);
				ColumnBuffer::Uint {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Decimal => {
				let container = self.unmarshal_serialized_data::<Decimal>(ffi);
				ColumnBuffer::Decimal {
					container,
					precision: Precision::MAX,
					scale: Scale::MIN,
				}
			}
			ColumnTypeCode::Any => {
				let container = self.unmarshal_any_data(ffi);
				ColumnBuffer::Any(container)
			}
			ColumnTypeCode::DictionaryId => {
				let container = self.unmarshal_dictionary_id_data(ffi);
				ColumnBuffer::DictionaryId(container)
			}
			ColumnTypeCode::Undefined => ColumnBuffer::none_typed(Type::Boolean, row_count),
		};

		// If defined_bitvec is present, wrap in Option
		if !ffi.defined_bitvec.is_empty() {
			let bitvec = self.unmarshal_bitvec(&ffi.defined_bitvec, row_count);
			ColumnBuffer::Option {
				inner: Box::new(inner),
				bitvec,
			}
		} else {
			inner
		}
	}
}

impl Arena {
	/// Convert a `ColumnBuffer` into the FFI wire layout, **borrowing** the
	/// native storage where possible (`cap == 0` sentinel).
	///
	/// The workspace's column types are laid out so most of them can be
	/// borrowed without transformation:
	///
	/// - Numerics (`Float{4,8}`, `Int{1..16}`, `Uint{1..16}`): `NumberContainer` wraps `Vec<T>`; the FFI `data`
	///   borrow is `as_ptr() + byte_len`.
	/// - Bool: `BoolContainer` wraps a `BitVec` whose `bits: Vec<u8>` is already in the LSB-first packed format the
	///   FFI expects.
	/// - Date/DateTime/Time: each is `#[repr(transparent)]` over its encoded primitive (`i32` or `u64`), so
	///   `Vec<T>` is layout-compatible with `[i32]`/`[u64]`.
	/// - Duration: `#[repr(C)]` over `(i32, i32, i64)` = 16 bytes/element.
	/// - IdentityId / Uuid4 / Uuid7: `#[repr(transparent)]` newtypes over 16-byte UUIDs; storage is `Vec<T>` packed
	///   contiguously.
	/// - Utf8 / Blob: storage is `(Vec<u8>, Vec<u64>)` (data + offsets), the exact FFI wire format. Both buffers
	///   borrow with `cap == 0`.
	/// - DictionaryId: `[u128]`-compatible borrow.
	/// - Int/Uint/Decimal/Any: still serialized via postcard (out of scope for the borrow path; tracked as future
	///   work).
	pub(super) fn marshal_column_data_bytes(&mut self, data: &ColumnBuffer) -> (BufferFFI, BufferFFI) {
		match data {
			ColumnBuffer::Bool(container) => {
				// `BitVec.bits: Vec<u8>` is already the packed wire format
				// (LSB-first within each byte). Borrow it directly.
				let bytes = container.data().as_packed_bytes();
				(
					BufferFFI {
						ptr: bytes.as_ptr(),
						len: bytes.len(),
						cap: 0,
					},
					BufferFFI::empty(),
				)
			}

			// Numeric types - direct slice borrow.
			ColumnBuffer::Float4(container) => self.marshal_numeric_slice::<f32>(container),
			ColumnBuffer::Float8(container) => self.marshal_numeric_slice::<f64>(container),
			ColumnBuffer::Int1(container) => self.marshal_numeric_slice::<i8>(container),
			ColumnBuffer::Int2(container) => self.marshal_numeric_slice::<i16>(container),
			ColumnBuffer::Int4(container) => self.marshal_numeric_slice::<i32>(container),
			ColumnBuffer::Int8(container) => self.marshal_numeric_slice::<i64>(container),
			ColumnBuffer::Int16(container) => self.marshal_numeric_slice::<i128>(container),
			ColumnBuffer::Uint1(container) => self.marshal_numeric_slice::<u8>(container),
			ColumnBuffer::Uint2(container) => self.marshal_numeric_slice::<u16>(container),
			ColumnBuffer::Uint4(container) => self.marshal_numeric_slice::<u32>(container),
			ColumnBuffer::Uint8(container) => self.marshal_numeric_slice::<u64>(container),
			ColumnBuffer::Uint16(container) => self.marshal_numeric_slice::<u128>(container),

			// Temporal types - `#[repr(transparent)]` makes `Vec<T>` layout-
			// compatible with `[i32]` / `[u64]` so we borrow directly. No
			// per-element transformation needed.
			ColumnBuffer::Date(container) => {
				let dates: &[Date] = container;
				self.marshal_numeric_slice::<Date>(dates)
			}
			ColumnBuffer::DateTime(container) => {
				let datetimes: &[DateTime] = container;
				self.marshal_numeric_slice::<DateTime>(datetimes)
			}
			ColumnBuffer::Time(container) => {
				let times: &[Time] = container;
				self.marshal_numeric_slice::<Time>(times)
			}
			ColumnBuffer::Duration(container) => {
				// `Duration` is `#[repr(C)]` (i32 months, i32 days, i64 nanos)
				// = 16 bytes/element. Borrow the slice as raw bytes.
				let durations: &[Duration] = container;
				self.marshal_numeric_slice::<Duration>(durations)
			}

			// UUID-style types - `#[repr(transparent)]` newtypes over 16-byte
			// UUIDs. Borrow the `Vec<T>` directly as 16-bytes-per-element.
			ColumnBuffer::IdentityId(container) => {
				let ids: &[IdentityId] = container;
				self.marshal_numeric_slice::<IdentityId>(ids)
			}
			ColumnBuffer::Uuid4(container) => {
				let uuids: &[Uuid4] = container;
				self.marshal_numeric_slice::<Uuid4>(uuids)
			}
			ColumnBuffer::Uuid7(container) => {
				let uuids: &[Uuid7] = container;
				self.marshal_numeric_slice::<Uuid7>(uuids)
			}

			// Variable-length types - native storage matches the wire format
			// exactly, so borrow `data_bytes()` + `offsets()` directly.
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let data_bytes = container.data_bytes();
				let offsets = container.offsets();
				let offsets_byte_len = mem::size_of_val(offsets);
				(
					BufferFFI {
						ptr: data_bytes.as_ptr(),
						len: data_bytes.len(),
						cap: 0,
					},
					BufferFFI {
						ptr: offsets.as_ptr() as *const u8,
						len: offsets_byte_len,
						cap: 0,
					},
				)
			}
			ColumnBuffer::Blob {
				container,
				..
			} => {
				let data_bytes = container.data_bytes();
				let offsets = container.offsets();
				let offsets_byte_len = mem::size_of_val(offsets);
				(
					BufferFFI {
						ptr: data_bytes.as_ptr(),
						len: data_bytes.len(),
						cap: 0,
					},
					BufferFFI {
						ptr: offsets.as_ptr() as *const u8,
						len: offsets_byte_len,
						cap: 0,
					},
				)
			}

			// Complex types - still serialized via postcard. Out of scope for
			// the borrow path because their native storage (BigInt heap
			// allocations etc.) doesn't lend itself to a flat payload.
			ColumnBuffer::Int {
				container,
				..
			} => {
				let values: &[Int] = container;
				self.marshal_serialized(values)
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				let values: &[Uint] = container;
				self.marshal_serialized(values)
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				let values: &[Decimal] = container;
				self.marshal_serialized(values)
			}
			ColumnBuffer::Any(container) => {
				let mut offsets: Vec<u64> = Vec::with_capacity(container.len() + 1);
				let mut data_bytes: Vec<u8> = Vec::new();
				offsets.push(0);
				for i in 0..container.len() {
					// Serialize as `&Value` (not `Option<&Value>`) to stay
					// symmetric with `unmarshal_any_data`, which decodes
					// plain `Value`.
					let serialized = match container.get(i) {
						Some(v) => to_allocvec(v).unwrap_or_default(),
						None => to_allocvec(&Value::none()).unwrap_or_default(),
					};
					data_bytes.extend_from_slice(&serialized);
					offsets.push(data_bytes.len() as u64);
				}
				self.marshal_with_offsets(&data_bytes, &offsets)
			}

			// DictionaryId - postcard-per-element so the U1/U2/U4/U8/U16
			// enum variant survives the round trip. Output side mirror in
			// `host_builder` and `test_registry` `finalize_buffer`.
			ColumnBuffer::DictionaryId(container) => {
				let values: Vec<&DictionaryEntryId> = container.data().iter().collect();
				self.marshal_serialized(&values)
			}

			ColumnBuffer::Option {
				inner,
				..
			} => self.marshal_column_data_bytes(inner),
		}
	}

	/// Marshal a numeric slice as a **borrow** of the native column buffer.
	///
	/// `NumberContainer<T>` stores its data as a contiguous `Vec<T>`; its
	/// pointer is stable for the lifetime of the source `Columns`, which the
	/// caller keeps alive throughout the FFI call. We hand the guest a
	/// `BufferFFI` aimed directly at that storage, with `cap = 0` as the
	/// borrow sentinel: the guest must read it during the call only and not
	/// retain or free it. The arena no longer copies these bytes.
	pub(super) fn marshal_numeric_slice<T: Copy>(&mut self, slice: &[T]) -> (BufferFFI, BufferFFI) {
		let byte_len = mem::size_of_val(slice);
		if byte_len == 0 {
			return (BufferFFI::empty(), BufferFFI::empty());
		}

		(
			BufferFFI {
				ptr: slice.as_ptr() as *const u8,
				len: byte_len,
				cap: 0,
			},
			BufferFFI::empty(),
		)
	}

	/// Marshal serialized values with offsets
	pub(super) fn marshal_serialized<T: Serialize>(&mut self, values: &[T]) -> (BufferFFI, BufferFFI) {
		let mut offsets: Vec<u64> = Vec::with_capacity(values.len() + 1);
		let mut data: Vec<u8> = Vec::new();

		offsets.push(0);
		for value in values {
			let serialized = to_allocvec(value).unwrap_or_default();
			data.extend_from_slice(&serialized);
			offsets.push(data.len() as u64);
		}

		self.marshal_with_offsets(&data, &offsets)
	}

	/// Helper: marshal data and offsets to arena
	pub(super) fn marshal_with_offsets(&mut self, data: &[u8], offsets: &[u64]) -> (BufferFFI, BufferFFI) {
		let data_ptr = self.copy_bytes(data);
		let offsets_byte_len = mem::size_of_val(offsets);
		let offsets_ptr = self.alloc(offsets_byte_len) as *mut u64;
		if !offsets_ptr.is_null() {
			unsafe {
				ptr::copy_nonoverlapping(offsets.as_ptr(), offsets_ptr, offsets.len());
			}
		}

		(
			BufferFFI {
				ptr: data_ptr,
				len: data.len(),
				cap: data.len(),
			},
			BufferFFI {
				ptr: offsets_ptr as *const u8,
				len: offsets_byte_len,
				cap: offsets_byte_len,
			},
		)
	}

	/// Marshal a BitVec (definedness bitmap) to FFI
	pub(super) fn marshal_bitvec(&mut self, bitvec: &BitVec, len: usize) -> BufferFFI {
		let byte_count = len.div_ceil(8);
		let ptr = self.alloc(byte_count);
		if !ptr.is_null() {
			unsafe {
				ptr::write_bytes(ptr, 0, byte_count);
			}
			for i in 0..len {
				if bitvec.get(i) {
					unsafe {
						*ptr.add(i / 8) |= 1 << (i % 8);
					}
				}
			}
		}
		BufferFFI {
			ptr,
			len: byte_count,
			cap: byte_count,
		}
	}

	/// Unmarshal a BitVec (definedness bitmap) from FFI
	pub(super) fn unmarshal_bitvec(&self, ffi: &BufferFFI, row_count: usize) -> BitVec {
		if ffi.is_empty() {
			return BitVec::empty();
		}
		unsafe {
			let bytes = slice::from_raw_parts(ffi.ptr, ffi.len);
			BitVec::from_raw(bytes.to_vec(), row_count)
		}
	}
}
