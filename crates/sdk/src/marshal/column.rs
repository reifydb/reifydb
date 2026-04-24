// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Column marshalling and unmarshalling

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
		blob::Blob,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::dictionary::DictionaryContainer,
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
	/// Marshal Columns to FFI representation
	pub fn marshal_columns(&mut self, columns: &Columns) -> ColumnsFFI {
		let row_count = columns.row_count();
		let column_count = columns.len();

		if row_count == 0 && column_count == 0 {
			return ColumnsFFI::empty();
		}

		// Marshal row numbers
		let row_numbers_ptr = if !columns.row_numbers.is_empty() {
			let size = columns.row_numbers.len() * size_of::<u64>();
			let ptr = self.alloc(size) as *mut u64;
			if !ptr.is_null() {
				unsafe {
					for (i, rn) in columns.row_numbers.iter().enumerate() {
						*ptr.add(i) = (*rn).into();
					}
				}
			}
			ptr as *const u64
		} else {
			ptr::null()
		};

		// Marshal created_at timestamps
		let created_at_ptr = {
			let size = columns.created_at.len() * size_of::<u64>();
			let ptr = self.alloc(size) as *mut u64;
			if !ptr.is_null() {
				unsafe {
					for (i, dt) in columns.created_at.iter().enumerate() {
						*ptr.add(i) = dt.to_nanos();
					}
				}
			}
			ptr as *const u64
		};

		// Marshal updated_at timestamps
		let updated_at_ptr = {
			let size = columns.updated_at.len() * size_of::<u64>();
			let ptr = self.alloc(size) as *mut u64;
			if !ptr.is_null() {
				unsafe {
					for (i, dt) in columns.updated_at.iter().enumerate() {
						*ptr.add(i) = dt.to_nanos();
					}
				}
			}
			ptr as *const u64
		};

		// Marshal each column
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

	/// Unmarshal Columns from FFI representation
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
		let name_bytes = name.text().as_bytes();
		let name_ptr = self.copy_bytes(name_bytes);
		let name_buf = BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
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

	/// Unmarshal ColumnBuffer from FFI representation
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
				let u128_container = self.unmarshal_numeric_data::<u128>(ffi);
				let entries: Vec<DictionaryEntryId> =
					u128_container.data().iter().map(|&v| DictionaryEntryId::U16(v)).collect();
				ColumnBuffer::DictionaryId(DictionaryContainer::new(entries))
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
	pub(super) fn marshal_column_data_bytes(&mut self, data: &ColumnBuffer) -> (BufferFFI, BufferFFI) {
		match data {
			// Fixed-size numeric types - use Deref to get slice
			ColumnBuffer::Bool(container) => {
				// BoolContainer stores packed bits internally
				let len = container.len();
				let byte_count = len.div_ceil(8);
				let ptr = self.alloc(byte_count);
				if !ptr.is_null() {
					unsafe {
						ptr::write_bytes(ptr, 0, byte_count);
					}
					for i in 0..len {
						if let Some(val) = container.get(i)
							&& val
						{
							unsafe {
								*ptr.add(i / 8) |= 1 << (i % 8);
							}
						}
					}
				}
				(
					BufferFFI {
						ptr,
						len: byte_count,
						cap: byte_count,
					},
					BufferFFI::empty(),
				)
			}

			// Numeric types - use Deref to [T]
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

			// Temporal types - extract encoded values
			ColumnBuffer::Date(container) => {
				let dates: &[Date] = container;
				let encoded: Vec<i32> = dates.iter().map(|d| d.to_days_since_epoch()).collect();
				self.marshal_numeric_slice(&encoded)
			}
			ColumnBuffer::DateTime(container) => {
				let datetimes: &[DateTime] = container;
				let encoded: Vec<i64> = datetimes.iter().map(|dt| dt.timestamp()).collect();
				self.marshal_numeric_slice(&encoded)
			}
			ColumnBuffer::Time(container) => {
				let times: &[Time] = container;
				let encoded: Vec<u64> = times.iter().map(|t| t.to_nanos_since_midnight()).collect();
				self.marshal_numeric_slice(&encoded)
			}
			ColumnBuffer::Duration(container) => {
				// Duration has 3 fields (months, days, nanos), serialize with postcard
				let durations: &[Duration] = container;
				self.marshal_serialized(durations)
			}

			// UUID types - 16 bytes each
			ColumnBuffer::IdentityId(container) => {
				let ids: &[IdentityId] = container;
				let bytes: Vec<u8> =
					ids.iter().flat_map(|id| id.0.as_bytes().iter().copied()).collect();
				let ptr = self.copy_bytes(&bytes);
				(
					BufferFFI {
						ptr,
						len: bytes.len(),
						cap: bytes.len(),
					},
					BufferFFI::empty(),
				)
			}
			ColumnBuffer::Uuid4(container) => {
				let uuids: &[Uuid4] = container;
				let bytes: Vec<u8> =
					uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
				let ptr = self.copy_bytes(&bytes);
				(
					BufferFFI {
						ptr,
						len: bytes.len(),
						cap: bytes.len(),
					},
					BufferFFI::empty(),
				)
			}
			ColumnBuffer::Uuid7(container) => {
				let uuids: &[Uuid7] = container;
				let bytes: Vec<u8> =
					uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
				let ptr = self.copy_bytes(&bytes);
				(
					BufferFFI {
						ptr,
						len: bytes.len(),
						cap: bytes.len(),
					},
					BufferFFI::empty(),
				)
			}

			// Variable-length types with offsets
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let strings: &[String] = container;
				self.marshal_strings(strings)
			}
			ColumnBuffer::Blob {
				container,
				..
			} => {
				// Blob is a newtype around Vec<u8>, get bytes from each
				let blobs: &[Blob] = container;
				self.marshal_blob_slices(blobs)
			}

			// Complex types - serialize with postcard
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
					let value = container.get(i);
					let serialized = to_allocvec(&value).unwrap_or_default();
					data_bytes.extend_from_slice(&serialized);
					offsets.push(data_bytes.len() as u64);
				}
				self.marshal_with_offsets(&data_bytes, &offsets)
			}

			// DictionaryId - serialize as u128 values
			ColumnBuffer::DictionaryId(container) => {
				let encoded: Vec<u128> = container.data().iter().map(|id| id.to_u128()).collect();
				self.marshal_numeric_slice(&encoded)
			}

			ColumnBuffer::Option {
				inner,
				..
			} => self.marshal_column_data_bytes(inner),
		}
	}

	/// Marshal a numeric slice to raw bytes
	pub(super) fn marshal_numeric_slice<T: Copy>(&mut self, slice: &[T]) -> (BufferFFI, BufferFFI) {
		let byte_len = mem::size_of_val(slice);
		if byte_len == 0 {
			return (BufferFFI::empty(), BufferFFI::empty());
		}

		let ptr = self.alloc(byte_len);
		if !ptr.is_null() {
			unsafe {
				ptr::copy_nonoverlapping(slice.as_ptr() as *const u8, ptr, byte_len);
			}
		}
		(
			BufferFFI {
				ptr,
				len: byte_len,
				cap: byte_len,
			},
			BufferFFI::empty(),
		)
	}

	/// Marshal strings with offsets (Arrow-style)
	pub(super) fn marshal_strings(&mut self, strings: &[String]) -> (BufferFFI, BufferFFI) {
		let mut offsets: Vec<u64> = Vec::with_capacity(strings.len() + 1);
		let mut data: Vec<u8> = Vec::new();

		offsets.push(0);
		for s in strings {
			data.extend_from_slice(s.as_bytes());
			offsets.push(data.len() as u64);
		}

		self.marshal_with_offsets(&data, &offsets)
	}

	/// Marshal Blob slices with offsets (Arrow-style)
	pub(super) fn marshal_blob_slices(&mut self, blobs: &[Blob]) -> (BufferFFI, BufferFFI) {
		let mut offsets: Vec<u64> = Vec::with_capacity(blobs.len() + 1);
		let mut data: Vec<u8> = Vec::new();

		offsets.push(0);
		for blob in blobs {
			data.extend_from_slice(blob.as_bytes());
			offsets.push(data.len() as u64);
		}

		self.marshal_with_offsets(&data, &offsets)
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
