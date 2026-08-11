// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, mem::size_of, ptr, slice, str};

use reifydb_abi::data::{
	buffer::ExternCBuffer,
	column::{ColumnTypeCode, ExternCColumn, ExternCColumnData, ExternCColumns},
};
use reifydb_codec::extern_c::cells::{
	encode_any_cell, encode_decimal_cell, encode_dictionary_id_cell, encode_int_cell, encode_uint_cell,
};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		row_number::RowNumber,
		system_columns::SystemColumns,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};
use tracing::instrument;

use super::util::column_data_to_type_code;
use crate::extern_c::arena::Arena;

impl Arena {
	#[instrument(name = "flow::marshal::columns", level = "trace", skip_all, fields(row_count = columns.row_count(), column_count = columns.len()))]
	pub fn marshal_columns(&mut self, columns: &Columns) -> ExternCColumns {
		let row_count = columns.row_count();
		let column_count = columns.len();

		if row_count == 0 && column_count == 0 {
			return ExternCColumns::empty();
		}

		let row_numbers_ptr = if !columns.row_numbers().is_empty() {
			columns.row_numbers().as_ptr() as *const u64
		} else {
			ptr::null()
		};

		let time_ptr = if !columns.time().is_empty() {
			columns.time().as_ptr() as *const u64
		} else {
			ptr::null()
		};

		let columns_size = column_count * size_of::<ExternCColumn>();
		let columns_ptr = self.alloc(columns_size) as *mut ExternCColumn;

		if !columns_ptr.is_null() {
			// SAFETY: `columns_ptr` is non-null here and the arena reserved
			// `column_count * size_of::<ExternCColumn>()` bytes at alignment 8, so every `add(i)` with
			// `i < column_count` is in bounds; ExternCColumn is Copy, so the stores drop nothing.
			unsafe {
				for (i, col) in columns.iter().enumerate() {
					let marshalled = self.marshal_column_ref(col.name(), col.data());
					*columns_ptr.add(i) = marshalled;
				}
			}
		}

		ExternCColumns {
			row_count,
			column_count,
			row_numbers: row_numbers_ptr,
			columns: columns_ptr as *const ExternCColumn,
			time: time_ptr,
		}
	}

	pub fn unmarshal_columns(&self, extern_c: &ExternCColumns) -> Columns {
		if extern_c.is_empty() || extern_c.columns.is_null() {
			return Columns::empty();
		}

		let row_numbers: Vec<RowNumber> = if !extern_c.row_numbers.is_null() && extern_c.row_count > 0 {
			// SAFETY: guarded non-null with `row_count > 0`; `marshal_columns` sets this field from a
			// live `&[RowNumber]` (repr(transparent) u64), so it is aligned and covers `row_count`
			// initialised elements.
			unsafe {
				let slice = slice::from_raw_parts(extern_c.row_numbers, extern_c.row_count);
				slice.iter().map(|&n| RowNumber(n)).collect()
			}
		} else {
			Vec::new()
		};

		let time: Vec<DateTime> = if !extern_c.time.is_null() && extern_c.row_count > 0 {
			// SAFETY: guarded non-null with `row_count > 0`; `marshal_columns` sets this field from a
			// live `&[DateTime]` (repr(transparent) u64), so it is aligned and covers `row_count`
			// initialised elements.
			unsafe {
				let slice = slice::from_raw_parts(extern_c.time, extern_c.row_count);
				slice.iter().map(|&n| DateTime::from_nanos(n)).collect()
			}
		} else {
			Vec::new()
		};

		let mut columns: Vec<ColumnWithName> = Vec::with_capacity(extern_c.column_count);
		// SAFETY: `extern_c.columns` was checked non-null above; `marshal_columns` points it at an 8-aligned
		// arena array of exactly `column_count` initialised `ExternCColumn`.
		unsafe {
			let cols_slice = slice::from_raw_parts(extern_c.columns, extern_c.column_count);
			for col in cols_slice {
				columns.push(self.unmarshal_column(col, extern_c.row_count));
			}
		}

		if row_numbers.is_empty() {
			Columns::new(columns)
		} else {
			Columns::with_system(
				columns,
				SystemColumns::new(row_numbers, Vec::new(), Vec::new(), Vec::new(), time),
			)
		}
	}
}

impl Arena {
	#[instrument(name = "flow::marshal::column", level = "trace", skip_all, fields(name = name.text()))]
	pub(super) fn marshal_column_ref(&mut self, name: &Fragment, data: &ColumnBuffer) -> ExternCColumn {
		let name_bytes = name.text().as_bytes();
		let name_buf = ExternCBuffer {
			ptr: name_bytes.as_ptr(),
			len: name_bytes.len(),
			cap: 0,
		};

		let data = self.marshal_column_data(data);

		ExternCColumn {
			name: name_buf,
			data,
		}
	}

	pub(super) fn marshal_column_data(&mut self, data: &ColumnBuffer) -> ExternCColumnData {
		let row_count = data.len();

		if row_count == 0 {
			return ExternCColumnData {
				type_code: column_data_to_type_code(data),
				row_count: 0,
				data: ExternCBuffer::empty(),
				defined_bitvec: ExternCBuffer::empty(),
				offsets: ExternCBuffer::empty(),
			};
		}

		let (inner_data, bitvec) = data.unwrap_option();
		let type_code = column_data_to_type_code(inner_data);

		let defined_bitvec = match bitvec {
			Some(bv) => self.marshal_bitvec(bv, row_count),
			None => ExternCBuffer::empty(),
		};

		let (data_buffer, offsets_buffer) = self.marshal_column_data_bytes(inner_data);

		ExternCColumnData {
			type_code,
			row_count,
			data: data_buffer,
			defined_bitvec,
			offsets: offsets_buffer,
		}
	}

	pub(super) fn unmarshal_column(&self, extern_c: &ExternCColumn, row_count: usize) -> ColumnWithName {
		let name = if !extern_c.name.ptr.is_null() && extern_c.name.len > 0 {
			// SAFETY: the branch above rules out a null or zero-length name buffer; the producer owns
			// those `name.len` initialised bytes for the duration of the call.
			unsafe {
				let bytes = slice::from_raw_parts(extern_c.name.ptr, extern_c.name.len);
				let s = str::from_utf8(bytes).unwrap_or("");
				Fragment::internal(s)
			}
		} else {
			Fragment::internal("")
		};

		let data = self.unmarshal_column_data(&extern_c.data, row_count);

		ColumnWithName::new(name, data)
	}

	pub(super) fn unmarshal_column_data(&self, extern_c: &ExternCColumnData, row_count: usize) -> ColumnBuffer {
		if row_count == 0 {
			return ColumnBuffer::none_typed(ValueType::Boolean, 0);
		}

		let inner = match extern_c.type_code {
			ColumnTypeCode::Bool => {
				let container = self.unmarshal_bool_data(extern_c);
				ColumnBuffer::Bool(container)
			}
			ColumnTypeCode::Float4 => {
				let container = self.unmarshal_numeric_data::<f32>(extern_c);
				ColumnBuffer::Float4(container)
			}
			ColumnTypeCode::Float8 => {
				let container = self.unmarshal_numeric_data::<f64>(extern_c);
				ColumnBuffer::Float8(container)
			}
			ColumnTypeCode::Int1 => {
				let container = self.unmarshal_numeric_data::<i8>(extern_c);
				ColumnBuffer::Int1(container)
			}
			ColumnTypeCode::Int2 => {
				let container = self.unmarshal_numeric_data::<i16>(extern_c);
				ColumnBuffer::Int2(container)
			}
			ColumnTypeCode::Int4 => {
				let container = self.unmarshal_numeric_data::<i32>(extern_c);
				ColumnBuffer::Int4(container)
			}
			ColumnTypeCode::Int8 => {
				let container = self.unmarshal_numeric_data::<i64>(extern_c);
				ColumnBuffer::Int8(container)
			}
			ColumnTypeCode::Int16 => {
				let container = self.unmarshal_numeric_data::<i128>(extern_c);
				ColumnBuffer::Int16(container)
			}
			ColumnTypeCode::Uint1 => {
				let container = self.unmarshal_numeric_data::<u8>(extern_c);
				ColumnBuffer::Uint1(container)
			}
			ColumnTypeCode::Uint2 => {
				let container = self.unmarshal_numeric_data::<u16>(extern_c);
				ColumnBuffer::Uint2(container)
			}
			ColumnTypeCode::Uint4 => {
				let container = self.unmarshal_numeric_data::<u32>(extern_c);
				ColumnBuffer::Uint4(container)
			}
			ColumnTypeCode::Uint8 => {
				let container = self.unmarshal_numeric_data::<u64>(extern_c);
				ColumnBuffer::Uint8(container)
			}
			ColumnTypeCode::Uint16 => {
				let container = self.unmarshal_numeric_data::<u128>(extern_c);
				ColumnBuffer::Uint16(container)
			}
			ColumnTypeCode::Utf8 => {
				let container = self.unmarshal_utf8_data(extern_c);
				ColumnBuffer::Utf8 {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Date => {
				let container = self.unmarshal_date_data(extern_c);
				ColumnBuffer::Date(container)
			}
			ColumnTypeCode::DateTime => {
				let container = self.unmarshal_datetime_data(extern_c);
				ColumnBuffer::DateTime(container)
			}
			ColumnTypeCode::Time => {
				let container = self.unmarshal_time_data(extern_c);
				ColumnBuffer::Time(container)
			}
			ColumnTypeCode::Duration => {
				let container = self.unmarshal_duration_data(extern_c);
				ColumnBuffer::Duration(container)
			}
			ColumnTypeCode::IdentityId => {
				let container = self.unmarshal_identity_id_data(extern_c);
				ColumnBuffer::IdentityId(container)
			}
			ColumnTypeCode::Uuid4 => {
				let container = self.unmarshal_uuid4_data(extern_c);
				ColumnBuffer::Uuid4(container)
			}
			ColumnTypeCode::Uuid7 => {
				let container = self.unmarshal_uuid7_data(extern_c);
				ColumnBuffer::Uuid7(container)
			}
			ColumnTypeCode::Blob => {
				let container = self.unmarshal_blob_data(extern_c);
				ColumnBuffer::Blob {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Int => {
				let container = self.unmarshal_int_data(extern_c);
				ColumnBuffer::Int {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Uint => {
				let container = self.unmarshal_uint_data(extern_c);
				ColumnBuffer::Uint {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Decimal => {
				let container = self.unmarshal_decimal_data(extern_c);
				ColumnBuffer::Decimal {
					container,
					precision: Precision::MAX,
					scale: Scale::MIN,
				}
			}
			ColumnTypeCode::Any => {
				let container = self.unmarshal_any_data(extern_c);
				ColumnBuffer::Any(container)
			}
			ColumnTypeCode::DictionaryId => {
				let container = self.unmarshal_dictionary_id_data(extern_c);
				ColumnBuffer::DictionaryId(container)
			}
			ColumnTypeCode::Undefined => ColumnBuffer::none_typed(ValueType::Boolean, row_count),
		};

		if !extern_c.defined_bitvec.is_empty() {
			let bitvec = self.unmarshal_bitvec(&extern_c.defined_bitvec, row_count);
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
	pub(super) fn marshal_column_data_bytes(&mut self, data: &ColumnBuffer) -> (ExternCBuffer, ExternCBuffer) {
		match data {
			ColumnBuffer::Option {
				inner,
				..
			} => self.marshal_column_data_bytes(inner),
			ColumnBuffer::Int {
				..
			}
			| ColumnBuffer::Uint {
				..
			}
			| ColumnBuffer::Decimal {
				..
			}
			| ColumnBuffer::Any(_)
			| ColumnBuffer::DictionaryId(_) => self.marshal_column_data_serialize(data),
			_ => self.marshal_column_data_zerocopy(data),
		}
	}

	#[instrument(name = "flow::marshal::data::zerocopy", level = "trace", skip_all, fields(type_code = ?column_data_to_type_code(data), row_count = data.len()))]
	#[inline]
	pub(super) fn marshal_column_data_zerocopy(&mut self, data: &ColumnBuffer) -> (ExternCBuffer, ExternCBuffer) {
		match data {
			ColumnBuffer::Bool(container) => {
				let bytes = container.data().as_packed_bytes();
				(
					ExternCBuffer {
						ptr: bytes.as_ptr(),
						len: bytes.len(),
						cap: 0,
					},
					ExternCBuffer::empty(),
				)
			}

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
				let durations: &[Duration] = container;
				self.marshal_numeric_slice::<Duration>(durations)
			}

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

			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let data_bytes = container.data_bytes();
				let offsets = container.offsets();
				let offsets_byte_len = mem::size_of_val(offsets);
				(
					ExternCBuffer {
						ptr: data_bytes.as_ptr(),
						len: data_bytes.len(),
						cap: 0,
					},
					ExternCBuffer {
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
					ExternCBuffer {
						ptr: data_bytes.as_ptr(),
						len: data_bytes.len(),
						cap: 0,
					},
					ExternCBuffer {
						ptr: offsets.as_ptr() as *const u8,
						len: offsets_byte_len,
						cap: 0,
					},
				)
			}

			_ => unreachable!("marshal_column_data_zerocopy received non-zerocopy column type"),
		}
	}

	#[instrument(name = "flow::marshal::data::serialize", level = "trace", skip_all, fields(type_code = ?column_data_to_type_code(data), row_count = data.len()))]
	#[inline]
	pub(super) fn marshal_column_data_serialize(&mut self, data: &ColumnBuffer) -> (ExternCBuffer, ExternCBuffer) {
		match data {
			ColumnBuffer::Int {
				container,
				..
			} => {
				let values: &[Int] = container;
				self.marshal_encoded_cells(values.len(), |i, buf| encode_int_cell(&values[i], buf))
			}
			ColumnBuffer::Uint {
				container,
				..
			} => {
				let values: &[Uint] = container;
				self.marshal_encoded_cells(values.len(), |i, buf| encode_uint_cell(&values[i], buf))
			}
			ColumnBuffer::Decimal {
				container,
				..
			} => {
				let values: &[Decimal] = container;
				self.marshal_encoded_cells(values.len(), |i, buf| encode_decimal_cell(&values[i], buf))
			}
			ColumnBuffer::Any(container) => self.marshal_encoded_cells(container.len(), |i, buf| {
				let none = Value::none();
				let value = container.get(i).unwrap_or(&none);
				encode_any_cell(value, buf).expect("unsupported value in any column cell");
			}),

			ColumnBuffer::DictionaryId(container) => {
				let values = container.data();
				self.marshal_encoded_cells(values.len(), |i, buf| {
					encode_dictionary_id_cell(&values[i], buf)
				})
			}

			_ => unreachable!("marshal_column_data_serialize received non-serialize column type"),
		}
	}

	fn marshal_encoded_cells(
		&mut self,
		count: usize,
		mut write: impl FnMut(usize, &mut Vec<u8>),
	) -> (ExternCBuffer, ExternCBuffer) {
		let mut offsets: Vec<u64> = Vec::with_capacity(count + 1);
		let mut data: Vec<u8> = Vec::new();
		offsets.push(0);
		for i in 0..count {
			write(i, &mut data);
			offsets.push(data.len() as u64);
		}
		self.marshal_with_offsets(&data, &offsets)
	}

	pub(super) fn marshal_numeric_slice<T: Copy>(&mut self, slice: &[T]) -> (ExternCBuffer, ExternCBuffer) {
		let byte_len = mem::size_of_val(slice);
		if byte_len == 0 {
			return (ExternCBuffer::empty(), ExternCBuffer::empty());
		}

		(
			ExternCBuffer {
				ptr: slice.as_ptr() as *const u8,
				len: byte_len,
				cap: 0,
			},
			ExternCBuffer::empty(),
		)
	}

	pub(super) fn marshal_with_offsets(&mut self, data: &[u8], offsets: &[u64]) -> (ExternCBuffer, ExternCBuffer) {
		let data_ptr = self.copy_bytes(data);
		let offsets_byte_len = mem::size_of_val(offsets);
		let offsets_ptr = self.alloc(offsets_byte_len) as *mut u64;
		if !offsets_ptr.is_null() {
			// SAFETY: the arena returned a non-null 8-aligned block of `size_of_val(offsets)` bytes,
			// exactly `offsets.len()` u64, which cannot overlap the caller's slice.
			unsafe {
				ptr::copy_nonoverlapping(offsets.as_ptr(), offsets_ptr, offsets.len());
			}
		}

		(
			ExternCBuffer {
				ptr: data_ptr,
				len: data.len(),
				cap: data.len(),
			},
			ExternCBuffer {
				ptr: offsets_ptr as *const u8,
				len: offsets_byte_len,
				cap: offsets_byte_len,
			},
		)
	}

	#[instrument(name = "flow::marshal::bitvec", level = "trace", skip_all, fields(len = len))]
	pub(super) fn marshal_bitvec(&mut self, bitvec: &BitVec, len: usize) -> ExternCBuffer {
		let byte_count = len.div_ceil(8);
		let ptr = self.alloc(byte_count);
		if !ptr.is_null() {
			// SAFETY: the arena returned a non-null block of `byte_count` writable bytes.
			unsafe {
				ptr::write_bytes(ptr, 0, byte_count);
			}
			for i in 0..len {
				if bitvec.get(i) {
					// SAFETY: `i < len` implies `i / 8 < len.div_ceil(8) == byte_count`, and
					// the write_bytes above initialised every one of those bytes.
					unsafe {
						*ptr.add(i / 8) |= 1 << (i % 8);
					}
				}
			}
		}
		ExternCBuffer {
			ptr,
			len: byte_count,
			cap: byte_count,
		}
	}

	pub(super) fn unmarshal_bitvec(&self, extern_c: &ExternCBuffer, row_count: usize) -> BitVec {
		if extern_c.is_empty() {
			return BitVec::empty();
		}
		// SAFETY: `is_empty` above ruled out a null pointer and a zero length; the producer owns
		// `extern_c.len` initialised bytes at `extern_c.ptr` for the duration of the call.
		unsafe {
			let bytes = slice::from_raw_parts(extern_c.ptr, extern_c.len);
			BitVec::from_raw(bytes.to_vec(), row_count)
		}
	}
}
