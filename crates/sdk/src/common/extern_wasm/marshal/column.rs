// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, mem::size_of, ptr};

use reifydb_codec::extern_c::cells::{
	encode_any_cell, encode_decimal_cell, encode_dictionary_id_cell, encode_int_cell, encode_uint_cell,
};
use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		Value,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use tracing::instrument;

use super::util::column_data_to_type_code;
use crate::{
	common::extern_c::wire::{
		buffer::ExternCBuffer,
		columns::{ExternCColumn, ExternCColumnData, ExternCColumns},
	},
	flow::operator::extern_c::binding::arena::Arena,
};

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
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
	use reifydb_value::{
		fragment::Fragment,
		value::{
			container::temporal::TemporalContainer, date::Date, datetime::DateTime, duration::Duration,
			time::Time,
		},
	};

	use crate::flow::operator::{
		change::{BorrowedColumn, BorrowedColumns},
		extern_c::binding::arena::Arena,
	};

	fn read_back<T>(data: ColumnBuffer, read: impl Fn(&BorrowedColumn, usize) -> Option<T>) -> Vec<T> {
		let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), data)]);
		let mut arena = Arena::new();
		let ffi = arena.marshal_columns(&columns);
		// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
		let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
		let column = borrowed.column_at_index(0).expect("one column was marshalled");
		(0..columns.row_count())
			.map(|row| read(&column, row).expect("every marshalled row must read back"))
			.collect()
	}

	#[test]
	fn datetime_column_marshal_borrow_roundtrip() {
		// The marshal is zero-copy raw u64 nanos, so a reader using a seconds constructor would rescale every
		// value.
		let values = vec![
			DateTime::from_nanos(0),
			DateTime::from_nanos(1_700_000_000_000_000_000),
			DateTime::from_nanos(u64::MAX),
		];
		let got = read_back(ColumnBuffer::DateTime(TemporalContainer::new(values.clone())), |column, row| {
			column.datetime_at(row)
		});
		assert_eq!(got, values);
	}

	#[test]
	fn date_column_marshal_borrow_roundtrip() {
		// The marshal is zero-copy raw i32 days since the epoch, so the reader must read the same units.
		let values = vec![Date::default(), Date::new(2024, 3, 15).unwrap(), Date::new(1970, 1, 1).unwrap()];
		let got = read_back(ColumnBuffer::Date(TemporalContainer::new(values.clone())), |column, row| {
			column.date_at(row)
		});
		assert_eq!(got, values);
	}

	#[test]
	fn time_column_marshal_borrow_roundtrip() {
		// The marshal is zero-copy raw u64 nanos since midnight, so the reader must read the same units.
		let values = vec![
			Time::default(),
			Time::new(14, 30, 45, 123_456_789).unwrap(),
			Time::new(23, 59, 59, 999_999_999).unwrap(),
		];
		let got = read_back(ColumnBuffer::Time(TemporalContainer::new(values.clone())), |column, row| {
			column.time_at(row)
		});
		assert_eq!(got, values);
	}

	#[test]
	fn duration_column_marshal_borrow_roundtrip() {
		// The marshal is zero-copy 16-byte structs, so a reader expecting postcard plus offsets reads bytes
		// never written.
		let values = vec![
			Duration::default(),
			Duration::new(13, 5, 3_600_000_000_000).expect("duration"),
			Duration::from_seconds(-30).expect("duration"),
		];
		let got = read_back(ColumnBuffer::Duration(TemporalContainer::new(values.clone())), |column, row| {
			column.duration_at(row)
		});
		assert_eq!(got, values);
	}
}
