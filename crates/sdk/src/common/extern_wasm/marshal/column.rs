// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, mem, mem::size_of, ptr};

use reifydb_codec::extern_c::cells::{
	encode_any_cell, encode_decimal_cell, encode_dictionary_id_cell, encode_int_cell, encode_uint_cell,
};
use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		Value,
		container::varlen::VarlenContainer,
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
				(self.marshal_packed_bits(container.data()), ExternCBuffer::empty())
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
			} => self.marshal_varlen(container.inner()),
			ColumnBuffer::Blob {
				container,
				..
			} => self.marshal_varlen(container.inner()),

			other => unreachable!(
				"marshal_column_data_zerocopy received a non-zerocopy {} column",
				other.get_type()
			),
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

	fn marshal_varlen(&mut self, container: &VarlenContainer) -> (ExternCBuffer, ExternCBuffer) {
		let (data, offsets) = container.compact_parts();
		let offsets_byte_len = mem::size_of_val(offsets.as_ref());
		let offsets_buffer = match offsets {
			Cow::Borrowed(offsets) => ExternCBuffer {
				ptr: offsets.as_ptr() as *const u8,
				len: offsets_byte_len,
				cap: 0,
			},
			Cow::Owned(offsets) => ExternCBuffer {
				ptr: self.copy_offsets(&offsets) as *const u8,
				len: offsets_byte_len,
				cap: offsets_byte_len,
			},
		};
		(
			ExternCBuffer {
				ptr: data.as_ptr(),
				len: data.len(),
				cap: 0,
			},
			offsets_buffer,
		)
	}

	fn marshal_packed_bits(&mut self, bitvec: &BitVec) -> ExternCBuffer {
		let tail = bitvec.len() % 8;
		match bitvec.to_packed_bytes() {
			Cow::Borrowed(bytes) if tail == 0 || bytes[bytes.len() - 1] >> tail == 0 => ExternCBuffer {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
				cap: 0,
			},
			bytes => {
				let ptr = self.copy_bytes(&bytes);
				if tail != 0 {
					// SAFETY: `tail != 0` makes `bytes` non-empty, so `ptr` is a non-null arena
					// copy of it and its last byte is in bounds.
					unsafe {
						*ptr.add(bytes.len() - 1) &= (1u8 << tail) - 1;
					}
				}
				ExternCBuffer {
					ptr,
					len: bytes.len(),
					cap: bytes.len(),
				}
			}
		}
	}

	fn copy_offsets(&mut self, offsets: &[u64]) -> *mut u64 {
		let offsets_ptr = self.alloc(mem::size_of_val(offsets)) as *mut u64;
		if !offsets_ptr.is_null() {
			// SAFETY: `offsets_ptr` is a non-null 8-aligned arena block of exactly `offsets.len()` u64 that
			// cannot overlap `offsets`.
			unsafe {
				ptr::copy_nonoverlapping(offsets.as_ptr(), offsets_ptr, offsets.len());
			}
		}
		offsets_ptr
	}

	pub(super) fn marshal_with_offsets(&mut self, data: &[u8], offsets: &[u64]) -> (ExternCBuffer, ExternCBuffer) {
		let data_ptr = self.copy_bytes(data);
		let offsets_byte_len = mem::size_of_val(offsets);
		let offsets_ptr = self.copy_offsets(offsets);

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
			blob::Blob, container::temporal::TemporalContainer, date::Date, datetime::DateTime,
			duration::Duration, time::Time,
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

	fn marshalled_parts(data: ColumnBuffer) -> (Vec<u8>, Vec<u64>, usize) {
		let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), data)]);
		let mut arena = Arena::new();
		let ffi = arena.marshal_columns(&columns);
		// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
		let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
		let column = borrowed.column_at_index(0).expect("one column was marshalled");
		(column.data_bytes().to_vec(), column.offsets().to_vec(), column.row_count())
	}

	#[test]
	fn frozen_utf8_slice_hands_guest_compact_parts() {
		// A guest must never see the parent byte buffer or a non-zero first offset.
		let mut parent = ColumnBuffer::utf8(["aa", "bb", "cc", "dd"]);
		parent.freeze();
		let (data, offsets, rows) = marshalled_parts(parent.slice(1, 3));
		assert_eq!(rows, 2);
		assert_eq!(data, b"bbcc");
		assert_eq!(offsets, vec![0u64, 2, 4]);
	}

	#[test]
	fn frozen_utf8_slice_reads_back_the_sliced_rows() {
		// Absolute offsets leaking to the guest would read rows shifted by the slice start.
		let mut parent = ColumnBuffer::utf8(["aa", "bb", "cc", "dd"]);
		parent.freeze();
		let got = read_back(parent.slice(2, 4), |column, row| column.utf8_at(row).map(str::to_string));
		assert_eq!(got, vec!["cc".to_string(), "dd".to_string()]);
	}

	#[test]
	fn unsliced_utf8_column_bytes_match_container_layout() {
		// Unsliced input must reach the guest byte for byte as before the shared storage change.
		let (data, offsets, rows) = marshalled_parts(ColumnBuffer::utf8(["a", "bc", "def"]));
		assert_eq!(rows, 3);
		assert_eq!(data, b"abcdef");
		assert_eq!(offsets, vec![0u64, 1, 3, 6]);
	}

	#[test]
	fn frozen_blob_slice_hands_guest_compact_parts() {
		// The blob arm must rebase exactly like utf8, or blob_at reads the parent's bytes.
		let mut parent =
			ColumnBuffer::blob([Blob::new(vec![1, 2]), Blob::new(vec![3]), Blob::new(vec![4, 5, 6])]);
		parent.freeze();
		let (data, offsets, rows) = marshalled_parts(parent.slice(1, 3));
		assert_eq!(rows, 2);
		assert_eq!(data, vec![3u8, 4, 5, 6]);
		assert_eq!(offsets, vec![0u64, 1, 4]);
	}

	#[test]
	fn taken_bool_column_hands_guest_clean_tail_bits() {
		// A prefix view borrows the parent's last byte, whose bits past the row count must read as zero.
		let (data, offsets, rows) = marshalled_parts(ColumnBuffer::bool([true; 8]).take(3));
		assert_eq!(rows, 3);
		assert_eq!(data, vec![0b0000_0111u8]);
		assert!(offsets.is_empty());
	}

	#[test]
	fn sliced_bool_column_starts_at_bit_zero() {
		// A slice with a bit offset must reach the guest with row 0 at bit 0.
		let (data, _, rows) =
			marshalled_parts(ColumnBuffer::bool([false, true, true, false, true]).slice(1, 4));
		assert_eq!(rows, 3);
		assert_eq!(data, vec![0b0000_0011u8]);
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
