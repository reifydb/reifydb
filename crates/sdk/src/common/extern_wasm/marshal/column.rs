// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, mem, mem::size_of, ptr};

use arrow_array::{Array, GenericByteArray, RecordBatch, types::ByteArrayType};
use arrow_buffer::{BooleanBuffer, i256};
use reifydb_codec::extern_c::cells::{encode_any_cell, encode_dictionary_id_cell};
use reifydb_value::{
	Result,
	util::bitmap::packed_bytes,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		container::{
			any_array,
			decimal_array::DecimalView,
			dictionary_array,
			temporal_array::{dates, datetimes, durations, times},
			uuid_array::{identity_ids, uuid4s, uuid7s},
			varlen_array::compact_parts,
			wide_int_array::wides,
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		system_columns::{SystemColumn, row_numbers, system_column, time, user_columns},
		time::Time,
		uuid::{Uuid4, Uuid7},
	},
};
use tracing::instrument;

use super::util::column_data_to_type_code;
use crate::{
	common::{
		extern_c::wire::{
			buffer::ExternCBuffer,
			columns::{ExternCColumn, ExternCColumnData, ExternCColumns},
		},
		family::column_params,
	},
	flow::operator::extern_c::binding::arena::Arena,
};

impl Arena {
	#[instrument(name = "flow::marshal::columns", level = "trace", skip_all, fields(row_count = batch.num_rows(), column_count = user_columns(batch).count()))]
	pub fn marshal_columns(&mut self, batch: &RecordBatch) -> Result<ExternCColumns> {
		let row_count = batch.num_rows();
		let views = user_columns(batch)
			.map(|(field, array)| ColumnView::try_from((array, field.as_ref())))
			.collect::<Result<Vec<_>>>()?;
		let column_count = views.len();

		if row_count == 0 && column_count == 0 {
			return Ok(ExternCColumns::empty());
		}

		let row_numbers = row_numbers(batch)?;
		let row_numbers_ptr = if !row_numbers.is_empty() {
			row_numbers.as_ptr() as *const u64
		} else {
			ptr::null()
		};

		let time_ptr = match system_column(batch, SystemColumn::Time) {
			Some(array) if !array.is_empty() && array.null_count() == 0 => {
				time(batch)?.as_ptr() as *const i64
			}
			_ => ptr::null(),
		};

		let columns_size = column_count * size_of::<ExternCColumn>();
		let columns_ptr = self.alloc(columns_size) as *mut ExternCColumn;

		if !columns_ptr.is_null() {
			// SAFETY: `columns_ptr` is non-null here and the arena reserved
			// `column_count * size_of::<ExternCColumn>()` bytes at alignment 8, so every `add(i)` with
			// `i < column_count` is in bounds; ExternCColumn is Copy, so the stores drop nothing.
			unsafe {
				for (i, view) in views.iter().enumerate() {
					let marshalled = self.marshal_column_ref(view.field.name(), view);
					*columns_ptr.add(i) = marshalled;
				}
			}
		}

		Ok(ExternCColumns {
			row_count,
			column_count,
			row_numbers: row_numbers_ptr,
			columns: columns_ptr as *const ExternCColumn,
			time: time_ptr,
		})
	}
}

impl Arena {
	#[instrument(name = "flow::marshal::column", level = "trace", skip_all, fields(name = name))]
	pub(super) fn marshal_column_ref(&mut self, name: &str, view: &ColumnView<'_>) -> ExternCColumn {
		let name_bytes = name.as_bytes();
		let name_buf = ExternCBuffer {
			ptr: name_bytes.as_ptr(),
			len: name_bytes.len(),
			cap: 0,
		};

		let data = self.marshal_column_data(view);

		ExternCColumn {
			name: name_buf,
			data,
		}
	}

	pub(super) fn marshal_column_data(&mut self, view: &ColumnView<'_>) -> ExternCColumnData {
		let row_count = view.len();
		let (precision, scale) = column_params(view);

		if row_count == 0 {
			return ExternCColumnData {
				type_code: column_data_to_type_code(view),
				precision,
				scale,
				row_count: 0,
				data: ExternCBuffer::empty(),
				defined_bitvec: ExternCBuffer::empty(),
				offsets: ExternCBuffer::empty(),
			};
		}

		let type_code = column_data_to_type_code(view);

		let defined_bitvec = match view.logical_nulls() {
			Some(nulls) => self.marshal_bitvec(nulls.inner()),
			None if view.is_nullable() => self.marshal_bitvec(&BooleanBuffer::new_set(row_count)),
			None => ExternCBuffer::empty(),
		};

		let (data_buffer, offsets_buffer) = self.marshal_column_data_bytes(view);

		ExternCColumnData {
			type_code,
			precision,
			scale,
			row_count,
			data: data_buffer,
			defined_bitvec,
			offsets: offsets_buffer,
		}
	}
}

impl Arena {
	pub(super) fn marshal_column_data_bytes(&mut self, view: &ColumnView<'_>) -> (ExternCBuffer, ExternCBuffer) {
		match &view.data {
			ViewData::Any {
				..
			}
			| ViewData::DictionaryId {
				..
			} => self.marshal_column_data_serialize(view),
			ViewData::None {
				..
			} => (ExternCBuffer::empty(), ExternCBuffer::empty()),
			_ => self.marshal_column_data_zerocopy(view),
		}
	}

	#[instrument(name = "flow::marshal::data::zerocopy", level = "trace", skip_all, fields(type_code = ?column_data_to_type_code(view), row_count = view.len()))]
	#[inline]
	pub(super) fn marshal_column_data_zerocopy(&mut self, view: &ColumnView<'_>) -> (ExternCBuffer, ExternCBuffer) {
		match &view.data {
			ViewData::Bool(container) => {
				(self.marshal_packed_bits(container.values()), ExternCBuffer::empty())
			}

			ViewData::Float4(container) => self.marshal_numeric_slice::<f32>(container.values()),
			ViewData::Float8(container) => self.marshal_numeric_slice::<f64>(container.values()),
			ViewData::Int1(container) => self.marshal_numeric_slice::<i8>(container.values()),
			ViewData::Int2(container) => self.marshal_numeric_slice::<i16>(container.values()),
			ViewData::Int4(container) => self.marshal_numeric_slice::<i32>(container.values()),
			ViewData::Int8(container) => self.marshal_numeric_slice::<i64>(container.values()),
			ViewData::Int16(container) => self.marshal_copied(&wides::<i128>(container)),
			ViewData::Uint1(container) => self.marshal_numeric_slice::<u8>(container.values()),
			ViewData::Uint2(container) => self.marshal_numeric_slice::<u16>(container.values()),
			ViewData::Uint4(container) => self.marshal_numeric_slice::<u32>(container.values()),
			ViewData::Uint8(container) => self.marshal_numeric_slice::<u64>(container.values()),
			ViewData::Uint16(container) => self.marshal_copied(&wides::<u128>(container)),
			ViewData::Decimal(array) => self.marshal_unscaled(array),

			ViewData::Date(container) => self.marshal_numeric_slice::<Date>(dates(container)),
			ViewData::DateTime(container) => self.marshal_numeric_slice::<DateTime>(datetimes(container)),
			ViewData::Time(container) => self.marshal_numeric_slice::<Time>(times(container)),
			ViewData::Duration(container) => self.marshal_numeric_slice::<Duration>(durations(container)),

			ViewData::IdentityId(container) => {
				self.marshal_numeric_slice::<IdentityId>(identity_ids(container))
			}
			ViewData::Uuid4(container) => self.marshal_numeric_slice::<Uuid4>(uuid4s(container)),
			ViewData::Uuid7(container) => self.marshal_numeric_slice::<Uuid7>(uuid7s(container)),

			ViewData::Utf8 {
				container,
				..
			} => self.marshal_varlen(*container),
			ViewData::Blob {
				container,
				..
			} => self.marshal_varlen(*container),

			_ => unreachable!(
				"marshal_column_data_zerocopy received a non-zerocopy {} column",
				view.get_type()
			),
		}
	}

	#[instrument(name = "flow::marshal::data::serialize", level = "trace", skip_all, fields(type_code = ?column_data_to_type_code(view), row_count = view.len()))]
	#[inline]
	pub(super) fn marshal_column_data_serialize(
		&mut self,
		view: &ColumnView<'_>,
	) -> (ExternCBuffer, ExternCBuffer) {
		match &view.data {
			ViewData::Any {
				container,
				..
			} => {
				let values: Vec<Value> = any_array::values(container);
				self.marshal_encoded_cells(values.len(), |i, buf| {
					encode_any_cell(&values[i], buf).expect("unsupported value in any column cell");
				})
			}

			ViewData::DictionaryId {
				container,
				..
			} => {
				let values: Vec<DictionaryEntryId> = dictionary_array::iter(container).collect();
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

	fn marshal_unscaled(&mut self, array: &DecimalView<'_>) -> (ExternCBuffer, ExternCBuffer) {
		match array {
			DecimalView::Decimal128(array) => self.marshal_numeric_slice::<i128>(array.values()),
			DecimalView::Decimal256(array) => self.marshal_numeric_slice::<i256>(array.values()),
		}
	}

	fn marshal_copied<T: Copy>(&mut self, values: &[T]) -> (ExternCBuffer, ExternCBuffer) {
		let byte_len = mem::size_of_val(values);
		if byte_len == 0 {
			return (ExternCBuffer::empty(), ExternCBuffer::empty());
		}
		let ptr = self.alloc_aligned(byte_len, mem::align_of::<T>());
		// SAFETY: `ptr` is a fresh non-null arena block of `byte_len == values.len() * size_of::<T>()` writable
		// bytes at `align_of::<T>()`, so it holds exactly `values.len()` aligned T and cannot overlap `values`.
		unsafe {
			ptr::copy_nonoverlapping(values.as_ptr(), ptr as *mut T, values.len());
		}
		(
			ExternCBuffer {
				ptr,
				len: byte_len,
				cap: byte_len,
			},
			ExternCBuffer::empty(),
		)
	}

	fn marshal_varlen<T>(&mut self, array: &GenericByteArray<T>) -> (ExternCBuffer, ExternCBuffer)
	where
		T: ByteArrayType<Offset = i64>,
	{
		let (data, offsets) = compact_parts(array);
		let offsets_byte_len = mem::size_of_val(offsets.as_ref());
		let offsets_buffer = match offsets {
			Cow::Borrowed(offsets) => ExternCBuffer {
				ptr: offsets.as_ptr() as *const u8,
				len: offsets_byte_len,
				cap: 0,
			},
			Cow::Owned(offsets) => {
				let offsets: Vec<u64> = offsets.into_iter().map(|offset| offset as u64).collect();
				ExternCBuffer {
					ptr: self.copy_offsets(&offsets) as *const u8,
					len: offsets_byte_len,
					cap: offsets_byte_len,
				}
			}
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

	fn marshal_packed_bits(&mut self, bits: &BooleanBuffer) -> ExternCBuffer {
		match packed_bytes(bits) {
			Cow::Borrowed(bytes) => ExternCBuffer {
				ptr: bytes.as_ptr(),
				len: bytes.len(),
				cap: 0,
			},
			Cow::Owned(bytes) => ExternCBuffer {
				ptr: self.copy_bytes(&bytes),
				len: bytes.len(),
				cap: bytes.len(),
			},
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

	#[instrument(name = "flow::marshal::bitvec", level = "trace", skip_all, fields(len = bitvec.len()))]
	pub(super) fn marshal_bitvec(&mut self, bitvec: &BooleanBuffer) -> ExternCBuffer {
		let packed = packed_bytes(bitvec);
		ExternCBuffer {
			ptr: self.copy_bytes(&packed),
			len: packed.len(),
			cap: packed.len(),
		}
	}
}

#[cfg(test)]
mod tests {
	use arrow_array::ArrayRef;
	use arrow_schema::FieldRef;
	use reifydb_codec::tag::ValueKind;
	use reifydb_core::value::{
		batch::batch,
		column::{
			builder::ColumnBuilder,
			factory::{blob, bool, date, datetime, duration, time, uint16, uint16_with_bitvec, utf8},
		},
	};
	use reifydb_value::value::{
		blob::Blob, column_view::ColumnView, date::Date, datetime::DateTime, duration::Duration, time::Time,
	};

	use crate::flow::operator::{
		change::{BorrowedColumn, BorrowedColumns},
		extern_c::binding::arena::Arena,
	};

	fn read_back<T>(data: (FieldRef, ArrayRef), read: impl Fn(&BorrowedColumn, usize) -> Option<T>) -> Vec<T> {
		let columns = batch(vec![data]).unwrap();
		let mut arena = Arena::new();
		let ffi = arena.marshal_columns(&columns).unwrap();
		// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
		let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
		let column = borrowed.columns().next().expect("one column was marshalled");
		(0..columns.num_rows())
			.map(|row| read(&column, row).expect("every marshalled row must read back"))
			.collect()
	}

	fn marshalled_parts(data: (FieldRef, ArrayRef)) -> (Vec<u8>, Vec<u64>, usize) {
		let columns = batch(vec![data]).unwrap();
		let mut arena = Arena::new();
		let ffi = arena.marshal_columns(&columns).unwrap();
		// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
		let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
		let column = borrowed.columns().next().expect("one column was marshalled");
		(column.data_bytes().to_vec(), column.offsets().to_vec(), column.row_count())
	}

	fn frozen(column: (FieldRef, ArrayRef)) -> (FieldRef, ArrayRef) {
		let view = ColumnView::try_from(&column).unwrap();
		ColumnBuilder::from_view(&view).finish("c")
	}

	fn slice(column: (FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
		let (field, array) = column;
		(field, array.slice(start, end - start))
	}

	#[test]
	fn frozen_utf8_slice_hands_guest_compact_parts() {
		// A guest must never see the parent byte buffer or a non-zero first offset.
		let parent = frozen(utf8("c", ["aa", "bb", "cc", "dd"]));
		let (data, offsets, rows) = marshalled_parts(slice(parent, 1, 3));
		assert_eq!(rows, 2);
		assert_eq!(data, b"bbcc");
		assert_eq!(offsets, vec![0u64, 2, 4]);
	}

	#[test]
	fn frozen_utf8_slice_reads_back_the_sliced_rows() {
		// Absolute offsets leaking to the guest would read rows shifted by the slice start.
		let parent = frozen(utf8("c", ["aa", "bb", "cc", "dd"]));
		let got = read_back(slice(parent, 2, 4), |column, row| {
			assert!(column.type_code() == ValueKind::Utf8 && column.is_defined_at(row));
			column.str_at(row).map(str::to_string)
		});
		assert_eq!(got, vec!["cc".to_string(), "dd".to_string()]);
	}

	#[test]
	fn unsliced_utf8_column_bytes_match_container_layout() {
		// Unsliced input must reach the guest byte for byte as before the shared storage change.
		let (data, offsets, rows) = marshalled_parts(utf8("c", ["a", "bc", "def"]));
		assert_eq!(rows, 3);
		assert_eq!(data, b"abcdef");
		assert_eq!(offsets, vec![0u64, 1, 3, 6]);
	}

	#[test]
	fn frozen_blob_slice_hands_guest_compact_parts() {
		// The blob arm must rebase exactly like utf8, or a guest blob read gets the parent's bytes.
		let parent = frozen(blob("c", [Blob::new(vec![1, 2]), Blob::new(vec![3]), Blob::new(vec![4, 5, 6])]));
		let (data, offsets, rows) = marshalled_parts(slice(parent, 1, 3));
		assert_eq!(rows, 2);
		assert_eq!(data, vec![3u8, 4, 5, 6]);
		assert_eq!(offsets, vec![0u64, 1, 4]);
	}

	#[test]
	fn taken_bool_column_hands_guest_clean_tail_bits() {
		// A prefix view borrows the parent's last byte, whose bits past the row count must read as zero.
		let (data, offsets, rows) = marshalled_parts(slice(bool("c", [true; 8]), 0, 3));
		assert_eq!(rows, 3);
		assert_eq!(data, vec![0b0000_0111u8]);
		assert!(offsets.is_empty());
	}

	#[test]
	fn sliced_bool_column_starts_at_bit_zero() {
		// A slice with a bit offset must reach the guest with row 0 at bit 0.
		let (data, _, rows) = marshalled_parts(slice(bool("c", [false, true, true, false, true]), 1, 4));
		assert_eq!(rows, 3);
		assert_eq!(data, vec![0b0000_0011u8]);
	}

	#[test]
	fn uint16_rows_reach_the_guest_16_aligned() {
		// An 8-aligned copy makes the guest's &[u128] view of the rows undefined behaviour.
		let values = [1u128 << 64, u128::MAX, 0, (1u128 << 64) - 1];
		let inputs = [
			uint16("c", values),
			uint16_with_bitvec("c", values.iter().copied().chain([0]), vec![true, true, true, true, false]),
		];
		for padding in [0usize, 8, 16, 24] {
			for input in inputs.clone() {
				let columns = batch(vec![input]).unwrap();
				let mut arena = Arena::new();
				arena.alloc(padding);
				let ffi = arena.marshal_columns(&columns).unwrap();
				// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
				let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
				let column = borrowed.columns().next().expect("one column was marshalled");
				let data = column.data_bytes();
				assert_eq!(
					data.as_ptr() as usize % 16,
					0,
					"padding {padding}: rows handed over 8-aligned"
				);
				assert_eq!(
					data.len(),
					column.row_count() * 16,
					"padding {padding}: u128 rows must stay 16 bytes"
				);
				for (row, value) in values.iter().enumerate() {
					assert_eq!(
						data[row * 16..(row + 1) * 16],
						value.to_le_bytes(),
						"padding {padding}"
					);
					assert!(
						column.type_code() == ValueKind::Uint16 && column.is_defined_at(row),
						"padding {padding}"
					);
					// SAFETY: asserted above: 16-aligned rows, `row_count` u128 cells long.
					let cell = unsafe { column.as_slice::<u128>() }
						.and_then(|cells| cells.get(row).copied());
					assert_eq!(cell, Some(*value), "padding {padding}");
				}
			}
		}
	}

	#[test]
	fn datetime_column_marshal_borrow_roundtrip() {
		// The marshal is zero-copy raw i64 nanos, so a reader using a seconds constructor would rescale every
		// value.
		let values = vec![
			DateTime::from_nanos(0),
			DateTime::from_nanos(1_700_000_000_000_000_000),
			DateTime::from_nanos(i64::MAX),
		];
		let got = read_back(datetime("c", values.clone()), |column, row| {
			assert!(column.type_code() == ValueKind::DateTime && column.is_defined_at(row));
			// SAFETY: DateTime cells marshal as aligned raw i64, and `DateTime` is transparent over i64.
			unsafe { column.as_slice::<DateTime>() }?.get(row).copied()
		});
		assert_eq!(got, values);
	}

	#[test]
	fn date_column_marshal_borrow_roundtrip() {
		// The marshal is zero-copy raw i32 days since the epoch, so the reader must read the same units.
		let values = vec![Date::default(), Date::new(2024, 3, 15).unwrap(), Date::new(1970, 1, 1).unwrap()];
		let got = read_back(date("c", values.clone()), |column, row| {
			assert!(column.type_code() == ValueKind::Date && column.is_defined_at(row));
			// SAFETY: Date cells marshal as aligned raw i32, and `Date` is transparent over i32.
			unsafe { column.as_slice::<Date>() }?.get(row).copied()
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
		let got = read_back(time("c", values.clone()), |column, row| column.time_at(row));
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
		let got = read_back(duration("c", values.clone()), |column, row| {
			assert!(column.type_code() == ValueKind::Duration && column.is_defined_at(row));
			// SAFETY: Duration cells marshal aligned and `repr(C)`, so every bit pattern is valid.
			unsafe { column.as_slice::<Duration>() }?.get(row).copied()
		});
		assert_eq!(got, values);
	}
}
