// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod column;
pub mod util;

use std::{mem, mem::size_of, ptr, slice, str};

use arrow_array::{
	Array, BooleanArray, Date32Array, FixedSizeBinaryArray, IntervalMonthDayNanoArray, LargeBinaryArray,
	LargeStringArray, Time64NanosecondArray, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use reifydb_codec::{
	extern_c::cells::{
		decode_any_cell, decode_decimal_cell, decode_duration_cell, decode_int_cell, decode_uint_cell,
		encode_any_cell, encode_decimal_cell, encode_duration_cell, encode_int_cell, encode_uint_cell,
	},
	tag::ValueKind,
};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	Result,
	fragment::Fragment,
	util::bitmap::resize,
	value::{
		Value,
		blob::Blob,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any_array::{self, any_array},
			bignum_array::{decimal_array, decimals, int_array, ints, uint_array, uints},
			decimal_array::u128s,
			dictionary_array,
			temporal_array::{
				date_array, dates, datetime_array, datetimes, duration_array, durations, time_array,
				times,
			},
			uuid_array::{identity_id_array, identity_ids, uuid4_array, uuid4s, uuid7_array, uuid7s},
			varlen_array::blob_array,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		is::IsNumber,
		row_number::RowNumber,
		system_columns::SystemColumns,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};
use uuid::Uuid;

use crate::common::extern_wasm::{
	layout::{EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn, ExternWasmColumns},
	marshal::util::{column_data_to_type_code, ensure_marshallable},
};

pub fn marshal_columns_to_bytes(columns: &Columns) -> Result<Vec<u8>> {
	ensure_marshallable(columns)?;
	let row_count = columns.row_count();
	let column_count = columns.len();

	let header_total = EXTERN_WASM_COLUMNS_HEADER_SIZE + column_count * EXTERN_WASM_COLUMN_SIZE;
	let mut buf: Vec<u8> = vec![0u8; header_total];

	let (rn_offset, rn_len) = if !columns.row_numbers().is_empty() {
		let offset = header_total as u32;
		for rn in columns.row_numbers().iter() {
			let val: u64 = (*rn).into();
			buf.extend_from_slice(&val.to_le_bytes());
		}
		let len = (buf.len() - header_total) as u32;
		(offset, len)
	} else {
		(0u32, 0u32)
	};

	let mut col_descriptors: Vec<ExternWasmColumn> = Vec::with_capacity(column_count);

	for col in columns.iter() {
		let name_bytes = col.name().text().as_bytes();
		let name_offset = buf.len() as u32;
		buf.extend_from_slice(name_bytes);
		let name_len = name_bytes.len() as u32;

		let data = col.data();
		let data_row_count = data.len() as u32;
		let type_code = column_data_to_type_code(data).byte();

		let (bitvec_offset, bitvec_len) = if let Some(nulls) = data.nulls() {
			marshal_bitvec_to_buf(&mut buf, nulls.inner())
		} else if data_row_count > 0 {
			let all_ones = BooleanBuffer::new_set(data_row_count as usize);
			marshal_bitvec_to_buf(&mut buf, &all_ones)
		} else {
			(0u32, 0u32)
		};

		let (data_offset, data_len, offsets_offset, offsets_len) =
			marshal_column_data_bytes_to_buf(&mut buf, data);

		col_descriptors.push(ExternWasmColumn {
			name_offset,
			name_len,
			type_code,
			data_row_count,
			data_offset,
			data_len,
			bitvec_offset,
			bitvec_len,
			offsets_offset,
			offsets_len,
		});
	}

	let header = ExternWasmColumns {
		row_count: row_count as u32,
		column_count: column_count as u32,
		row_numbers_offset: rn_offset,
		row_numbers_len: rn_len,
	};

	let mut hdr_buf = Vec::with_capacity(EXTERN_WASM_COLUMNS_HEADER_SIZE);
	header.write_to_bytes(&mut hdr_buf);
	buf[..EXTERN_WASM_COLUMNS_HEADER_SIZE].copy_from_slice(&hdr_buf);

	for (i, desc) in col_descriptors.iter().enumerate() {
		let offset = EXTERN_WASM_COLUMNS_HEADER_SIZE + i * EXTERN_WASM_COLUMN_SIZE;
		desc.write_at(&mut buf, offset);
	}

	Ok(buf)
}

pub fn unmarshal_columns_from_bytes(bytes: &[u8]) -> Columns {
	if bytes.len() < EXTERN_WASM_COLUMNS_HEADER_SIZE {
		return Columns::empty();
	}

	let header = ExternWasmColumns::read_from_bytes(bytes);
	let row_count = header.row_count as usize;
	let column_count = header.column_count as usize;

	if row_count == 0 && column_count == 0 {
		return Columns::empty();
	}

	let row_numbers: Vec<RowNumber> = if header.row_numbers_offset > 0 && header.row_numbers_len > 0 {
		let start = header.row_numbers_offset as usize;
		let end = start + header.row_numbers_len as usize;
		let rn_bytes = &bytes[start..end];
		rn_bytes.chunks_exact(8)
			.map(|chunk| {
				let val = u64::from_le_bytes([
					chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
				]);
				RowNumber(val)
			})
			.collect()
	} else {
		Vec::new()
	};

	let mut columns: Vec<ColumnWithName> = Vec::with_capacity(column_count);
	for i in 0..column_count {
		let desc_start = EXTERN_WASM_COLUMNS_HEADER_SIZE + i * EXTERN_WASM_COLUMN_SIZE;
		let desc = ExternWasmColumn::read_from_bytes(&bytes[desc_start..]);

		let name = if desc.name_len > 0 {
			let start = desc.name_offset as usize;
			let end = start + desc.name_len as usize;
			let s = str::from_utf8(&bytes[start..end]).unwrap_or("");
			Fragment::internal(s)
		} else {
			Fragment::internal("")
		};

		let data_row_count = desc.data_row_count as usize;
		let type_code = ValueKind::from_byte(desc.type_code).unwrap_or(ValueKind::None);

		let bitvec = if desc.bitvec_len > 0 {
			let start = desc.bitvec_offset as usize;
			let end = start + desc.bitvec_len as usize;
			unmarshal_bitvec_from_bytes(&bytes[start..end], data_row_count)
		} else {
			BooleanBuffer::new_set(data_row_count)
		};

		let data_slice = if desc.data_len > 0 {
			let start = desc.data_offset as usize;
			let end = start + desc.data_len as usize;
			&bytes[start..end]
		} else {
			&[]
		};

		let offsets_slice = if desc.offsets_len > 0 {
			let start = desc.offsets_offset as usize;
			let end = start + desc.offsets_len as usize;
			&bytes[start..end]
		} else {
			&[]
		};

		let data = unmarshal_column_data(type_code, data_row_count, data_slice, bitvec, offsets_slice);

		columns.push(ColumnWithName::new(name, data));
	}

	if row_numbers.is_empty() {
		Columns::new(columns)
	} else {
		let n = row_numbers.len();
		let now = DateTime::default();
		Columns::with_system(
			columns,
			SystemColumns::new(
				row_numbers,
				Vec::new(),
				vec![now; n],
				vec![now; n],
				vec![now; n],
				Vec::new(),
			),
		)
	}
}

fn marshal_bitvec_to_buf(buf: &mut Vec<u8>, bitvec: &BooleanBuffer) -> (u32, u32) {
	let len = bitvec.len();
	if len == 0 {
		return (0, 0);
	}

	let byte_count = len.div_ceil(8);
	let offset = buf.len() as u32;

	buf.resize(buf.len() + byte_count, 0);
	let start = offset as usize;

	for (i, bit) in bitvec.iter().enumerate() {
		if bit {
			buf[start + i / 8] |= 1 << (i % 8);
		}
	}

	(offset, byte_count as u32)
}

fn unmarshal_bitvec_from_bytes(bytes: &[u8], len: usize) -> BooleanBuffer {
	let mut bits = Vec::with_capacity(len);
	for i in 0..len {
		let byte_idx = i / 8;
		let bit_idx = i % 8;
		let bit = if byte_idx < bytes.len() {
			(bytes[byte_idx] & (1 << bit_idx)) != 0
		} else {
			true
		};
		bits.push(bit);
	}
	BooleanBuffer::from(bits)
}

fn marshal_column_data_bytes_to_buf(buf: &mut Vec<u8>, data: &ColumnBuffer) -> (u32, u32, u32, u32) {
	match data {
		ColumnBuffer::Bool(container) => {
			let len = container.len();
			if len == 0 {
				return (0, 0, 0, 0);
			}
			let byte_count = len.div_ceil(8);
			let offset = buf.len() as u32;
			buf.resize(buf.len() + byte_count, 0);
			let start = offset as usize;
			for i in 0..len {
				if container.value(i) {
					buf[start + i / 8] |= 1 << (i % 8);
				}
			}
			(offset, byte_count as u32, 0, 0)
		}

		ColumnBuffer::Float4(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Float8(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Int1(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Int2(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Int4(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Int8(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Int16(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Uint1(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Uint2(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Uint4(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Uint8(container) => marshal_numeric_to_buf(buf, container.values()),
		ColumnBuffer::Uint16(container) => marshal_numeric_to_buf(buf, &u128s(container)),

		ColumnBuffer::Date(container) => {
			let encoded: Vec<i32> = dates(container).iter().map(|d| d.to_days_since_epoch()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ColumnBuffer::DateTime(container) => {
			let encoded: Vec<i64> = datetimes(container).iter().map(|dt| dt.to_epoch_secs()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ColumnBuffer::Time(container) => {
			let encoded: Vec<u64> = times(container).iter().map(|t| t.to_nanos_since_midnight()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ColumnBuffer::Duration(container) => {
			let values: &[Duration] = durations(container);
			marshal_cells_to_buf(buf, values.len(), |i, out| encode_duration_cell(&values[i], out))
		}

		ColumnBuffer::IdentityId(container) => {
			let ids: &[IdentityId] = identity_ids(container);
			let bytes: Vec<u8> = ids.iter().flat_map(|id| id.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}
		ColumnBuffer::Uuid4(container) => {
			let uuids: &[Uuid4] = uuid4s(container);
			let bytes: Vec<u8> = uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}
		ColumnBuffer::Uuid7(container) => {
			let uuids: &[Uuid7] = uuid7s(container);
			let bytes: Vec<u8> = uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}

		ColumnBuffer::Utf8 {
			container,
			..
		} => marshal_strings_iter_to_buf(buf, (0..container.len()).map(|i| container.value(i))),
		ColumnBuffer::Blob {
			container,
			..
		} => marshal_blobs_iter_to_buf(buf, (0..container.len()).map(|i| container.value(i))),

		ColumnBuffer::Int {
			container,
			..
		} => {
			let values: Vec<Int> = ints(container);
			marshal_cells_to_buf(buf, values.len(), |i, out| encode_int_cell(&values[i], out))
		}
		ColumnBuffer::Uint {
			container,
			..
		} => {
			let values: Vec<Uint> = uints(container);
			marshal_cells_to_buf(buf, values.len(), |i, out| encode_uint_cell(&values[i], out))
		}
		ColumnBuffer::Decimal {
			container,
			..
		} => {
			let values: Vec<Decimal> = decimals(container);
			marshal_cells_to_buf(buf, values.len(), |i, out| encode_decimal_cell(&values[i], out))
		}
		ColumnBuffer::Any {
			container,
			..
		} => {
			let values: Vec<Value> = any_array::values(container);
			let mut offsets: Vec<u64> = Vec::with_capacity(values.len() + 1);
			let mut data_bytes: Vec<u8> = Vec::new();
			offsets.push(0);
			for value in &values {
				encode_any_cell(value, &mut data_bytes).expect("unsupported value in any column cell");
				offsets.push(data_bytes.len() as u64);
			}
			marshal_data_with_offsets_to_buf(buf, &data_bytes, &offsets)
		}

		ColumnBuffer::DictionaryId {
			container,
			..
		} => {
			let encoded: Vec<u128> = dictionary_array::iter(container).map(|id| id.to_u128()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}

		ColumnBuffer::Digest {
			inner,
			accuracy,
			..
		} => panic!("a Digest({inner}, {accuracy}) column cannot be marshalled to a wasm guest"),
	}
}

fn marshal_numeric_to_buf<T: Copy>(buf: &mut Vec<u8>, slice: &[T]) -> (u32, u32, u32, u32) {
	let byte_len = mem::size_of_val(slice);
	if byte_len == 0 {
		return (0, 0, 0, 0);
	}
	let offset = buf.len() as u32;
	let src = slice.as_ptr() as *const u8;
	// SAFETY: `src`/`byte_len` describe exactly `slice`'s own live allocation reinterpreted as bytes,
	// and every `T` reaching here is a padding-free primitive, so all `byte_len` bytes are initialised.
	buf.extend_from_slice(unsafe { slice::from_raw_parts(src, byte_len) });
	(offset, byte_len as u32, 0, 0)
}

fn marshal_raw_bytes_to_buf(buf: &mut Vec<u8>, data: &[u8]) -> (u32, u32, u32, u32) {
	if data.is_empty() {
		return (0, 0, 0, 0);
	}
	let offset = buf.len() as u32;
	buf.extend_from_slice(data);
	(offset, data.len() as u32, 0, 0)
}

fn marshal_strings_iter_to_buf<'a, I: Iterator<Item = &'a str>>(buf: &mut Vec<u8>, strings: I) -> (u32, u32, u32, u32) {
	let mut offsets: Vec<u64> = Vec::new();
	let mut data: Vec<u8> = Vec::new();
	offsets.push(0);
	for s in strings {
		data.extend_from_slice(s.as_bytes());
		offsets.push(data.len() as u64);
	}
	marshal_data_with_offsets_to_buf(buf, &data, &offsets)
}

fn marshal_blobs_iter_to_buf<'a, I: Iterator<Item = &'a [u8]>>(buf: &mut Vec<u8>, blobs: I) -> (u32, u32, u32, u32) {
	let mut offsets: Vec<u64> = Vec::new();
	let mut data: Vec<u8> = Vec::new();
	offsets.push(0);
	for b in blobs {
		data.extend_from_slice(b);
		offsets.push(data.len() as u64);
	}
	marshal_data_with_offsets_to_buf(buf, &data, &offsets)
}

fn marshal_cells_to_buf(
	buf: &mut Vec<u8>,
	count: usize,
	mut write: impl FnMut(usize, &mut Vec<u8>),
) -> (u32, u32, u32, u32) {
	let mut offsets: Vec<u64> = Vec::with_capacity(count + 1);
	let mut data: Vec<u8> = Vec::new();
	offsets.push(0);
	for i in 0..count {
		write(i, &mut data);
		offsets.push(data.len() as u64);
	}
	marshal_data_with_offsets_to_buf(buf, &data, &offsets)
}

fn marshal_data_with_offsets_to_buf(buf: &mut Vec<u8>, data: &[u8], offsets: &[u64]) -> (u32, u32, u32, u32) {
	let data_offset = buf.len() as u32;
	buf.extend_from_slice(data);
	let data_len = data.len() as u32;

	let offsets_offset = buf.len() as u32;
	let offsets_byte_len = mem::size_of_val(offsets);
	let src = offsets.as_ptr() as *const u8;
	// SAFETY: `src`/`offsets_byte_len` describe exactly the `&[u64]`'s own live allocation reinterpreted
	// as bytes; u64 has no padding, so every one of those bytes is initialised.
	buf.extend_from_slice(unsafe { slice::from_raw_parts(src, offsets_byte_len) });
	let offsets_len = offsets_byte_len as u32;

	(data_offset, data_len, offsets_offset, offsets_len)
}

fn unmarshal_column_data(
	type_code: ValueKind,
	row_count: usize,
	data: &[u8],
	bitvec: BooleanBuffer,
	offsets_bytes: &[u8],
) -> ColumnBuffer {
	if row_count == 0 {
		return ColumnBuffer::none_typed(ValueType::Any, 0);
	}

	let inner = match type_code {
		ValueKind::Boolean => {
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let byte_idx = i / 8;
				let bit_idx = i % 8;
				let val = if byte_idx < data.len() {
					(data[byte_idx] & (1 << bit_idx)) != 0
				} else {
					false
				};
				values.push(val);
			}
			ColumnBuffer::Bool(BooleanArray::from(values))
		}
		ValueKind::Float4 => ColumnBuffer::float4(unmarshal_numeric::<f32>(data, row_count)),
		ValueKind::Float8 => ColumnBuffer::float8(unmarshal_numeric::<f64>(data, row_count)),
		ValueKind::Int1 => ColumnBuffer::int1(unmarshal_numeric::<i8>(data, row_count)),
		ValueKind::Int2 => ColumnBuffer::int2(unmarshal_numeric::<i16>(data, row_count)),
		ValueKind::Int4 => ColumnBuffer::int4(unmarshal_numeric::<i32>(data, row_count)),
		ValueKind::Int8 => ColumnBuffer::int8(unmarshal_numeric::<i64>(data, row_count)),
		ValueKind::Int16 => ColumnBuffer::int16(unmarshal_numeric::<i128>(data, row_count)),
		ValueKind::Uint1 => ColumnBuffer::uint1(unmarshal_numeric::<u8>(data, row_count)),
		ValueKind::Uint2 => ColumnBuffer::uint2(unmarshal_numeric::<u16>(data, row_count)),
		ValueKind::Uint4 => ColumnBuffer::uint4(unmarshal_numeric::<u32>(data, row_count)),
		ValueKind::Uint8 => ColumnBuffer::uint8(unmarshal_numeric::<u64>(data, row_count)),
		ValueKind::Uint16 => ColumnBuffer::uint16(unmarshal_numeric::<u128>(data, row_count)),
		ValueKind::Utf8 => {
			let container = unmarshal_utf8(data, row_count, offsets_bytes);
			ColumnBuffer::Utf8 {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ValueKind::Date => ColumnBuffer::Date(unmarshal_date(data, row_count)),
		ValueKind::DateTime => ColumnBuffer::DateTime(unmarshal_datetime(data, row_count)),
		ValueKind::Time => ColumnBuffer::Time(unmarshal_time(data, row_count)),
		ValueKind::Duration => ColumnBuffer::Duration(unmarshal_duration(data, row_count, offsets_bytes)),
		ValueKind::IdentityId => ColumnBuffer::IdentityId(unmarshal_identity_id(data, row_count)),
		ValueKind::Uuid4 => ColumnBuffer::Uuid4(unmarshal_uuid4(data, row_count)),
		ValueKind::Uuid7 => ColumnBuffer::Uuid7(unmarshal_uuid7(data, row_count)),
		ValueKind::Blob => {
			let container = unmarshal_blob(data, row_count, offsets_bytes);
			ColumnBuffer::Blob {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ValueKind::Int => {
			let container = int_array(unmarshal_cells(data, row_count, offsets_bytes, decode_int_cell));
			ColumnBuffer::Int {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ValueKind::Uint => {
			let container = uint_array(unmarshal_cells(data, row_count, offsets_bytes, decode_uint_cell));
			ColumnBuffer::Uint {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ValueKind::Decimal => {
			let container = decimal_array(unmarshal_cells(data, row_count, offsets_bytes, |b| {
				decode_decimal_cell(b).unwrap_or_default()
			}));
			ColumnBuffer::Decimal {
				container,
				precision: Precision::MAX,
				scale: Scale::MIN,
			}
		}
		ValueKind::Any => ColumnBuffer::Any {
			container: unmarshal_any(data, row_count, offsets_bytes),
			declared_type: None,
		},
		ValueKind::DictionaryId => {
			let entries: Vec<DictionaryEntryId> = unmarshal_numeric::<u128>(data, row_count)
				.into_iter()
				.map(DictionaryEntryId::U16)
				.collect();
			ColumnBuffer::dictionary_id(entries)
		}
		ValueKind::None
		| ValueKind::Type
		| ValueKind::List
		| ValueKind::Record
		| ValueKind::Tuple
		| ValueKind::Digest => {
			return ColumnBuffer::none_typed(ValueType::Any, row_count);
		}
	};

	maybe_wrap_option(inner, bitvec)
}

fn read_offsets(bytes: &[u8]) -> Vec<u64> {
	bytes.chunks_exact(size_of::<u64>()).map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap())).collect()
}

fn maybe_wrap_option(inner: ColumnBuffer, bitvec: BooleanBuffer) -> ColumnBuffer {
	let has_nulls = bitvec.iter().any(|b| !b);
	if has_nulls {
		let len = inner.len();
		inner.with_nulls(NullBuffer::new(resize(&bitvec, len)))
	} else {
		inner
	}
}

fn unmarshal_numeric<T: Copy + Default + IsNumber>(data: &[u8], row_count: usize) -> Vec<T> {
	if data.is_empty() {
		return vec![T::default(); row_count];
	}
	let count = data.len() / size_of::<T>();
	let mut values = vec![T::default(); count];
	// SAFETY: `count` is floored from `data.len()`, so the source holds `count * size_of::<T>()` readable
	// bytes and the freshly allocated `values` holds that many writable bytes at alignment 1; the two
	// allocations are disjoint, and every `T` reaching here is a primitive with no invalid bit patterns.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), values.as_mut_ptr() as *mut u8, count * size_of::<T>());
	}
	values
}

fn unmarshal_utf8(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> LargeStringArray {
	if data.is_empty() || offsets_bytes.is_empty() {
		return LargeStringArray::from(vec![String::new(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut strings = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let s = str::from_utf8(&data[start..end]).unwrap_or("").to_string();
		strings.push(s);
	}
	LargeStringArray::from(strings)
}

fn unmarshal_date(data: &[u8], row_count: usize) -> Date32Array {
	if data.is_empty() {
		return date_array(vec![Date::default(); row_count]);
	}
	let count = data.len() / size_of::<i32>();
	let mut raw = vec![0i32; count];
	// SAFETY: `count` is floored from `data.len()`, so the source holds `count * size_of::<i32>()`
	// readable bytes and the freshly allocated `raw` holds that many writable bytes at alignment 1; the
	// two allocations are disjoint, and every bit pattern is a valid `i32`.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr() as *mut u8, count * size_of::<i32>());
	}
	let dates: Vec<Date> = raw.iter().map(|&days| Date::from_days_since_epoch(days).unwrap_or_default()).collect();
	date_array(dates)
}

fn unmarshal_datetime(data: &[u8], row_count: usize) -> UInt64Array {
	if data.is_empty() {
		return datetime_array(vec![DateTime::default(); row_count]);
	}
	let count = data.len() / size_of::<i64>();
	let mut raw = vec![0i64; count];
	// SAFETY: `count` is floored from `data.len()`, so the source holds `count * size_of::<i64>()`
	// readable bytes and the freshly allocated `raw` holds that many writable bytes at alignment 1; the
	// two allocations are disjoint, and every bit pattern is a valid `i64`.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr() as *mut u8, count * size_of::<i64>());
	}
	let datetimes: Vec<DateTime> =
		raw.iter().map(|&ts| DateTime::from_epoch_secs(ts).unwrap_or_default()).collect();
	datetime_array(datetimes)
}

fn unmarshal_time(data: &[u8], row_count: usize) -> Time64NanosecondArray {
	if data.is_empty() {
		return time_array(vec![Time::default(); row_count]);
	}
	let count = data.len() / size_of::<u64>();
	let mut raw = vec![0u64; count];
	// SAFETY: `count` is floored from `data.len()`, so the source holds `count * size_of::<u64>()`
	// readable bytes and the freshly allocated `raw` holds that many writable bytes at alignment 1; the
	// two allocations are disjoint, and every bit pattern is a valid `u64`.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr() as *mut u8, count * size_of::<u64>());
	}
	let times: Vec<Time> = raw.iter().map(|&ns| Time::from_nanos_since_midnight(ns).unwrap_or_default()).collect();
	time_array(times)
}

fn unmarshal_duration(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> IntervalMonthDayNanoArray {
	if data.is_empty() || offsets_bytes.is_empty() {
		return duration_array(vec![Duration::default(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut durations = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let duration: Duration = decode_duration_cell(&data[start..end]).unwrap_or_default();
		durations.push(duration);
	}
	duration_array(durations)
}

fn unmarshal_identity_id(data: &[u8], row_count: usize) -> FixedSizeBinaryArray {
	if data.is_empty() {
		return identity_id_array(vec![IdentityId::default(); row_count]);
	}
	let ids: Vec<IdentityId> = data
		.chunks(16)
		.map(|chunk| {
			let mut arr = [0u8; 16];
			arr.copy_from_slice(chunk);
			IdentityId(Uuid7(Uuid::from_bytes(arr)))
		})
		.collect();
	identity_id_array(ids)
}

fn unmarshal_uuid4(data: &[u8], row_count: usize) -> FixedSizeBinaryArray {
	if data.is_empty() {
		return uuid4_array(vec![Uuid4::default(); row_count]);
	}
	let uuids: Vec<Uuid4> = data
		.chunks(16)
		.map(|chunk| {
			let mut arr = [0u8; 16];
			arr.copy_from_slice(chunk);
			Uuid4(Uuid::from_bytes(arr))
		})
		.collect();
	uuid4_array(uuids)
}

fn unmarshal_uuid7(data: &[u8], row_count: usize) -> FixedSizeBinaryArray {
	if data.is_empty() {
		return uuid7_array(vec![Uuid7::default(); row_count]);
	}
	let uuids: Vec<Uuid7> = data
		.chunks(16)
		.map(|chunk| {
			let mut arr = [0u8; 16];
			arr.copy_from_slice(chunk);
			Uuid7(Uuid::from_bytes(arr))
		})
		.collect();
	uuid7_array(uuids)
}

fn unmarshal_blob(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> LargeBinaryArray {
	if data.is_empty() || offsets_bytes.is_empty() {
		return blob_array(&vec![Blob::empty(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut blobs = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		blobs.push(Blob::new(data[start..end].to_vec()));
	}
	blob_array(&blobs)
}

fn unmarshal_cells<T: Default + Clone + IsNumber>(
	data: &[u8],
	row_count: usize,
	offsets_bytes: &[u8],
	decode: impl Fn(&[u8]) -> T,
) -> Vec<T> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return vec![T::default(); row_count];
	}
	let offsets = read_offsets(offsets_bytes);
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		values.push(decode(&data[start..end]));
	}
	values
}

fn unmarshal_any(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> LargeBinaryArray {
	if data.is_empty() || offsets_bytes.is_empty() {
		return any_array(vec![Value::none(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let value: Value = decode_any_cell(&data[start..end]).unwrap_or_else(|_| Value::none());
		values.push(value);
	}
	any_array(values)
}
