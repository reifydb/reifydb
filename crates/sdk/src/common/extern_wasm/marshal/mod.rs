// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod column;
pub mod util;

use std::{mem, mem::size_of, ptr, slice, str, sync::Arc};

use arrow_array::{
	Array, ArrayRef, Date32Array, FixedSizeBinaryArray, IntervalMonthDayNanoArray, LargeBinaryArray,
	LargeStringArray, RecordBatch, Time64NanosecondArray, TimestampNanosecondArray, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_codec::{
	extern_c::cells::{
		decode_any_cell, decode_dictionary_id_cell, decode_duration_cell, encode_any_cell,
		encode_dictionary_id_cell, encode_duration_cell,
	},
	tag::ValueKind,
};
use reifydb_core::value::{
	batch::{batch, empty_batch},
	column::{
		factory::{
			bool, dictionary_id, float4, float8, int1, int2, int4, int8, int16, none, uint1, uint2, uint4,
			uint8, uint16,
		},
		nulls::with_nulls,
	},
};
use reifydb_value::{
	Result,
	util::bitmap::resize,
	value::{
		Value,
		blob::Blob,
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::{
			any_array::{self, any_array_optional},
			decimal_array::DecimalView,
			dictionary_array,
			temporal_array::{
				date_array, dates, datetime_array, datetimes, duration_array, durations, time_array,
				times,
			},
			uuid_array::{identity_id_array, identity_ids, uuid4_array, uuid4s, uuid7_array, uuid7s},
			varlen_array::blob_array,
			wide_int_array::wides,
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		is::IsNumber,
		row_number::RowNumber,
		system_columns::{SystemColumn, with_system_column},
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{FieldType, named},
		},
	},
};
use uuid::Uuid;

use crate::{
	common::{
		extern_wasm::{
			layout::{
				EXTERN_WASM_COLUMN_SIZE, EXTERN_WASM_COLUMNS_HEADER_SIZE, ExternWasmColumn,
				ExternWasmColumns,
			},
			marshal::util::{column_data_to_type_code, ensure_view_marshallable},
		},
		family::{cell_width, column_params, decode_family_column, family_params, is_family},
	},
	error::{Result as SdkResult, SdkError},
};

const COLUMN_FLAG_OPTIONAL: u8 = 0b0000_0001;

pub fn marshal_columns_to_bytes(
	columns: &[(FieldRef, ArrayRef)],
	row_count: usize,
	row_numbers: &[RowNumber],
) -> Result<Vec<u8>> {
	let views = columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
	for view in &views {
		ensure_view_marshallable(view)?;
	}
	let column_count = views.len();

	let header_total = EXTERN_WASM_COLUMNS_HEADER_SIZE + column_count * EXTERN_WASM_COLUMN_SIZE;
	let mut buf: Vec<u8> = vec![0u8; header_total];

	let (rn_offset, rn_len) = if !row_numbers.is_empty() {
		let offset = header_total as u32;
		for rn in row_numbers.iter() {
			let val: u64 = (*rn).into();
			buf.extend_from_slice(&val.to_le_bytes());
		}
		let len = (buf.len() - header_total) as u32;
		(offset, len)
	} else {
		(0u32, 0u32)
	};

	let mut col_descriptors: Vec<ExternWasmColumn> = Vec::with_capacity(column_count);

	for view in &views {
		let name_bytes = view.field.name().as_bytes();
		let name_offset = buf.len() as u32;
		buf.extend_from_slice(name_bytes);
		let name_len = name_bytes.len() as u32;

		let data_row_count = view.len() as u32;
		let type_code = column_data_to_type_code(view).byte();
		let (precision, scale) = column_params(view);
		let flags = if view.is_nullable() {
			COLUMN_FLAG_OPTIONAL
		} else {
			0
		};

		let (bitvec_offset, bitvec_len) = if let Some(nulls) = view.logical_nulls() {
			marshal_bitvec_to_buf(&mut buf, nulls.inner())
		} else if data_row_count > 0 {
			let all_ones = BooleanBuffer::new_set(data_row_count as usize);
			marshal_bitvec_to_buf(&mut buf, &all_ones)
		} else {
			(0u32, 0u32)
		};

		let (data_offset, data_len, offsets_offset, offsets_len) =
			marshal_column_data_bytes_to_buf(&mut buf, view);

		col_descriptors.push(ExternWasmColumn {
			name_offset,
			name_len,
			type_code,
			precision,
			scale,
			data_row_count,
			data_offset,
			data_len,
			bitvec_offset,
			bitvec_len,
			offsets_offset,
			offsets_len,
			flags,
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

pub fn unmarshal_columns_from_bytes(bytes: &[u8]) -> SdkResult<RecordBatch> {
	if bytes.len() < EXTERN_WASM_COLUMNS_HEADER_SIZE {
		return Err(malformed(format!(
			"guest sent {} bytes, fewer than the {EXTERN_WASM_COLUMNS_HEADER_SIZE} byte header",
			bytes.len()
		)));
	}

	let header = ExternWasmColumns::read_from_bytes(bytes);
	let row_count = header.row_count as usize;
	let column_count = header.column_count as usize;

	if row_count == 0 && column_count == 0 {
		return Ok(empty_batch());
	}
	if column_count == 0 {
		return Err(malformed(format!("guest sent {row_count} rows but no columns")));
	}

	let descriptors_end = column_count
		.checked_mul(EXTERN_WASM_COLUMN_SIZE)
		.and_then(|len| len.checked_add(EXTERN_WASM_COLUMNS_HEADER_SIZE))
		.ok_or_else(|| {
			malformed(format!("guest declared {column_count} columns, more than can be addressed"))
		})?;
	if descriptors_end > bytes.len() {
		return Err(malformed(format!(
			"guest declared {column_count} columns needing {descriptors_end} bytes, but sent {}",
			bytes.len()
		)));
	}

	let row_numbers: Vec<u64> = if header.row_numbers_offset > 0 && header.row_numbers_len > 0 {
		let region = region(bytes, header.row_numbers_offset, header.row_numbers_len, "row numbers")?;
		if region.len() % 8 != 0 {
			return Err(malformed(format!(
				"guest sent {} bytes of row numbers, not a multiple of the 8 byte row number width",
				region.len()
			)));
		}
		region.chunks_exact(8).map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap())).collect()
	} else {
		Vec::new()
	};

	let mut columns: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(column_count);
	for i in 0..column_count {
		let desc_start = EXTERN_WASM_COLUMNS_HEADER_SIZE + i * EXTERN_WASM_COLUMN_SIZE;
		let desc = ExternWasmColumn::read_from_bytes(&bytes[desc_start..]);

		let name_bytes = region(bytes, desc.name_offset, desc.name_len, "column name")?;
		let name = str::from_utf8(name_bytes)
			.map_err(|_| malformed(format!("guest column {i} name is not utf8")))?;

		if desc.flags & !COLUMN_FLAG_OPTIONAL != 0 {
			return Err(malformed(format!("guest column {i} sets unknown flag bits {:#010b}", desc.flags)));
		}

		let data_row_count = desc.data_row_count as usize;
		let type_code = ValueKind::from_byte(desc.type_code).ok_or_else(|| {
			malformed(format!("guest column {i} carries unknown type code {}", desc.type_code))
		})?;

		let bitvec = if desc.bitvec_len > 0 {
			let bitvec_bytes = region(bytes, desc.bitvec_offset, desc.bitvec_len, "bitvec")?;
			if bitvec_bytes.len() * 8 < data_row_count {
				return Err(malformed(format!(
					"guest column {i} sent {} bitvec bytes for {data_row_count} rows",
					bitvec_bytes.len()
				)));
			}
			unmarshal_bitvec_from_bytes(bitvec_bytes, data_row_count)
		} else {
			BooleanBuffer::new_set(data_row_count)
		};

		let data_slice = region(bytes, desc.data_offset, desc.data_len, "data")?;
		let offsets_slice = region(bytes, desc.offsets_offset, desc.offsets_len, "offsets")?;

		let inner = if is_family(type_code) {
			let (precision, scale) =
				family_params(type_code, desc.precision, desc.scale).ok_or_else(|| {
					malformed(format!(
						"guest column {i} carries {type_code:?} precision {} and scale {}",
						desc.precision, desc.scale
					))
				})?;
			unmarshal_family(name, type_code, precision, scale, data_row_count, data_slice)?
		} else {
			unmarshal_column_data(name, type_code, data_row_count, data_slice, &bitvec, offsets_slice)?
		};

		columns.push(maybe_wrap_option(inner, bitvec, desc.flags, i)?);
	}

	let columns = batch(columns)?;
	if row_numbers.is_empty() {
		Ok(columns)
	} else {
		Ok(with_system_column(columns, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(row_numbers)))?)
	}
}

fn malformed(reason: String) -> SdkError {
	SdkError::InvalidInput(reason)
}

fn region<'a>(bytes: &'a [u8], offset: u32, len: u32, what: &str) -> SdkResult<&'a [u8]> {
	if len == 0 {
		return Ok(&[]);
	}
	let start = offset as usize;
	let end = start + len as usize;
	bytes.get(start..end).ok_or_else(|| {
		malformed(format!("guest {what} region spans {start}..{end}, past the {} bytes sent", bytes.len()))
	})
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

fn marshal_column_data_bytes_to_buf(buf: &mut Vec<u8>, view: &ColumnView<'_>) -> (u32, u32, u32, u32) {
	match &view.data {
		ViewData::Bool(container) => {
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

		ViewData::Float4(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Float8(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Int1(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Int2(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Int4(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Int8(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Int16(container) => marshal_numeric_to_buf(buf, &wides::<i128>(container)),
		ViewData::Uint1(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Uint2(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Uint4(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Uint8(container) => marshal_numeric_to_buf(buf, container.values()),
		ViewData::Uint16(container) => marshal_numeric_to_buf(buf, &wides::<u128>(container)),

		ViewData::Date(container) => {
			let encoded: Vec<i32> = dates(container).iter().map(|d| d.to_days_since_epoch()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ViewData::DateTime(container) => {
			let encoded: Vec<i64> = datetimes(container).iter().map(|dt| dt.to_nanos()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ViewData::Time(container) => {
			let encoded: Vec<u64> = times(container).iter().map(|t| t.to_nanos_since_midnight()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ViewData::Duration(container) => {
			let values: &[Duration] = durations(container);
			marshal_cells_to_buf(buf, values.len(), |i, out| encode_duration_cell(&values[i], out))
		}

		ViewData::IdentityId(container) => {
			let ids: &[IdentityId] = identity_ids(container);
			let bytes: Vec<u8> = ids.iter().flat_map(|id| id.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}
		ViewData::Uuid4(container) => {
			let uuids: &[Uuid4] = uuid4s(container);
			let bytes: Vec<u8> = uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}
		ViewData::Uuid7(container) => {
			let uuids: &[Uuid7] = uuid7s(container);
			let bytes: Vec<u8> = uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}

		ViewData::Utf8 {
			container,
			..
		} => marshal_strings_iter_to_buf(buf, (0..container.len()).map(|i| container.value(i))),
		ViewData::Blob {
			container,
			..
		} => marshal_blobs_iter_to_buf(buf, (0..container.len()).map(|i| container.value(i))),

		ViewData::Decimal(array) => match array {
			DecimalView::Decimal128(array) => marshal_numeric_to_buf(buf, array.values()),
			DecimalView::Decimal256(array) => marshal_numeric_to_buf(buf, array.values()),
		},
		ViewData::Any {
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

		ViewData::DictionaryId {
			container,
			..
		} => {
			let entries: Vec<DictionaryEntryId> = dictionary_array::iter(container).collect();
			let mut offsets: Vec<u64> = Vec::with_capacity(entries.len() + 1);
			let mut data_bytes: Vec<u8> = Vec::new();
			offsets.push(0);
			for entry in &entries {
				encode_dictionary_id_cell(entry, &mut data_bytes);
				offsets.push(data_bytes.len() as u64);
			}
			marshal_data_with_offsets_to_buf(buf, &data_bytes, &offsets)
		}

		ViewData::Digest {
			inner,
			accuracy,
			..
		} => panic!("a Digest({inner}, {accuracy}) column cannot be marshalled to a wasm guest"),
		ViewData::None {
			..
		} => (0, 0, 0, 0),
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
	name: &str,
	type_code: ValueKind,
	row_count: usize,
	data: &[u8],
	bitvec: &BooleanBuffer,
	offsets_bytes: &[u8],
) -> SdkResult<(FieldRef, ArrayRef)> {
	if row_count == 0 {
		return Ok(none(name, 0));
	}

	let column = match type_code {
		ValueKind::Boolean => {
			if data.len() * 8 < row_count {
				return Err(malformed(format!(
					"guest sent {} boolean bytes for {row_count} rows",
					data.len()
				)));
			}
			let values: Vec<bool> = (0..row_count).map(|i| (data[i / 8] & (1 << (i % 8))) != 0).collect();
			bool(name, values)
		}
		ValueKind::Float4 => float4(name, unmarshal_numeric::<f32>(data, row_count)?),
		ValueKind::Float8 => float8(name, unmarshal_numeric::<f64>(data, row_count)?),
		ValueKind::Int1 => int1(name, unmarshal_numeric::<i8>(data, row_count)?),
		ValueKind::Int2 => int2(name, unmarshal_numeric::<i16>(data, row_count)?),
		ValueKind::Int4 => int4(name, unmarshal_numeric::<i32>(data, row_count)?),
		ValueKind::Int8 => int8(name, unmarshal_numeric::<i64>(data, row_count)?),
		ValueKind::Int16 => int16(name, unmarshal_numeric::<i128>(data, row_count)?),
		ValueKind::Uint1 => uint1(name, unmarshal_numeric::<u8>(data, row_count)?),
		ValueKind::Uint2 => uint2(name, unmarshal_numeric::<u16>(data, row_count)?),
		ValueKind::Uint4 => uint4(name, unmarshal_numeric::<u32>(data, row_count)?),
		ValueKind::Uint8 => uint8(name, unmarshal_numeric::<u64>(data, row_count)?),
		ValueKind::Uint16 => uint16(name, unmarshal_numeric::<u128>(data, row_count)?),
		ValueKind::Utf8 => typed(name, ValueType::Utf8, unmarshal_utf8(data, row_count, offsets_bytes)?),
		ValueKind::Date => typed(name, ValueType::Date, unmarshal_date(data, row_count)?),
		ValueKind::DateTime => typed(name, ValueType::DateTime, unmarshal_datetime(data, row_count)?),
		ValueKind::Time => typed(name, ValueType::Time, unmarshal_time(data, row_count)?),
		ValueKind::Duration => {
			typed(name, ValueType::Duration, unmarshal_duration(data, row_count, offsets_bytes)?)
		}
		ValueKind::IdentityId => typed(name, ValueType::IdentityId, unmarshal_identity_id(data, row_count)?),
		ValueKind::Uuid4 => typed(name, ValueType::Uuid4, unmarshal_uuid4(data, row_count)?),
		ValueKind::Uuid7 => typed(name, ValueType::Uuid7, unmarshal_uuid7(data, row_count)?),
		ValueKind::Blob => typed(name, ValueType::Blob, unmarshal_blob(data, row_count, offsets_bytes)?),
		ValueKind::Any => typed(name, ValueType::Any, unmarshal_any(data, row_count, offsets_bytes, bitvec)?),
		ValueKind::DictionaryId => {
			dictionary_id(name, unmarshal_dictionary_ids(data, row_count, offsets_bytes)?)
		}
		ValueKind::Decimal => {
			return Err(malformed(format!("guest {type_code:?} column reached the var-len decoder")));
		}
		ValueKind::None => none(name, row_count),
		ValueKind::Type | ValueKind::List | ValueKind::Record | ValueKind::Tuple | ValueKind::Digest => {
			return Err(malformed(format!(
				"guest {type_code:?} column is not supported by the wasm marshal"
			)));
		}
	};

	Ok(column)
}

fn unmarshal_family(
	name: &str,
	type_code: ValueKind,
	precision: Precision,
	scale: Scale,
	row_count: usize,
	data: &[u8],
) -> SdkResult<(FieldRef, ArrayRef)> {
	if row_count == 0 {
		return Ok(none(name, 0));
	}
	let zeros;
	let data = if data.is_empty() {
		zeros = vec![0u8; row_count * cell_width(precision)];
		&zeros[..]
	} else {
		data
	};
	decode_family_column(name, type_code, precision, scale, data, row_count)
		.map_err(|e| malformed(format!("guest {type_code:?} column: {e}")))
}

fn typed(name: &str, value_type: ValueType, array: impl Array + 'static) -> (FieldRef, ArrayRef) {
	named(name, FieldType::from(value_type), Arc::new(array))
}

fn read_offsets(bytes: &[u8]) -> Vec<u64> {
	bytes.chunks_exact(size_of::<u64>()).map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap())).collect()
}

fn cell_ranges(data: &[u8], row_count: usize, offsets_bytes: &[u8], what: &str) -> SdkResult<Vec<(usize, usize)>> {
	let offsets = read_offsets(offsets_bytes);
	if offsets.len() < row_count + 1 {
		return Err(malformed(format!(
			"guest sent {} offsets for {row_count} rows of {what}, needing {}",
			offsets.len(),
			row_count + 1
		)));
	}
	(0..row_count)
		.map(|i| {
			let (start, end) = (offsets[i] as usize, offsets[i + 1] as usize);
			if start > end || end > data.len() {
				return Err(malformed(format!(
					"guest {what} cell {i} spans {start}..{end}, outside the {} data bytes sent",
					data.len()
				)));
			}
			Ok((start, end))
		})
		.collect()
}

fn maybe_wrap_option(
	inner: (FieldRef, ArrayRef),
	bitvec: BooleanBuffer,
	flags: u8,
	index: usize,
) -> SdkResult<(FieldRef, ArrayRef)> {
	if inner.1.data_type().is_null() {
		return Ok(inner);
	}
	let has_nones = bitvec.iter().any(|b| !b);
	if flags & COLUMN_FLAG_OPTIONAL == 0 {
		if has_nones {
			return Err(malformed(format!("guest column {index} marks a none row but is not optional")));
		}
		return Ok(inner);
	}
	let len = inner.1.len();
	Ok(with_nulls(inner, NullBuffer::new(resize(&bitvec, len)))?)
}

fn unmarshal_numeric<T: Copy + Default + IsNumber>(data: &[u8], row_count: usize) -> SdkResult<Vec<T>> {
	if data.is_empty() {
		return Ok(vec![T::default(); row_count]);
	}
	let needed = row_count * size_of::<T>();
	if data.len() < needed {
		return Err(malformed(format!(
			"guest sent {} data bytes for {row_count} rows of {} bytes each",
			data.len(),
			size_of::<T>()
		)));
	}
	let mut values = vec![T::default(); row_count];
	// SAFETY: `needed` bytes were just bounds-checked against `data`, and the freshly allocated `values`
	// holds exactly that many writable bytes at alignment 1; the two allocations are disjoint, and every
	// `T` reaching here is a primitive with no invalid bit patterns.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), values.as_mut_ptr() as *mut u8, needed);
	}
	Ok(values)
}

fn unmarshal_raw<T: Copy + Default>(data: &[u8], row_count: usize, what: &str) -> SdkResult<Vec<T>> {
	let needed = row_count * size_of::<T>();
	if data.len() < needed {
		return Err(malformed(format!(
			"guest sent {} {what} bytes for {row_count} rows of {} bytes each",
			data.len(),
			size_of::<T>()
		)));
	}
	let mut values = vec![T::default(); row_count];
	// SAFETY: `needed` bytes were just bounds-checked against `data`, and the freshly allocated `values`
	// holds exactly that many writable bytes at alignment 1; the two allocations are disjoint, and every
	// `T` reaching here is a primitive with no invalid bit patterns.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), values.as_mut_ptr() as *mut u8, needed);
	}
	Ok(values)
}

fn unmarshal_utf8(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> SdkResult<LargeStringArray> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return Ok(LargeStringArray::from(vec![String::new(); row_count]));
	}
	let strings = cell_ranges(data, row_count, offsets_bytes, "utf8")?
		.into_iter()
		.map(|(start, end)| {
			str::from_utf8(&data[start..end])
				.map(str::to_string)
				.map_err(|_| malformed("guest utf8 cell is not utf8".to_string()))
		})
		.collect::<SdkResult<Vec<String>>>()?;
	Ok(LargeStringArray::from(strings))
}

fn unmarshal_date(data: &[u8], row_count: usize) -> SdkResult<Date32Array> {
	if data.is_empty() {
		return Ok(date_array(vec![Date::default(); row_count]));
	}
	let dates = unmarshal_raw::<i32>(data, row_count, "date")?
		.into_iter()
		.map(|days| {
			Date::from_days_since_epoch(days)
				.ok_or_else(|| malformed(format!("guest date is {days} days since the epoch")))
		})
		.collect::<SdkResult<Vec<Date>>>()?;
	Ok(date_array(dates))
}

fn unmarshal_datetime(data: &[u8], row_count: usize) -> SdkResult<TimestampNanosecondArray> {
	if data.is_empty() {
		return Ok(datetime_array(vec![DateTime::default(); row_count]));
	}
	let datetimes: Vec<DateTime> =
		unmarshal_raw::<i64>(data, row_count, "datetime")?.into_iter().map(DateTime::from_nanos).collect();
	Ok(datetime_array(datetimes))
}

fn unmarshal_time(data: &[u8], row_count: usize) -> SdkResult<Time64NanosecondArray> {
	if data.is_empty() {
		return Ok(time_array(vec![Time::default(); row_count]));
	}
	let times = unmarshal_raw::<u64>(data, row_count, "time")?
		.into_iter()
		.map(|nanos| {
			Time::from_nanos_since_midnight(nanos)
				.ok_or_else(|| malformed(format!("guest time is {nanos} nanoseconds since midnight")))
		})
		.collect::<SdkResult<Vec<Time>>>()?;
	Ok(time_array(times))
}

fn unmarshal_duration(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> SdkResult<IntervalMonthDayNanoArray> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return Ok(duration_array(vec![Duration::default(); row_count]));
	}
	let durations = cell_ranges(data, row_count, offsets_bytes, "duration")?
		.into_iter()
		.map(|(start, end)| {
			decode_duration_cell(&data[start..end])
				.map_err(|e| malformed(format!("guest duration cell: {e}")))
		})
		.collect::<SdkResult<Vec<Duration>>>()?;
	Ok(duration_array(durations))
}

fn unmarshal_uuids<T>(data: &[u8], row_count: usize, what: &str, build: impl Fn(Uuid) -> T) -> SdkResult<Vec<T>> {
	let needed = row_count * 16;
	if data.len() < needed {
		return Err(malformed(format!("guest sent {} {what} bytes for {row_count} rows", data.len())));
	}
	Ok(data[..needed].chunks_exact(16).map(|chunk| build(Uuid::from_bytes(chunk.try_into().unwrap()))).collect())
}

fn unmarshal_identity_id(data: &[u8], row_count: usize) -> SdkResult<FixedSizeBinaryArray> {
	if data.is_empty() {
		return Ok(identity_id_array(vec![IdentityId::default(); row_count]));
	}
	let ids = unmarshal_uuids(data, row_count, "identity id", |uuid| uuid)?;
	let ids = ids
		.into_iter()
		.map(|uuid| match uuid.get_version_num() {
			7 => Ok(IdentityId(Uuid7(uuid))),
			version => Err(malformed(format!("guest identity id is a uuid v{version}, not a uuid v7"))),
		})
		.collect::<SdkResult<Vec<IdentityId>>>()?;
	Ok(identity_id_array(ids))
}

fn unmarshal_uuid4(data: &[u8], row_count: usize) -> SdkResult<FixedSizeBinaryArray> {
	if data.is_empty() {
		return Ok(uuid4_array(vec![Uuid4::default(); row_count]));
	}
	Ok(uuid4_array(unmarshal_uuids(data, row_count, "uuid4", Uuid4)?))
}

fn unmarshal_uuid7(data: &[u8], row_count: usize) -> SdkResult<FixedSizeBinaryArray> {
	if data.is_empty() {
		return Ok(uuid7_array(vec![Uuid7::default(); row_count]));
	}
	Ok(uuid7_array(unmarshal_uuids(data, row_count, "uuid7", Uuid7)?))
}

fn unmarshal_blob(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> SdkResult<LargeBinaryArray> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return Ok(blob_array(&vec![Blob::empty(); row_count]));
	}
	let blobs: Vec<Blob> = cell_ranges(data, row_count, offsets_bytes, "blob")?
		.into_iter()
		.map(|(start, end)| Blob::new(data[start..end].to_vec()))
		.collect();
	Ok(blob_array(&blobs))
}

fn unmarshal_dictionary_ids(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> SdkResult<Vec<DictionaryEntryId>> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return Ok(vec![DictionaryEntryId::default(); row_count]);
	}
	cell_ranges(data, row_count, offsets_bytes, "dictionary id")?
		.into_iter()
		.map(|(start, end)| {
			decode_dictionary_id_cell(&data[start..end])
				.map_err(|e| malformed(format!("guest dictionary id cell: {e}")))
		})
		.collect()
}

fn unmarshal_any(
	data: &[u8],
	row_count: usize,
	offsets_bytes: &[u8],
	defined: &BooleanBuffer,
) -> SdkResult<LargeBinaryArray> {
	let cells = if data.is_empty() || offsets_bytes.is_empty() {
		vec![Value::none(); row_count]
	} else {
		cell_ranges(data, row_count, offsets_bytes, "any")?
			.into_iter()
			.map(|(start, end)| {
				decode_any_cell(&data[start..end])
					.map_err(|e| malformed(format!("guest any cell: {e}")))
			})
			.collect::<SdkResult<Vec<Value>>>()?
	};
	let values = cells
		.into_iter()
		.enumerate()
		.map(|(row, value)| match value {
			Value::None {
				..
			} if defined.value(row) => Err(malformed(format!("guest any cell at row {row} holds a none under a set valid bit"))),
			Value::None {
				..
			} => Ok(None),
			value => Ok(Some(value)),
		})
		.collect::<SdkResult<Vec<Option<Value>>>>()?;
	Ok(any_array_optional(values))
}
