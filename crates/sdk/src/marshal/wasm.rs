// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM flat binary marshalling and unmarshalling for columnar data
//!
//! Provides `marshal_columns_to_bytes` and `unmarshal_columns_from_bytes` which
//! convert between `Columns` and a flat `Vec<u8>` using u32 offsets (no pointers),
//! suitable for passing through WASM linear memory.

use std::mem::size_of;

use postcard::from_bytes;
use reifydb_abi::data::{
	column::ColumnTypeCode,
	wasm::{COLUMNS_WASM_HEADER_SIZE, COLUMN_WASM_SIZE, ColumnWasm, ColumnsWasm},
};
use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		Value,
		blob::Blob,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		is::IsNumber,
		row_number::RowNumber,
		r#type::Type,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::util::column_data_to_type_code;

/// Marshal `Columns` into a flat binary buffer suitable for WASM linear memory.
pub fn marshal_columns_to_bytes(columns: &Columns) -> Vec<u8> {
	let row_count = columns.row_count();
	let column_count = columns.len();

	// Pre-allocate with header + column descriptors
	let header_total = COLUMNS_WASM_HEADER_SIZE + column_count * COLUMN_WASM_SIZE;
	let mut buf: Vec<u8> = vec![0u8; header_total];

	// Marshal row numbers
	let (rn_offset, rn_len) = if !columns.row_numbers.is_empty() {
		let offset = header_total as u32;
		for rn in columns.row_numbers.iter() {
			let val: u64 = (*rn).into();
			buf.extend_from_slice(&val.to_le_bytes());
		}
		let len = (buf.len() - header_total) as u32;
		(offset, len)
	} else {
		(0u32, 0u32)
	};

	// Marshal each column
	let mut col_descriptors: Vec<ColumnWasm> = Vec::with_capacity(column_count);

	for col in columns.iter() {
		// Name
		let name_bytes = col.name.text().as_bytes();
		let name_offset = buf.len() as u32;
		buf.extend_from_slice(name_bytes);
		let name_len = name_bytes.len() as u32;

		let data = col.data();
		let data_row_count = data.len() as u32;
		let type_code = column_data_to_type_code(data) as u32;

		// Unwrap Option to get inner data + bitvec
		let (inner_data, opt_bitvec) = data.unwrap_option();

		// Bitvec
		let (bitvec_offset, bitvec_len) = if let Some(bv) = opt_bitvec {
			marshal_bitvec_to_buf(&mut buf, bv)
		} else if data_row_count > 0 {
			// All defined — write all-ones bitvec
			let all_ones = BitVec::repeat(data_row_count as usize, true);
			marshal_bitvec_to_buf(&mut buf, &all_ones)
		} else {
			(0u32, 0u32)
		};

		// Data + offsets
		let (data_offset, data_len, offsets_offset, offsets_len) = marshal_column_data_bytes_to_buf(&mut buf, inner_data);

		col_descriptors.push(ColumnWasm {
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

	// Write ColumnsWasm header at position 0
	let header = ColumnsWasm {
		row_count: row_count as u32,
		column_count: column_count as u32,
		row_numbers_offset: rn_offset,
		row_numbers_len: rn_len,
	};
	// Overwrite bytes 0..16
	let mut hdr_buf = Vec::with_capacity(COLUMNS_WASM_HEADER_SIZE);
	header.write_to_bytes(&mut hdr_buf);
	buf[..COLUMNS_WASM_HEADER_SIZE].copy_from_slice(&hdr_buf);

	// Write column descriptors at positions 16..16+N*40
	for (i, desc) in col_descriptors.iter().enumerate() {
		let offset = COLUMNS_WASM_HEADER_SIZE + i * COLUMN_WASM_SIZE;
		desc.write_at(&mut buf, offset);
	}

	buf
}

/// Unmarshal `Columns` from a flat binary buffer.
pub fn unmarshal_columns_from_bytes(bytes: &[u8]) -> Columns {
	if bytes.len() < COLUMNS_WASM_HEADER_SIZE {
		return Columns::empty();
	}

	let header = ColumnsWasm::read_from_bytes(bytes);
	let row_count = header.row_count as usize;
	let column_count = header.column_count as usize;

	if row_count == 0 && column_count == 0 {
		return Columns::empty();
	}

	// Unmarshal row numbers
	let row_numbers: Vec<RowNumber> = if header.row_numbers_offset > 0 && header.row_numbers_len > 0 {
		let start = header.row_numbers_offset as usize;
		let end = start + header.row_numbers_len as usize;
		let rn_bytes = &bytes[start..end];
		rn_bytes
			.chunks_exact(8)
			.map(|chunk| {
				let val = u64::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]]);
				RowNumber(val)
			})
			.collect()
	} else {
		Vec::new()
	};

	// Unmarshal columns
	let mut columns: Vec<Column> = Vec::with_capacity(column_count);
	for i in 0..column_count {
		let desc_start = COLUMNS_WASM_HEADER_SIZE + i * COLUMN_WASM_SIZE;
		let desc = ColumnWasm::read_from_bytes(&bytes[desc_start..]);

		// Name
		let name = if desc.name_len > 0 {
			let start = desc.name_offset as usize;
			let end = start + desc.name_len as usize;
			let s = std::str::from_utf8(&bytes[start..end]).unwrap_or("");
			Fragment::internal(s)
		} else {
			Fragment::internal("")
		};

		let data_row_count = desc.data_row_count as usize;
		let type_code = type_code_from_u32(desc.type_code);

		// Bitvec
		let bitvec = if desc.bitvec_len > 0 {
			let start = desc.bitvec_offset as usize;
			let end = start + desc.bitvec_len as usize;
			unmarshal_bitvec_from_bytes(&bytes[start..end], data_row_count)
		} else {
			BitVec::repeat(data_row_count, true)
		};

		// Data slice
		let data_slice = if desc.data_len > 0 {
			let start = desc.data_offset as usize;
			let end = start + desc.data_len as usize;
			&bytes[start..end]
		} else {
			&[]
		};

		// Offsets slice
		let offsets_slice = if desc.offsets_len > 0 {
			let start = desc.offsets_offset as usize;
			let end = start + desc.offsets_len as usize;
			&bytes[start..end]
		} else {
			&[]
		};

		let data = unmarshal_column_data(type_code, data_row_count, data_slice, bitvec, offsets_slice);

		columns.push(Column { name, data });
	}

	if row_numbers.is_empty() {
		Columns::new(columns)
	} else {
		Columns::with_row_numbers(columns, row_numbers)
	}
}

// ============================================================================
// Bitvec helpers
// ============================================================================

fn marshal_bitvec_to_buf(buf: &mut Vec<u8>, bitvec: &BitVec) -> (u32, u32) {
	let len = bitvec.len();
	if len == 0 {
		return (0, 0);
	}

	let byte_count = (len + 7) / 8;
	let offset = buf.len() as u32;

	// Zero-initialize
	buf.resize(buf.len() + byte_count, 0);
	let start = offset as usize;

	for (i, bit) in bitvec.iter().enumerate() {
		if bit {
			buf[start + i / 8] |= 1 << (i % 8);
		}
	}

	(offset, byte_count as u32)
}

fn unmarshal_bitvec_from_bytes(bytes: &[u8], len: usize) -> BitVec {
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
	BitVec::from_slice(&bits)
}

// ============================================================================
// Column data marshalling (to buf)
// ============================================================================

/// Marshal column data bytes + offsets into buf, returning (data_offset, data_len, offsets_offset, offsets_len).
fn marshal_column_data_bytes_to_buf(buf: &mut Vec<u8>, data: &ColumnData) -> (u32, u32, u32, u32) {
	match data {
		ColumnData::Bool(container) => {
			let len = container.len();
			if len == 0 {
				return (0, 0, 0, 0);
			}
			let byte_count = (len + 7) / 8;
			let offset = buf.len() as u32;
			buf.resize(buf.len() + byte_count, 0);
			let start = offset as usize;
			for i in 0..len {
				if let Some(val) = container.get(i) {
					if val {
						buf[start + i / 8] |= 1 << (i % 8);
					}
				}
			}
			(offset, byte_count as u32, 0, 0)
		}

		ColumnData::Float4(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Float8(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Int1(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Int2(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Int4(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Int8(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Int16(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Uint1(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Uint2(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Uint4(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Uint8(container) => marshal_numeric_to_buf(buf, &**container),
		ColumnData::Uint16(container) => marshal_numeric_to_buf(buf, &**container),

		ColumnData::Date(container) => {
			let dates: &[Date] = &**container;
			let encoded: Vec<i32> = dates.iter().map(|d| d.to_days_since_epoch()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ColumnData::DateTime(container) => {
			let datetimes: &[DateTime] = &**container;
			let encoded: Vec<i64> = datetimes.iter().map(|dt| dt.timestamp()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ColumnData::Time(container) => {
			let times: &[Time] = &**container;
			let encoded: Vec<u64> = times.iter().map(|t| t.to_nanos_since_midnight()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}
		ColumnData::Duration(container) => {
			let durations: &[Duration] = &**container;
			marshal_serialized_to_buf(buf, durations)
		}

		ColumnData::IdentityId(container) => {
			let ids: &[IdentityId] = &**container;
			let bytes: Vec<u8> = ids.iter().flat_map(|id| id.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}
		ColumnData::Uuid4(container) => {
			let uuids: &[Uuid4] = &**container;
			let bytes: Vec<u8> = uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}
		ColumnData::Uuid7(container) => {
			let uuids: &[Uuid7] = &**container;
			let bytes: Vec<u8> = uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
			marshal_raw_bytes_to_buf(buf, &bytes)
		}

		ColumnData::Utf8 { container, .. } => {
			let strings: &[String] = &**container;
			marshal_strings_to_buf(buf, strings)
		}
		ColumnData::Blob { container, .. } => {
			let blobs: &[Blob] = &**container;
			marshal_blobs_to_buf(buf, blobs)
		}

		ColumnData::Int { container, .. } => {
			let values: &[Int] = &**container;
			marshal_serialized_to_buf(buf, values)
		}
		ColumnData::Uint { container, .. } => {
			let values: &[Uint] = &**container;
			marshal_serialized_to_buf(buf, values)
		}
		ColumnData::Decimal { container, .. } => {
			let values: &[reifydb_type::value::decimal::Decimal] = &**container;
			marshal_serialized_to_buf(buf, values)
		}
		ColumnData::Any(container) => {
			let mut offsets: Vec<u64> = Vec::with_capacity(container.len() + 1);
			let mut data_bytes: Vec<u8> = Vec::new();
			offsets.push(0);
			for i in 0..container.len() {
				let value = container.get(i);
				let serialized = postcard::to_allocvec(&value).unwrap_or_default();
				data_bytes.extend_from_slice(&serialized);
				offsets.push(data_bytes.len() as u64);
			}
			marshal_data_with_offsets_to_buf(buf, &data_bytes, &offsets)
		}

		ColumnData::DictionaryId(container) => {
			let encoded: Vec<u128> = container.data().iter().map(|id| id.to_u128()).collect();
			marshal_numeric_to_buf(buf, &encoded)
		}

		ColumnData::Option { inner, .. } => marshal_column_data_bytes_to_buf(buf, inner),
	}
}

/// Marshal a numeric slice into buf. Returns (data_offset, data_len, 0, 0).
fn marshal_numeric_to_buf<T: Copy>(buf: &mut Vec<u8>, slice: &[T]) -> (u32, u32, u32, u32) {
	let byte_len = slice.len() * size_of::<T>();
	if byte_len == 0 {
		return (0, 0, 0, 0);
	}
	let offset = buf.len() as u32;
	let src = slice.as_ptr() as *const u8;
	buf.extend_from_slice(unsafe { std::slice::from_raw_parts(src, byte_len) });
	(offset, byte_len as u32, 0, 0)
}

/// Marshal raw bytes into buf. Returns (data_offset, data_len, 0, 0).
fn marshal_raw_bytes_to_buf(buf: &mut Vec<u8>, data: &[u8]) -> (u32, u32, u32, u32) {
	if data.is_empty() {
		return (0, 0, 0, 0);
	}
	let offset = buf.len() as u32;
	buf.extend_from_slice(data);
	(offset, data.len() as u32, 0, 0)
}

/// Marshal strings with offsets into buf.
fn marshal_strings_to_buf(buf: &mut Vec<u8>, strings: &[String]) -> (u32, u32, u32, u32) {
	let mut offsets: Vec<u64> = Vec::with_capacity(strings.len() + 1);
	let mut data: Vec<u8> = Vec::new();
	offsets.push(0);
	for s in strings {
		data.extend_from_slice(s.as_bytes());
		offsets.push(data.len() as u64);
	}
	marshal_data_with_offsets_to_buf(buf, &data, &offsets)
}

/// Marshal blobs with offsets into buf.
fn marshal_blobs_to_buf(buf: &mut Vec<u8>, blobs: &[Blob]) -> (u32, u32, u32, u32) {
	let mut offsets: Vec<u64> = Vec::with_capacity(blobs.len() + 1);
	let mut data: Vec<u8> = Vec::new();
	offsets.push(0);
	for blob in blobs {
		data.extend_from_slice(blob.as_bytes());
		offsets.push(data.len() as u64);
	}
	marshal_data_with_offsets_to_buf(buf, &data, &offsets)
}

/// Marshal serialized values with offsets into buf.
fn marshal_serialized_to_buf<T: Serialize>(buf: &mut Vec<u8>, values: &[T]) -> (u32, u32, u32, u32) {
	let mut offsets: Vec<u64> = Vec::with_capacity(values.len() + 1);
	let mut data: Vec<u8> = Vec::new();
	offsets.push(0);
	for value in values {
		let serialized = postcard::to_allocvec(value).unwrap_or_default();
		data.extend_from_slice(&serialized);
		offsets.push(data.len() as u64);
	}
	marshal_data_with_offsets_to_buf(buf, &data, &offsets)
}

/// Append data bytes and offset array to buf.
/// Returns (data_offset, data_len, offsets_offset, offsets_len).
fn marshal_data_with_offsets_to_buf(buf: &mut Vec<u8>, data: &[u8], offsets: &[u64]) -> (u32, u32, u32, u32) {
	let data_offset = buf.len() as u32;
	buf.extend_from_slice(data);
	let data_len = data.len() as u32;

	let offsets_offset = buf.len() as u32;
	let offsets_byte_len = offsets.len() * size_of::<u64>();
	let src = offsets.as_ptr() as *const u8;
	buf.extend_from_slice(unsafe { std::slice::from_raw_parts(src, offsets_byte_len) });
	let offsets_len = offsets_byte_len as u32;

	(data_offset, data_len, offsets_offset, offsets_len)
}

// ============================================================================
// Column data unmarshalling (from bytes)
// ============================================================================

fn type_code_from_u32(v: u32) -> ColumnTypeCode {
	match v {
		0 => ColumnTypeCode::Bool,
		1 => ColumnTypeCode::Float4,
		2 => ColumnTypeCode::Float8,
		3 => ColumnTypeCode::Int1,
		4 => ColumnTypeCode::Int2,
		5 => ColumnTypeCode::Int4,
		6 => ColumnTypeCode::Int8,
		7 => ColumnTypeCode::Int16,
		8 => ColumnTypeCode::Uint1,
		9 => ColumnTypeCode::Uint2,
		10 => ColumnTypeCode::Uint4,
		11 => ColumnTypeCode::Uint8,
		12 => ColumnTypeCode::Uint16,
		13 => ColumnTypeCode::Utf8,
		14 => ColumnTypeCode::Date,
		15 => ColumnTypeCode::DateTime,
		16 => ColumnTypeCode::Time,
		17 => ColumnTypeCode::Duration,
		18 => ColumnTypeCode::IdentityId,
		19 => ColumnTypeCode::Uuid4,
		20 => ColumnTypeCode::Uuid7,
		21 => ColumnTypeCode::Blob,
		22 => ColumnTypeCode::Int,
		23 => ColumnTypeCode::Uint,
		24 => ColumnTypeCode::Decimal,
		25 => ColumnTypeCode::Any,
		26 => ColumnTypeCode::DictionaryId,
		_ => ColumnTypeCode::Undefined,
	}
}

fn unmarshal_column_data(
	type_code: ColumnTypeCode,
	row_count: usize,
	data: &[u8],
	bitvec: BitVec,
	offsets_bytes: &[u8],
) -> ColumnData {
	if row_count == 0 {
		return ColumnData::none_typed(Type::Any, 0);
	}

	let inner = match type_code {
		ColumnTypeCode::Bool => {
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
			ColumnData::Bool(BoolContainer::new(values))
		}
		ColumnTypeCode::Float4 => ColumnData::Float4(unmarshal_numeric::<f32>(data, row_count)),
		ColumnTypeCode::Float8 => ColumnData::Float8(unmarshal_numeric::<f64>(data, row_count)),
		ColumnTypeCode::Int1 => ColumnData::Int1(unmarshal_numeric::<i8>(data, row_count)),
		ColumnTypeCode::Int2 => ColumnData::Int2(unmarshal_numeric::<i16>(data, row_count)),
		ColumnTypeCode::Int4 => ColumnData::Int4(unmarshal_numeric::<i32>(data, row_count)),
		ColumnTypeCode::Int8 => ColumnData::Int8(unmarshal_numeric::<i64>(data, row_count)),
		ColumnTypeCode::Int16 => ColumnData::Int16(unmarshal_numeric::<i128>(data, row_count)),
		ColumnTypeCode::Uint1 => ColumnData::Uint1(unmarshal_numeric::<u8>(data, row_count)),
		ColumnTypeCode::Uint2 => ColumnData::Uint2(unmarshal_numeric::<u16>(data, row_count)),
		ColumnTypeCode::Uint4 => ColumnData::Uint4(unmarshal_numeric::<u32>(data, row_count)),
		ColumnTypeCode::Uint8 => ColumnData::Uint8(unmarshal_numeric::<u64>(data, row_count)),
		ColumnTypeCode::Uint16 => ColumnData::Uint16(unmarshal_numeric::<u128>(data, row_count)),
		ColumnTypeCode::Utf8 => {
			let container = unmarshal_utf8(data, row_count, offsets_bytes);
			ColumnData::Utf8 {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Date => ColumnData::Date(unmarshal_date(data, row_count)),
		ColumnTypeCode::DateTime => ColumnData::DateTime(unmarshal_datetime(data, row_count)),
		ColumnTypeCode::Time => ColumnData::Time(unmarshal_time(data, row_count)),
		ColumnTypeCode::Duration => ColumnData::Duration(unmarshal_duration(data, row_count, offsets_bytes)),
		ColumnTypeCode::IdentityId => ColumnData::IdentityId(unmarshal_identity_id(data, row_count)),
		ColumnTypeCode::Uuid4 => ColumnData::Uuid4(unmarshal_uuid4(data, row_count)),
		ColumnTypeCode::Uuid7 => ColumnData::Uuid7(unmarshal_uuid7(data, row_count)),
		ColumnTypeCode::Blob => {
			let container = unmarshal_blob(data, row_count, offsets_bytes);
			ColumnData::Blob {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Int => {
			let container = unmarshal_serialized::<Int>(data, row_count, offsets_bytes);
			ColumnData::Int {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Uint => {
			let container = unmarshal_serialized::<Uint>(data, row_count, offsets_bytes);
			ColumnData::Uint {
				container,
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Decimal => {
			let container =
				unmarshal_serialized::<reifydb_type::value::decimal::Decimal>(data, row_count, offsets_bytes);
			ColumnData::Decimal {
				container,
				precision: Precision::MAX,
				scale: Scale::MIN,
			}
		}
		ColumnTypeCode::Any => ColumnData::Any(unmarshal_any(data, row_count, offsets_bytes)),
		ColumnTypeCode::DictionaryId => {
			let u128_container = unmarshal_numeric::<u128>(data, row_count);
			let entries: Vec<DictionaryEntryId> = u128_container.iter().map(|v| DictionaryEntryId::U16(v.unwrap_or_default())).collect();
			ColumnData::DictionaryId(DictionaryContainer::new(entries))
		}
		ColumnTypeCode::Undefined => return ColumnData::none_typed(Type::Any, row_count),
	};

	// Wrap in Option if bitvec has any false (null) values
	maybe_wrap_option(inner, bitvec)
}

// ============================================================================
// Unmarshal helpers
// ============================================================================

fn read_offsets(bytes: &[u8]) -> Vec<u64> {
	bytes
		.chunks_exact(size_of::<u64>())
		.map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
		.collect()
}

/// Wrap ColumnData in Option if bitvec has any false (null) entries.
fn maybe_wrap_option(inner: ColumnData, bitvec: BitVec) -> ColumnData {
	let has_nulls = bitvec.iter().any(|b| !b);
	if has_nulls {
		ColumnData::Option {
			inner: Box::new(inner),
			bitvec,
		}
	} else {
		inner
	}
}

fn unmarshal_numeric<T: Copy + Default + IsNumber>(data: &[u8], row_count: usize) -> NumberContainer<T> {
	if data.is_empty() {
		return NumberContainer::new(vec![T::default(); row_count]);
	}
	let count = data.len() / size_of::<T>();
	let mut values = vec![T::default(); count];
	unsafe {
		std::ptr::copy_nonoverlapping(data.as_ptr(), values.as_mut_ptr() as *mut u8, count * size_of::<T>());
	}
	NumberContainer::new(values)
}

fn unmarshal_utf8(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> Utf8Container {
	if data.is_empty() || offsets_bytes.is_empty() {
		return Utf8Container::new(vec![String::new(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut strings = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let s = std::str::from_utf8(&data[start..end]).unwrap_or("").to_string();
		strings.push(s);
	}
	Utf8Container::new(strings)
}

fn unmarshal_date(data: &[u8], row_count: usize) -> TemporalContainer<Date> {
	if data.is_empty() {
		return TemporalContainer::new(vec![Date::default(); row_count]);
	}
	let count = data.len() / size_of::<i32>();
	let mut raw = vec![0i32; count];
	unsafe {
		std::ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr() as *mut u8, count * size_of::<i32>());
	}
	let dates: Vec<Date> = raw.iter().map(|&days| Date::from_days_since_epoch(days).unwrap_or_default()).collect();
	TemporalContainer::new(dates)
}

fn unmarshal_datetime(data: &[u8], row_count: usize) -> TemporalContainer<DateTime> {
	if data.is_empty() {
		return TemporalContainer::new(vec![DateTime::default(); row_count]);
	}
	let count = data.len() / size_of::<i64>();
	let mut raw = vec![0i64; count];
	unsafe {
		std::ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr() as *mut u8, count * size_of::<i64>());
	}
	let datetimes: Vec<DateTime> = raw.iter().map(|&ts| DateTime::from_timestamp(ts).unwrap_or_default()).collect();
	TemporalContainer::new(datetimes)
}

fn unmarshal_time(data: &[u8], row_count: usize) -> TemporalContainer<Time> {
	if data.is_empty() {
		return TemporalContainer::new(vec![Time::default(); row_count]);
	}
	let count = data.len() / size_of::<u64>();
	let mut raw = vec![0u64; count];
	unsafe {
		std::ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr() as *mut u8, count * size_of::<u64>());
	}
	let times: Vec<Time> = raw.iter().map(|&ns| Time::from_nanos_since_midnight(ns).unwrap_or_default()).collect();
	TemporalContainer::new(times)
}

fn unmarshal_duration(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> TemporalContainer<Duration> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return TemporalContainer::new(vec![Duration::default(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut durations = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let duration: Duration = from_bytes(&data[start..end]).unwrap_or_default();
		durations.push(duration);
	}
	TemporalContainer::new(durations)
}

fn unmarshal_identity_id(data: &[u8], row_count: usize) -> IdentityIdContainer {
	if data.is_empty() {
		return IdentityIdContainer::new(vec![IdentityId::default(); row_count]);
	}
	let ids: Vec<IdentityId> = data
		.chunks(16)
		.map(|chunk| {
			let mut arr = [0u8; 16];
			arr.copy_from_slice(chunk);
			IdentityId(Uuid7(Uuid::from_bytes(arr)))
		})
		.collect();
	IdentityIdContainer::new(ids)
}

fn unmarshal_uuid4(data: &[u8], row_count: usize) -> UuidContainer<Uuid4> {
	if data.is_empty() {
		return UuidContainer::new(vec![Uuid4::default(); row_count]);
	}
	let uuids: Vec<Uuid4> = data
		.chunks(16)
		.map(|chunk| {
			let mut arr = [0u8; 16];
			arr.copy_from_slice(chunk);
			Uuid4(Uuid::from_bytes(arr))
		})
		.collect();
	UuidContainer::new(uuids)
}

fn unmarshal_uuid7(data: &[u8], row_count: usize) -> UuidContainer<Uuid7> {
	if data.is_empty() {
		return UuidContainer::new(vec![Uuid7::default(); row_count]);
	}
	let uuids: Vec<Uuid7> = data
		.chunks(16)
		.map(|chunk| {
			let mut arr = [0u8; 16];
			arr.copy_from_slice(chunk);
			Uuid7(Uuid::from_bytes(arr))
		})
		.collect();
	UuidContainer::new(uuids)
}

fn unmarshal_blob(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> BlobContainer {
	if data.is_empty() || offsets_bytes.is_empty() {
		return BlobContainer::new(vec![Blob::empty(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut blobs = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		blobs.push(Blob::new(data[start..end].to_vec()));
	}
	BlobContainer::new(blobs)
}

fn unmarshal_serialized<T: Default + Clone + DeserializeOwned + IsNumber>(
	data: &[u8],
	row_count: usize,
	offsets_bytes: &[u8],
) -> NumberContainer<T> {
	if data.is_empty() || offsets_bytes.is_empty() {
		return NumberContainer::new(vec![T::default(); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let value: T = from_bytes(&data[start..end]).unwrap_or_default();
		values.push(value);
	}
	NumberContainer::new(values)
}

fn unmarshal_any(data: &[u8], row_count: usize, offsets_bytes: &[u8]) -> AnyContainer {
	if data.is_empty() || offsets_bytes.is_empty() {
		return AnyContainer::new(vec![Box::new(Value::none()); row_count]);
	}
	let offsets = read_offsets(offsets_bytes);
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		let value: Value = postcard::from_bytes(&data[start..end]).unwrap_or_else(|_| Value::none());
		values.push(Box::new(value));
	}
	AnyContainer::new(values)
}
