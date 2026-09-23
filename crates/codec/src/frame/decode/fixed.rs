// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::BooleanArray;
use arrow_buffer::{BooleanBuffer, bit_util::get_bit};
use reifydb_value::{
	encoding::LeBytes,
	value::{
		container::{
			decimal_array::{int16_array, uint16_array},
			dictionary_array::dictionary_array,
			temporal_array::{date_array, datetime_array, duration_array, time_array},
			uuid_array::{identity_id_array, uuid4_array, uuid7_array},
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		frame::data::FrameColumnData,
		identity::IdentityId,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

use super::column_type_from_code;
use crate::{
	error::DecodeError,
	frame::encoding::{
		delta::{
			decode_delta_f32, decode_delta_f64, decode_delta_i8, decode_delta_i16, decode_delta_i32,
			decode_delta_i64, decode_delta_i128, decode_delta_rle_f32, decode_delta_rle_f64,
			decode_delta_rle_i8, decode_delta_rle_i16, decode_delta_rle_i32, decode_delta_rle_i64,
			decode_delta_rle_i128, decode_delta_rle_u8, decode_delta_rle_u16, decode_delta_rle_u32,
			decode_delta_rle_u64, decode_delta_rle_u128, decode_delta_u8, decode_delta_u16,
			decode_delta_u32, decode_delta_u64, decode_delta_u128,
		},
		rle::{decode_rle, decode_rle_i32, decode_rle_i64, decode_rle_u64},
	},
};

pub(crate) fn decode_fixed_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Option<Result<FrameColumnData, DecodeError>> {
	let ty = match column_type_from_code(type_code) {
		Ok(ty) => ty,
		Err(e) => return Some(Err(e)),
	};

	let result =
		match ty {
			ValueType::Boolean => match packed_bits(data, row_count) {
				Ok(()) => {
					let bits = BooleanBuffer::collect_bool(row_count, |i| get_bit(data, i));
					Ok(FrameColumnData::Bool(BooleanArray::from(bits)))
				}
				Err(e) => Err(e),
			},
			ValueType::Float4 => decode_le_array::<f32>(data, row_count)
				.map(|values| FrameColumnData::Float4(values.into())),
			ValueType::Float8 => decode_le_array::<f64>(data, row_count)
				.map(|values| FrameColumnData::Float8(values.into())),
			ValueType::Int1 => decode_le_array::<i8>(data, row_count)
				.map(|values| FrameColumnData::Int1(values.into())),
			ValueType::Int2 => decode_le_array::<i16>(data, row_count)
				.map(|values| FrameColumnData::Int2(values.into())),
			ValueType::Int4 => decode_le_array::<i32>(data, row_count)
				.map(|values| FrameColumnData::Int4(values.into())),
			ValueType::Int8 => decode_le_array::<i64>(data, row_count)
				.map(|values| FrameColumnData::Int8(values.into())),
			ValueType::Int16 => decode_le_array::<i128>(data, row_count)
				.map(|values| FrameColumnData::Int16(int16_array(values))),
			ValueType::Uint1 => decode_le_array::<u8>(data, row_count)
				.map(|values| FrameColumnData::Uint1(values.into())),
			ValueType::Uint2 => decode_le_array::<u16>(data, row_count)
				.map(|values| FrameColumnData::Uint2(values.into())),
			ValueType::Uint4 => decode_le_array::<u32>(data, row_count)
				.map(|values| FrameColumnData::Uint4(values.into())),
			ValueType::Uint8 => decode_le_array::<u64>(data, row_count)
				.map(|values| FrameColumnData::Uint8(values.into())),
			ValueType::Uint16 => decode_le_array::<u128>(data, row_count)
				.map(|values| FrameColumnData::Uint16(uint16_array(values))),
			ValueType::Date => decode_date_plain(data, row_count),
			ValueType::DateTime => decode_datetime_plain(data, row_count),
			ValueType::Time => decode_time_plain(data, row_count),
			ValueType::Duration => decode_duration_plain(data, row_count),
			ValueType::IdentityId => decode_le_array::<IdentityId>(data, row_count)
				.map(|values| FrameColumnData::IdentityId(identity_id_array(values))),
			ValueType::Uuid4 => decode_le_array::<Uuid4>(data, row_count)
				.map(|values| FrameColumnData::Uuid4(uuid4_array(values))),
			ValueType::Uuid7 => decode_le_array::<Uuid7>(data, row_count)
				.map(|values| FrameColumnData::Uuid7(uuid7_array(values))),
			ValueType::DictionaryId => decode_dictionary_ids(data, row_count),
			_ => return None,
		};

	Some(result)
}

pub(crate) fn decode_rle_column(type_code: u8, row_count: usize, data: &[u8]) -> Result<FrameColumnData, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	match ty {
		ValueType::Int1 => {
			let values = decode_rle(data, row_count, 1, |b| b[0] as i8)?;
			Ok(FrameColumnData::Int1(values.into()))
		}
		ValueType::Int2 => {
			let values = decode_rle(data, row_count, 2, |b| i16::from_le_bytes([b[0], b[1]]))?;
			Ok(FrameColumnData::Int2(values.into()))
		}
		ValueType::Int4 => {
			let values = decode_rle_i32(data, row_count)?;
			Ok(FrameColumnData::Int4(values.into()))
		}
		ValueType::Int8 => {
			let values = decode_rle_i64(data, row_count)?;
			Ok(FrameColumnData::Int8(values.into()))
		}
		ValueType::Uint1 => {
			let values = decode_rle(data, row_count, 1, |b| b[0])?;
			Ok(FrameColumnData::Uint1(values.into()))
		}
		ValueType::Uint2 => {
			let values = decode_rle(data, row_count, 2, |b| u16::from_le_bytes([b[0], b[1]]))?;
			Ok(FrameColumnData::Uint2(values.into()))
		}
		ValueType::Uint4 => {
			let values = decode_rle(data, row_count, 4, |b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))?;
			Ok(FrameColumnData::Uint4(values.into()))
		}
		ValueType::Uint8 => {
			let values = decode_rle_u64(data, row_count)?;
			Ok(FrameColumnData::Uint8(values.into()))
		}
		ValueType::Int16 => {
			let values = decode_rle(data, row_count, 16, |b| {
				i128::from_le_bytes([
					b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
					b[12], b[13], b[14], b[15],
				])
			})?;
			Ok(FrameColumnData::Int16(int16_array(values)))
		}
		ValueType::Uint16 => {
			let values = decode_rle(data, row_count, 16, |b| {
				u128::from_le_bytes([
					b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
					b[12], b[13], b[14], b[15],
				])
			})?;
			Ok(FrameColumnData::Uint16(uint16_array(values)))
		}
		ValueType::Float4 => {
			let values = decode_rle(data, row_count, 4, |b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))?;
			Ok(FrameColumnData::Float4(values.into()))
		}
		ValueType::Float8 => {
			let values = decode_rle(data, row_count, 8, |b| {
				f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
			})?;
			Ok(FrameColumnData::Float8(values.into()))
		}
		ValueType::Date => {
			let raw = decode_rle_i32(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|d| {
					Date::from_days_since_epoch(d).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid date days: {}", d))
					})
				})
				.collect();
			Ok(FrameColumnData::Date(date_array(values?)))
		}
		ValueType::DateTime => {
			let raw = decode_rle_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(FrameColumnData::DateTime(datetime_array(values)))
		}
		ValueType::Time => {
			let raw = decode_rle_u64(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|n| {
					Time::from_nanos_since_midnight(n).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid time nanos: {}", n))
					})
				})
				.collect();
			Ok(FrameColumnData::Time(time_array(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("RLE not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_delta_column(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	match ty {
		ValueType::Int1 => {
			let values = decode_delta_i8(data, row_count)?;
			Ok(FrameColumnData::Int1(values.into()))
		}
		ValueType::Int2 => {
			let values = decode_delta_i16(data, row_count)?;
			Ok(FrameColumnData::Int2(values.into()))
		}
		ValueType::Int4 => {
			let values = decode_delta_i32(data, row_count)?;
			Ok(FrameColumnData::Int4(values.into()))
		}
		ValueType::Int8 => {
			let values = decode_delta_i64(data, row_count)?;
			Ok(FrameColumnData::Int8(values.into()))
		}
		ValueType::Uint1 => {
			let values = decode_delta_u8(data, row_count)?;
			Ok(FrameColumnData::Uint1(values.into()))
		}
		ValueType::Uint2 => {
			let values = decode_delta_u16(data, row_count)?;
			Ok(FrameColumnData::Uint2(values.into()))
		}
		ValueType::Uint4 => {
			let values = decode_delta_u32(data, row_count)?;
			Ok(FrameColumnData::Uint4(values.into()))
		}
		ValueType::Uint8 => {
			let values = decode_delta_u64(data, row_count)?;
			Ok(FrameColumnData::Uint8(values.into()))
		}
		ValueType::Int16 => {
			let values = decode_delta_i128(data, row_count)?;
			Ok(FrameColumnData::Int16(int16_array(values)))
		}
		ValueType::Uint16 => {
			let values = decode_delta_u128(data, row_count)?;
			Ok(FrameColumnData::Uint16(uint16_array(values)))
		}
		ValueType::Float4 => {
			let values = decode_delta_f32(data, row_count)?;
			Ok(FrameColumnData::Float4(values.into()))
		}
		ValueType::Float8 => {
			let values = decode_delta_f64(data, row_count)?;
			Ok(FrameColumnData::Float8(values.into()))
		}
		ValueType::Date => {
			let raw = decode_delta_i32(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|d| {
					Date::from_days_since_epoch(d).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid date days: {}", d))
					})
				})
				.collect();
			Ok(FrameColumnData::Date(date_array(values?)))
		}
		ValueType::DateTime => {
			let raw = decode_delta_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(FrameColumnData::DateTime(datetime_array(values)))
		}
		ValueType::Time => {
			let raw = decode_delta_u64(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|n| {
					Time::from_nanos_since_midnight(n).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid time nanos: {}", n))
					})
				})
				.collect();
			Ok(FrameColumnData::Time(time_array(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("Delta not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_delta_rle_column(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	match ty {
		ValueType::Int1 => {
			let values = decode_delta_rle_i8(data, row_count)?;
			Ok(FrameColumnData::Int1(values.into()))
		}
		ValueType::Int2 => {
			let values = decode_delta_rle_i16(data, row_count)?;
			Ok(FrameColumnData::Int2(values.into()))
		}
		ValueType::Int4 => {
			let values = decode_delta_rle_i32(data, row_count)?;
			Ok(FrameColumnData::Int4(values.into()))
		}
		ValueType::Int8 => {
			let values = decode_delta_rle_i64(data, row_count)?;
			Ok(FrameColumnData::Int8(values.into()))
		}
		ValueType::Uint1 => {
			let values = decode_delta_rle_u8(data, row_count)?;
			Ok(FrameColumnData::Uint1(values.into()))
		}
		ValueType::Uint2 => {
			let values = decode_delta_rle_u16(data, row_count)?;
			Ok(FrameColumnData::Uint2(values.into()))
		}
		ValueType::Uint4 => {
			let values = decode_delta_rle_u32(data, row_count)?;
			Ok(FrameColumnData::Uint4(values.into()))
		}
		ValueType::Uint8 => {
			let values = decode_delta_rle_u64(data, row_count)?;
			Ok(FrameColumnData::Uint8(values.into()))
		}
		ValueType::Int16 => {
			let values = decode_delta_rle_i128(data, row_count)?;
			Ok(FrameColumnData::Int16(int16_array(values)))
		}
		ValueType::Uint16 => {
			let values = decode_delta_rle_u128(data, row_count)?;
			Ok(FrameColumnData::Uint16(uint16_array(values)))
		}
		ValueType::Float4 => {
			let values = decode_delta_rle_f32(data, row_count)?;
			Ok(FrameColumnData::Float4(values.into()))
		}
		ValueType::Float8 => {
			let values = decode_delta_rle_f64(data, row_count)?;
			Ok(FrameColumnData::Float8(values.into()))
		}
		ValueType::Date => {
			let raw = decode_delta_rle_i32(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|d| {
					Date::from_days_since_epoch(d).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid date days: {}", d))
					})
				})
				.collect();
			Ok(FrameColumnData::Date(date_array(values?)))
		}
		ValueType::DateTime => {
			let raw = decode_delta_rle_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(FrameColumnData::DateTime(datetime_array(values)))
		}
		ValueType::Time => {
			let raw = decode_delta_rle_u64(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|n| {
					Time::from_nanos_since_midnight(n).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid time nanos: {}", n))
					})
				})
				.collect();
			Ok(FrameColumnData::Time(time_array(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("DeltaRLE not supported for type {:?}", ty))),
	}
}

fn decode_le_array<T: LeBytes>(data: &[u8], row_count: usize) -> Result<Vec<T>, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		values.push(T::read_le(fixed_slot(data, i, T::ENCODED_SIZE)?));
	}
	Ok(values)
}

fn packed_bits(data: &[u8], row_count: usize) -> Result<(), DecodeError> {
	let expected = row_count.div_ceil(8);
	if data.len() < expected {
		return Err(DecodeError::UnexpectedEof {
			expected,
			available: data.len(),
		});
	}
	Ok(())
}

fn fixed_slot(data: &[u8], index: usize, width: usize) -> Result<&[u8], DecodeError> {
	let start = index * width;
	let end = start + width;
	data.get(start..end).ok_or(DecodeError::UnexpectedEof {
		expected: end,
		available: data.len(),
	})
}

fn decode_date_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let days = i32::read_le(fixed_slot(data, i, Date::ENCODED_SIZE)?);
		let date = Date::from_days_since_epoch(days)
			.ok_or_else(|| DecodeError::InvalidData(format!("invalid date days: {}", days)))?;
		values.push(date);
	}
	Ok(FrameColumnData::Date(date_array(values)))
}

fn decode_datetime_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		values.push(DateTime::read_le(fixed_slot(data, i, DateTime::ENCODED_SIZE)?));
	}
	Ok(FrameColumnData::DateTime(datetime_array(values)))
}

fn decode_time_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let nanos = u64::read_le(fixed_slot(data, i, Time::ENCODED_SIZE)?);
		let time = Time::from_nanos_since_midnight(nanos)
			.ok_or_else(|| DecodeError::InvalidData(format!("invalid time nanos: {}", nanos)))?;
		values.push(time);
	}
	Ok(FrameColumnData::Time(time_array(values)))
}

fn decode_duration_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let slot = fixed_slot(data, i, Duration::ENCODED_SIZE)?;
		let months = i32::read_le(slot);
		let days = i32::read_le(&slot[i32::ENCODED_SIZE..]);
		let nanos = i64::read_le(&slot[2 * i32::ENCODED_SIZE..]);
		let dur = Duration::new(months, days, nanos)
			.map_err(|e| DecodeError::InvalidData(format!("invalid duration: {}", e)))?;
		values.push(dur);
	}
	Ok(FrameColumnData::Duration(duration_array(values)))
}

fn decode_dictionary_ids(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	if row_count == 0 {
		return Ok(FrameColumnData::DictionaryId {
			container: dictionary_array([]),
			dictionary_id: None,
		});
	}
	let disc = *data.first().ok_or(DecodeError::UnexpectedEof {
		expected: 1,
		available: 0,
	})?;
	let width = match disc {
		1 | 2 | 4 | 8 | 16 => disc as usize,
		_ => {
			return Err(DecodeError::InvalidData(format!("invalid dictionary discriminator: {}", disc)));
		}
	};
	let rows = &data[1..];
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let slot = fixed_slot(rows, i, width)?;
		values.push(match disc {
			1 => DictionaryEntryId::U1(u8::read_le(slot)),
			2 => DictionaryEntryId::U2(u16::read_le(slot)),
			4 => DictionaryEntryId::U4(u32::read_le(slot)),
			8 => DictionaryEntryId::U8(u64::read_le(slot)),
			_ => DictionaryEntryId::U16(u128::read_le(slot)),
		});
	}
	Ok(FrameColumnData::DictionaryId {
		container: dictionary_array(values),
		dictionary_id: None,
	})
}
