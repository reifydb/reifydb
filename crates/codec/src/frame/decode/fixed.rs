// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{
	Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
	UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, bit_util::get_bit, i256};
use reifydb_value::{
	encoding::LeBytes,
	value::{
		container::{
			decimal_array::DecimalArray,
			dictionary_array::dictionary_array,
			temporal_array::{date_array, datetime_array, duration_array, time_array},
			uuid_array::{identity_id_array, uuid4_array, uuid7_array},
			wide_int_array::wide_array,
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

use super::column_type_from_code;
use crate::{
	error::DecodeError,
	frame::{
		encoding::{
			delta::{
				decode_delta_f32, decode_delta_f64, decode_delta_i8, decode_delta_i16,
				decode_delta_i32, decode_delta_i64, decode_delta_i128, decode_delta_i256,
				decode_delta_rle_f32, decode_delta_rle_f64, decode_delta_rle_i8, decode_delta_rle_i16,
				decode_delta_rle_i32, decode_delta_rle_i64, decode_delta_rle_i128,
				decode_delta_rle_i256, decode_delta_rle_u8, decode_delta_rle_u16, decode_delta_rle_u32,
				decode_delta_rle_u64, decode_delta_rle_u128, decode_delta_u8, decode_delta_u16,
				decode_delta_u32, decode_delta_u64, decode_delta_u128,
			},
			rle::{decode_rle, decode_rle_i32, decode_rle_i64, decode_rle_u64},
		},
		format::Encoding,
	},
	tag::ValueKind,
	unscaled::{NARROW, WIDE, check_unscaled, decode_params, family_type, read_le, width},
};

pub(crate) fn decode_fixed_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Option<Result<ArrayRef, DecodeError>> {
	let ty = match column_type_from_code(type_code) {
		Ok(ty) => ty,
		Err(e) => return Some(Err(e)),
	};

	let result =
		match ty {
			ValueType::Boolean => match packed_bits(data, row_count) {
				Ok(()) => {
					let bits = BooleanBuffer::collect_bool(row_count, |i| get_bit(data, i));
					Ok(array_ref(BooleanArray::from(bits)))
				}
				Err(e) => Err(e),
			},
			ValueType::Float4 => decode_le_array::<f32>(data, row_count)
				.map(|values| array_ref(Float32Array::from(values))),
			ValueType::Float8 => decode_le_array::<f64>(data, row_count)
				.map(|values| array_ref(Float64Array::from(values))),
			ValueType::Int1 => {
				decode_le_array::<i8>(data, row_count).map(|values| array_ref(Int8Array::from(values)))
			}
			ValueType::Int2 => decode_le_array::<i16>(data, row_count)
				.map(|values| array_ref(Int16Array::from(values))),
			ValueType::Int4 => decode_le_array::<i32>(data, row_count)
				.map(|values| array_ref(Int32Array::from(values))),
			ValueType::Int8 => decode_le_array::<i64>(data, row_count)
				.map(|values| array_ref(Int64Array::from(values))),
			ValueType::Int16 => {
				decode_le_array::<i128>(data, row_count).map(|values| array_ref(wide_array(values)))
			}
			ValueType::Uint1 => {
				decode_le_array::<u8>(data, row_count).map(|values| array_ref(UInt8Array::from(values)))
			}
			ValueType::Uint2 => decode_le_array::<u16>(data, row_count)
				.map(|values| array_ref(UInt16Array::from(values))),
			ValueType::Uint4 => decode_le_array::<u32>(data, row_count)
				.map(|values| array_ref(UInt32Array::from(values))),
			ValueType::Uint8 => decode_le_array::<u64>(data, row_count)
				.map(|values| array_ref(UInt64Array::from(values))),
			ValueType::Uint16 => {
				decode_le_array::<u128>(data, row_count).map(|values| array_ref(wide_array(values)))
			}
			ValueType::Date => decode_date_plain(data, row_count),
			ValueType::DateTime => decode_datetime_plain(data, row_count),
			ValueType::Time => decode_time_plain(data, row_count),
			ValueType::Duration => decode_duration_plain(data, row_count),
			ValueType::IdentityId => decode_le_array::<IdentityId>(data, row_count)
				.map(|values| array_ref(identity_id_array(values))),
			ValueType::Uuid4 => {
				decode_le_array::<Uuid4>(data, row_count).map(|values| array_ref(uuid4_array(values)))
			}
			ValueType::Uuid7 => {
				decode_le_array::<Uuid7>(data, row_count).map(|values| array_ref(uuid7_array(values)))
			}
			ValueType::DictionaryId => decode_dictionary_ids(data, row_count),
			_ => return None,
		};

	Some(result)
}

pub(crate) fn decode_rle_column(type_code: u8, row_count: usize, data: &[u8]) -> Result<ArrayRef, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	match ty {
		ValueType::Int1 => {
			let values = decode_rle(data, row_count, 1, |b| b[0] as i8)?;
			Ok(array_ref(Int8Array::from(values)))
		}
		ValueType::Int2 => {
			let values = decode_rle(data, row_count, 2, |b| i16::from_le_bytes([b[0], b[1]]))?;
			Ok(array_ref(Int16Array::from(values)))
		}
		ValueType::Int4 => {
			let values = decode_rle_i32(data, row_count)?;
			Ok(array_ref(Int32Array::from(values)))
		}
		ValueType::Int8 => {
			let values = decode_rle_i64(data, row_count)?;
			Ok(array_ref(Int64Array::from(values)))
		}
		ValueType::Uint1 => {
			let values = decode_rle(data, row_count, 1, |b| b[0])?;
			Ok(array_ref(UInt8Array::from(values)))
		}
		ValueType::Uint2 => {
			let values = decode_rle(data, row_count, 2, |b| u16::from_le_bytes([b[0], b[1]]))?;
			Ok(array_ref(UInt16Array::from(values)))
		}
		ValueType::Uint4 => {
			let values = decode_rle(data, row_count, 4, |b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))?;
			Ok(array_ref(UInt32Array::from(values)))
		}
		ValueType::Uint8 => {
			let values = decode_rle_u64(data, row_count)?;
			Ok(array_ref(UInt64Array::from(values)))
		}
		ValueType::Int16 => {
			let values = decode_rle(data, row_count, 16, |b| {
				i128::from_le_bytes([
					b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
					b[12], b[13], b[14], b[15],
				])
			})?;
			Ok(array_ref(wide_array(values)))
		}
		ValueType::Uint16 => {
			let values = decode_rle(data, row_count, 16, |b| {
				u128::from_le_bytes([
					b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
					b[12], b[13], b[14], b[15],
				])
			})?;
			Ok(array_ref(wide_array(values)))
		}
		ValueType::Float4 => {
			let values = decode_rle(data, row_count, 4, |b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))?;
			Ok(array_ref(Float32Array::from(values)))
		}
		ValueType::Float8 => {
			let values = decode_rle(data, row_count, 8, |b| {
				f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
			})?;
			Ok(array_ref(Float64Array::from(values)))
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
			Ok(array_ref(date_array(values?)))
		}
		ValueType::DateTime => {
			let raw = decode_rle_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(|n| DateTime::from_nanos(n as i64)).collect();
			Ok(array_ref(datetime_array(values)))
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
			Ok(array_ref(time_array(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("RLE not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_delta_column(type_code: u8, row_count: usize, data: &[u8]) -> Result<ArrayRef, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	match ty {
		ValueType::Int1 => {
			let values = decode_delta_i8(data, row_count)?;
			Ok(array_ref(Int8Array::from(values)))
		}
		ValueType::Int2 => {
			let values = decode_delta_i16(data, row_count)?;
			Ok(array_ref(Int16Array::from(values)))
		}
		ValueType::Int4 => {
			let values = decode_delta_i32(data, row_count)?;
			Ok(array_ref(Int32Array::from(values)))
		}
		ValueType::Int8 => {
			let values = decode_delta_i64(data, row_count)?;
			Ok(array_ref(Int64Array::from(values)))
		}
		ValueType::Uint1 => {
			let values = decode_delta_u8(data, row_count)?;
			Ok(array_ref(UInt8Array::from(values)))
		}
		ValueType::Uint2 => {
			let values = decode_delta_u16(data, row_count)?;
			Ok(array_ref(UInt16Array::from(values)))
		}
		ValueType::Uint4 => {
			let values = decode_delta_u32(data, row_count)?;
			Ok(array_ref(UInt32Array::from(values)))
		}
		ValueType::Uint8 => {
			let values = decode_delta_u64(data, row_count)?;
			Ok(array_ref(UInt64Array::from(values)))
		}
		ValueType::Int16 => {
			let values = decode_delta_i128(data, row_count)?;
			Ok(array_ref(wide_array(values)))
		}
		ValueType::Uint16 => {
			let values = decode_delta_u128(data, row_count)?;
			Ok(array_ref(wide_array(values)))
		}
		ValueType::Float4 => {
			let values = decode_delta_f32(data, row_count)?;
			Ok(array_ref(Float32Array::from(values)))
		}
		ValueType::Float8 => {
			let values = decode_delta_f64(data, row_count)?;
			Ok(array_ref(Float64Array::from(values)))
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
			Ok(array_ref(date_array(values?)))
		}
		ValueType::DateTime => {
			let raw = decode_delta_i64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(array_ref(datetime_array(values)))
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
			Ok(array_ref(time_array(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("Delta not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_delta_rle_column(type_code: u8, row_count: usize, data: &[u8]) -> Result<ArrayRef, DecodeError> {
	let ty = column_type_from_code(type_code)?;
	match ty {
		ValueType::Int1 => {
			let values = decode_delta_rle_i8(data, row_count)?;
			Ok(array_ref(Int8Array::from(values)))
		}
		ValueType::Int2 => {
			let values = decode_delta_rle_i16(data, row_count)?;
			Ok(array_ref(Int16Array::from(values)))
		}
		ValueType::Int4 => {
			let values = decode_delta_rle_i32(data, row_count)?;
			Ok(array_ref(Int32Array::from(values)))
		}
		ValueType::Int8 => {
			let values = decode_delta_rle_i64(data, row_count)?;
			Ok(array_ref(Int64Array::from(values)))
		}
		ValueType::Uint1 => {
			let values = decode_delta_rle_u8(data, row_count)?;
			Ok(array_ref(UInt8Array::from(values)))
		}
		ValueType::Uint2 => {
			let values = decode_delta_rle_u16(data, row_count)?;
			Ok(array_ref(UInt16Array::from(values)))
		}
		ValueType::Uint4 => {
			let values = decode_delta_rle_u32(data, row_count)?;
			Ok(array_ref(UInt32Array::from(values)))
		}
		ValueType::Uint8 => {
			let values = decode_delta_rle_u64(data, row_count)?;
			Ok(array_ref(UInt64Array::from(values)))
		}
		ValueType::Int16 => {
			let values = decode_delta_rle_i128(data, row_count)?;
			Ok(array_ref(wide_array(values)))
		}
		ValueType::Uint16 => {
			let values = decode_delta_rle_u128(data, row_count)?;
			Ok(array_ref(wide_array(values)))
		}
		ValueType::Float4 => {
			let values = decode_delta_rle_f32(data, row_count)?;
			Ok(array_ref(Float32Array::from(values)))
		}
		ValueType::Float8 => {
			let values = decode_delta_rle_f64(data, row_count)?;
			Ok(array_ref(Float64Array::from(values)))
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
			Ok(array_ref(date_array(values?)))
		}
		ValueType::DateTime => {
			let raw = decode_delta_rle_i64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(array_ref(datetime_array(values)))
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
			Ok(array_ref(time_array(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("DeltaRLE not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_unscaled_column(
	kind: ValueKind,
	encoding: Encoding,
	row_count: usize,
	data: &[u8],
	extra: &[u8],
) -> Result<(ValueType, ArrayRef), DecodeError> {
	let &[precision, scale] = extra else {
		return Err(DecodeError::InvalidData(format!(
			"{kind:?} column needs precision and scale in 2 extra bytes, found {}",
			extra.len()
		)));
	};
	let (precision, scale) = decode_params(precision, scale)?;
	let value_type = family_type(kind, precision, scale)?;
	let narrow = width(precision) == NARROW;
	let values: Vec<i256> = match encoding {
		Encoding::Plain => (0..row_count)
			.map(|i| fixed_slot(data, i, width(precision)).map(read_le))
			.collect::<Result<_, _>>()?,
		Encoding::Rle if narrow => decode_rle(data, row_count, NARROW, |b| read_le(&b[..NARROW]))?,
		Encoding::Rle => decode_rle(data, row_count, WIDE, |b| read_le(&b[..WIDE]))?,
		Encoding::Delta if narrow => {
			decode_delta_i128(data, row_count)?.into_iter().map(i256::from_i128).collect()
		}
		Encoding::Delta => decode_delta_i256(data, row_count)?,
		Encoding::DeltaRle if narrow => {
			decode_delta_rle_i128(data, row_count)?.into_iter().map(i256::from_i128).collect()
		}
		Encoding::DeltaRle => decode_delta_rle_i256(data, row_count)?,
		Encoding::Dict | Encoding::BitPack => {
			return Err(DecodeError::InvalidData(format!(
				"{encoding:?} encoding not supported for type {kind:?}"
			)));
		}
	};
	for &value in &values {
		check_unscaled(kind, value, precision)?;
	}
	let array = DecimalArray::from_unscaled(precision, scale, values);
	Ok((value_type, array.into_array()))
}

fn array_ref<A: Array + 'static>(array: A) -> ArrayRef {
	Arc::new(array)
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

fn decode_date_plain(data: &[u8], row_count: usize) -> Result<ArrayRef, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let days = i32::read_le(fixed_slot(data, i, Date::ENCODED_SIZE)?);
		let date = Date::from_days_since_epoch(days)
			.ok_or_else(|| DecodeError::InvalidData(format!("invalid date days: {}", days)))?;
		values.push(date);
	}
	Ok(array_ref(date_array(values)))
}

fn decode_datetime_plain(data: &[u8], row_count: usize) -> Result<ArrayRef, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		values.push(DateTime::read_le(fixed_slot(data, i, DateTime::ENCODED_SIZE)?));
	}
	Ok(array_ref(datetime_array(values)))
}

fn decode_time_plain(data: &[u8], row_count: usize) -> Result<ArrayRef, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let nanos = u64::read_le(fixed_slot(data, i, Time::ENCODED_SIZE)?);
		let time = Time::from_nanos_since_midnight(nanos)
			.ok_or_else(|| DecodeError::InvalidData(format!("invalid time nanos: {}", nanos)))?;
		values.push(time);
	}
	Ok(array_ref(time_array(values)))
}

fn decode_duration_plain(data: &[u8], row_count: usize) -> Result<ArrayRef, DecodeError> {
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
	Ok(array_ref(duration_array(values)))
}

fn decode_dictionary_ids(data: &[u8], row_count: usize) -> Result<ArrayRef, DecodeError> {
	if row_count == 0 {
		return Ok(array_ref(dictionary_array([])));
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
	Ok(array_ref(dictionary_array(values)))
}
