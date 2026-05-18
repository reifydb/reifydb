// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		container::{
			bool::BoolContainer, dictionary::DictionaryContainer, identity_id::IdentityIdContainer,
			number::NumberContainer, temporal::TemporalContainer, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		frame::data::FrameColumnData,
		identity::IdentityId,
		time::Time,
		r#type::Type,
		uuid::{Uuid4, Uuid7},
	},
};
use uuid::Uuid;

use crate::{
	encoding::{
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
	error::DecodeError,
};

pub(crate) fn decode_fixed_plain(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Option<Result<FrameColumnData, DecodeError>> {
	let ty = Type::from_u8(type_code);

	let result = match ty {
		Type::Boolean => {
			let bv = decode_bitvec(data, row_count);
			Ok(FrameColumnData::Bool(BoolContainer::new(bv.to_vec())))
		}
		Type::Float4 => {
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				values.push(f32::from_le_bytes([
					data[i * 4],
					data[i * 4 + 1],
					data[i * 4 + 2],
					data[i * 4 + 3],
				]));
			}
			Ok(FrameColumnData::Float4(NumberContainer::new(values)))
		}
		Type::Float8 => {
			let values = decode_fixed_array::<f64, 8>(data, row_count, f64::from_le_bytes);
			Ok(FrameColumnData::Float8(NumberContainer::new(values)))
		}
		Type::Int1 => {
			let values: Vec<i8> = data[..row_count].iter().map(|&b| b as i8).collect();
			Ok(FrameColumnData::Int1(NumberContainer::new(values)))
		}
		Type::Int2 => {
			let values = decode_fixed_array::<i16, 2>(data, row_count, i16::from_le_bytes);
			Ok(FrameColumnData::Int2(NumberContainer::new(values)))
		}
		Type::Int4 => {
			let values = decode_fixed_array::<i32, 4>(data, row_count, i32::from_le_bytes);
			Ok(FrameColumnData::Int4(NumberContainer::new(values)))
		}
		Type::Int8 => {
			let values = decode_fixed_array::<i64, 8>(data, row_count, i64::from_le_bytes);
			Ok(FrameColumnData::Int8(NumberContainer::new(values)))
		}
		Type::Int16 => {
			let values = decode_fixed_array::<i128, 16>(data, row_count, i128::from_le_bytes);
			Ok(FrameColumnData::Int16(NumberContainer::new(values)))
		}
		Type::Uint1 => {
			let values: Vec<u8> = data[..row_count].to_vec();
			Ok(FrameColumnData::Uint1(NumberContainer::new(values)))
		}
		Type::Uint2 => {
			let values = decode_fixed_array::<u16, 2>(data, row_count, u16::from_le_bytes);
			Ok(FrameColumnData::Uint2(NumberContainer::new(values)))
		}
		Type::Uint4 => {
			let values = decode_fixed_array::<u32, 4>(data, row_count, u32::from_le_bytes);
			Ok(FrameColumnData::Uint4(NumberContainer::new(values)))
		}
		Type::Uint8 => {
			let values = decode_fixed_array::<u64, 8>(data, row_count, u64::from_le_bytes);
			Ok(FrameColumnData::Uint8(NumberContainer::new(values)))
		}
		Type::Uint16 => {
			let values = decode_fixed_array::<u128, 16>(data, row_count, u128::from_le_bytes);
			Ok(FrameColumnData::Uint16(NumberContainer::new(values)))
		}
		Type::Date => decode_date_plain(data, row_count),
		Type::DateTime => decode_datetime_plain(data, row_count),
		Type::Time => decode_time_plain(data, row_count),
		Type::Duration => decode_duration_plain(data, row_count),
		Type::IdentityId => {
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let off = i * 16;
				let mut bytes = [0u8; 16];
				bytes.copy_from_slice(&data[off..off + 16]);
				let uuid = Uuid::from_bytes(bytes);
				values.push(IdentityId::new(Uuid7(uuid)));
			}
			Ok(FrameColumnData::IdentityId(IdentityIdContainer::new(values)))
		}
		Type::Uuid4 => {
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let off = i * 16;
				let mut bytes = [0u8; 16];
				bytes.copy_from_slice(&data[off..off + 16]);
				values.push(Uuid4(Uuid::from_bytes(bytes)));
			}
			Ok(FrameColumnData::Uuid4(UuidContainer::new(values)))
		}
		Type::Uuid7 => {
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let off = i * 16;
				let mut bytes = [0u8; 16];
				bytes.copy_from_slice(&data[off..off + 16]);
				values.push(Uuid7(Uuid::from_bytes(bytes)));
			}
			Ok(FrameColumnData::Uuid7(UuidContainer::new(values)))
		}
		Type::DictionaryId => decode_dictionary_ids(data, row_count),
		_ => return None,
	};

	Some(result)
}

pub(crate) fn decode_rle_column(type_code: u8, row_count: usize, data: &[u8]) -> Result<FrameColumnData, DecodeError> {
	let ty = Type::from_u8(type_code);
	match ty {
		Type::Int1 => {
			let values = decode_rle(data, row_count, 1, |b| b[0] as i8)?;
			Ok(FrameColumnData::Int1(NumberContainer::new(values)))
		}
		Type::Int2 => {
			let values = decode_rle(data, row_count, 2, |b| i16::from_le_bytes([b[0], b[1]]))?;
			Ok(FrameColumnData::Int2(NumberContainer::new(values)))
		}
		Type::Int4 => {
			let values = decode_rle_i32(data, row_count)?;
			Ok(FrameColumnData::Int4(NumberContainer::new(values)))
		}
		Type::Int8 => {
			let values = decode_rle_i64(data, row_count)?;
			Ok(FrameColumnData::Int8(NumberContainer::new(values)))
		}
		Type::Uint1 => {
			let values = decode_rle(data, row_count, 1, |b| b[0])?;
			Ok(FrameColumnData::Uint1(NumberContainer::new(values)))
		}
		Type::Uint2 => {
			let values = decode_rle(data, row_count, 2, |b| u16::from_le_bytes([b[0], b[1]]))?;
			Ok(FrameColumnData::Uint2(NumberContainer::new(values)))
		}
		Type::Uint4 => {
			let values = decode_rle(data, row_count, 4, |b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))?;
			Ok(FrameColumnData::Uint4(NumberContainer::new(values)))
		}
		Type::Uint8 => {
			let values = decode_rle_u64(data, row_count)?;
			Ok(FrameColumnData::Uint8(NumberContainer::new(values)))
		}
		Type::Int16 => {
			let values = decode_rle(data, row_count, 16, |b| {
				i128::from_le_bytes([
					b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
					b[12], b[13], b[14], b[15],
				])
			})?;
			Ok(FrameColumnData::Int16(NumberContainer::new(values)))
		}
		Type::Uint16 => {
			let values = decode_rle(data, row_count, 16, |b| {
				u128::from_le_bytes([
					b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
					b[12], b[13], b[14], b[15],
				])
			})?;
			Ok(FrameColumnData::Uint16(NumberContainer::new(values)))
		}
		Type::Float4 => {
			let values = decode_rle(data, row_count, 4, |b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))?;
			Ok(FrameColumnData::Float4(NumberContainer::new(values)))
		}
		Type::Float8 => {
			let values = decode_rle(data, row_count, 8, |b| {
				f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
			})?;
			Ok(FrameColumnData::Float8(NumberContainer::new(values)))
		}
		Type::Date => {
			let raw = decode_rle_i32(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|d| {
					Date::from_days_since_epoch(d).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid date days: {}", d))
					})
				})
				.collect();
			Ok(FrameColumnData::Date(TemporalContainer::new(values?)))
		}
		Type::DateTime => {
			let raw = decode_rle_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(FrameColumnData::DateTime(TemporalContainer::new(values)))
		}
		Type::Time => {
			let raw = decode_rle_u64(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|n| {
					Time::from_nanos_since_midnight(n).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid time nanos: {}", n))
					})
				})
				.collect();
			Ok(FrameColumnData::Time(TemporalContainer::new(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("RLE not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_delta_column(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let ty = Type::from_u8(type_code);
	match ty {
		Type::Int1 => {
			let values = decode_delta_i8(data, row_count)?;
			Ok(FrameColumnData::Int1(NumberContainer::new(values)))
		}
		Type::Int2 => {
			let values = decode_delta_i16(data, row_count)?;
			Ok(FrameColumnData::Int2(NumberContainer::new(values)))
		}
		Type::Int4 => {
			let values = decode_delta_i32(data, row_count)?;
			Ok(FrameColumnData::Int4(NumberContainer::new(values)))
		}
		Type::Int8 => {
			let values = decode_delta_i64(data, row_count)?;
			Ok(FrameColumnData::Int8(NumberContainer::new(values)))
		}
		Type::Uint1 => {
			let values = decode_delta_u8(data, row_count)?;
			Ok(FrameColumnData::Uint1(NumberContainer::new(values)))
		}
		Type::Uint2 => {
			let values = decode_delta_u16(data, row_count)?;
			Ok(FrameColumnData::Uint2(NumberContainer::new(values)))
		}
		Type::Uint4 => {
			let values = decode_delta_u32(data, row_count)?;
			Ok(FrameColumnData::Uint4(NumberContainer::new(values)))
		}
		Type::Uint8 => {
			let values = decode_delta_u64(data, row_count)?;
			Ok(FrameColumnData::Uint8(NumberContainer::new(values)))
		}
		Type::Int16 => {
			let values = decode_delta_i128(data, row_count)?;
			Ok(FrameColumnData::Int16(NumberContainer::new(values)))
		}
		Type::Uint16 => {
			let values = decode_delta_u128(data, row_count)?;
			Ok(FrameColumnData::Uint16(NumberContainer::new(values)))
		}
		Type::Float4 => {
			let values = decode_delta_f32(data, row_count)?;
			Ok(FrameColumnData::Float4(NumberContainer::new(values)))
		}
		Type::Float8 => {
			let values = decode_delta_f64(data, row_count)?;
			Ok(FrameColumnData::Float8(NumberContainer::new(values)))
		}
		Type::Date => {
			let raw = decode_delta_i32(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|d| {
					Date::from_days_since_epoch(d).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid date days: {}", d))
					})
				})
				.collect();
			Ok(FrameColumnData::Date(TemporalContainer::new(values?)))
		}
		Type::DateTime => {
			let raw = decode_delta_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(FrameColumnData::DateTime(TemporalContainer::new(values)))
		}
		Type::Time => {
			let raw = decode_delta_u64(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|n| {
					Time::from_nanos_since_midnight(n).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid time nanos: {}", n))
					})
				})
				.collect();
			Ok(FrameColumnData::Time(TemporalContainer::new(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("Delta not supported for type {:?}", ty))),
	}
}

pub(crate) fn decode_delta_rle_column(
	type_code: u8,
	row_count: usize,
	data: &[u8],
) -> Result<FrameColumnData, DecodeError> {
	let ty = Type::from_u8(type_code);
	match ty {
		Type::Int1 => {
			let values = decode_delta_rle_i8(data, row_count)?;
			Ok(FrameColumnData::Int1(NumberContainer::new(values)))
		}
		Type::Int2 => {
			let values = decode_delta_rle_i16(data, row_count)?;
			Ok(FrameColumnData::Int2(NumberContainer::new(values)))
		}
		Type::Int4 => {
			let values = decode_delta_rle_i32(data, row_count)?;
			Ok(FrameColumnData::Int4(NumberContainer::new(values)))
		}
		Type::Int8 => {
			let values = decode_delta_rle_i64(data, row_count)?;
			Ok(FrameColumnData::Int8(NumberContainer::new(values)))
		}
		Type::Uint1 => {
			let values = decode_delta_rle_u8(data, row_count)?;
			Ok(FrameColumnData::Uint1(NumberContainer::new(values)))
		}
		Type::Uint2 => {
			let values = decode_delta_rle_u16(data, row_count)?;
			Ok(FrameColumnData::Uint2(NumberContainer::new(values)))
		}
		Type::Uint4 => {
			let values = decode_delta_rle_u32(data, row_count)?;
			Ok(FrameColumnData::Uint4(NumberContainer::new(values)))
		}
		Type::Uint8 => {
			let values = decode_delta_rle_u64(data, row_count)?;
			Ok(FrameColumnData::Uint8(NumberContainer::new(values)))
		}
		Type::Int16 => {
			let values = decode_delta_rle_i128(data, row_count)?;
			Ok(FrameColumnData::Int16(NumberContainer::new(values)))
		}
		Type::Uint16 => {
			let values = decode_delta_rle_u128(data, row_count)?;
			Ok(FrameColumnData::Uint16(NumberContainer::new(values)))
		}
		Type::Float4 => {
			let values = decode_delta_rle_f32(data, row_count)?;
			Ok(FrameColumnData::Float4(NumberContainer::new(values)))
		}
		Type::Float8 => {
			let values = decode_delta_rle_f64(data, row_count)?;
			Ok(FrameColumnData::Float8(NumberContainer::new(values)))
		}
		Type::Date => {
			let raw = decode_delta_rle_i32(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|d| {
					Date::from_days_since_epoch(d).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid date days: {}", d))
					})
				})
				.collect();
			Ok(FrameColumnData::Date(TemporalContainer::new(values?)))
		}
		Type::DateTime => {
			let raw = decode_delta_rle_u64(data, row_count)?;
			let values: Vec<_> = raw.into_iter().map(DateTime::from_nanos).collect();
			Ok(FrameColumnData::DateTime(TemporalContainer::new(values)))
		}
		Type::Time => {
			let raw = decode_delta_rle_u64(data, row_count)?;
			let values: Result<Vec<_>, _> = raw
				.into_iter()
				.map(|n| {
					Time::from_nanos_since_midnight(n).ok_or_else(|| {
						DecodeError::InvalidData(format!("invalid time nanos: {}", n))
					})
				})
				.collect();
			Ok(FrameColumnData::Time(TemporalContainer::new(values?)))
		}
		_ => Err(DecodeError::InvalidData(format!("DeltaRLE not supported for type {:?}", ty))),
	}
}

fn decode_fixed_array<T, const SIZE: usize>(data: &[u8], row_count: usize, from_bytes: fn([u8; SIZE]) -> T) -> Vec<T> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let off = i * SIZE;
		let mut bytes = [0u8; SIZE];
		bytes.copy_from_slice(&data[off..off + SIZE]);
		values.push(from_bytes(bytes));
	}
	values
}

fn decode_bitvec(data: &[u8], len: usize) -> BitVec {
	BitVec::from_raw(data.to_vec(), len)
}

fn decode_date_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let days = i32::from_le_bytes([data[i * 4], data[i * 4 + 1], data[i * 4 + 2], data[i * 4 + 3]]);
		let date = Date::from_days_since_epoch(days)
			.ok_or_else(|| DecodeError::InvalidData(format!("invalid date days: {}", days)))?;
		values.push(date);
	}
	Ok(FrameColumnData::Date(TemporalContainer::new(values)))
}

fn decode_datetime_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let nanos = u64::from_le_bytes([
			data[i * 8],
			data[i * 8 + 1],
			data[i * 8 + 2],
			data[i * 8 + 3],
			data[i * 8 + 4],
			data[i * 8 + 5],
			data[i * 8 + 6],
			data[i * 8 + 7],
		]);
		values.push(DateTime::from_nanos(nanos));
	}
	Ok(FrameColumnData::DateTime(TemporalContainer::new(values)))
}

fn decode_time_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let nanos = u64::from_le_bytes([
			data[i * 8],
			data[i * 8 + 1],
			data[i * 8 + 2],
			data[i * 8 + 3],
			data[i * 8 + 4],
			data[i * 8 + 5],
			data[i * 8 + 6],
			data[i * 8 + 7],
		]);
		let time = Time::from_nanos_since_midnight(nanos)
			.ok_or_else(|| DecodeError::InvalidData(format!("invalid time nanos: {}", nanos)))?;
		values.push(time);
	}
	Ok(FrameColumnData::Time(TemporalContainer::new(values)))
}

fn decode_duration_plain(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	let mut values = Vec::with_capacity(row_count);
	for i in 0..row_count {
		let off = i * 16;
		let months = i32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
		let days = i32::from_le_bytes([data[off + 4], data[off + 5], data[off + 6], data[off + 7]]);
		let nanos = i64::from_le_bytes([
			data[off + 8],
			data[off + 9],
			data[off + 10],
			data[off + 11],
			data[off + 12],
			data[off + 13],
			data[off + 14],
			data[off + 15],
		]);
		let dur = Duration::new(months, days, nanos)
			.map_err(|e| DecodeError::InvalidData(format!("invalid duration: {}", e)))?;
		values.push(dur);
	}
	Ok(FrameColumnData::Duration(TemporalContainer::new(values)))
}

fn decode_dictionary_ids(data: &[u8], row_count: usize) -> Result<FrameColumnData, DecodeError> {
	if row_count == 0 || data.is_empty() {
		return Ok(FrameColumnData::DictionaryId(DictionaryContainer::new(vec![])));
	}
	let disc = data[0];
	let mut values = Vec::with_capacity(row_count);
	let mut pos = 1;
	for _ in 0..row_count {
		let id = match disc {
			1 => {
				let v = data[pos];
				pos += 1;
				DictionaryEntryId::U1(v)
			}
			2 => {
				let v = u16::from_le_bytes([data[pos], data[pos + 1]]);
				pos += 2;
				DictionaryEntryId::U2(v)
			}
			4 => {
				let v = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
				pos += 4;
				DictionaryEntryId::U4(v)
			}
			8 => {
				let v = u64::from_le_bytes([
					data[pos],
					data[pos + 1],
					data[pos + 2],
					data[pos + 3],
					data[pos + 4],
					data[pos + 5],
					data[pos + 6],
					data[pos + 7],
				]);
				pos += 8;
				DictionaryEntryId::U8(v)
			}
			16 => {
				let v = u128::from_le_bytes([
					data[pos],
					data[pos + 1],
					data[pos + 2],
					data[pos + 3],
					data[pos + 4],
					data[pos + 5],
					data[pos + 6],
					data[pos + 7],
					data[pos + 8],
					data[pos + 9],
					data[pos + 10],
					data[pos + 11],
					data[pos + 12],
					data[pos + 13],
					data[pos + 14],
					data[pos + 15],
				]);
				pos += 16;
				DictionaryEntryId::U16(v)
			}
			_ => {
				return Err(DecodeError::InvalidData(format!(
					"invalid dictionary discriminator: {}",
					disc
				)));
			}
		};
		values.push(id);
	}
	Ok(FrameColumnData::DictionaryId(DictionaryContainer::new(values)))
}
