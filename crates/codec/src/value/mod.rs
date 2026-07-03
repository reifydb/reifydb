// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, str::from_utf8};

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use reifydb_value::{
	params::Params,
	value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use uuid::Uuid;

use crate::{
	error::{DecodeError, EncodeError},
	reader::Reader,
	tag::ValueKind,
	typeinfo::{decode_value_type, encode_value_type},
};

pub fn encode_value(value: &Value) -> Result<Vec<u8>, EncodeError> {
	let mut buf = Vec::new();
	encode_value_into(value, &mut buf)?;
	Ok(buf)
}

pub fn encode_value_into(value: &Value, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	buf.push(ValueKind::of_value(value).byte());
	match value {
		Value::None {
			inner,
		} => encode_value_type(inner, buf)?,
		Value::Boolean(b) => buf.push(*b as u8),
		Value::Float4(f) => buf.extend_from_slice(&f.to_le_bytes()),
		Value::Float8(f) => buf.extend_from_slice(&f.to_le_bytes()),
		Value::Int1(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int2(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int4(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int8(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Utf8(s) => encode_len_prefixed(s.as_bytes(), buf),
		Value::Uint1(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint2(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint4(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint8(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint16(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Date(d) => buf.extend_from_slice(&d.to_days_since_epoch().to_le_bytes()),
		Value::DateTime(dt) => buf.extend_from_slice(&dt.to_nanos().to_le_bytes()),
		Value::Time(t) => buf.extend_from_slice(&t.to_nanos_since_midnight().to_le_bytes()),
		Value::Duration(d) => {
			buf.extend_from_slice(&d.get_months().to_le_bytes());
			buf.extend_from_slice(&d.get_days().to_le_bytes());
			buf.extend_from_slice(&d.get_nanos().to_le_bytes());
		}
		Value::IdentityId(id) => buf.extend_from_slice(id.0.0.as_bytes()),
		Value::Uuid4(u) => buf.extend_from_slice(u.0.as_bytes()),
		Value::Uuid7(u) => buf.extend_from_slice(u.0.as_bytes()),
		Value::Blob(b) => encode_len_prefixed(b.as_bytes(), buf),
		Value::Int(v) => encode_len_prefixed(&v.0.to_signed_bytes_le(), buf),
		Value::Uint(v) => encode_len_prefixed(&v.0.to_signed_bytes_le(), buf),
		Value::Decimal(v) => encode_len_prefixed(v.to_string().as_bytes(), buf),
		Value::Any(inner) => encode_value_into(inner, buf)?,
		Value::DictionaryId(id) => match id {
			DictionaryEntryId::U1(v) => {
				buf.push(1);
				buf.extend_from_slice(&v.to_le_bytes());
			}
			DictionaryEntryId::U2(v) => {
				buf.push(2);
				buf.extend_from_slice(&v.to_le_bytes());
			}
			DictionaryEntryId::U4(v) => {
				buf.push(4);
				buf.extend_from_slice(&v.to_le_bytes());
			}
			DictionaryEntryId::U8(v) => {
				buf.push(8);
				buf.extend_from_slice(&v.to_le_bytes());
			}
			DictionaryEntryId::U16(v) => {
				buf.push(16);
				buf.extend_from_slice(&v.to_le_bytes());
			}
		},
		Value::Type(ty) => encode_value_type(ty, buf)?,
		Value::List(items) | Value::Tuple(items) => {
			buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
			for item in items {
				encode_value_into(item, buf)?;
			}
		}
		Value::Record(fields) => {
			buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
			for (key, field_value) in fields {
				encode_len_prefixed(key.as_bytes(), buf);
				encode_value_into(field_value, buf)?;
			}
		}
	}
	Ok(())
}

fn encode_len_prefixed(bytes: &[u8], buf: &mut Vec<u8>) {
	buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
	buf.extend_from_slice(bytes);
}

pub fn decode_value(bytes: &[u8]) -> Result<Value, DecodeError> {
	let mut r = Reader::new(bytes);
	let value = decode_value_from(&mut r)?;
	if !r.is_empty() {
		return Err(DecodeError::TrailingBytes(r.remaining()));
	}
	Ok(value)
}

pub fn decode_value_from(r: &mut Reader) -> Result<Value, DecodeError> {
	let byte = r.u8()?;
	let kind = ValueKind::from_byte(byte).ok_or(DecodeError::UnknownTypeCode(byte))?;
	match kind {
		ValueKind::None => Ok(Value::None {
			inner: decode_value_type(r)?,
		}),
		ValueKind::Boolean => Ok(Value::Boolean(r.u8()? != 0)),
		ValueKind::Float4 => OrderedF32::try_from(r.f32()?)
			.map(Value::Float4)
			.map_err(|e| DecodeError::InvalidData(format!("invalid float4: {e}"))),
		ValueKind::Float8 => OrderedF64::try_from(r.f64()?)
			.map(Value::Float8)
			.map_err(|e| DecodeError::InvalidData(format!("invalid float8: {e}"))),
		ValueKind::Int1 => Ok(Value::Int1(r.i8()?)),
		ValueKind::Int2 => Ok(Value::Int2(r.i16()?)),
		ValueKind::Int4 => Ok(Value::Int4(r.i32()?)),
		ValueKind::Int8 => Ok(Value::Int8(r.i64()?)),
		ValueKind::Int16 => Ok(Value::Int16(r.i128()?)),
		ValueKind::Utf8 => Ok(Value::Utf8(decode_len_prefixed_str(r)?.to_string())),
		ValueKind::Uint1 => Ok(Value::Uint1(r.u8()?)),
		ValueKind::Uint2 => Ok(Value::Uint2(r.u16()?)),
		ValueKind::Uint4 => Ok(Value::Uint4(r.u32()?)),
		ValueKind::Uint8 => Ok(Value::Uint8(r.u64()?)),
		ValueKind::Uint16 => Ok(Value::Uint16(r.u128()?)),
		ValueKind::Date => {
			let days = r.i32()?;
			Date::from_days_since_epoch(days)
				.map(Value::Date)
				.ok_or_else(|| DecodeError::InvalidData(format!("invalid date: {days}")))
		}
		ValueKind::DateTime => Ok(Value::DateTime(DateTime::from_nanos(r.u64()?))),
		ValueKind::Time => {
			let nanos = r.u64()?;
			Time::from_nanos_since_midnight(nanos)
				.map(Value::Time)
				.ok_or_else(|| DecodeError::InvalidData(format!("invalid time: {nanos}")))
		}
		ValueKind::Duration => {
			let months = r.i32()?;
			let days = r.i32()?;
			let nanos = r.i64()?;
			Duration::new(months, days, nanos)
				.map(Value::Duration)
				.map_err(|e| DecodeError::InvalidData(format!("invalid duration: {e}")))
		}
		ValueKind::IdentityId => Ok(Value::IdentityId(IdentityId::new(Uuid7(decode_uuid(r)?)))),
		ValueKind::Uuid4 => Ok(Value::Uuid4(Uuid4(decode_uuid(r)?))),
		ValueKind::Uuid7 => Ok(Value::Uuid7(Uuid7(decode_uuid(r)?))),
		ValueKind::Blob => Ok(Value::Blob(Blob::new(decode_len_prefixed_bytes(r)?.to_vec()))),
		ValueKind::Int => Ok(Value::Int(Int(BigInt::from_signed_bytes_le(decode_len_prefixed_bytes(r)?)))),
		ValueKind::Uint => Ok(Value::Uint(Uint(BigInt::from_signed_bytes_le(decode_len_prefixed_bytes(r)?)))),
		ValueKind::Decimal => {
			let s = decode_len_prefixed_str(r)?;
			let dec: BigDecimal =
				s.parse().map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {e}")))?;
			Ok(Value::Decimal(Decimal::new(dec)))
		}
		ValueKind::Any => Ok(Value::Any(Box::new(decode_value_from(r)?))),
		ValueKind::DictionaryId => {
			let width = r.u8()?;
			let id = match width {
				1 => DictionaryEntryId::U1(r.u8()?),
				2 => DictionaryEntryId::U2(r.u16()?),
				4 => DictionaryEntryId::U4(r.u32()?),
				8 => DictionaryEntryId::U8(r.u64()?),
				16 => DictionaryEntryId::U16(r.u128()?),
				other => {
					return Err(DecodeError::InvalidData(format!(
						"invalid dictionary id width: {other}"
					)));
				}
			};
			Ok(Value::DictionaryId(id))
		}
		ValueKind::Type => Ok(Value::Type(decode_value_type(r)?)),
		ValueKind::List => Ok(Value::List(decode_value_sequence(r)?)),
		ValueKind::Tuple => Ok(Value::Tuple(decode_value_sequence(r)?)),
		ValueKind::Record => {
			let count = r.u32()?;
			let mut fields = Vec::with_capacity((count as usize).min(4096));
			for _ in 0..count {
				let key = decode_len_prefixed_str(r)?.to_string();
				fields.push((key, decode_value_from(r)?));
			}
			Ok(Value::Record(fields))
		}
	}
}

fn decode_value_sequence(r: &mut Reader) -> Result<Vec<Value>, DecodeError> {
	let count = r.u32()?;
	let mut items = Vec::with_capacity((count as usize).min(4096));
	for _ in 0..count {
		items.push(decode_value_from(r)?);
	}
	Ok(items)
}

fn decode_len_prefixed_bytes<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], DecodeError> {
	let len = r.u32()? as usize;
	r.take(len)
}

fn decode_len_prefixed_str<'a>(r: &mut Reader<'a>) -> Result<&'a str, DecodeError> {
	from_utf8(decode_len_prefixed_bytes(r)?).map_err(|e| DecodeError::InvalidData(format!("invalid UTF-8: {e}")))
}

fn decode_uuid(r: &mut Reader) -> Result<Uuid, DecodeError> {
	Ok(Uuid::from_bytes(r.take(16)?.try_into().unwrap()))
}

pub fn encode_params(params: &Params) -> Result<Vec<u8>, EncodeError> {
	let mut buf = Vec::new();
	match params {
		Params::None => buf.push(0),
		Params::Positional(values) => {
			buf.push(1);
			buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
			for value in values.iter() {
				encode_value_into(value, &mut buf)?;
			}
		}
		Params::Named(map) => {
			buf.push(2);
			buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
			for (key, value) in map.iter() {
				encode_len_prefixed(key.as_bytes(), &mut buf);
				encode_value_into(value, &mut buf)?;
			}
		}
	}
	Ok(buf)
}

pub fn decode_params(bytes: &[u8]) -> Result<Params, DecodeError> {
	let mut r = Reader::new(bytes);
	let discriminant = r.u8()?;
	let params = match discriminant {
		0 => Params::None,
		1 => {
			let count = r.u32()?;
			let mut values = Vec::with_capacity((count as usize).min(4096));
			for _ in 0..count {
				values.push(decode_value_from(&mut r)?);
			}
			Params::Positional(values.into())
		}
		2 => {
			let count = r.u32()?;
			let mut map = HashMap::with_capacity((count as usize).min(4096));
			for _ in 0..count {
				let key = decode_len_prefixed_str(&mut r)?.to_string();
				map.insert(key, decode_value_from(&mut r)?);
			}
			Params::Named(map.into())
		}
		other => {
			return Err(DecodeError::InvalidData(format!("invalid params discriminant: {other}")));
		}
	};
	if !r.is_empty() {
		return Err(DecodeError::TrailingBytes(r.remaining()));
	}
	Ok(params)
}
