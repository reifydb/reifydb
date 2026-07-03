// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::from_utf8;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use reifydb_value::value::{
	Value, decimal::Decimal, dictionary::DictionaryEntryId, duration::Duration, int::Int, uint::Uint,
};

use crate::{
	error::{DecodeError, EncodeError},
	reader::Reader,
	value::{decode_value, encode_value_into},
};

pub fn encode_int_cell(value: &Int, buf: &mut Vec<u8>) {
	buf.extend_from_slice(&value.0.to_signed_bytes_le());
}

pub fn decode_int_cell(bytes: &[u8]) -> Int {
	Int(BigInt::from_signed_bytes_le(bytes))
}

pub fn encode_uint_cell(value: &Uint, buf: &mut Vec<u8>) {
	buf.extend_from_slice(&value.0.to_signed_bytes_le());
}

pub fn decode_uint_cell(bytes: &[u8]) -> Uint {
	Uint(BigInt::from_signed_bytes_le(bytes))
}

pub fn encode_decimal_cell(value: &Decimal, buf: &mut Vec<u8>) {
	buf.extend_from_slice(value.to_string().as_bytes());
}

pub fn decode_decimal_cell(bytes: &[u8]) -> Result<Decimal, DecodeError> {
	let s = from_utf8(bytes).map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {e}")))?;
	let dec: BigDecimal = s.parse().map_err(|e| DecodeError::InvalidData(format!("invalid decimal: {e}")))?;
	Ok(Decimal::new(dec))
}

pub fn encode_any_cell(value: &Value, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	encode_value_into(value, buf)
}

pub fn decode_any_cell(bytes: &[u8]) -> Result<Value, DecodeError> {
	decode_value(bytes)
}

pub fn encode_dictionary_id_cell(id: &DictionaryEntryId, buf: &mut Vec<u8>) {
	match id {
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
	}
}

pub fn decode_dictionary_id_cell(bytes: &[u8]) -> Result<DictionaryEntryId, DecodeError> {
	let mut r = Reader::new(bytes);
	let width = r.u8()?;
	let id = match width {
		1 => DictionaryEntryId::U1(r.u8()?),
		2 => DictionaryEntryId::U2(r.u16()?),
		4 => DictionaryEntryId::U4(r.u32()?),
		8 => DictionaryEntryId::U8(r.u64()?),
		16 => DictionaryEntryId::U16(r.u128()?),
		other => {
			return Err(DecodeError::InvalidData(format!("invalid dictionary id width: {other}")));
		}
	};
	if !r.is_empty() {
		return Err(DecodeError::TrailingBytes(r.remaining()));
	}
	Ok(id)
}

pub fn encode_duration_cell(value: &Duration, buf: &mut Vec<u8>) {
	buf.extend_from_slice(&value.get_months().to_le_bytes());
	buf.extend_from_slice(&value.get_days().to_le_bytes());
	buf.extend_from_slice(&value.get_nanos().to_le_bytes());
}

pub fn decode_duration_cell(bytes: &[u8]) -> Result<Duration, DecodeError> {
	let mut r = Reader::new(bytes);
	let months = r.i32()?;
	let days = r.i32()?;
	let nanos = r.i64()?;
	if !r.is_empty() {
		return Err(DecodeError::TrailingBytes(r.remaining()));
	}
	Duration::new(months, days, nanos).map_err(|e| DecodeError::InvalidData(format!("invalid duration: {e}")))
}
