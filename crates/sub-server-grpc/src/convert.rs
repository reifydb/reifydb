// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use num_bigint::BigInt;
use reifydb_type::{
	params::Params,
	util::bitvec::BitVec,
	value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		frame::{data::FrameColumnData, frame::Frame},
		identity::IdentityId,
		int::Int,
		time::Time,
		r#type::Type,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use uuid::Uuid;

use crate::{
	error::GrpcError,
	generated::{
		self, Frame as ProtoFrame, FrameColumn as ProtoFrameColumn, TypedValue,
		params::Params as ProtoParamsOneof,
	},
};

pub fn proto_params_to_params(proto: generated::Params) -> Result<Params, GrpcError> {
	match proto.params {
		None => Ok(Params::None),
		Some(ProtoParamsOneof::Positional(pos)) => {
			let values: Result<Vec<Value>, GrpcError> =
				pos.values.into_iter().map(typed_value_to_value).collect();
			Ok(Params::Positional(values?))
		}
		Some(ProtoParamsOneof::Named(named)) => {
			let map: Result<HashMap<String, Value>, GrpcError> = named
				.values
				.into_iter()
				.map(|(k, tv)| typed_value_to_value(tv).map(|v| (k, v)))
				.collect();
			Ok(Params::Named(map?))
		}
	}
}

fn expect_bytes(ty: Type, expected: usize, actual: usize) -> Result<(), GrpcError> {
	if actual != expected {
		return Err(GrpcError::InvalidByteLength {
			r#type: ty,
			expected,
			actual,
		});
	}
	Ok(())
}

fn typed_value_to_value(tv: TypedValue) -> Result<Value, GrpcError> {
	let ty = Type::from_u8(tv.r#type as u8);
	let data = &tv.value;
	match ty {
		Type::Option(inner) => Ok(Value::None {
			inner: *inner,
		}),
		Type::Boolean => {
			expect_bytes(ty, 1, data.len())?;
			Ok(Value::Boolean(data[0] != 0))
		}
		Type::Float4 => {
			expect_bytes(ty, 4, data.len())?;
			Ok(Value::float4(f32::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Float8 => {
			expect_bytes(ty, 8, data.len())?;
			Ok(Value::float8(f64::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Int1 => {
			expect_bytes(ty, 1, data.len())?;
			Ok(Value::Int1(data[0] as i8))
		}
		Type::Int2 => {
			expect_bytes(ty, 2, data.len())?;
			Ok(Value::Int2(i16::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Int4 => {
			expect_bytes(ty, 4, data.len())?;
			Ok(Value::Int4(i32::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Int8 => {
			expect_bytes(ty, 8, data.len())?;
			Ok(Value::Int8(i64::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Int16 => {
			expect_bytes(ty, 16, data.len())?;
			Ok(Value::Int16(i128::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Uint1 => {
			expect_bytes(ty, 1, data.len())?;
			Ok(Value::Uint1(data[0]))
		}
		Type::Uint2 => {
			expect_bytes(ty, 2, data.len())?;
			Ok(Value::Uint2(u16::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Uint4 => {
			expect_bytes(ty, 4, data.len())?;
			Ok(Value::Uint4(u32::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Uint8 => {
			expect_bytes(ty, 8, data.len())?;
			Ok(Value::Uint8(u64::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Uint16 => {
			expect_bytes(ty, 16, data.len())?;
			Ok(Value::Uint16(u128::from_le_bytes(data.as_slice().try_into().unwrap())))
		}
		Type::Utf8 => {
			let s = String::from_utf8(data.clone()).map_err(GrpcError::InvalidUtf8)?;
			Ok(Value::Utf8(s))
		}
		Type::Date => {
			expect_bytes(ty, 4, data.len())?;
			let days = i32::from_le_bytes(data.as_slice().try_into().unwrap());
			let d = Date::from_days_since_epoch(days).ok_or(GrpcError::InvalidDate {
				days,
			})?;
			Ok(Value::Date(d))
		}
		Type::DateTime => {
			expect_bytes(ty, 12, data.len())?;
			let secs = i64::from_le_bytes(data[..8].try_into().unwrap());
			let nanos = u32::from_le_bytes(data[8..12].try_into().unwrap());
			let dt = DateTime::from_parts(secs, nanos)
				.map_err(|e| GrpcError::InvalidDateTime(e.to_string()))?;
			Ok(Value::DateTime(dt))
		}
		Type::Time => {
			expect_bytes(ty, 8, data.len())?;
			let nanos = u64::from_le_bytes(data.as_slice().try_into().unwrap());
			let t = Time::from_nanos_since_midnight(nanos).ok_or(GrpcError::InvalidTime {
				nanos,
			})?;
			Ok(Value::Time(t))
		}
		Type::Duration => {
			expect_bytes(ty, 16, data.len())?;
			let months = i32::from_le_bytes(data[..4].try_into().unwrap());
			let days = i32::from_le_bytes(data[4..8].try_into().unwrap());
			let nanos = i64::from_le_bytes(data[8..16].try_into().unwrap());
			Ok(Value::Duration(Duration::new(months, days, nanos)))
		}
		Type::Blob => Ok(Value::Blob(Blob::new(data.clone()))),
		Type::IdentityId => {
			expect_bytes(ty, 16, data.len())?;
			let uuid = Uuid::from_bytes(data.as_slice().try_into().unwrap());
			Ok(Value::IdentityId(IdentityId(Uuid7(uuid))))
		}
		Type::Uuid4 => {
			expect_bytes(ty, 16, data.len())?;
			let uuid = Uuid::from_bytes(data.as_slice().try_into().unwrap());
			Ok(Value::Uuid4(Uuid4(uuid)))
		}
		Type::Uuid7 => {
			expect_bytes(ty, 16, data.len())?;
			let uuid = Uuid::from_bytes(data.as_slice().try_into().unwrap());
			Ok(Value::Uuid7(Uuid7(uuid)))
		}
		Type::Decimal => {
			let s = String::from_utf8(data.clone()).map_err(GrpcError::InvalidUtf8)?;
			let d = s.parse::<Decimal>().map_err(|e| GrpcError::InvalidDecimal(e.to_string()))?;
			Ok(Value::Decimal(d))
		}
		Type::Int => {
			let big = BigInt::from_signed_bytes_le(data);
			Ok(Value::Int(Int(big)))
		}
		Type::Uint => {
			let big = BigInt::from_signed_bytes_le(data);
			Ok(Value::Uint(Uint(big)))
		}
		Type::Any | Type::DictionaryId | Type::List(_) | Type::Record(_) | Type::Tuple(_) => {
			Err(GrpcError::UnsupportedParamType(ty))
		}
	}
}

pub fn frames_to_proto(frames: Vec<Frame>) -> Vec<ProtoFrame> {
	frames.into_iter()
		.map(|frame| {
			let row_numbers = frame.row_numbers.iter().map(|rn| rn.value()).collect();
			let columns = frame
				.columns
				.into_iter()
				.map(|col| {
					let (type_u8, data, bitvec) = encode_column_data(&col.data);
					ProtoFrameColumn {
						name: col.name,
						r#type: type_u8 as u32,
						data,
						bitvec,
					}
				})
				.collect();
			ProtoFrame {
				row_numbers,
				columns,
			}
		})
		.collect()
}

fn encode_column_data(col: &FrameColumnData) -> (u8, Vec<u8>, Vec<u8>) {
	match col {
		FrameColumnData::Bool(c) => {
			let bv: &BitVec = c;
			let encoded = encode_bitvec(bv);
			(Type::Boolean.to_u8(), encoded, vec![])
		}
		FrameColumnData::Float4(c) => {
			let slice: &[f32] = c;
			let mut buf = Vec::with_capacity(slice.len() * 4);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Float4.to_u8(), buf, vec![])
		}
		FrameColumnData::Float8(c) => {
			let slice: &[f64] = c;
			let mut buf = Vec::with_capacity(slice.len() * 8);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Float8.to_u8(), buf, vec![])
		}
		FrameColumnData::Int1(c) => {
			let slice: &[i8] = c;
			let mut buf = Vec::with_capacity(slice.len());
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Int1.to_u8(), buf, vec![])
		}
		FrameColumnData::Int2(c) => {
			let slice: &[i16] = c;
			let mut buf = Vec::with_capacity(slice.len() * 2);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Int2.to_u8(), buf, vec![])
		}
		FrameColumnData::Int4(c) => {
			let slice: &[i32] = c;
			let mut buf = Vec::with_capacity(slice.len() * 4);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Int4.to_u8(), buf, vec![])
		}
		FrameColumnData::Int8(c) => {
			let slice: &[i64] = c;
			let mut buf = Vec::with_capacity(slice.len() * 8);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Int8.to_u8(), buf, vec![])
		}
		FrameColumnData::Int16(c) => {
			let slice: &[i128] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Int16.to_u8(), buf, vec![])
		}
		FrameColumnData::Uint1(c) => {
			let slice: &[u8] = c;
			let mut buf = Vec::with_capacity(slice.len());
			buf.extend_from_slice(slice);
			(Type::Uint1.to_u8(), buf, vec![])
		}
		FrameColumnData::Uint2(c) => {
			let slice: &[u16] = c;
			let mut buf = Vec::with_capacity(slice.len() * 2);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Uint2.to_u8(), buf, vec![])
		}
		FrameColumnData::Uint4(c) => {
			let slice: &[u32] = c;
			let mut buf = Vec::with_capacity(slice.len() * 4);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Uint4.to_u8(), buf, vec![])
		}
		FrameColumnData::Uint8(c) => {
			let slice: &[u64] = c;
			let mut buf = Vec::with_capacity(slice.len() * 8);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Uint8.to_u8(), buf, vec![])
		}
		FrameColumnData::Uint16(c) => {
			let slice: &[u128] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(&v.to_le_bytes());
			}
			(Type::Uint16.to_u8(), buf, vec![])
		}
		FrameColumnData::Utf8(c) => {
			let slice: &[String] = c;
			let mut buf = Vec::new();
			for s in slice {
				let bytes = s.as_bytes();
				buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(bytes);
			}
			(Type::Utf8.to_u8(), buf, vec![])
		}
		FrameColumnData::Date(c) => {
			let slice: &[Date] = c;
			let mut buf = Vec::with_capacity(slice.len() * 4);
			for v in slice {
				buf.extend_from_slice(&v.to_days_since_epoch().to_le_bytes());
			}
			(Type::Date.to_u8(), buf, vec![])
		}
		FrameColumnData::DateTime(c) => {
			let slice: &[DateTime] = c;
			let mut buf = Vec::with_capacity(slice.len() * 12);
			for v in slice {
				buf.extend_from_slice(&v.timestamp().to_le_bytes());
				buf.extend_from_slice(&v.nanosecond().to_le_bytes());
			}
			(Type::DateTime.to_u8(), buf, vec![])
		}
		FrameColumnData::Time(c) => {
			let slice: &[Time] = c;
			let mut buf = Vec::with_capacity(slice.len() * 8);
			for v in slice {
				buf.extend_from_slice(&v.to_nanos_since_midnight().to_le_bytes());
			}
			(Type::Time.to_u8(), buf, vec![])
		}
		FrameColumnData::Duration(c) => {
			let slice: &[Duration] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(&v.get_months().to_le_bytes());
				buf.extend_from_slice(&v.get_days().to_le_bytes());
				buf.extend_from_slice(&v.get_nanos().to_le_bytes());
			}
			(Type::Duration.to_u8(), buf, vec![])
		}
		FrameColumnData::IdentityId(c) => {
			let slice: &[IdentityId] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(v.0.0.as_bytes());
			}
			(Type::IdentityId.to_u8(), buf, vec![])
		}
		FrameColumnData::Uuid4(c) => {
			let slice: &[Uuid4] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(v.0.as_bytes());
			}
			(Type::Uuid4.to_u8(), buf, vec![])
		}
		FrameColumnData::Uuid7(c) => {
			let slice: &[Uuid7] = c;
			let mut buf = Vec::with_capacity(slice.len() * 16);
			for v in slice {
				buf.extend_from_slice(v.0.as_bytes());
			}
			(Type::Uuid7.to_u8(), buf, vec![])
		}
		FrameColumnData::Blob(c) => {
			let slice: &[Blob] = c;
			let mut buf = Vec::new();
			for v in slice {
				let bytes = v.as_bytes();
				buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(bytes);
			}
			(Type::Blob.to_u8(), buf, vec![])
		}
		FrameColumnData::Int(c) => {
			let slice: &[Int] = c;
			let mut buf = Vec::new();
			for v in slice {
				let bytes = v.0.to_signed_bytes_le();
				buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(&bytes);
			}
			(Type::Int.to_u8(), buf, vec![])
		}
		FrameColumnData::Uint(c) => {
			let slice: &[Uint] = c;
			let mut buf = Vec::new();
			for v in slice {
				let bytes = v.0.to_signed_bytes_le();
				buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(&bytes);
			}
			(Type::Uint.to_u8(), buf, vec![])
		}
		FrameColumnData::Decimal(c) => {
			let slice: &[Decimal] = c;
			let mut buf = Vec::new();
			for v in slice {
				let s = v.to_string();
				let bytes = s.as_bytes();
				buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(bytes);
			}
			(Type::Decimal.to_u8(), buf, vec![])
		}
		FrameColumnData::Any(c) => {
			let mut buf = Vec::new();
			for i in 0..c.len() {
				let val = c.get_value(i);
				encode_any_value(&val, &mut buf);
			}
			(Type::Any.to_u8(), buf, vec![])
		}
		FrameColumnData::DictionaryId(c) => {
			let mut buf = Vec::new();
			if c.len() > 0 {
				let first = c.get(0);
				let disc = match first {
					Some(DictionaryEntryId::U1(_)) => 1u8,
					Some(DictionaryEntryId::U2(_)) => 2u8,
					Some(DictionaryEntryId::U4(_)) => 4u8,
					Some(DictionaryEntryId::U8(_)) => 8u8,
					Some(DictionaryEntryId::U16(_)) => 16u8,
					None => 1u8,
				};
				buf.push(disc);
				for i in 0..c.len() {
					if let Some(id) = c.get(i) {
						match id {
							DictionaryEntryId::U1(v) => buf.push(v),
							DictionaryEntryId::U2(v) => {
								buf.extend_from_slice(&v.to_le_bytes())
							}
							DictionaryEntryId::U4(v) => {
								buf.extend_from_slice(&v.to_le_bytes())
							}
							DictionaryEntryId::U8(v) => {
								buf.extend_from_slice(&v.to_le_bytes())
							}
							DictionaryEntryId::U16(v) => {
								buf.extend_from_slice(&v.to_le_bytes())
							}
						}
					} else {
						// Write zero bytes for the width
						for _ in 0..disc as usize {
							buf.push(0);
						}
					}
				}
			}
			(Type::DictionaryId.to_u8(), buf, vec![])
		}
		FrameColumnData::Option {
			inner,
			bitvec,
		} => {
			let (inner_type_u8, data, _) = encode_column_data(inner);
			let type_u8 = 0x80 | inner_type_u8;
			let bitvec_bytes = encode_bitvec(bitvec);
			(type_u8, data, bitvec_bytes)
		}
	}
}

fn encode_bitvec(bv: &BitVec) -> Vec<u8> {
	let len = bv.len();
	let mut buf = Vec::with_capacity(4 + (len + 7) / 8);
	buf.extend_from_slice(&(len as u32).to_le_bytes());
	for i in 0..((len + 7) / 8) {
		let mut byte = 0u8;
		for bit in 0..8 {
			let idx = i * 8 + bit;
			if idx < len && bv.get(idx) {
				byte |= 1 << bit;
			}
		}
		buf.push(byte);
	}
	buf
}

fn encode_any_value(val: &Value, buf: &mut Vec<u8>) {
	let type_tag = val.get_type().to_u8();
	buf.push(type_tag);
	match val {
		Value::None {
			..
		} => {}
		Value::Boolean(b) => buf.push(if *b {
			1
		} else {
			0
		}),
		Value::Float4(f) => buf.extend_from_slice(&f.to_le_bytes()),
		Value::Float8(f) => buf.extend_from_slice(&f.to_le_bytes()),
		Value::Int1(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int2(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int4(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int8(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint1(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint2(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint4(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint8(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Uint16(v) => buf.extend_from_slice(&v.to_le_bytes()),
		Value::Utf8(s) => {
			let bytes = s.as_bytes();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(bytes);
		}
		Value::Date(d) => buf.extend_from_slice(&d.to_days_since_epoch().to_le_bytes()),
		Value::DateTime(dt) => {
			buf.extend_from_slice(&dt.timestamp().to_le_bytes());
			buf.extend_from_slice(&dt.nanosecond().to_le_bytes());
		}
		Value::Time(t) => buf.extend_from_slice(&t.to_nanos_since_midnight().to_le_bytes()),
		Value::Duration(d) => {
			buf.extend_from_slice(&d.get_months().to_le_bytes());
			buf.extend_from_slice(&d.get_days().to_le_bytes());
			buf.extend_from_slice(&d.get_nanos().to_le_bytes());
		}
		Value::IdentityId(id) => buf.extend_from_slice(id.0.0.as_bytes()),
		Value::Uuid4(u) => buf.extend_from_slice(u.0.as_bytes()),
		Value::Uuid7(u) => buf.extend_from_slice(u.0.as_bytes()),
		Value::Blob(b) => {
			let bytes = b.as_bytes();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(bytes);
		}
		Value::Int(v) => {
			let bytes = v.0.to_signed_bytes_le();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(&bytes);
		}
		Value::Uint(v) => {
			let bytes = v.0.to_signed_bytes_le();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(&bytes);
		}
		Value::Decimal(d) => {
			let s = d.to_string();
			let bytes = s.as_bytes();
			buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
			buf.extend_from_slice(bytes);
		}
		Value::Any(inner) => encode_any_value(inner, buf),
		Value::DictionaryId(id) => {
			let v = id.to_u128();
			buf.extend_from_slice(&v.to_le_bytes());
		}
		Value::Type(t) => buf.push(t.to_u8()),
		Value::List(items) | Value::Tuple(items) => {
			buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
			for item in items {
				encode_any_value(item, buf);
			}
		}
		Value::Record(fields) => {
			buf.extend_from_slice(&(fields.len() as u32).to_le_bytes());
			for (key, value) in fields {
				let key_bytes = key.as_bytes();
				buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
				buf.extend_from_slice(key_bytes);
				encode_any_value(value, buf);
			}
		}
	}
}
