// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::str;

use reifydb_type::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	duration::Duration,
	identity::IdentityId,
	ordered_f32::OrderedF32,
	ordered_f64::OrderedF64,
	time::Time,
	r#type::Type,
	uuid::{Uuid4, Uuid7},
};
use uuid::Uuid;

use crate::encoded::{row::EncodedRow, schema::Schema};

/// Encodes an inner value to a `[type_byte][payload]` byte vector.
///
/// Panics for unsupported types (Int, Uint, Decimal, DictionaryId, Any, None, List, Type).
pub fn encode_value(value: &Value) -> Vec<u8> {
	match value {
		Value::Boolean(v) => vec![Type::Boolean.to_u8(), *v as u8],

		Value::Uint1(v) => vec![Type::Uint1.to_u8(), *v],
		Value::Uint2(v) => {
			let mut b = vec![Type::Uint2.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}
		Value::Uint4(v) => {
			let mut b = vec![Type::Uint4.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}
		Value::Uint8(v) => {
			let mut b = vec![Type::Uint8.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}
		Value::Uint16(v) => {
			let mut b = vec![Type::Uint16.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}

		Value::Int1(v) => vec![Type::Int1.to_u8(), *v as u8],
		Value::Int2(v) => {
			let mut b = vec![Type::Int2.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}
		Value::Int4(v) => {
			let mut b = vec![Type::Int4.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}
		Value::Int8(v) => {
			let mut b = vec![Type::Int8.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}
		Value::Int16(v) => {
			let mut b = vec![Type::Int16.to_u8()];
			b.extend_from_slice(&v.to_le_bytes());
			b
		}

		Value::Float4(v) => {
			let mut b = vec![Type::Float4.to_u8()];
			b.extend_from_slice(&v.value().to_bits().to_le_bytes());
			b
		}
		Value::Float8(v) => {
			let mut b = vec![Type::Float8.to_u8()];
			b.extend_from_slice(&v.value().to_bits().to_le_bytes());
			b
		}

		Value::Date(v) => {
			let mut b = vec![Type::Date.to_u8()];
			b.extend_from_slice(&v.to_days_since_epoch().to_le_bytes());
			b
		}
		Value::DateTime(v) => {
			let mut b = vec![Type::DateTime.to_u8()];
			b.extend_from_slice(&v.to_nanos().to_le_bytes());
			b
		}
		Value::Time(v) => {
			let mut b = vec![Type::Time.to_u8()];
			b.extend_from_slice(&v.to_nanos_since_midnight().to_le_bytes());
			b
		}
		Value::Duration(v) => {
			let mut b = vec![Type::Duration.to_u8()];
			b.extend_from_slice(&v.get_months().to_le_bytes());
			b.extend_from_slice(&v.get_days().to_le_bytes());
			b.extend_from_slice(&v.get_nanos().to_le_bytes());
			b
		}

		Value::Uuid4(v) => {
			let mut b = vec![Type::Uuid4.to_u8()];
			b.extend_from_slice(v.as_bytes());
			b
		}
		Value::Uuid7(v) => {
			let mut b = vec![Type::Uuid7.to_u8()];
			b.extend_from_slice(v.as_bytes());
			b
		}
		Value::IdentityId(v) => {
			let mut b = vec![Type::IdentityId.to_u8()];
			b.extend_from_slice(v.as_bytes());
			b
		}

		Value::Utf8(v) => {
			let s = v.as_bytes();
			let mut b = vec![Type::Utf8.to_u8()];
			b.extend_from_slice(&(s.len() as u32).to_le_bytes());
			b.extend_from_slice(s);
			b
		}
		Value::Blob(v) => {
			let s = v.as_bytes();
			let mut b = vec![Type::Blob.to_u8()];
			b.extend_from_slice(&(s.len() as u32).to_le_bytes());
			b.extend_from_slice(s);
			b
		}

		Value::Int(_)
		| Value::Uint(_)
		| Value::Decimal(_)
		| Value::DictionaryId(_)
		| Value::Any(_)
		| Value::None {
			..
		}
		| Value::Type(_)
		| Value::List(_)
		| Value::Record(_)
		| Value::Tuple(_) => unreachable!("unsupported value type in Any encoding: {:?}", value),
	}
}

/// Decodes bytes produced by `encode_value` back into a `Value`.
pub fn decode_value(bytes: &[u8]) -> Value {
	let type_byte = bytes[0];
	let p = &bytes[1..];
	let ty = Type::from_u8(type_byte);

	match ty {
		Type::Boolean => Value::Boolean(p[0] != 0),

		Type::Uint1 => Value::Uint1(p[0]),
		Type::Uint2 => Value::Uint2(u16::from_le_bytes([p[0], p[1]])),
		Type::Uint4 => Value::Uint4(u32::from_le_bytes([p[0], p[1], p[2], p[3]])),
		Type::Uint8 => Value::Uint8(u64::from_le_bytes(p[..8].try_into().unwrap())),
		Type::Uint16 => Value::Uint16(u128::from_le_bytes(p[..16].try_into().unwrap())),

		Type::Int1 => Value::Int1(p[0] as i8),
		Type::Int2 => Value::Int2(i16::from_le_bytes([p[0], p[1]])),
		Type::Int4 => Value::Int4(i32::from_le_bytes([p[0], p[1], p[2], p[3]])),
		Type::Int8 => Value::Int8(i64::from_le_bytes(p[..8].try_into().unwrap())),
		Type::Int16 => Value::Int16(i128::from_le_bytes(p[..16].try_into().unwrap())),

		Type::Float4 => {
			let bits = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
			Value::Float4(OrderedF32::try_from(f32::from_bits(bits)).unwrap())
		}
		Type::Float8 => {
			let bits = u64::from_le_bytes(p[..8].try_into().unwrap());
			Value::Float8(OrderedF64::try_from(f64::from_bits(bits)).unwrap())
		}

		Type::Date => {
			let days = i32::from_le_bytes([p[0], p[1], p[2], p[3]]);
			Value::Date(Date::from_days_since_epoch(days).unwrap())
		}
		Type::DateTime => {
			let nanos = u64::from_le_bytes(p[..8].try_into().unwrap());
			Value::DateTime(DateTime::from_nanos(nanos))
		}
		Type::Time => {
			let nanos = u64::from_le_bytes(p[..8].try_into().unwrap());
			Value::Time(Time::from_nanos_since_midnight(nanos).unwrap())
		}
		Type::Duration => {
			let months = i32::from_le_bytes([p[0], p[1], p[2], p[3]]);
			let days = i32::from_le_bytes([p[4], p[5], p[6], p[7]]);
			let nanos = i64::from_le_bytes(p[8..16].try_into().unwrap());
			Value::Duration(Duration::new(months, days, nanos))
		}

		Type::Uuid4 => {
			let b: [u8; 16] = p[..16].try_into().unwrap();
			Value::Uuid4(Uuid4::from(Uuid::from_bytes(b)))
		}
		Type::Uuid7 => {
			let b: [u8; 16] = p[..16].try_into().unwrap();
			Value::Uuid7(Uuid7::from(Uuid::from_bytes(b)))
		}
		Type::IdentityId => {
			let b: [u8; 16] = p[..16].try_into().unwrap();
			Value::IdentityId(IdentityId::from(Uuid7::from(Uuid::from_bytes(b))))
		}

		Type::Utf8 => {
			let len = u32::from_le_bytes([p[0], p[1], p[2], p[3]]) as usize;
			let s = str::from_utf8(&p[4..4 + len]).unwrap();
			Value::Utf8(s.to_string())
		}
		Type::Blob => {
			let len = u32::from_le_bytes([p[0], p[1], p[2], p[3]]) as usize;
			Value::Blob(Blob::from_slice(&p[4..4 + len]))
		}

		_ => unreachable!("unsupported type byte {} in Any decoding", type_byte),
	}
}

impl Schema {
	pub fn set_any(&self, row: &mut EncodedRow, index: usize, value: &Value) {
		debug_assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), Type::Any);
		let encoded = encode_value(value);
		self.replace_dynamic_data(row, index, &encoded);
	}

	pub fn get_any(&self, row: &EncodedRow, index: usize) -> Value {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Any);

		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let data_start = dynamic_start + offset;
		let data_slice = &row.as_slice()[data_start..data_start + length];

		decode_value(data_slice)
	}
}

#[cfg(test)]
pub mod tests {
	use std::f64::consts::E;

	use reifydb_type::value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		duration::Duration,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		r#type::Type,
		uuid::{Uuid4, Uuid7},
	};

	use crate::encoded::schema::Schema;

	#[test]
	fn test_any_boolean() {
		let schema = Schema::testing(&[Type::Any]);
		let mut row = schema.allocate();
		schema.set_any(&mut row, 0, &Value::Boolean(true));
		assert_eq!(schema.get_any(&row, 0), Value::Boolean(true));
	}

	#[test]
	fn test_any_integers() {
		let schema = Schema::testing(&[Type::Any]);

		let cases: &[Value] = &[
			Value::Int1(-42),
			Value::Int2(-1000),
			Value::Int4(-100000),
			Value::Int8(i64::MIN),
			Value::Int16(i128::MAX),
			Value::Uint1(255),
			Value::Uint2(65535),
			Value::Uint4(u32::MAX),
			Value::Uint8(u64::MAX),
			Value::Uint16(u128::MAX),
		];

		for case in cases {
			let mut row = schema.allocate();
			schema.set_any(&mut row, 0, case);
			assert_eq!(&schema.get_any(&row, 0), case);
		}
	}

	#[test]
	fn test_any_floats() {
		let schema = Schema::testing(&[Type::Any]);

		let f4 = Value::Float4(OrderedF32::try_from(3.14f32).unwrap());
		let mut row = schema.allocate();
		schema.set_any(&mut row, 0, &f4);
		assert_eq!(schema.get_any(&row, 0), f4);

		let f8 = Value::Float8(OrderedF64::try_from(E).unwrap());
		let mut row2 = schema.allocate();
		schema.set_any(&mut row2, 0, &f8);
		assert_eq!(schema.get_any(&row2, 0), f8);
	}

	#[test]
	fn test_any_temporal() {
		let schema = Schema::testing(&[Type::Any]);

		let date = Value::Date(Date::new(2025, 7, 4).unwrap());
		let mut row = schema.allocate();
		schema.set_any(&mut row, 0, &date);
		assert_eq!(schema.get_any(&row, 0), date);

		let dt = Value::DateTime(DateTime::new(2025, 1, 1, 12, 0, 0, 0).unwrap());
		let mut row2 = schema.allocate();
		schema.set_any(&mut row2, 0, &dt);
		assert_eq!(schema.get_any(&row2, 0), dt);

		let t = Value::Time(Time::new(14, 30, 45, 123456789).unwrap());
		let mut row3 = schema.allocate();
		schema.set_any(&mut row3, 0, &t);
		assert_eq!(schema.get_any(&row3, 0), t);

		let dur = Value::Duration(Duration::from_seconds(3600));
		let mut row4 = schema.allocate();
		schema.set_any(&mut row4, 0, &dur);
		assert_eq!(schema.get_any(&row4, 0), dur);
	}

	#[test]
	fn test_any_uuid() {
		let schema = Schema::testing(&[Type::Any]);

		let u4 = Value::Uuid4(Uuid4::generate());
		let mut row = schema.allocate();
		schema.set_any(&mut row, 0, &u4);
		assert_eq!(schema.get_any(&row, 0), u4);

		let u7 = Value::Uuid7(Uuid7::generate());
		let mut row2 = schema.allocate();
		schema.set_any(&mut row2, 0, &u7);
		assert_eq!(schema.get_any(&row2, 0), u7);
	}

	#[test]
	fn test_any_utf8() {
		let schema = Schema::testing(&[Type::Any]);
		let v = Value::Utf8("hello, world!".to_string());
		let mut row = schema.allocate();
		schema.set_any(&mut row, 0, &v);
		assert_eq!(schema.get_any(&row, 0), v);
	}

	#[test]
	fn test_any_blob() {
		let schema = Schema::testing(&[Type::Any]);
		let v = Value::Blob(Blob::from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]));
		let mut row = schema.allocate();
		schema.set_any(&mut row, 0, &v);
		assert_eq!(schema.get_any(&row, 0), v);
	}

	#[test]
	fn test_any_none_via_set_value() {
		let schema = Schema::testing(&[Type::Any]);
		let mut row = schema.allocate();
		schema.set_value(&mut row, 0, &Value::none());
		assert!(!row.is_defined(0));
		assert_eq!(schema.get_value(&row, 0), Value::none());
	}

	#[test]
	fn test_any_roundtrip_via_set_get_value() {
		let schema = Schema::testing(&[Type::Any]);

		let cases: &[Value] = &[
			Value::Boolean(false),
			Value::Int4(42),
			Value::Utf8("test".to_string()),
			Value::Uint8(1234567890),
		];

		for inner in cases {
			let wrapped = Value::any(inner.clone());
			let mut row = schema.allocate();
			schema.set_value(&mut row, 0, &wrapped);
			let retrieved = schema.get_value(&row, 0);
			assert_eq!(retrieved, wrapped, "roundtrip failed for {:?}", inner);
		}
	}

	#[test]
	fn test_any_multiple_fields() {
		let schema = Schema::testing(&[Type::Any, Type::Int4, Type::Any]);
		let mut row = schema.allocate();

		schema.set_any(&mut row, 0, &Value::Utf8("first".to_string()));
		schema.set_i32(&mut row, 1, 99);
		schema.set_any(&mut row, 2, &Value::Boolean(true));

		assert_eq!(schema.get_any(&row, 0), Value::Utf8("first".to_string()));
		assert_eq!(schema.get_i32(&row, 1), 99);
		assert_eq!(schema.get_any(&row, 2), Value::Boolean(true));
	}

	#[test]
	fn test_update_any() {
		let schema = Schema::testing(&[Type::Any]);
		let mut row = schema.allocate();

		schema.set_any(&mut row, 0, &Value::Int4(42));
		assert_eq!(schema.get_any(&row, 0), Value::Int4(42));

		// Overwrite with a different type
		schema.set_any(&mut row, 0, &Value::Utf8("hello".to_string()));
		assert_eq!(schema.get_any(&row, 0), Value::Utf8("hello".to_string()));

		// Overwrite again with boolean
		schema.set_any(&mut row, 0, &Value::Boolean(true));
		assert_eq!(schema.get_any(&row, 0), Value::Boolean(true));
	}

	#[test]
	fn test_update_any_with_other_dynamic_fields() {
		let schema = Schema::testing(&[Type::Any, Type::Utf8, Type::Any]);
		let mut row = schema.allocate();

		schema.set_any(&mut row, 0, &Value::Int4(1));
		schema.set_utf8(&mut row, 1, "middle");
		schema.set_any(&mut row, 2, &Value::Boolean(false));

		// Update first any with a larger value
		schema.set_any(&mut row, 0, &Value::Utf8("a long string value".to_string()));

		assert_eq!(schema.get_any(&row, 0), Value::Utf8("a long string value".to_string()));
		assert_eq!(schema.get_utf8(&row, 1), "middle");
		assert_eq!(schema.get_any(&row, 2), Value::Boolean(false));
	}
}
