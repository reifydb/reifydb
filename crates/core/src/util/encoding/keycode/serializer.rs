// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::Sign;
use reifydb_type::value::{
	Value,
	blob::Blob,
	date::Date,
	datetime::DateTime,
	decimal::Decimal,
	dictionary::DictionaryEntryId,
	duration::Duration,
	identity::IdentityId,
	int::Int,
	row_number::RowNumber,
	time::Time,
	r#type::Type,
	uint::Uint,
	uuid::{Uuid4, Uuid7},
};
use serde::Serialize;

use super::{
	catalog, encode_bool, encode_bytes, encode_f32, encode_f64, encode_i8, encode_i16, encode_i32, encode_i64,
	encode_i128, encode_u8, encode_u16, encode_u32, encode_u64, encode_u128, serialize,
};
use crate::{
	encoded::key::EncodedKey,
	interface::catalog::{id::IndexId, primitive::PrimitiveId},
};

/// A builder for constructing binary keys using keycode encoding
pub struct KeySerializer {
	buffer: Vec<u8>,
}

impl KeySerializer {
	/// Create new serializer with default capacity
	pub fn new() -> Self {
		Self {
			buffer: Vec::new(),
		}
	}

	/// Create with pre-allocated capacity
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			buffer: Vec::with_capacity(capacity),
		}
	}

	/// Extend with bool value
	pub fn extend_bool(&mut self, value: bool) -> &mut Self {
		self.buffer.push(encode_bool(value));
		self
	}

	/// Extend with f32 value
	pub fn extend_f32(&mut self, value: f32) -> &mut Self {
		self.buffer.extend_from_slice(&encode_f32(value));
		self
	}

	/// Extend with f64 value
	pub fn extend_f64(&mut self, value: f64) -> &mut Self {
		self.buffer.extend_from_slice(&encode_f64(value));
		self
	}

	/// Extend with i8 value
	pub fn extend_i8<T: Into<i8>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i8(value.into()));
		self
	}

	/// Extend with i16 value
	pub fn extend_i16<T: Into<i16>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i16(value.into()));
		self
	}

	/// Extend with i32 value
	pub fn extend_i32<T: Into<i32>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i32(value.into()));
		self
	}

	/// Extend with i64 value
	pub fn extend_i64<T: Into<i64>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i64(value.into()));
		self
	}

	/// Extend with i128 value
	pub fn extend_i128<T: Into<i128>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i128(value.into()));
		self
	}

	/// Extend with u8 value
	pub fn extend_u8<T: Into<u8>>(&mut self, value: T) -> &mut Self {
		self.buffer.push(encode_u8(value.into()));
		self
	}

	/// Extend with u16 value
	pub fn extend_u16<T: Into<u16>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u16(value.into()));
		self
	}

	/// Extend with u32 value
	pub fn extend_u32<T: Into<u32>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u32(value.into()));
		self
	}

	/// Extend with u64 value
	pub fn extend_u64<T: Into<u64>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u64(value.into()));
		self
	}

	/// Extend with u128 value
	pub fn extend_u128<T: Into<u128>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u128(value.into()));
		self
	}

	/// Extend with raw bytes
	pub fn extend_bytes<T: AsRef<[u8]>>(&mut self, bytes: T) -> &mut Self {
		encode_bytes(bytes.as_ref(), &mut self.buffer);
		self
	}

	/// Extend with string (UTF-8 bytes)
	pub fn extend_str<T: AsRef<str>>(&mut self, s: T) -> &mut Self {
		self.extend_bytes(s.as_ref().as_bytes())
	}

	/// Consume serializer and return final buffer
	pub fn finish(self) -> Vec<u8> {
		self.buffer
	}

	/// Consume serializer and return an EncodedKey directly
	pub fn to_encoded_key(self) -> EncodedKey {
		EncodedKey::new(self.buffer)
	}

	/// Extend with a PrimitiveId value (includes type discriminator)
	pub fn extend_primitive_id(&mut self, primitive: impl Into<PrimitiveId>) -> &mut Self {
		let primitive = primitive.into();
		self.buffer.extend_from_slice(&catalog::serialize_primitive_id(&primitive));
		self
	}

	/// Extend with an IndexId value (includes type discriminator)  
	pub fn extend_index_id(&mut self, index: impl Into<IndexId>) -> &mut Self {
		let index = index.into();
		self.buffer.extend_from_slice(&catalog::serialize_index_id(&index));
		self
	}

	/// Extend with a serializable value using keycode encoding
	pub fn extend_serialize<T: Serialize>(&mut self, value: &T) -> &mut Self {
		self.buffer.extend_from_slice(&serialize(value));
		self
	}

	/// Extend with raw bytes (no encoding)
	pub fn extend_raw(&mut self, bytes: &[u8]) -> &mut Self {
		self.buffer.extend_from_slice(bytes);
		self
	}

	/// Get current buffer length
	pub fn len(&self) -> usize {
		self.buffer.len()
	}

	/// Check if buffer is empty
	pub fn is_empty(&self) -> bool {
		self.buffer.is_empty()
	}

	/// Extend with Date value
	pub fn extend_date(&mut self, date: &Date) -> &mut Self {
		self.extend_i32(date.to_days_since_epoch())
	}

	/// Extend with DateTime value
	pub fn extend_datetime(&mut self, datetime: &DateTime) -> &mut Self {
		self.extend_i64(datetime.to_nanos_since_epoch())
	}

	/// Extend with Time value
	pub fn extend_time(&mut self, time: &Time) -> &mut Self {
		self.extend_u64(time.to_nanos_since_midnight())
	}

	/// Extend with Duration value
	pub fn extend_duration(&mut self, duration: &Duration) -> &mut Self {
		self.extend_i64(duration.get_nanos())
	}

	/// Extend with RowNumber value
	pub fn extend_row_number(&mut self, row_number: &RowNumber) -> &mut Self {
		self.extend_u64(row_number.0)
	}

	/// Extend with IdentityId value
	pub fn extend_identity_id(&mut self, id: &IdentityId) -> &mut Self {
		self.extend_bytes(id.as_bytes())
	}

	/// Extend with Uuid4 value
	pub fn extend_uuid4(&mut self, uuid: &Uuid4) -> &mut Self {
		self.extend_bytes(uuid.as_bytes())
	}

	/// Extend with Uuid7 value
	pub fn extend_uuid7(&mut self, uuid: &Uuid7) -> &mut Self {
		self.extend_bytes(uuid.as_bytes())
	}

	/// Extend with Blob value
	pub fn extend_blob(&mut self, blob: &Blob) -> &mut Self {
		self.extend_bytes(blob.as_ref())
	}

	/// Extend with arbitrary precision Int value
	pub fn extend_int(&mut self, int: &Int) -> &mut Self {
		// For arbitrary precision, encode as bytes with sign prefix
		let (sign, bytes) = int.to_bytes_be();
		// Encode sign as a byte (0 for negative, 1 for positive)
		self.buffer.push(match sign {
			Sign::Minus => 0,
			_ => 1,
		});
		self.extend_u32(bytes.len() as u32);
		self.buffer.extend_from_slice(&bytes);
		self
	}

	/// Extend with arbitrary precision Uint value
	pub fn extend_uint(&mut self, uint: &Uint) -> &mut Self {
		// For arbitrary precision unsigned, encode as bytes with length prefix
		let (_sign, bytes) = uint.0.to_bytes_be();
		self.extend_u32(bytes.len() as u32);
		self.buffer.extend_from_slice(&bytes);
		self
	}

	/// Extend with Decimal value
	pub fn extend_decimal(&mut self, decimal: &Decimal) -> &mut Self {
		// Encode decimal as string representation for now
		// This ensures ordering is preserved for decimal values
		let s = decimal.to_string();
		self.extend_str(&s);
		self
	}

	/// Extend with a Value based on its type, including a type marker byte for self-describing encoding.
	/// The marker is written as a raw byte (not complement-encoded) so that `read_value()` can decode it.
	pub fn extend_value(&mut self, value: &Value) -> &mut Self {
		match value {
			Value::None {
				inner,
				..
			} => {
				self.buffer.push(0x00);
				self.buffer.push(match inner {
					Type::Any => 0x00,
					Type::Boolean => 0x01,
					Type::Float4 => 0x02,
					Type::Float8 => 0x03,
					Type::Int1 => 0x04,
					Type::Int2 => 0x05,
					Type::Int4 => 0x06,
					Type::Int8 => 0x07,
					Type::Int16 => 0x08,
					Type::Utf8 => 0x09,
					Type::Uint1 => 0x0a,
					Type::Uint2 => 0x0b,
					Type::Uint4 => 0x0c,
					Type::Uint8 => 0x0d,
					Type::Uint16 => 0x0e,
					Type::Date => 0x0f,
					Type::DateTime => 0x10,
					Type::Time => 0x11,
					Type::Duration => 0x12,
					Type::IdentityId => 0x14,
					Type::Uuid4 => 0x15,
					Type::Uuid7 => 0x16,
					Type::Blob => 0x17,
					Type::Int => 0x18,
					Type::Uint => 0x19,
					Type::Decimal => 0x1a,
					Type::DictionaryId => 0x1b,
					_ => unreachable!(
						"Option/List/Record/Tuple types cannot be encoded as None inner type in keys"
					),
				});
			}
			Value::Boolean(b) => {
				self.buffer.push(0x01);
				self.extend_bool(*b);
			}
			Value::Float4(f) => {
				self.buffer.push(0x02);
				self.extend_f32(**f);
			}
			Value::Float8(f) => {
				self.buffer.push(0x03);
				self.extend_f64(**f);
			}
			Value::Int1(i) => {
				self.buffer.push(0x04);
				self.extend_i8(*i);
			}
			Value::Int2(i) => {
				self.buffer.push(0x05);
				self.extend_i16(*i);
			}
			Value::Int4(i) => {
				self.buffer.push(0x06);
				self.extend_i32(*i);
			}
			Value::Int8(i) => {
				self.buffer.push(0x07);
				self.extend_i64(*i);
			}
			Value::Int16(i) => {
				self.buffer.push(0x08);
				self.extend_i128(*i);
			}
			Value::Utf8(s) => {
				self.buffer.push(0x09);
				self.extend_str(s);
			}
			Value::Uint1(u) => {
				self.buffer.push(0x0a);
				self.extend_u8(*u);
			}
			Value::Uint2(u) => {
				self.buffer.push(0x0b);
				self.extend_u16(*u);
			}
			Value::Uint4(u) => {
				self.buffer.push(0x0c);
				self.extend_u32(*u);
			}
			Value::Uint8(u) => {
				self.buffer.push(0x0d);
				self.extend_u64(*u);
			}
			Value::Uint16(u) => {
				self.buffer.push(0x0e);
				self.extend_u128(*u);
			}
			Value::Date(d) => {
				self.buffer.push(0x0f);
				self.extend_date(d);
			}
			Value::DateTime(dt) => {
				self.buffer.push(0x10);
				self.extend_datetime(dt);
			}
			Value::Time(t) => {
				self.buffer.push(0x11);
				self.extend_time(t);
			}
			Value::Duration(i) => {
				self.buffer.push(0x12);
				self.extend_duration(i);
			}
			Value::IdentityId(id) => {
				self.buffer.push(0x14);
				self.extend_identity_id(id);
			}
			Value::Uuid4(uuid) => {
				self.buffer.push(0x15);
				self.extend_uuid4(uuid);
			}
			Value::Uuid7(uuid) => {
				self.buffer.push(0x16);
				self.extend_uuid7(uuid);
			}
			Value::Blob(b) => {
				self.buffer.push(0x17);
				self.extend_blob(b);
			}
			Value::Int(i) => {
				self.buffer.push(0x18);
				self.extend_int(i);
			}
			Value::Uint(u) => {
				self.buffer.push(0x19);
				self.extend_uint(u);
			}
			Value::Decimal(d) => {
				self.buffer.push(0x1a);
				self.extend_decimal(d);
			}
			Value::Any(_) | Value::Type(_) | Value::List(_) | Value::Record(_) | Value::Tuple(_) => {
				unreachable!("Any/Type/List/Record/Tuple values cannot be serialized in keys");
			}
			Value::DictionaryId(id) => {
				self.buffer.push(0x1b);
				match id {
					DictionaryEntryId::U1(v) => {
						self.buffer.push(0x00);
						self.extend_u8(*v);
					}
					DictionaryEntryId::U2(v) => {
						self.buffer.push(0x01);
						self.extend_u16(*v);
					}
					DictionaryEntryId::U4(v) => {
						self.buffer.push(0x02);
						self.extend_u32(*v);
					}
					DictionaryEntryId::U8(v) => {
						self.buffer.push(0x03);
						self.extend_u64(*v);
					}
					DictionaryEntryId::U16(v) => {
						self.buffer.push(0x04);
						self.extend_u128(*v);
					}
				}
			}
		}
		self
	}
}

impl Default for KeySerializer {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
pub mod tests {
	use std::{f64, str::FromStr};

	use num_bigint::BigInt;
	use reifydb_type::{
		util::hex,
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
			row_number::RowNumber,
			time::Time,
			r#type::Type,
			uint::Uint,
			uuid::{Uuid4, Uuid7},
		},
	};

	use crate::{
		interface::catalog::{
			id::{IndexId, PrimaryKeyId, TableId},
			primitive::PrimitiveId,
		},
		util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	};

	#[test]
	fn test_new() {
		let serializer = KeySerializer::new();
		assert!(serializer.is_empty());
		assert_eq!(serializer.len(), 0);
	}

	#[test]
	fn test_with_capacity() {
		let serializer = KeySerializer::with_capacity(100);
		assert!(serializer.is_empty());
		assert_eq!(serializer.len(), 0);
	}

	#[test]
	fn test_extend_bool() {
		let mut serializer = KeySerializer::new();
		serializer.extend_bool(true);
		let result = serializer.finish();
		assert_eq!(result, vec![0x00]);
		assert_eq!(hex::encode(&result), "00");

		let mut serializer = KeySerializer::new();
		serializer.extend_bool(false);
		let result = serializer.finish();
		assert_eq!(result, vec![0x01]);
		assert_eq!(hex::encode(&result), "01");
	}

	#[test]
	fn test_extend_f32() {
		let mut serializer = KeySerializer::new();
		serializer.extend_f32(3.14f32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);
		assert_eq!(hex::encode(&result), "3fb70a3c");

		let mut serializer = KeySerializer::new();
		serializer.extend_f32(-3.14f32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);
		assert_eq!(hex::encode(&result), "c048f5c3");

		let mut serializer = KeySerializer::new();
		serializer.extend_f32(0.0f32);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7fffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_f32(f32::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00800000");

		let mut serializer = KeySerializer::new();
		serializer.extend_f32(f32::MIN);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ff7fffff");
	}

	#[test]
	fn test_extend_f64() {
		let mut serializer = KeySerializer::new();
		serializer.extend_f64(f64::consts::PI);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);
		assert_eq!(hex::encode(&result), "3ff6de04abbbd2e7");

		let mut serializer = KeySerializer::new();
		serializer.extend_f64(-f64::consts::PI);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);
		assert_eq!(hex::encode(&result), "c00921fb54442d18");

		let mut serializer = KeySerializer::new();
		serializer.extend_f64(0.0f64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7fffffffffffffff");
	}

	#[test]
	fn test_extend_i8() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i8(0i8);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7f");

		let mut serializer = KeySerializer::new();
		serializer.extend_i8(1i8);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7e");

		let mut serializer = KeySerializer::new();
		serializer.extend_i8(-1i8);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "80");

		let mut serializer = KeySerializer::new();
		serializer.extend_i8(i8::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00");

		let mut serializer = KeySerializer::new();
		serializer.extend_i8(i8::MIN);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ff");
	}

	#[test]
	fn test_extend_i16() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i16(0i16);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7fff");

		let mut serializer = KeySerializer::new();
		serializer.extend_i16(1i16);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7ffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_i16(-1i16);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "8000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i16(i16::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "0000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i16(i16::MIN);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffff");
	}

	#[test]
	fn test_extend_i32() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i32(0i32);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7fffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_i32(1i32);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7ffffffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_i32(-1i32);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "80000000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i32(i32::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00000000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i32(i32::MIN);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffff");
	}

	#[test]
	fn test_extend_i64() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i64(0i64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7fffffffffffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_i64(1i64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7ffffffffffffffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_i64(-1i64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "8000000000000000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i64(i64::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "0000000000000000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i64(i64::MIN);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffffffffffff");
	}

	#[test]
	fn test_extend_i128() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i128(0i128);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7fffffffffffffffffffffffffffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_i128(1i128);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "7ffffffffffffffffffffffffffffffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_i128(-1i128);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "80000000000000000000000000000000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i128(i128::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00000000000000000000000000000000");

		let mut serializer = KeySerializer::new();
		serializer.extend_i128(i128::MIN);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffffffffffffffffffffffffffff");
	}

	#[test]
	fn test_extend_u8() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(0u8);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ff");

		let mut serializer = KeySerializer::new();
		serializer.extend_u8(1u8);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "fe");

		let mut serializer = KeySerializer::new();
		serializer.extend_u8(255u8);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00");
	}

	#[test]
	fn test_extend_u16() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u16(0u16);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_u16(1u16);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "fffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_u16(255u16);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ff00");

		let mut serializer = KeySerializer::new();
		serializer.extend_u16(u16::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "0000");
	}

	#[test]
	fn test_extend_u32() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u32(0u32);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_u32(1u32);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "fffffffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_u32(u32::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00000000");
	}

	#[test]
	fn test_extend_u64() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(0u64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffffffffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_u64(1u64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "fffffffffffffffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_u64(65535u64);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffffffff0000");

		let mut serializer = KeySerializer::new();
		serializer.extend_u64(u64::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "0000000000000000");
	}

	#[test]
	fn test_extend_u128() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u128(0u128);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "ffffffffffffffffffffffffffffffff");

		let mut serializer = KeySerializer::new();
		serializer.extend_u128(1u128);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "fffffffffffffffffffffffffffffffe");

		let mut serializer = KeySerializer::new();
		serializer.extend_u128(u128::MAX);
		let result = serializer.finish();
		assert_eq!(hex::encode(&result), "00000000000000000000000000000000");
	}

	#[test]
	fn test_extend_bytes() {
		let mut serializer = KeySerializer::new();
		serializer.extend_bytes(b"hello");
		let result = serializer.finish();
		// Should have "hello" plus terminator (0xff, 0xff)
		assert_eq!(result, vec![b'h', b'e', b'l', b'l', b'o', 0xff, 0xff]);

		// Test with 0xff in the data
		let mut serializer = KeySerializer::new();
		serializer.extend_bytes(&[0x01, 0xff, 0x02]);
		let result = serializer.finish();
		// 0xff should be escaped as 0xff, 0x00
		assert_eq!(result, vec![0x01, 0xff, 0x00, 0x02, 0xff, 0xff]);
	}

	#[test]
	fn test_extend_str() {
		let mut serializer = KeySerializer::new();
		serializer.extend_str("hello world");
		let result = serializer.finish();
		// Should encode as UTF-8 bytes plus terminator
		assert!(result.len() > "hello world".len());
		assert!(result.ends_with(&[0xff, 0xff]));
	}

	#[test]
	fn test_extend_raw() {
		let mut serializer = KeySerializer::new();
		serializer.extend_raw(&[0x01, 0x02, 0x03]);
		let result = serializer.finish();
		assert_eq!(result, vec![0x01, 0x02, 0x03]);
	}

	#[test]
	fn test_chaining() {
		let mut serializer = KeySerializer::new();
		serializer.extend_bool(true).extend_i32(42i32).extend_str("test").extend_u64(1000u64);
		let result = serializer.finish();

		// Should have bool (1 byte) + i32 (4 bytes) + "test" with terminator (6 bytes) + u64 (8 bytes)
		assert!(result.len() >= 19);

		let mut de = KeyDeserializer::from_bytes(&result);
		assert_eq!(de.read_bool().unwrap(), true);
		assert_eq!(de.read_i32().unwrap(), 42);
		assert_eq!(de.read_str().unwrap(), "test");
		assert_eq!(de.read_u64().unwrap(), 1000);
		assert!(de.is_empty());
	}

	#[test]
	fn test_ordering_descending_i32() {
		// Test that descending order is preserved: larger values -> smaller bytes
		let mut ser1 = KeySerializer::new();
		ser1.extend_i32(1i32);
		let bytes1 = ser1.finish();

		let mut ser2 = KeySerializer::new();
		ser2.extend_i32(100i32);
		let bytes2 = ser2.finish();

		let mut ser3 = KeySerializer::new();
		ser3.extend_i32(1000i32);
		let bytes3 = ser3.finish();

		// In descending order: larger values encode to smaller bytes
		// So: bytes_1000 < bytes_100 < bytes_1
		assert!(bytes3 < bytes2, "encode(1000) should be < encode(100)");
		assert!(bytes2 < bytes1, "encode(100) should be < encode(1)");
	}

	#[test]
	fn test_ordering_descending_u64() {
		let mut ser1 = KeySerializer::new();
		ser1.extend_u64(1u64);
		let bytes1 = ser1.finish();

		let mut ser2 = KeySerializer::new();
		ser2.extend_u64(100u64);
		let bytes2 = ser2.finish();

		let mut ser3 = KeySerializer::new();
		ser3.extend_u64(10000u64);
		let bytes3 = ser3.finish();

		// Descending: larger u64 -> smaller bytes
		assert!(bytes3 < bytes2, "encode(10000) should be < encode(100)");
		assert!(bytes2 < bytes1, "encode(100) should be < encode(1)");
	}

	#[test]
	fn test_ordering_descending_negative() {
		// Test negative numbers ordering
		// In descending order: -1 > -100 > -1000
		// So encoded bytes: encode(-1) < encode(-100) < encode(-1000)
		let mut ser1 = KeySerializer::new();
		ser1.extend_i32(-1i32);
		let bytes_neg1 = ser1.finish();

		let mut ser2 = KeySerializer::new();
		ser2.extend_i32(-100i32);
		let bytes_neg100 = ser2.finish();

		let mut ser3 = KeySerializer::new();
		ser3.extend_i32(-1000i32);
		let bytes_neg1000 = ser3.finish();

		// In descending: -1 > -100 > -1000, so encode(-1) < encode(-100) < encode(-1000)
		assert!(bytes_neg1 < bytes_neg100, "encode(-1) should be < encode(-100)");
		assert!(bytes_neg100 < bytes_neg1000, "encode(-100) should be < encode(-1000)");
	}

	#[test]
	fn test_ordering_mixed_sign() {
		// Test that positive/negative ordering is correct
		let mut ser_neg = KeySerializer::new();
		ser_neg.extend_i32(-1i32);
		let bytes_neg = ser_neg.finish();

		let mut ser_zero = KeySerializer::new();
		ser_zero.extend_i32(0i32);
		let bytes_zero = ser_zero.finish();

		let mut ser_pos = KeySerializer::new();
		ser_pos.extend_i32(1i32);
		let bytes_pos = ser_pos.finish();

		// In descending: 1 > 0 > -1, so encode(1) < encode(0) < encode(-1)
		assert!(bytes_pos < bytes_zero, "encode(1) should be < encode(0)");
		assert!(bytes_zero < bytes_neg, "encode(0) should be < encode(-1)");
	}

	#[test]
	fn test_date() {
		let mut serializer = KeySerializer::new();
		let date = Date::from_ymd(2024, 1, 1).unwrap();
		serializer.extend_date(&date);
		let result = serializer.finish();
		assert_eq!(result.len(), 4); // i32 encoding
	}

	#[test]
	fn test_datetime() {
		let mut serializer = KeySerializer::new();
		let datetime = DateTime::from_ymd_hms(2024, 1, 1, 12, 0, 0).unwrap();
		serializer.extend_datetime(&datetime);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // i64 encoding
	}

	#[test]
	fn test_time() {
		let mut serializer = KeySerializer::new();
		let time = Time::from_hms(12, 30, 45).unwrap();
		serializer.extend_time(&time);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // u64 encoding
	}

	#[test]
	fn test_interval() {
		let mut serializer = KeySerializer::new();
		let duration = Duration::from_nanoseconds(1000000);
		serializer.extend_duration(&duration);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // i64 encoding
	}

	#[test]
	fn test_row_number() {
		let mut serializer = KeySerializer::new();
		let row_number = RowNumber(42);
		serializer.extend_row_number(&row_number);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // u64 encoding
	}

	#[test]
	fn test_identity_id() {
		let mut serializer = KeySerializer::new();
		let id = IdentityId::generate();
		serializer.extend_identity_id(&id);
		let result = serializer.finish();
		assert!(result.len() > 0);
	}

	#[test]
	fn test_uuid4() {
		let mut serializer = KeySerializer::new();
		let uuid = Uuid4::generate();
		serializer.extend_uuid4(&uuid);
		let result = serializer.finish();
		// UUID is 16 bytes plus encoding overhead
		assert!(result.len() > 16);
	}

	#[test]
	fn test_uuid7() {
		let mut serializer = KeySerializer::new();
		let uuid = Uuid7::generate();
		serializer.extend_uuid7(&uuid);
		let result = serializer.finish();
		// UUID is 16 bytes plus encoding overhead
		assert!(result.len() > 16);
	}

	#[test]
	fn test_blob() {
		let mut serializer = KeySerializer::new();
		let blob = Blob::from(vec![0x01, 0x02, 0x03]);
		serializer.extend_blob(&blob);
		let result = serializer.finish();
		// Should have data plus terminator
		assert!(result.len() > 3);
	}

	#[test]
	fn test_int() {
		let mut serializer = KeySerializer::new();
		let int = Int(BigInt::from(42));
		serializer.extend_int(&int);
		let result = serializer.finish();
		// Should have sign byte + length + data
		assert!(result.len() > 0);
	}

	#[test]
	fn test_uint() {
		let mut serializer = KeySerializer::new();
		let uint = Uint(BigInt::from(42));
		serializer.extend_uint(&uint);
		let result = serializer.finish();
		// Should have length + data
		assert!(result.len() > 0);
	}

	#[test]
	fn test_decimal() {
		let mut serializer = KeySerializer::new();
		let decimal = Decimal::from_str("3.14").unwrap();
		serializer.extend_decimal(&decimal);
		let result = serializer.finish();
		// Should encode as string
		assert!(result.len() > 0);
	}

	#[test]
	fn test_extend_value() {
		// Test None (Any inner type)
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::none());
		let result = serializer.finish();
		assert_eq!(result, vec![0x00, 0x00]); // marker + Any inner type marker

		// Test None with typed inner
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::none_of(Type::Int4));
		let result = serializer.finish();
		assert_eq!(result, vec![0x00, 0x06]); // marker + Int4 inner type marker

		// Test boolean
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Boolean(true));
		let result = serializer.finish();
		assert_eq!(result[0], 0x01); // Boolean marker
		assert_eq!(result.len(), 2); // marker + encoded bool

		// Test integer
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Int4(42));
		let result = serializer.finish();
		assert_eq!(result[0], 0x06); // Int4 marker
		assert_eq!(result.len(), 5); // marker + 4 bytes

		// Test string
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Utf8("test".to_string()));
		let result = serializer.finish();
		assert_eq!(result[0], 0x09); // Utf8 marker
		assert!(result.ends_with(&[0xff, 0xff]));
	}

	#[test]
	fn test_roundtrip_none() {
		let value = Value::none();
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_none_typed() {
		let value = Value::none_of(Type::Int4);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_boolean_true() {
		let value = Value::Boolean(true);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_boolean_false() {
		let value = Value::Boolean(false);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_float4() {
		let value = Value::Float4(OrderedF32::try_from(3.14f32).unwrap());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_float8() {
		let value = Value::Float8(OrderedF64::try_from(3.14).unwrap());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_int1() {
		let value = Value::Int1(-42);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_int2() {
		let value = Value::Int2(-1000);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_int4() {
		let value = Value::Int4(42);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_int8() {
		let value = Value::Int8(-1_000_000);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_int16() {
		let value = Value::Int16(123_456_789);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_utf8() {
		let value = Value::Utf8("hello world".to_string());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uint1() {
		let value = Value::Uint1(255);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uint2() {
		let value = Value::Uint2(65535);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uint4() {
		let value = Value::Uint4(100_000);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uint8() {
		let value = Value::Uint8(999);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uint16() {
		let value = Value::Uint16(u128::MAX);
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_date() {
		let value = Value::Date(Date::from_ymd(2024, 6, 15).unwrap());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_datetime() {
		let value = Value::DateTime(DateTime::from_ymd_hms(2024, 6, 15, 12, 30, 45).unwrap());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_time() {
		let value = Value::Time(Time::from_hms(12, 30, 45).unwrap());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_duration() {
		let value = Value::Duration(Duration::from_nanoseconds(1_000_000));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_identity_id() {
		let value = Value::IdentityId(IdentityId::generate());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uuid4() {
		let value = Value::Uuid4(Uuid4::generate());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uuid7() {
		let value = Value::Uuid7(Uuid7::generate());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_blob() {
		let value = Value::Blob(Blob::from(vec![0x01, 0x02, 0x03]));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_int() {
		let value = Value::Int(Int(BigInt::from(-42)));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_uint() {
		let value = Value::Uint(Uint(BigInt::from(42)));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_decimal() {
		let value = Value::Decimal(Decimal::from_str("3.14").unwrap());
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_dictionary_id_u1() {
		let value = Value::DictionaryId(DictionaryEntryId::U1(42));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_dictionary_id_u2() {
		let value = Value::DictionaryId(DictionaryEntryId::U2(1000));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_dictionary_id_u4() {
		let value = Value::DictionaryId(DictionaryEntryId::U4(100_000));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_dictionary_id_u8() {
		let value = Value::DictionaryId(DictionaryEntryId::U8(10_000_000_000));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_dictionary_id_u16() {
		let value = Value::DictionaryId(DictionaryEntryId::U16(u128::MAX));
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let bytes = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_value().unwrap(), value);
		assert!(de.is_empty());
	}

	#[test]
	fn test_roundtrip_all() {
		let values = vec![
			Value::none(),
			Value::none_of(Type::Int4),
			Value::Boolean(true),
			Value::Boolean(false),
			Value::Float4(OrderedF32::try_from(3.14f32).unwrap()),
			Value::Float8(OrderedF64::try_from(3.14).unwrap()),
			Value::Int1(-42),
			Value::Int2(-1000),
			Value::Int4(42),
			Value::Int8(-1_000_000),
			Value::Int16(123_456_789),
			Value::Utf8("hello world".to_string()),
			Value::Uint1(255),
			Value::Uint2(65535),
			Value::Uint4(100_000),
			Value::Uint8(999),
			Value::Uint16(u128::MAX),
			Value::Date(Date::from_ymd(2024, 6, 15).unwrap()),
			Value::DateTime(DateTime::from_ymd_hms(2024, 6, 15, 12, 30, 45).unwrap()),
			Value::Time(Time::from_hms(12, 30, 45).unwrap()),
			Value::Duration(Duration::from_nanoseconds(1_000_000)),
			Value::IdentityId(IdentityId::generate()),
			Value::Uuid4(Uuid4::generate()),
			Value::Uuid7(Uuid7::generate()),
			Value::Blob(Blob::from(vec![0x01, 0x02, 0x03])),
			Value::Int(Int(BigInt::from(-42))),
			Value::Uint(Uint(BigInt::from(42))),
			Value::Decimal(Decimal::from_str("3.14").unwrap()),
			Value::DictionaryId(DictionaryEntryId::U8(42)),
		];

		let mut ser = KeySerializer::new();
		for v in &values {
			ser.extend_value(v);
		}
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		for expected in &values {
			let actual = de.read_value().unwrap();
			assert_eq!(&actual, expected);
		}
		assert!(de.is_empty());
	}

	/// Compile-time exhaustiveness guard: if a new Value variant is added,
	/// this test will fail to compile. Add a corresponding `test_roundtrip_<variant>`
	/// test above, then add the new variant arm here.
	#[test]
	fn test_roundtrip_exhaustiveness_guard() {
		let value = Value::none();
		match value {
			Value::None {
				..
			} => {}
			Value::Boolean(_) => {}
			Value::Float4(_) => {}
			Value::Float8(_) => {}
			Value::Int1(_) => {}
			Value::Int2(_) => {}
			Value::Int4(_) => {}
			Value::Int8(_) => {}
			Value::Int16(_) => {}
			Value::Utf8(_) => {}
			Value::Uint1(_) => {}
			Value::Uint2(_) => {}
			Value::Uint4(_) => {}
			Value::Uint8(_) => {}
			Value::Uint16(_) => {}
			Value::Date(_) => {}
			Value::DateTime(_) => {}
			Value::Time(_) => {}
			Value::Duration(_) => {}
			Value::IdentityId(_) => {}
			Value::Uuid4(_) => {}
			Value::Uuid7(_) => {}
			Value::Blob(_) => {}
			Value::Int(_) => {}
			Value::Uint(_) => {}
			Value::Decimal(_) => {}
			Value::DictionaryId(_) => {}
			// Not serializable in keys:
			Value::Any(_) => {}
			Value::Type(_) => {}
			Value::List(_) => {}
			Value::Record(_) => {}
			Value::Tuple(_) => {}
		}
	}

	#[test]
	fn test_to_encoded_key() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i32(42);
		let key = serializer.to_encoded_key();
		assert_eq!(key.len(), 4);
	}

	#[test]
	fn test_index_id() {
		let mut serializer = KeySerializer::new();
		serializer.extend_index_id(IndexId::Primary(PrimaryKeyId(123456789)));
		let result = serializer.finish();

		// IndexId Primary uses 1 byte prefix + 8 bytes u64 with bitwise NOT
		assert_eq!(result.len(), 9);
		assert_eq!(result[0], 0x01); // Primary variant prefix

		// Verify it's using bitwise NOT (smaller values produce larger encoded values)
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_index_id(IndexId::Primary(PrimaryKeyId(1)));
		let result2 = serializer2.finish();

		// result2 (for IndexId(1)) should be > result (for IndexId(123456789))
		// Compare from byte 1 onwards (after the variant prefix)
		assert!(result2[1..] > result[1..]);
	}

	#[test]
	fn test_primitive_id() {
		let mut serializer = KeySerializer::new();
		serializer.extend_primitive_id(PrimitiveId::Table(TableId(987654321)));
		let result = serializer.finish();

		// PrimitiveId Table uses 1 byte prefix + 8 bytes u64 with bitwise NOT
		assert_eq!(result.len(), 9);
		assert_eq!(result[0], 0x01); // Table variant prefix

		// Verify ordering
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_primitive_id(PrimitiveId::Table(TableId(987654322)));
		let result2 = serializer2.finish();

		// result2 (for larger PrimitiveId) should be < result (inverted ordering)
		// Compare from byte 1 onwards (after the variant prefix)
		assert!(result2[1..] < result[1..]);
	}
}
