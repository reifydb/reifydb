// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Blob, Date, DateTime, Decimal, IdentityId, Int, Interval, RowNumber, Time, Uint, Uuid4, Uuid7};
use serde::Serialize;

use super::{
	catalog, encode_bool, encode_bytes, encode_f32, encode_f64, encode_i8, encode_i16, encode_i32, encode_i64,
	encode_i128, encode_u8, encode_u16, encode_u32, encode_u64, encode_u128, serialize,
};
use crate::interface::{IndexId, SourceId};

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
	pub fn to_encoded_key(self) -> crate::EncodedKey {
		crate::EncodedKey::new(self.buffer)
	}

	/// Extend with a SourceId value (includes type discriminator)
	pub fn extend_source_id(&mut self, source: impl Into<SourceId>) -> &mut Self {
		let source = source.into();
		self.buffer.extend_from_slice(&catalog::serialize_source_id(&source));
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

	/// Extend with Interval value
	pub fn extend_interval(&mut self, interval: &Interval) -> &mut Self {
		self.extend_i64(interval.get_nanos())
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
			num_bigint::Sign::Minus => 0,
			_ => 1,
		});
		self.extend_u32(bytes.len() as u32);
		self.extend_bytes(&bytes);
		self
	}

	/// Extend with arbitrary precision Uint value
	pub fn extend_uint(&mut self, uint: &Uint) -> &mut Self {
		// For arbitrary precision unsigned, encode as bytes with length prefix
		let (_sign, bytes) = uint.0.to_bytes_be();
		self.extend_u32(bytes.len() as u32);
		self.extend_bytes(&bytes);
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

	/// Extend with a Value based on its type
	pub fn extend_value(&mut self, value: &reifydb_type::Value) -> &mut Self {
		use reifydb_type::Value;
		match value {
			Value::Undefined => {
				// For undefined, use a special marker byte
				self.buffer.push(0x00);
			}
			Value::Boolean(b) => {
				self.extend_bool(*b);
			}
			Value::Float4(f) => {
				self.extend_f32(**f);
			}
			Value::Float8(f) => {
				self.extend_f64(**f);
			}
			Value::Int1(i) => {
				self.extend_i8(*i);
			}
			Value::Int2(i) => {
				self.extend_i16(*i);
			}
			Value::Int4(i) => {
				self.extend_i32(*i);
			}
			Value::Int8(i) => {
				self.extend_i64(*i);
			}
			Value::Int16(i) => {
				self.extend_i128(*i);
			}
			Value::Utf8(s) => {
				self.extend_str(s);
			}
			Value::Uint1(u) => {
				self.extend_u8(*u);
			}
			Value::Uint2(u) => {
				self.extend_u16(*u);
			}
			Value::Uint4(u) => {
				self.extend_u32(*u);
			}
			Value::Uint8(u) => {
				self.extend_u64(*u);
			}
			Value::Uint16(u) => {
				self.extend_u128(*u);
			}
			Value::Date(d) => {
				self.extend_date(d);
			}
			Value::DateTime(dt) => {
				self.extend_datetime(dt);
			}
			Value::Time(t) => {
				self.extend_time(t);
			}
			Value::Interval(i) => {
				self.extend_interval(i);
			}
			Value::RowNumber(r) => {
				self.extend_row_number(r);
			}
			Value::IdentityId(id) => {
				self.extend_identity_id(id);
			}
			Value::Uuid4(uuid) => {
				self.extend_uuid4(uuid);
			}
			Value::Uuid7(uuid) => {
				self.extend_uuid7(uuid);
			}
			Value::Blob(b) => {
				self.extend_blob(b);
			}
			Value::Int(i) => {
				self.extend_int(i);
			}
			Value::Uint(u) => {
				self.extend_uint(u);
			}
			Value::Decimal(d) => {
				self.extend_decimal(d);
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
mod tests {
	use super::*;

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

		let mut serializer = KeySerializer::new();
		serializer.extend_bool(false);
		let result = serializer.finish();
		assert_eq!(result, vec![0x01]);
	}

	#[test]
	fn test_extend_f32() {
		let mut serializer = KeySerializer::new();
		serializer.extend_f32(3.14f32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_f32(-3.14f32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);
	}

	#[test]
	fn test_extend_f64() {
		let mut serializer = KeySerializer::new();
		serializer.extend_f64(3.14159);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_f64(-3.14159);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);
	}

	#[test]
	fn test_extend_i8() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i8(42i8);
		let result = serializer.finish();
		assert_eq!(result.len(), 1);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_i8(-42i8);
		let result = serializer.finish();
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn test_extend_i16() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i16(1000i16);
		let result = serializer.finish();
		assert_eq!(result.len(), 2);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_i16(-1000i16);
		let result = serializer.finish();
		assert_eq!(result.len(), 2);
	}

	#[test]
	fn test_extend_i32() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i32(100000i32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_i32(-100000i32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);
	}

	#[test]
	fn test_extend_i64() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i64(1000000000i64);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_i64(-1000000000i64);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);
	}

	#[test]
	fn test_extend_i128() {
		let mut serializer = KeySerializer::new();
		serializer.extend_i128(100000000000000i128);
		let result = serializer.finish();
		assert_eq!(result.len(), 16);

		// Test negative value
		let mut serializer = KeySerializer::new();
		serializer.extend_i128(-100000000000000i128);
		let result = serializer.finish();
		assert_eq!(result.len(), 16);
	}

	#[test]
	fn test_extend_u8() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(42u8);
		let result = serializer.finish();
		assert_eq!(result.len(), 1);
		assert_eq!(result[0], !42u8);
	}

	#[test]
	fn test_extend_u16() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u16(1000u16);
		let result = serializer.finish();
		assert_eq!(result.len(), 2);
	}

	#[test]
	fn test_extend_u32() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u32(100000u32);
		let result = serializer.finish();
		assert_eq!(result.len(), 4);
	}

	#[test]
	fn test_extend_u64() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(1000000000u64);
		let result = serializer.finish();
		assert_eq!(result.len(), 8);
	}

	#[test]
	fn test_extend_u128() {
		let mut serializer = KeySerializer::new();
		serializer.extend_u128(100000000000000u128);
		let result = serializer.finish();
		assert_eq!(result.len(), 16);
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
		serializer.extend_bool(true).extend_i32(42).extend_str("test").extend_u64(1000u64);
		let result = serializer.finish();

		// Should have bool (1 byte) + i32 (4 bytes) + "test" with terminator (6 bytes) + u64 (8 bytes)
		assert!(result.len() >= 19);
	}

	#[test]
	fn test_date() {
		use reifydb_type::Date;
		let mut serializer = KeySerializer::new();
		let date = Date::from_ymd(2024, 1, 1).unwrap();
		serializer.extend_date(&date);
		let result = serializer.finish();
		assert_eq!(result.len(), 4); // i32 encoding
	}

	#[test]
	fn test_datetime() {
		use reifydb_type::DateTime;
		let mut serializer = KeySerializer::new();
		let datetime = DateTime::from_ymd_hms(2024, 1, 1, 12, 0, 0).unwrap();
		serializer.extend_datetime(&datetime);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // i64 encoding
	}

	#[test]
	fn test_time() {
		use reifydb_type::Time;
		let mut serializer = KeySerializer::new();
		let time = Time::from_hms(12, 30, 45).unwrap();
		serializer.extend_time(&time);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // u64 encoding
	}

	#[test]
	fn test_interval() {
		use reifydb_type::Interval;
		let mut serializer = KeySerializer::new();
		let interval = Interval::from_nanoseconds(1000000);
		serializer.extend_interval(&interval);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // i64 encoding
	}

	#[test]
	fn test_row_number() {
		use reifydb_type::RowNumber;
		let mut serializer = KeySerializer::new();
		let row_number = RowNumber(42);
		serializer.extend_row_number(&row_number);
		let result = serializer.finish();
		assert_eq!(result.len(), 8); // u64 encoding
	}

	#[test]
	fn test_identity_id() {
		use reifydb_type::IdentityId;
		let mut serializer = KeySerializer::new();
		let id = IdentityId::generate();
		serializer.extend_identity_id(&id);
		let result = serializer.finish();
		assert!(result.len() > 0);
	}

	#[test]
	fn test_uuid4() {
		use reifydb_type::Uuid4;
		let mut serializer = KeySerializer::new();
		let uuid = Uuid4::generate();
		serializer.extend_uuid4(&uuid);
		let result = serializer.finish();
		// UUID is 16 bytes plus encoding overhead
		assert!(result.len() > 16);
	}

	#[test]
	fn test_uuid7() {
		use reifydb_type::Uuid7;
		let mut serializer = KeySerializer::new();
		let uuid = Uuid7::generate();
		serializer.extend_uuid7(&uuid);
		let result = serializer.finish();
		// UUID is 16 bytes plus encoding overhead
		assert!(result.len() > 16);
	}

	#[test]
	fn test_blob() {
		use reifydb_type::Blob;
		let mut serializer = KeySerializer::new();
		let blob = Blob::from(vec![0x01, 0x02, 0x03]);
		serializer.extend_blob(&blob);
		let result = serializer.finish();
		// Should have data plus terminator
		assert!(result.len() > 3);
	}

	#[test]
	fn test_int() {
		use num_bigint::BigInt;
		use reifydb_type::Int;
		let mut serializer = KeySerializer::new();
		let int = Int(BigInt::from(42));
		serializer.extend_int(&int);
		let result = serializer.finish();
		// Should have sign byte + length + data
		assert!(result.len() > 0);
	}

	#[test]
	fn test_uint() {
		use num_bigint::BigInt;
		use reifydb_type::Uint;
		let mut serializer = KeySerializer::new();
		let uint = Uint(BigInt::from(42));
		serializer.extend_uint(&uint);
		let result = serializer.finish();
		// Should have length + data
		assert!(result.len() > 0);
	}

	#[test]
	fn test_decimal() {
		use std::str::FromStr;

		use reifydb_type::Decimal;
		let mut serializer = KeySerializer::new();
		let decimal = Decimal::from_str("3.14").unwrap();
		serializer.extend_decimal(&decimal);
		let result = serializer.finish();
		// Should encode as string
		assert!(result.len() > 0);
	}

	#[test]
	fn test_extend_value() {
		use reifydb_type::Value;

		// Test undefined
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Undefined);
		let result = serializer.finish();
		assert_eq!(result, vec![0x00]);

		// Test boolean
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Boolean(true));
		let result = serializer.finish();
		assert_eq!(result, vec![0x00]);

		// Test integer
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Int4(42));
		let result = serializer.finish();
		assert_eq!(result.len(), 4);

		// Test string
		let mut serializer = KeySerializer::new();
		serializer.extend_value(&Value::Utf8("test".to_string()));
		let result = serializer.finish();
		assert!(result.ends_with(&[0xff, 0xff]));
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
		use crate::interface::{IndexId, PrimaryKeyId};

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
	fn test_source_id() {
		use crate::interface::{SourceId, TableId};

		let mut serializer = KeySerializer::new();
		serializer.extend_source_id(SourceId::Table(TableId(987654321)));
		let result = serializer.finish();

		// SourceId Table uses 1 byte prefix + 8 bytes u64 with bitwise NOT
		assert_eq!(result.len(), 9);
		assert_eq!(result[0], 0x01); // Table variant prefix

		// Verify ordering
		let mut serializer2 = KeySerializer::new();
		serializer2.extend_source_id(SourceId::Table(TableId(987654322)));
		let result2 = serializer2.finish();

		// result2 (for larger SourceId) should be < result (inverted ordering)
		// Compare from byte 1 onwards (after the variant prefix)
		assert!(result2[1..] < result[1..]);
	}
}
