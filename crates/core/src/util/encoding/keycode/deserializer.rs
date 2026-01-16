// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_bigint::Sign;
use reifydb_type::{
	error,
	error::diagnostic::serde::serde_keycode_error,
	value::{
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		row_number::RowNumber,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};

use super::{catalog, deserialize};
use crate::interface::catalog::{id::IndexId, primitive::PrimitiveId};

pub struct KeyDeserializer<'a> {
	buffer: &'a [u8],
	position: usize,
}

impl<'a> KeyDeserializer<'a> {
	pub fn from_bytes(buffer: &'a [u8]) -> Self {
		Self {
			buffer,
			position: 0,
		}
	}

	pub fn remaining(&self) -> usize {
		self.buffer.len().saturating_sub(self.position)
	}

	pub fn is_empty(&self) -> bool {
		self.remaining() == 0
	}

	pub fn position(&self) -> usize {
		self.position
	}

	fn read_exact(&mut self, count: usize) -> reifydb_type::Result<&'a [u8]> {
		if self.remaining() < count {
			return Err(error!(serde_keycode_error(format!(
				"unexpected end of key at position {}: need {} bytes, have {}",
				self.position,
				count,
				self.remaining()
			))));
		}
		let start = self.position;
		self.position += count;
		Ok(&self.buffer[start..self.position])
	}

	pub fn read_bool(&mut self) -> reifydb_type::Result<bool> {
		let bytes = self.read_exact(1)?;
		Ok(deserialize::<bool>(bytes)?)
	}

	pub fn read_f32(&mut self) -> reifydb_type::Result<f32> {
		let bytes = self.read_exact(4)?;
		Ok(deserialize::<f32>(bytes)?)
	}

	pub fn read_f64(&mut self) -> reifydb_type::Result<f64> {
		let bytes = self.read_exact(8)?;
		Ok(deserialize::<f64>(bytes)?)
	}

	pub fn read_i8(&mut self) -> reifydb_type::Result<i8> {
		let bytes = self.read_exact(1)?;
		Ok(deserialize::<i8>(bytes)?)
	}

	pub fn read_i16(&mut self) -> reifydb_type::Result<i16> {
		let bytes = self.read_exact(2)?;
		Ok(deserialize::<i16>(bytes)?)
	}

	pub fn read_i32(&mut self) -> reifydb_type::Result<i32> {
		let bytes = self.read_exact(4)?;
		Ok(deserialize::<i32>(bytes)?)
	}

	pub fn read_i64(&mut self) -> reifydb_type::Result<i64> {
		let bytes = self.read_exact(8)?;
		Ok(deserialize::<i64>(bytes)?)
	}

	pub fn read_i128(&mut self) -> reifydb_type::Result<i128> {
		let bytes = self.read_exact(16)?;
		Ok(deserialize::<i128>(bytes)?)
	}

	pub fn read_u8(&mut self) -> reifydb_type::Result<u8> {
		let bytes = self.read_exact(1)?;
		Ok(deserialize::<u8>(bytes)?)
	}

	pub fn read_u16(&mut self) -> reifydb_type::Result<u16> {
		let bytes = self.read_exact(2)?;
		Ok(deserialize::<u16>(bytes)?)
	}

	pub fn read_u32(&mut self) -> reifydb_type::Result<u32> {
		let bytes = self.read_exact(4)?;
		Ok(deserialize::<u32>(bytes)?)
	}

	pub fn read_u64(&mut self) -> reifydb_type::Result<u64> {
		let bytes = self.read_exact(8)?;
		Ok(deserialize::<u64>(bytes)?)
	}

	pub fn read_u128(&mut self) -> reifydb_type::Result<u128> {
		let bytes = self.read_exact(16)?;
		Ok(deserialize::<u128>(bytes)?)
	}

	pub fn read_bytes(&mut self) -> reifydb_type::Result<Vec<u8>> {
		let mut result = Vec::new();
		loop {
			if self.remaining() < 1 {
				return Err(error!(serde_keycode_error(format!(
					"unexpected end of key at position {}: bytes not terminated",
					self.position
				))));
			}
			let byte = self.buffer[self.position];
			self.position += 1;

			if byte == 0xff {
				if self.remaining() < 1 {
					return Err(error!(serde_keycode_error(format!(
						"unexpected end of key at position {}: incomplete escape sequence",
						self.position
					))));
				}
				let next_byte = self.buffer[self.position];
				self.position += 1;

				if next_byte == 0x00 {
					result.push(0xff);
				} else if next_byte == 0xff {
					break;
				} else {
					return Err(error!(serde_keycode_error(format!(
						"invalid escape sequence at position {}: 0xff 0x{:02x}",
						self.position - 1,
						next_byte
					))));
				}
			} else {
				result.push(byte);
			}
		}
		Ok(result)
	}

	pub fn read_str(&mut self) -> reifydb_type::Result<String> {
		let bytes = self.read_bytes()?;
		String::from_utf8(bytes).map_err(|e| {
			error!(serde_keycode_error(format!(
				"invalid UTF-8 in key at position {}: {}",
				self.position, e
			)))
		})
	}

	pub fn read_primitive_id(&mut self) -> reifydb_type::Result<PrimitiveId> {
		let bytes = self.read_exact(9)?;
		catalog::deserialize_primitive_id(bytes)
	}

	pub fn read_index_id(&mut self) -> reifydb_type::Result<IndexId> {
		let bytes = self.read_exact(9)?;
		catalog::deserialize_index_id(bytes)
	}

	pub fn read_date(&mut self) -> reifydb_type::Result<Date> {
		let days = self.read_i32()?;
		Date::from_days_since_epoch(days).ok_or_else(|| {
			error!(serde_keycode_error(format!(
				"invalid date at position {}: {} days since epoch",
				self.position, days
			)))
		})
	}

	pub fn read_datetime(&mut self) -> reifydb_type::Result<DateTime> {
		let nanos = self.read_i64()?;
		Ok(DateTime::from_nanos_since_epoch(nanos))
	}

	pub fn read_time(&mut self) -> reifydb_type::Result<Time> {
		let nanos = self.read_u64()?;
		Time::from_nanos_since_midnight(nanos).ok_or_else(|| {
			error!(serde_keycode_error(format!(
				"invalid time at position {}: {} nanos since midnight",
				self.position, nanos
			)))
		})
	}

	pub fn read_duration(&mut self) -> reifydb_type::Result<Duration> {
		let nanos = self.read_i64()?;
		Ok(Duration::from_nanoseconds(nanos))
	}

	pub fn read_row_number(&mut self) -> reifydb_type::Result<RowNumber> {
		let value = self.read_u64()?;
		Ok(RowNumber(value))
	}

	pub fn read_identity_id(&mut self) -> reifydb_type::Result<IdentityId> {
		let bytes = self.read_bytes()?;
		let uuid = uuid::Uuid::from_slice(&bytes).map_err(|e| {
			error!(serde_keycode_error(format!("invalid IdentityId at position {}: {}", self.position, e)))
		})?;
		Ok(IdentityId::from(Uuid7::from(uuid)))
	}

	pub fn read_uuid4(&mut self) -> reifydb_type::Result<Uuid4> {
		let bytes = self.read_bytes()?;
		let uuid = uuid::Uuid::from_slice(&bytes).map_err(|e| {
			error!(serde_keycode_error(format!("invalid Uuid4 at position {}: {}", self.position, e)))
		})?;
		Ok(Uuid4::from(uuid))
	}

	pub fn read_uuid7(&mut self) -> reifydb_type::Result<Uuid7> {
		let bytes = self.read_bytes()?;
		let uuid = uuid::Uuid::from_slice(&bytes).map_err(|e| {
			error!(serde_keycode_error(format!("invalid Uuid7 at position {}: {}", self.position, e)))
		})?;
		Ok(Uuid7::from(uuid))
	}

	pub fn read_blob(&mut self) -> reifydb_type::Result<Blob> {
		let bytes = self.read_bytes()?;
		Ok(Blob::from(bytes))
	}

	pub fn read_int(&mut self) -> reifydb_type::Result<Int> {
		let sign = self.read_exact(1)?[0];
		let len = self.read_u32()? as usize;
		let bytes = self.read_exact(len)?;

		let sign = match sign {
			0 => Sign::Minus,
			_ => Sign::Plus,
		};

		Ok(Int(num_bigint::BigInt::from_bytes_be(sign, bytes)))
	}

	pub fn read_uint(&mut self) -> reifydb_type::Result<Uint> {
		let len = self.read_u32()? as usize;
		let bytes = self.read_exact(len)?;
		Ok(Uint(num_bigint::BigInt::from_bytes_be(Sign::Plus, bytes)))
	}

	pub fn read_decimal(&mut self) -> reifydb_type::Result<Decimal> {
		let s = self.read_str()?;
		s.parse::<Decimal>().map_err(|e| {
			error!(serde_keycode_error(format!("invalid Decimal at position {}: {}", self.position, e)))
		})
	}

	pub fn read_value(&mut self) -> reifydb_type::Result<reifydb_type::value::Value> {
		use reifydb_type::value::Value;

		if self.remaining() < 1 {
			return Err(error!(serde_keycode_error(format!(
				"unexpected end of key at position {}: cannot read value type",
				self.position
			))));
		}

		let type_marker = self.buffer[self.position];
		self.position += 1;

		match type_marker {
			0x00 => {
				if self.remaining() > 0 && self.buffer[self.position] == 0x00 {
					Ok(Value::Boolean(true))
				} else {
					Ok(Value::Undefined)
				}
			}
			0x01 => {
				let b = self.read_bool()?;
				Ok(Value::Boolean(b))
			}
			0x02 => {
				let f = self.read_f32()?;
				Ok(Value::Float4(OrderedF32::try_from(f).map_err(|e| {
					error!(serde_keycode_error(format!(
						"invalid f32 at position {}: {}",
						self.position, e
					)))
				})?))
			}
			0x03 => {
				let f = self.read_f64()?;
				Ok(Value::Float8(OrderedF64::try_from(f).map_err(|e| {
					error!(serde_keycode_error(format!(
						"invalid f64 at position {}: {}",
						self.position, e
					)))
				})?))
			}
			0x04 => {
				let i = self.read_i8()?;
				Ok(Value::Int1(i))
			}
			0x05 => {
				let i = self.read_i16()?;
				Ok(Value::Int2(i))
			}
			0x06 => {
				let i = self.read_i32()?;
				Ok(Value::Int4(i))
			}
			0x07 => {
				let i = self.read_i64()?;
				Ok(Value::Int8(i))
			}
			0x08 => {
				let i = self.read_i128()?;
				Ok(Value::Int16(i))
			}
			0x09 => {
				let s = self.read_str()?;
				Ok(Value::Utf8(s))
			}
			0x0a => {
				let u = self.read_u8()?;
				Ok(Value::Uint1(u))
			}
			0x0b => {
				let u = self.read_u16()?;
				Ok(Value::Uint2(u))
			}
			0x0c => {
				let u = self.read_u32()?;
				Ok(Value::Uint4(u))
			}
			0x0d => {
				let u = self.read_u64()?;
				Ok(Value::Uint8(u))
			}
			0x0e => {
				let u = self.read_u128()?;
				Ok(Value::Uint16(u))
			}
			0x0f => {
				let d = self.read_date()?;
				Ok(Value::Date(d))
			}
			0x10 => {
				let dt = self.read_datetime()?;
				Ok(Value::DateTime(dt))
			}
			0x11 => {
				let t = self.read_time()?;
				Ok(Value::Time(t))
			}
			0x12 => {
				let i = self.read_duration()?;
				Ok(Value::Duration(i))
			}
			// 0x13 was RowNumber, now reserved
			0x13 => panic!("Type code 0x13 (RowNumber) is no longer supported"),
			0x14 => {
				let id = self.read_identity_id()?;
				Ok(Value::IdentityId(id))
			}
			0x15 => {
				let u = self.read_uuid4()?;
				Ok(Value::Uuid4(u))
			}
			0x16 => {
				let u = self.read_uuid7()?;
				Ok(Value::Uuid7(u))
			}
			0x17 => {
				let b = self.read_blob()?;
				Ok(Value::Blob(b))
			}
			0x18 => {
				let i = self.read_int()?;
				Ok(Value::Int(i))
			}
			0x19 => {
				let u = self.read_uint()?;
				Ok(Value::Uint(u))
			}
			0x1a => {
				let d = self.read_decimal()?;
				Ok(Value::Decimal(d))
			}
			_ => Err(error!(serde_keycode_error(format!(
				"unknown value type marker 0x{:02x} at position {}",
				type_marker,
				self.position - 1
			)))),
		}
	}

	pub fn read_raw(&mut self, count: usize) -> reifydb_type::Result<&'a [u8]> {
		self.read_exact(count)
	}
}

#[cfg(test)]
pub mod tests {
	use std::f64::consts::E;

	use reifydb_type::value::{
		date::Date, datetime::DateTime, duration::Duration, row_number::RowNumber, time::Time,
	};

	use crate::{
		interface::catalog::{id::IndexId, primitive::PrimitiveId},
		util::encoding::keycode::{deserializer::KeyDeserializer, serializer::KeySerializer},
	};

	#[test]
	fn test_read_bool() {
		let mut ser = KeySerializer::new();
		ser.extend_bool(true).extend_bool(false);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_bool().unwrap(), true);
		assert_eq!(de.read_bool().unwrap(), false);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_integers() {
		let mut ser = KeySerializer::new();
		ser.extend_i8(-42i8).extend_i16(-1000i16).extend_i32(100000i32).extend_i64(-1000000000i64);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_i8().unwrap(), -42);
		assert_eq!(de.read_i16().unwrap(), -1000);
		assert_eq!(de.read_i32().unwrap(), 100000);
		assert_eq!(de.read_i64().unwrap(), -1000000000);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_unsigned() {
		let mut ser = KeySerializer::new();
		ser.extend_u8(255u8).extend_u16(65535u16).extend_u32(4294967295u32).extend_u64(18446744073709551615u64);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_u8().unwrap(), 255);
		assert_eq!(de.read_u16().unwrap(), 65535);
		assert_eq!(de.read_u32().unwrap(), 4294967295);
		assert_eq!(de.read_u64().unwrap(), 18446744073709551615);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_floats() {
		let mut ser = KeySerializer::new();
		ser.extend_f32(3.14).extend_f64(E);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert!((de.read_f32().unwrap() - 3.14).abs() < 0.001);
		assert!((de.read_f64().unwrap() - E).abs() < 0.000001);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_bytes() {
		let mut ser = KeySerializer::new();
		ser.extend_bytes(b"hello").extend_bytes(&[0x01, 0xff, 0x02]);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_bytes().unwrap(), b"hello");
		assert_eq!(de.read_bytes().unwrap(), vec![0x01, 0xff, 0x02]);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_str() {
		let mut ser = KeySerializer::new();
		ser.extend_str("hello world").extend_str("👋");
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_str().unwrap(), "hello world");
		assert_eq!(de.read_str().unwrap(), "👋");
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_date() {
		let mut ser = KeySerializer::new();
		let date = Date::from_ymd(2024, 1, 1).unwrap();
		ser.extend_date(&date);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_date().unwrap(), date);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_datetime() {
		let mut ser = KeySerializer::new();
		let datetime = DateTime::from_ymd_hms(2024, 1, 1, 12, 30, 45).unwrap();
		ser.extend_datetime(&datetime);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_datetime().unwrap(), datetime);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_time() {
		let mut ser = KeySerializer::new();
		let time = Time::from_hms(12, 30, 45).unwrap();
		ser.extend_time(&time);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_time().unwrap(), time);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_duration() {
		let mut ser = KeySerializer::new();
		let duration = Duration::from_nanoseconds(1000000);
		ser.extend_duration(&duration);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_duration().unwrap(), duration);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_row_number() {
		let mut ser = KeySerializer::new();
		let row = RowNumber(42);
		ser.extend_row_number(&row);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_row_number().unwrap(), row);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_primitive_id() {
		let mut ser = KeySerializer::new();
		let primitive = PrimitiveId::table(42);
		ser.extend_primitive_id(primitive);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_primitive_id().unwrap(), primitive);
		assert!(de.is_empty());
	}

	#[test]
	fn test_read_index_id() {
		let mut ser = KeySerializer::new();
		let index = IndexId::primary(999);
		ser.extend_index_id(index);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_index_id().unwrap(), index);
		assert!(de.is_empty());
	}

	#[test]
	fn test_position_tracking() {
		let mut ser = KeySerializer::new();
		ser.extend_u8(1u8).extend_u16(2u16).extend_u32(3u32);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.position(), 0);
		assert_eq!(de.remaining(), 7);

		de.read_u8().unwrap();
		assert_eq!(de.position(), 1);
		assert_eq!(de.remaining(), 6);

		de.read_u16().unwrap();
		assert_eq!(de.position(), 3);
		assert_eq!(de.remaining(), 4);

		de.read_u32().unwrap();
		assert_eq!(de.position(), 7);
		assert_eq!(de.remaining(), 0);
		assert!(de.is_empty());
	}

	#[test]
	fn test_error_on_insufficient_bytes() {
		let bytes = vec![0x00, 0x01];
		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert!(de.read_u32().is_err());
	}

	#[test]
	fn test_chaining() {
		let mut ser = KeySerializer::new();
		ser.extend_bool(true).extend_i32(42i32).extend_str("test").extend_u64(1000u64);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_bool().unwrap(), true);
		assert_eq!(de.read_i32().unwrap(), 42);
		assert_eq!(de.read_str().unwrap(), "test");
		assert_eq!(de.read_u64().unwrap(), 1000);
		assert!(de.is_empty());
	}
}
