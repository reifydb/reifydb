// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::{BigInt, Sign};
use reifydb_type::{
	Result,
	error::{Error, TypeError},
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
use uuid::Uuid;

use super::{catalog, deserialize};
use crate::interface::catalog::{id::IndexId, shape::ShapeId};

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

	fn read_exact(&mut self, count: usize) -> Result<&'a [u8]> {
		if self.remaining() < count {
			return Err(Error::from(TypeError::SerdeKeycode {
				message: format!(
					"unexpected end of key at position {}: need {} bytes, have {}",
					self.position,
					count,
					self.remaining()
				),
			}));
		}
		let start = self.position;
		self.position += count;
		Ok(&self.buffer[start..self.position])
	}

	pub fn read_bool(&mut self) -> Result<bool> {
		let bytes = self.read_exact(1)?;
		deserialize::<bool>(bytes)
	}

	pub fn read_f32(&mut self) -> Result<f32> {
		let bytes = self.read_exact(4)?;
		deserialize::<f32>(bytes)
	}

	pub fn read_f64(&mut self) -> Result<f64> {
		let bytes = self.read_exact(8)?;
		deserialize::<f64>(bytes)
	}

	pub fn read_i8(&mut self) -> Result<i8> {
		let bytes = self.read_exact(1)?;
		deserialize::<i8>(bytes)
	}

	pub fn read_i16(&mut self) -> Result<i16> {
		let bytes = self.read_exact(2)?;
		deserialize::<i16>(bytes)
	}

	pub fn read_i32(&mut self) -> Result<i32> {
		let bytes = self.read_exact(4)?;
		deserialize::<i32>(bytes)
	}

	pub fn read_i64(&mut self) -> Result<i64> {
		let bytes = self.read_exact(8)?;
		deserialize::<i64>(bytes)
	}

	pub fn read_i128(&mut self) -> Result<i128> {
		let bytes = self.read_exact(16)?;
		deserialize::<i128>(bytes)
	}

	pub fn read_u8(&mut self) -> Result<u8> {
		let bytes = self.read_exact(1)?;
		deserialize::<u8>(bytes)
	}

	pub fn read_u16(&mut self) -> Result<u16> {
		let bytes = self.read_exact(2)?;
		deserialize::<u16>(bytes)
	}

	pub fn read_u32(&mut self) -> Result<u32> {
		let bytes = self.read_exact(4)?;
		deserialize::<u32>(bytes)
	}

	pub fn read_u64(&mut self) -> Result<u64> {
		let bytes = self.read_exact(8)?;
		deserialize::<u64>(bytes)
	}

	pub fn read_u128(&mut self) -> Result<u128> {
		let bytes = self.read_exact(16)?;
		deserialize::<u128>(bytes)
	}

	pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
		let mut result = Vec::new();
		loop {
			if self.remaining() < 1 {
				return Err(Error::from(TypeError::SerdeKeycode {
					message: format!(
						"unexpected end of key at position {}: bytes not terminated",
						self.position
					),
				}));
			}
			let byte = self.buffer[self.position];
			self.position += 1;

			if byte == 0xff {
				if self.remaining() < 1 {
					return Err(Error::from(TypeError::SerdeKeycode {
						message: format!(
							"unexpected end of key at position {}: incomplete escape sequence",
							self.position
						),
					}));
				}
				let next_byte = self.buffer[self.position];
				self.position += 1;

				if next_byte == 0x00 {
					result.push(0xff);
				} else if next_byte == 0xff {
					break;
				} else {
					return Err(Error::from(TypeError::SerdeKeycode {
						message: format!(
							"invalid escape sequence at position {}: 0xff 0x{:02x}",
							self.position - 1,
							next_byte
						),
					}));
				}
			} else {
				result.push(byte);
			}
		}
		Ok(result)
	}

	pub fn read_str(&mut self) -> Result<String> {
		let bytes = self.read_bytes()?;
		String::from_utf8(bytes).map_err(|e| {
			Error::from(TypeError::SerdeKeycode {
				message: format!("invalid UTF-8 in key at position {}: {}", self.position, e),
			})
		})
	}

	pub fn read_shape_id(&mut self) -> Result<ShapeId> {
		let bytes = self.read_exact(9)?;
		catalog::deserialize_shape_id(bytes)
	}

	pub fn read_index_id(&mut self) -> Result<IndexId> {
		let bytes = self.read_exact(9)?;
		catalog::deserialize_index_id(bytes)
	}

	pub fn read_date(&mut self) -> Result<Date> {
		let days = self.read_i32()?;
		Date::from_days_since_epoch(days).ok_or_else(|| {
			Error::from(TypeError::SerdeKeycode {
				message: format!(
					"invalid date at position {}: {} days since epoch",
					self.position, days
				),
			})
		})
	}

	pub fn read_datetime(&mut self) -> Result<DateTime> {
		let nanos = self.read_u64()?;
		Ok(DateTime::from_nanos(nanos))
	}

	pub fn read_time(&mut self) -> Result<Time> {
		let nanos = self.read_u64()?;
		Time::from_nanos_since_midnight(nanos).ok_or_else(|| {
			Error::from(TypeError::SerdeKeycode {
				message: format!(
					"invalid time at position {}: {} nanos since midnight",
					self.position, nanos
				),
			})
		})
	}

	pub fn read_duration(&mut self) -> Result<Duration> {
		let months = self.read_i32()?;
		let days = self.read_i32()?;
		let nanos = self.read_i64()?;
		Ok(Duration::new(months, days, nanos)?)
	}

	pub fn read_row_number(&mut self) -> Result<RowNumber> {
		let value = self.read_u64()?;
		Ok(RowNumber(value))
	}

	pub fn read_identity_id(&mut self) -> Result<IdentityId> {
		let bytes = self.read_bytes()?;
		let uuid = Uuid::from_slice(&bytes).map_err(|e| {
			Error::from(TypeError::SerdeKeycode {
				message: format!("invalid IdentityId at position {}: {}", self.position, e),
			})
		})?;
		Ok(IdentityId::from(Uuid7::from(uuid)))
	}

	pub fn read_uuid4(&mut self) -> Result<Uuid4> {
		let bytes = self.read_bytes()?;
		let uuid = Uuid::from_slice(&bytes).map_err(|e| {
			Error::from(TypeError::SerdeKeycode {
				message: format!("invalid Uuid4 at position {}: {}", self.position, e),
			})
		})?;
		Ok(Uuid4::from(uuid))
	}

	pub fn read_uuid7(&mut self) -> Result<Uuid7> {
		let bytes = self.read_bytes()?;
		let uuid = Uuid::from_slice(&bytes).map_err(|e| {
			Error::from(TypeError::SerdeKeycode {
				message: format!("invalid Uuid7 at position {}: {}", self.position, e),
			})
		})?;
		Ok(Uuid7::from(uuid))
	}

	pub fn read_blob(&mut self) -> Result<Blob> {
		let bytes = self.read_bytes()?;
		Ok(Blob::from(bytes))
	}

	pub fn read_int(&mut self) -> Result<Int> {
		let sign = self.read_exact(1)?[0];
		let len = self.read_u32()? as usize;
		let bytes = self.read_exact(len)?;

		let sign = match sign {
			0 => Sign::Minus,
			_ => Sign::Plus,
		};

		Ok(Int(BigInt::from_bytes_be(sign, bytes)))
	}

	pub fn read_uint(&mut self) -> Result<Uint> {
		let len = self.read_u32()? as usize;
		let bytes = self.read_exact(len)?;
		Ok(Uint(BigInt::from_bytes_be(Sign::Plus, bytes)))
	}

	pub fn read_decimal(&mut self) -> Result<Decimal> {
		let s = self.read_str()?;
		s.parse::<Decimal>().map_err(|e| {
			Error::from(TypeError::SerdeKeycode {
				message: format!("invalid Decimal at position {}: {}", self.position, e),
			})
		})
	}

	pub fn read_value(&mut self) -> Result<Value> {
		if self.remaining() < 1 {
			return Err(Error::from(TypeError::SerdeKeycode {
				message: format!(
					"unexpected end of key at position {}: cannot read value type",
					self.position
				),
			}));
		}

		let type_marker = self.buffer[self.position];
		self.position += 1;

		match type_marker {
			0x00 => {
				if self.remaining() < 1 {
					return Ok(Value::none());
				}
				let inner_marker = self.buffer[self.position];
				self.position += 1;
				let inner = match inner_marker {
					0x01 => Type::Boolean,
					0x02 => Type::Float4,
					0x03 => Type::Float8,
					0x04 => Type::Int1,
					0x05 => Type::Int2,
					0x06 => Type::Int4,
					0x07 => Type::Int8,
					0x08 => Type::Int16,
					0x09 => Type::Utf8,
					0x0a => Type::Uint1,
					0x0b => Type::Uint2,
					0x0c => Type::Uint4,
					0x0d => Type::Uint8,
					0x0e => Type::Uint16,
					0x0f => Type::Date,
					0x10 => Type::DateTime,
					0x11 => Type::Time,
					0x12 => Type::Duration,
					0x14 => Type::IdentityId,
					0x15 => Type::Uuid4,
					0x16 => Type::Uuid7,
					0x17 => Type::Blob,
					0x18 => Type::Int,
					0x19 => Type::Uint,
					0x1a => Type::Decimal,
					0x1b => Type::DictionaryId,
					_ => Type::Any,
				};
				Ok(Value::none_of(inner))
			}
			0x01 => {
				let b = self.read_bool()?;
				Ok(Value::Boolean(b))
			}
			0x02 => {
				let f = self.read_f32()?;
				Ok(Value::Float4(OrderedF32::try_from(f).map_err(|e| {
					Error::from(TypeError::SerdeKeycode {
						message: format!("invalid f32 at position {}: {}", self.position, e),
					})
				})?))
			}
			0x03 => {
				let f = self.read_f64()?;
				Ok(Value::Float8(OrderedF64::try_from(f).map_err(|e| {
					Error::from(TypeError::SerdeKeycode {
						message: format!("invalid f64 at position {}: {}", self.position, e),
					})
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
			0x1b => {
				let sub = self.read_exact(1)?[0];
				match sub {
					0x00 => Ok(Value::DictionaryId(DictionaryEntryId::U1(self.read_u8()?))),
					0x01 => Ok(Value::DictionaryId(DictionaryEntryId::U2(self.read_u16()?))),
					0x02 => Ok(Value::DictionaryId(DictionaryEntryId::U4(self.read_u32()?))),
					0x03 => Ok(Value::DictionaryId(DictionaryEntryId::U8(self.read_u64()?))),
					0x04 => Ok(Value::DictionaryId(DictionaryEntryId::U16(self.read_u128()?))),
					_ => Err(Error::from(TypeError::SerdeKeycode {
						message: format!(
							"unknown DictionaryEntryId sub-marker 0x{:02x} at position {}",
							sub,
							self.position - 1
						),
					})),
				}
			}
			_ => Err(Error::from(TypeError::SerdeKeycode {
				message: format!(
					"unknown value type marker 0x{:02x} at position {}",
					type_marker,
					self.position - 1
				),
			})),
		}
	}

	pub fn read_raw(&mut self, count: usize) -> Result<&'a [u8]> {
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
		interface::catalog::{id::IndexId, shape::ShapeId},
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
		let duration = Duration::from_nanoseconds(1000000).unwrap();
		ser.extend_duration(&duration);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_duration().unwrap(), duration);
		assert!(de.is_empty());
	}

	#[test]
	fn test_keycode_roundtrip_with_months_and_days() {
		let mut ser = KeySerializer::new();
		let duration = Duration::new(12, 5, 1_000_000_000).unwrap();
		ser.extend_duration(&duration);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_duration().unwrap(), duration);
		assert!(de.is_empty());
	}

	#[test]
	fn test_keycode_different_durations_produce_different_keys() {
		let d1 = Duration::new(12, 0, 0).unwrap();
		let d2 = Duration::zero();

		let mut s1 = KeySerializer::new();
		s1.extend_duration(&d1);
		let b1 = s1.finish();

		let mut s2 = KeySerializer::new();
		s2.extend_duration(&d2);
		let b2 = s2.finish();

		assert_ne!(b1, b2);
	}

	#[test]
	fn test_keycode_duration_ordering_preserved() {
		// Keycode encoding is descending: larger Duration → smaller bytes
		let durations = vec![
			Duration::new(0, 0, 0).unwrap(),
			Duration::new(0, 0, 1_000_000_000).unwrap(),
			Duration::new(0, 1, 0).unwrap(),
			Duration::new(1, 0, 0).unwrap(),
			Duration::new(12, 30, 0).unwrap(),
		];

		let keys: Vec<Vec<u8>> = durations
			.iter()
			.map(|d| {
				let mut ser = KeySerializer::new();
				ser.extend_duration(d);
				ser.finish()
			})
			.collect();

		for i in 0..keys.len() - 1 {
			assert!(
				keys[i] > keys[i + 1],
				"Key ordering broken: {:?} key should be > {:?} key (descending encoding)",
				durations[i],
				durations[i + 1]
			);
		}
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
	fn test_read_shape_id() {
		let mut ser = KeySerializer::new();
		let primitive = ShapeId::table(42);
		ser.extend_shape_id(primitive);
		let bytes = ser.finish();

		let mut de = KeyDeserializer::from_bytes(&bytes);
		assert_eq!(de.read_shape_id().unwrap(), primitive);
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
