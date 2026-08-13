// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::{BigInt, Sign};
use reifydb_value::{
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
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use uuid::Uuid;

use super::{CONTAINER_END, decode_u128_varint, deserialize};
use crate::tag::{TypeTag, ValueKind};

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

	pub fn remaining_bytes(&self) -> &'a [u8] {
		&self.buffer[self.position..]
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

	pub fn read_u128_varint(&mut self) -> Result<u128> {
		let mut slice = &self.buffer[self.position..];
		let u = decode_u128_varint(&mut slice)?;
		self.position = self.buffer.len() - slice.len();
		Ok(u)
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

	fn at_container_end(&mut self) -> Result<bool> {
		if self.remaining() < 1 {
			return Err(Error::from(TypeError::SerdeKeycode {
				message: format!(
					"unexpected end of key at position {}: container not terminated",
					self.position
				),
			}));
		}
		if self.buffer[self.position] == CONTAINER_END {
			self.position += 1;
			return Ok(true);
		}
		Ok(false)
	}

	fn read_container_items(&mut self) -> Result<Vec<Value>> {
		let mut items = Vec::new();
		while !self.at_container_end()? {
			items.push(self.read_value()?);
		}
		Ok(items)
	}

	fn read_record_fields(&mut self) -> Result<Vec<(String, Value)>> {
		let mut fields = Vec::new();
		while !self.at_container_end()? {
			let name = self.read_str()?;
			fields.push((name, self.read_value()?));
		}
		Ok(fields)
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

		let kind = ValueKind::from_byte(type_marker).ok_or_else(|| {
			Error::from(TypeError::SerdeKeycode {
				message: format!(
					"unknown value type marker 0x{:02x} at position {}",
					type_marker,
					self.position - 1
				),
			})
		})?;

		match kind {
			ValueKind::None => {
				if self.remaining() < 1 {
					return Ok(Value::none());
				}
				let inner_marker = self.buffer[self.position];
				self.position += 1;
				let inner = TypeTag::from_byte(inner_marker)
					.map_err(|e| {
						Error::from(TypeError::SerdeKeycode {
							message: format!(
								"invalid none inner type byte 0x{:02x} at position {}: {}",
								inner_marker,
								self.position - 1,
								e
							),
						})
					})?
					.to_type()
					.map_err(|e| {
						Error::from(TypeError::SerdeKeycode {
							message: format!(
								"invalid none inner type byte 0x{:02x} at position {}: {}",
								inner_marker,
								self.position - 1,
								e
							),
						})
					})?;
				Ok(Value::none_of(inner))
			}
			ValueKind::Float4 => {
				let f = self.read_f32()?;
				Ok(Value::Float4(OrderedF32::try_from(f).map_err(|e| {
					Error::from(TypeError::SerdeKeycode {
						message: format!("invalid f32 at position {}: {}", self.position, e),
					})
				})?))
			}
			ValueKind::Float8 => {
				let f = self.read_f64()?;
				Ok(Value::Float8(OrderedF64::try_from(f).map_err(|e| {
					Error::from(TypeError::SerdeKeycode {
						message: format!("invalid f64 at position {}: {}", self.position, e),
					})
				})?))
			}
			ValueKind::Boolean => Ok(Value::Boolean(self.read_bool()?)),
			ValueKind::Int1 => Ok(Value::Int1(self.read_i8()?)),
			ValueKind::Int2 => Ok(Value::Int2(self.read_i16()?)),
			ValueKind::Int4 => Ok(Value::Int4(self.read_i32()?)),
			ValueKind::Int8 => Ok(Value::Int8(self.read_i64()?)),
			ValueKind::Int16 => Ok(Value::Int16(self.read_i128()?)),
			ValueKind::Utf8 => Ok(Value::Utf8(self.read_str()?)),
			ValueKind::Uint1 => Ok(Value::Uint1(self.read_u8()?)),
			ValueKind::Uint2 => Ok(Value::Uint2(self.read_u16()?)),
			ValueKind::Uint4 => Ok(Value::Uint4(self.read_u32()?)),
			ValueKind::Uint8 => Ok(Value::Uint8(self.read_u64()?)),
			ValueKind::Uint16 => Ok(Value::Uint16(self.read_u128()?)),
			ValueKind::Date => Ok(Value::Date(self.read_date()?)),
			ValueKind::DateTime => Ok(Value::DateTime(self.read_datetime()?)),
			ValueKind::Time => Ok(Value::Time(self.read_time()?)),
			ValueKind::Duration => Ok(Value::Duration(self.read_duration()?)),
			ValueKind::IdentityId => Ok(Value::IdentityId(self.read_identity_id()?)),
			ValueKind::Uuid4 => Ok(Value::Uuid4(self.read_uuid4()?)),
			ValueKind::Uuid7 => Ok(Value::Uuid7(self.read_uuid7()?)),
			ValueKind::Blob => Ok(Value::Blob(self.read_blob()?)),
			ValueKind::Int => Ok(Value::Int(self.read_int()?)),
			ValueKind::Uint => Ok(Value::Uint(self.read_uint()?)),
			ValueKind::Decimal => Ok(Value::Decimal(self.read_decimal()?)),
			ValueKind::List => Ok(Value::List(self.read_container_items()?)),
			ValueKind::Tuple => Ok(Value::Tuple(self.read_container_items()?)),
			ValueKind::Record => Ok(Value::Record(self.read_record_fields()?)),
			ValueKind::Any | ValueKind::Type => Err(Error::from(TypeError::SerdeKeycode {
				message: format!(
					"value kind {:?} cannot be deserialized from keys (position {})",
					kind,
					self.position - 1
				),
			})),
			ValueKind::DictionaryId => {
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
		}
	}

	pub fn read_raw(&mut self, count: usize) -> Result<&'a [u8]> {
		self.read_exact(count)
	}
}
