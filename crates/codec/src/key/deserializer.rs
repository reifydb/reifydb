// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
use reifydb_value::{
	error::{TypeError, ValueError},
	value::{
		Value,
		blob::Blob,
		constraint::precision::Precision,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		time::Time,
		uuid::{Uuid4, Uuid7},
	},
};
use uuid::Uuid;

use super::{
	CONTAINER_END, decode_bool, decode_f32, decode_f64, decode_fixed, decode_i8, decode_i16, decode_i32,
	decode_i64, decode_i128, decode_i256, decode_u8, decode_u16, decode_u32, decode_u64, decode_u128,
	decode_u128_varint,
};
use crate::{
	error::DecodeError,
	tag::{TypeTag, ValueKind},
	unscaled::{NARROW, check_unscaled, decode_params, width},
};

pub struct KeyDeserializer<'a> {
	buffer: &'a [u8],
	offset: usize,
}

impl<'a> KeyDeserializer<'a> {
	pub fn from_bytes(buffer: &'a [u8]) -> Self {
		Self {
			buffer,
			offset: 0,
		}
	}

	pub fn remaining(&self) -> usize {
		self.buffer.len().saturating_sub(self.offset)
	}

	pub fn is_empty(&self) -> bool {
		self.remaining() == 0
	}

	pub fn offset(&self) -> usize {
		self.offset
	}

	pub fn remaining_bytes(&self) -> &'a [u8] {
		&self.buffer[self.offset..]
	}

	fn read_exact(&mut self, count: usize) -> Result<&'a [u8], ValueError> {
		if self.remaining() < count {
			return Err(ValueError::from(TypeError::SerdeKeycode {
				message: format!(
					"unexpected end of key at offset {}: need {} bytes, have {}",
					self.offset,
					count,
					self.remaining()
				),
			}));
		}
		let start = self.offset;
		self.offset += count;
		Ok(&self.buffer[start..self.offset])
	}

	pub fn read_bool(&mut self) -> Result<bool, ValueError> {
		let bytes = self.read_exact(1)?;
		decode_bool(bytes[0])
	}

	pub fn read_f32(&mut self) -> Result<f32, ValueError> {
		let bytes = self.read_exact(4)?;
		Ok(decode_f32(bytes.try_into()?))
	}

	pub fn read_f64(&mut self) -> Result<f64, ValueError> {
		let bytes = self.read_exact(8)?;
		Ok(decode_f64(bytes.try_into()?))
	}

	pub fn read_i8(&mut self) -> Result<i8, ValueError> {
		let bytes = self.read_exact(1)?;
		Ok(decode_i8(bytes.try_into()?))
	}

	pub fn read_i16(&mut self) -> Result<i16, ValueError> {
		let bytes = self.read_exact(2)?;
		Ok(decode_i16(bytes.try_into()?))
	}

	pub fn read_i32(&mut self) -> Result<i32, ValueError> {
		let bytes = self.read_exact(4)?;
		Ok(decode_i32(bytes.try_into()?))
	}

	pub fn read_i64(&mut self) -> Result<i64, ValueError> {
		let bytes = self.read_exact(8)?;
		Ok(decode_i64(bytes.try_into()?))
	}

	pub fn read_i128(&mut self) -> Result<i128, ValueError> {
		let bytes = self.read_exact(16)?;
		Ok(decode_i128(bytes.try_into()?))
	}

	pub fn read_u8(&mut self) -> Result<u8, ValueError> {
		let bytes = self.read_exact(1)?;
		Ok(decode_u8(bytes[0]))
	}

	pub fn read_u16(&mut self) -> Result<u16, ValueError> {
		let bytes = self.read_exact(2)?;
		Ok(decode_u16(bytes.try_into()?))
	}

	pub fn read_u32(&mut self) -> Result<u32, ValueError> {
		let bytes = self.read_exact(4)?;
		Ok(decode_u32(bytes.try_into()?))
	}

	pub fn read_u64(&mut self) -> Result<u64, ValueError> {
		let bytes = self.read_exact(8)?;
		Ok(decode_u64(bytes.try_into()?))
	}

	pub fn read_u128(&mut self) -> Result<u128, ValueError> {
		let bytes = self.read_exact(16)?;
		Ok(decode_u128(bytes.try_into()?))
	}

	pub fn read_fixed<const N: usize>(&mut self) -> Result<[u8; N], ValueError> {
		let bytes = self.read_exact(N)?;
		Ok(decode_fixed(bytes.try_into()?))
	}

	pub fn read_u128_varint(&mut self) -> Result<u128, ValueError> {
		let mut slice = &self.buffer[self.offset..];
		let u = decode_u128_varint(&mut slice)?;
		self.offset = self.buffer.len() - slice.len();
		Ok(u)
	}

	pub fn read_bytes(&mut self) -> Result<Vec<u8>, ValueError> {
		let mut result = Vec::new();
		loop {
			if self.remaining() < 1 {
				return Err(ValueError::from(TypeError::SerdeKeycode {
					message: format!(
						"unexpected end of key at offset {}: bytes not terminated",
						self.offset
					),
				}));
			}
			let byte = self.buffer[self.offset];
			self.offset += 1;

			if byte == 0xff {
				if self.remaining() < 1 {
					return Err(ValueError::from(TypeError::SerdeKeycode {
						message: format!(
							"unexpected end of key at offset {}: incomplete escape sequence",
							self.offset
						),
					}));
				}
				let next_byte = self.buffer[self.offset];
				self.offset += 1;

				if next_byte == 0x00 {
					result.push(0x00);
				} else if next_byte == 0xff {
					break;
				} else {
					return Err(ValueError::from(TypeError::SerdeKeycode {
						message: format!(
							"invalid escape sequence at offset {}: 0xff 0x{:02x}",
							self.offset - 1,
							next_byte
						),
					}));
				}
			} else {
				result.push(!byte);
			}
		}
		Ok(result)
	}

	pub fn read_str(&mut self) -> Result<String, ValueError> {
		let bytes = self.read_bytes()?;
		String::from_utf8(bytes).map_err(|e| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid UTF-8 in key at offset {}: {}", self.offset, e),
			})
		})
	}

	pub fn read_date(&mut self) -> Result<Date, ValueError> {
		let days = self.read_i32()?;
		Date::from_days_since_epoch(days).ok_or_else(|| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid date at offset {}: {} days since epoch", self.offset, days),
			})
		})
	}

	pub fn read_datetime(&mut self) -> Result<DateTime, ValueError> {
		let nanos = self.read_i64()?;
		Ok(DateTime::from_nanos(nanos))
	}

	pub fn read_time(&mut self) -> Result<Time, ValueError> {
		let nanos = self.read_u64()?;
		Time::from_nanos_since_midnight(nanos).ok_or_else(|| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!(
					"invalid time at offset {}: {} nanos since midnight",
					self.offset, nanos
				),
			})
		})
	}

	pub fn read_duration(&mut self) -> Result<Duration, ValueError> {
		let months = self.read_i32()?;
		let days = self.read_i32()?;
		let nanos = self.read_i64()?;
		Ok(Duration::new(months, days, nanos)?)
	}

	pub fn read_identity_id(&mut self) -> Result<IdentityId, ValueError> {
		let bytes = self.read_bytes()?;
		let uuid = Uuid::from_slice(&bytes).map_err(|e| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid IdentityId at offset {}: {}", self.offset, e),
			})
		})?;
		let version = uuid.get_version_num();
		if version != 7 && version != 0 {
			return Err(ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid IdentityId version {} at offset {}", version, self.offset),
			}));
		}
		Ok(IdentityId::from(Uuid7::from(uuid)))
	}

	pub fn read_uuid4(&mut self) -> Result<Uuid4, ValueError> {
		let bytes = self.read_bytes()?;
		let uuid = Uuid::from_slice(&bytes).map_err(|e| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid Uuid4 at offset {}: {}", self.offset, e),
			})
		})?;
		let version = uuid.get_version_num();
		if version != 4 && version != 0 {
			return Err(ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid Uuid4 version {} at offset {}", version, self.offset),
			}));
		}
		Ok(Uuid4::from(uuid))
	}

	pub fn read_uuid7(&mut self) -> Result<Uuid7, ValueError> {
		let bytes = self.read_bytes()?;
		let uuid = Uuid::from_slice(&bytes).map_err(|e| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid Uuid7 at offset {}: {}", self.offset, e),
			})
		})?;
		let version = uuid.get_version_num();
		if version != 7 && version != 0 {
			return Err(ValueError::from(TypeError::SerdeKeycode {
				message: format!("invalid Uuid7 version {} at offset {}", version, self.offset),
			}));
		}
		Ok(Uuid7::from(uuid))
	}

	pub fn read_blob(&mut self) -> Result<Blob, ValueError> {
		let bytes = self.read_bytes()?;
		Ok(Blob::from(bytes))
	}

	pub fn read_decimal(&mut self) -> Result<Decimal, ValueError> {
		let precision = self.read_u8()?;
		let scale = self.read_u8()?;
		let unscaled = self.read_unscaled(ValueKind::Decimal, precision, scale)?;
		Ok(Decimal::from_parts(unscaled, scale).expect("a checked unscaled value is within 76 digits"))
	}

	fn read_unscaled(&mut self, kind: ValueKind, precision: u8, scale: u8) -> Result<i256, ValueError> {
		let offset = self.offset;
		let (precision, _) = decode_params(precision, scale).map_err(|error| unscaled_error(offset, error))?;
		let unscaled = self.read_payload(precision)?;
		check_unscaled(kind, unscaled, precision).map_err(|error| unscaled_error(offset, error))
	}

	fn read_payload(&mut self, precision: Precision) -> Result<i256, ValueError> {
		if width(precision) == NARROW {
			Ok(i256::from_i128(self.read_i128()?))
		} else {
			let bytes = self.read_exact(32)?;
			Ok(decode_i256(bytes.try_into()?))
		}
	}

	fn at_container_end(&mut self) -> Result<bool, ValueError> {
		if self.remaining() < 1 {
			return Err(ValueError::from(TypeError::SerdeKeycode {
				message: format!(
					"unexpected end of key at offset {}: container not terminated",
					self.offset
				),
			}));
		}
		if self.buffer[self.offset] == CONTAINER_END {
			self.offset += 1;
			return Ok(true);
		}
		Ok(false)
	}

	fn read_container_items(&mut self) -> Result<Vec<Value>, ValueError> {
		let mut items = Vec::new();
		while !self.at_container_end()? {
			items.push(self.read_value()?);
		}
		Ok(items)
	}

	fn read_record_fields(&mut self) -> Result<Vec<(String, Value)>, ValueError> {
		let mut fields = Vec::new();
		while !self.at_container_end()? {
			let name = self.read_str()?;
			fields.push((name, self.read_value()?));
		}
		Ok(fields)
	}

	pub fn read_value(&mut self) -> Result<Value, ValueError> {
		if self.remaining() < 1 {
			return Err(ValueError::from(TypeError::SerdeKeycode {
				message: format!(
					"unexpected end of key at offset {}: cannot read value type",
					self.offset
				),
			}));
		}

		let type_marker = self.buffer[self.offset];
		self.offset += 1;

		let kind = ValueKind::from_byte(type_marker).ok_or_else(|| {
			ValueError::from(TypeError::SerdeKeycode {
				message: format!(
					"unknown value type marker 0x{:02x} at offset {}",
					type_marker,
					self.offset - 1
				),
			})
		})?;

		match kind {
			ValueKind::None => {
				if self.remaining() < 1 {
					return Ok(Value::none());
				}
				let inner_marker = self.buffer[self.offset];
				self.offset += 1;
				let inner = TypeTag::from_byte(inner_marker)
					.map_err(|e| {
						ValueError::from(TypeError::SerdeKeycode {
							message: format!(
								"invalid none inner type byte 0x{:02x} at offset {}: {}",
								inner_marker,
								self.offset - 1,
								e
							),
						})
					})?
					.to_type()
					.map_err(|e| {
						ValueError::from(TypeError::SerdeKeycode {
							message: format!(
								"invalid none inner type byte 0x{:02x} at offset {}: {}",
								inner_marker,
								self.offset - 1,
								e
							),
						})
					})?;
				Ok(Value::none_of(inner))
			}
			ValueKind::Float4 => {
				let f = self.read_f32()?;
				Ok(Value::Float4(OrderedF32::try_from(f).map_err(|e| {
					ValueError::from(TypeError::SerdeKeycode {
						message: format!("invalid f32 at offset {}: {}", self.offset, e),
					})
				})?))
			}
			ValueKind::Float8 => {
				let f = self.read_f64()?;
				Ok(Value::Float8(OrderedF64::try_from(f).map_err(|e| {
					ValueError::from(TypeError::SerdeKeycode {
						message: format!("invalid f64 at offset {}: {}", self.offset, e),
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
			ValueKind::Decimal => Ok(Value::Decimal(self.read_decimal()?)),
			ValueKind::List => Ok(Value::List(self.read_container_items()?)),
			ValueKind::Tuple => Ok(Value::Tuple(self.read_container_items()?)),
			ValueKind::Record => Ok(Value::Record(self.read_record_fields()?)),
			ValueKind::Any | ValueKind::Type | ValueKind::Digest | ValueKind::Enum => {
				Err(ValueError::from(TypeError::SerdeKeycode {
					message: format!(
						"value kind {:?} cannot be deserialized from keys (offset {})",
						kind,
						self.offset - 1
					),
				}))
			}
			ValueKind::DictionaryId => {
				let sub = self.read_exact(1)?[0];
				match sub {
					0x00 => Ok(Value::DictionaryId(DictionaryEntryId::U1(self.read_u8()?))),
					0x01 => Ok(Value::DictionaryId(DictionaryEntryId::U2(self.read_u16()?))),
					0x02 => Ok(Value::DictionaryId(DictionaryEntryId::U4(self.read_u32()?))),
					0x03 => Ok(Value::DictionaryId(DictionaryEntryId::U8(self.read_u64()?))),
					0x04 => Ok(Value::DictionaryId(DictionaryEntryId::U16(self.read_u128()?))),
					_ => Err(ValueError::from(TypeError::SerdeKeycode {
						message: format!(
							"unknown DictionaryEntryId sub-marker 0x{:02x} at offset {}",
							sub,
							self.offset - 1
						),
					})),
				}
			}
		}
	}

	pub fn read_raw(&mut self, count: usize) -> Result<&'a [u8], ValueError> {
		self.read_exact(count)
	}
}

fn unscaled_error(offset: usize, error: DecodeError) -> ValueError {
	ValueError::from(TypeError::SerdeKeycode {
		message: format!("invalid fixed-width number at offset {offset}: {error}"),
	})
}
