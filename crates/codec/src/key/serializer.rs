// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::Sign;
use reifydb_value::value::{
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
	uint::Uint,
	uuid::{Uuid4, Uuid7},
	value_type::ValueType,
};

use super::{
	CONTAINER_END, encode_bool, encode_bytes, encode_f32, encode_f64, encode_i8, encode_i16, encode_i32,
	encode_i64, encode_i128, encode_u8, encode_u16, encode_u32, encode_u64, encode_u128, encode_u128_varint,
};
use crate::{
	key::{buf::KeyBuf, encoded::EncodedKey, sort::SortOrder},
	tag::{TypeTag, ValueKind},
};

fn keycode_type_descending(ty: &ValueType) -> bool {
	matches!(
		ty,
		ValueType::Boolean
			| ValueType::Float4 | ValueType::Float8
			| ValueType::Int1 | ValueType::Int2
			| ValueType::Int4 | ValueType::Int8
			| ValueType::Int16 | ValueType::Uint1
			| ValueType::Uint2 | ValueType::Uint4
			| ValueType::Uint8 | ValueType::Uint16
			| ValueType::Date | ValueType::DateTime
			| ValueType::Time | ValueType::Duration
	)
}

pub struct KeySerializer {
	buffer: KeyBuf,
}

impl KeySerializer {
	pub fn new() -> Self {
		Self {
			buffer: KeyBuf::new(),
		}
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			buffer: KeyBuf::with_capacity(capacity),
		}
	}

	pub fn extend_bool(&mut self, value: bool) -> &mut Self {
		self.buffer.push(encode_bool(value));
		self
	}

	pub fn extend_f32(&mut self, value: f32) -> &mut Self {
		self.buffer.extend_from_slice(&encode_f32(value));
		self
	}

	pub fn extend_f64(&mut self, value: f64) -> &mut Self {
		self.buffer.extend_from_slice(&encode_f64(value));
		self
	}

	pub fn extend_i8<T: Into<i8>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i8(value.into()));
		self
	}

	pub fn extend_i16<T: Into<i16>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i16(value.into()));
		self
	}

	pub fn extend_i32<T: Into<i32>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i32(value.into()));
		self
	}

	pub fn extend_i64<T: Into<i64>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i64(value.into()));
		self
	}

	pub fn extend_i128<T: Into<i128>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_i128(value.into()));
		self
	}

	pub fn extend_u8<T: Into<u8>>(&mut self, value: T) -> &mut Self {
		self.buffer.push(encode_u8(value.into()));
		self
	}

	pub fn extend_u16<T: Into<u16>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u16(value.into()));
		self
	}

	pub fn extend_u32<T: Into<u32>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u32(value.into()));
		self
	}

	pub fn extend_u64<T: Into<u64>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u64(value.into()));
		self
	}

	pub fn extend_u128<T: Into<u128>>(&mut self, value: T) -> &mut Self {
		self.buffer.extend_from_slice(&encode_u128(value.into()));
		self
	}

	pub fn extend_u128_varint<T: Into<u128>>(&mut self, value: T) -> &mut Self {
		encode_u128_varint(value.into(), &mut self.buffer);
		self
	}

	pub fn extend_bytes<T: AsRef<[u8]>>(&mut self, bytes: T) -> &mut Self {
		encode_bytes(bytes.as_ref(), &mut self.buffer);
		self
	}

	pub fn extend_str<T: AsRef<str>>(&mut self, s: T) -> &mut Self {
		self.extend_bytes(s.as_ref().as_bytes())
	}

	pub fn finish(self) -> EncodedKey {
		self.buffer.finish()
	}

	pub fn to_encoded_key(self) -> EncodedKey {
		self.buffer.finish()
	}

	pub fn extend_raw(&mut self, bytes: &[u8]) -> &mut Self {
		self.buffer.extend_from_slice(bytes);
		self
	}

	pub fn extend_kind(&mut self, kind: ValueKind) -> &mut Self {
		self.buffer.push(kind.byte());
		self
	}

	pub fn extend_value_with_direction(&mut self, value: &Value, direction: SortOrder) -> &mut Self {
		let ty = match value {
			Value::None {
				inner,
			} => inner.clone(),
			present => present.get_type(),
		};
		let ascending = matches!(direction, SortOrder::Asc);
		if ascending == keycode_type_descending(&ty) {
			let mut tmp = KeySerializer::new();
			tmp.extend_value(value);
			let mut bytes = tmp.to_encoded_key().to_vec();
			for b in bytes.iter_mut() {
				*b = !*b;
			}
			self.extend_raw(&bytes)
		} else {
			self.extend_value(value)
		}
	}

	pub fn len(&self) -> usize {
		self.buffer.len()
	}

	pub fn is_empty(&self) -> bool {
		self.buffer.is_empty()
	}

	pub fn extend_date(&mut self, date: &Date) -> &mut Self {
		self.extend_i32(date.to_days_since_epoch())
	}

	pub fn extend_datetime(&mut self, datetime: &DateTime) -> &mut Self {
		self.extend_u64(datetime.to_nanos())
	}

	pub fn extend_time(&mut self, time: &Time) -> &mut Self {
		self.extend_u64(time.to_nanos_since_midnight())
	}

	pub fn extend_duration(&mut self, duration: &Duration) -> &mut Self {
		self.extend_i32(duration.get_months()).extend_i32(duration.get_days()).extend_i64(duration.get_nanos())
	}

	pub fn extend_row_number(&mut self, row_number: &RowNumber) -> &mut Self {
		self.extend_u64(row_number.0)
	}

	pub fn extend_identity_id(&mut self, id: &IdentityId) -> &mut Self {
		self.extend_bytes(id.as_bytes())
	}

	pub fn extend_uuid4(&mut self, uuid: &Uuid4) -> &mut Self {
		self.extend_bytes(uuid.as_bytes())
	}

	pub fn extend_uuid7(&mut self, uuid: &Uuid7) -> &mut Self {
		self.extend_bytes(uuid.as_bytes())
	}

	pub fn extend_blob(&mut self, blob: &Blob) -> &mut Self {
		self.extend_bytes(blob.as_ref() as &[u8])
	}

	pub fn extend_int(&mut self, int: &Int) -> &mut Self {
		let (sign, bytes) = int.to_bytes_be();

		self.buffer.push(match sign {
			Sign::Minus => 0,
			_ => 1,
		});
		self.extend_u32(bytes.len() as u32);
		self.buffer.extend_from_slice(&bytes);
		self
	}

	pub fn extend_uint(&mut self, uint: &Uint) -> &mut Self {
		let (_sign, bytes) = uint.0.to_bytes_be();
		self.extend_u32(bytes.len() as u32);
		self.buffer.extend_from_slice(&bytes);
		self
	}

	pub fn extend_decimal(&mut self, decimal: &Decimal) -> &mut Self {
		let s = decimal.to_string();
		self.extend_str(&s);
		self
	}

	pub fn extend_value(&mut self, value: &Value) -> &mut Self {
		match value {
			Value::None {
				inner,
				..
			} => {
				self.buffer.push(ValueKind::None.byte());
				match ValueKind::of_type(inner) {
					ValueKind::List | ValueKind::Record | ValueKind::Tuple => unreachable!(
						"List/Record/Tuple types cannot be encoded as none inner type in keys"
					),
					_ => {}
				}
				let tag = TypeTag::of_type(inner)
					.expect("option nesting in a key none inner exceeds the supported depth");
				self.buffer.push(tag.byte());
			}
			Value::Boolean(b) => {
				self.buffer.push(ValueKind::Boolean.byte());
				self.extend_bool(*b);
			}
			Value::Float4(f) => {
				self.buffer.push(ValueKind::Float4.byte());
				self.extend_f32(**f);
			}
			Value::Float8(f) => {
				self.buffer.push(ValueKind::Float8.byte());
				self.extend_f64(**f);
			}
			Value::Int1(i) => {
				self.buffer.push(ValueKind::Int1.byte());
				self.extend_i8(*i);
			}
			Value::Int2(i) => {
				self.buffer.push(ValueKind::Int2.byte());
				self.extend_i16(*i);
			}
			Value::Int4(i) => {
				self.buffer.push(ValueKind::Int4.byte());
				self.extend_i32(*i);
			}
			Value::Int8(i) => {
				self.buffer.push(ValueKind::Int8.byte());
				self.extend_i64(*i);
			}
			Value::Int16(i) => {
				self.buffer.push(ValueKind::Int16.byte());
				self.extend_i128(*i);
			}
			Value::Utf8(s) => {
				self.buffer.push(ValueKind::Utf8.byte());
				self.extend_str(s);
			}
			Value::Uint1(u) => {
				self.buffer.push(ValueKind::Uint1.byte());
				self.extend_u8(*u);
			}
			Value::Uint2(u) => {
				self.buffer.push(ValueKind::Uint2.byte());
				self.extend_u16(*u);
			}
			Value::Uint4(u) => {
				self.buffer.push(ValueKind::Uint4.byte());
				self.extend_u32(*u);
			}
			Value::Uint8(u) => {
				self.buffer.push(ValueKind::Uint8.byte());
				self.extend_u64(*u);
			}
			Value::Uint16(u) => {
				self.buffer.push(ValueKind::Uint16.byte());
				self.extend_u128(*u);
			}
			Value::Date(d) => {
				self.buffer.push(ValueKind::Date.byte());
				self.extend_date(d);
			}
			Value::DateTime(dt) => {
				self.buffer.push(ValueKind::DateTime.byte());
				self.extend_datetime(dt);
			}
			Value::Time(t) => {
				self.buffer.push(ValueKind::Time.byte());
				self.extend_time(t);
			}
			Value::Duration(i) => {
				self.buffer.push(ValueKind::Duration.byte());
				self.extend_duration(i);
			}
			Value::IdentityId(id) => {
				self.buffer.push(ValueKind::IdentityId.byte());
				self.extend_identity_id(id);
			}
			Value::Uuid4(uuid) => {
				self.buffer.push(ValueKind::Uuid4.byte());
				self.extend_uuid4(uuid);
			}
			Value::Uuid7(uuid) => {
				self.buffer.push(ValueKind::Uuid7.byte());
				self.extend_uuid7(uuid);
			}
			Value::Blob(b) => {
				self.buffer.push(ValueKind::Blob.byte());
				self.extend_blob(b);
			}
			Value::Int(i) => {
				self.buffer.push(ValueKind::Int.byte());
				self.extend_int(i);
			}
			Value::Uint(u) => {
				self.buffer.push(ValueKind::Uint.byte());
				self.extend_uint(u);
			}
			Value::Decimal(d) => {
				self.buffer.push(ValueKind::Decimal.byte());
				self.extend_decimal(d);
			}
			Value::List(items) => {
				self.buffer.push(ValueKind::List.byte());
				for item in items {
					self.extend_value(item);
				}
				self.buffer.push(CONTAINER_END);
			}
			Value::Tuple(items) => {
				self.buffer.push(ValueKind::Tuple.byte());
				for item in items {
					self.extend_value(item);
				}
				self.buffer.push(CONTAINER_END);
			}
			Value::Record(fields) => {
				self.buffer.push(ValueKind::Record.byte());
				for (name, value) in fields {
					self.extend_bytes(name.as_bytes());
					self.extend_value(value);
				}
				self.buffer.push(CONTAINER_END);
			}
			Value::Any(_) | Value::Type(_) => {
				unreachable!("Any/ValueType values cannot be serialized in keys");
			}
			Value::DictionaryId(id) => {
				self.buffer.push(ValueKind::DictionaryId.byte());
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
