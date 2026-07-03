// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, value_type::ValueType};

use crate::error::{DecodeError, EncodeError};

pub const MAX_OPTION_DEPTH: u8 = 3;
pub const RESERVED_KIND: u8 = 63;
pub const EXTENDED_TYPE_TAG: u8 = 0xFF;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueKind {
	None = 0,
	Boolean = 1,
	Float4 = 2,
	Float8 = 3,
	Int1 = 4,
	Int2 = 5,
	Int4 = 6,
	Int8 = 7,
	Int16 = 8,
	Utf8 = 9,
	Uint1 = 10,
	Uint2 = 11,
	Uint4 = 12,
	Uint8 = 13,
	Uint16 = 14,
	Date = 15,
	DateTime = 16,
	Time = 17,
	Duration = 18,
	IdentityId = 19,
	Uuid4 = 20,
	Uuid7 = 21,
	Blob = 22,
	Int = 23,
	Uint = 24,
	Decimal = 25,
	Any = 26,
	DictionaryId = 27,
	Type = 28,
	List = 29,
	Record = 30,
	Tuple = 31,
}

impl ValueKind {
	pub const ALL: [ValueKind; 32] = [
		ValueKind::None,
		ValueKind::Boolean,
		ValueKind::Float4,
		ValueKind::Float8,
		ValueKind::Int1,
		ValueKind::Int2,
		ValueKind::Int4,
		ValueKind::Int8,
		ValueKind::Int16,
		ValueKind::Utf8,
		ValueKind::Uint1,
		ValueKind::Uint2,
		ValueKind::Uint4,
		ValueKind::Uint8,
		ValueKind::Uint16,
		ValueKind::Date,
		ValueKind::DateTime,
		ValueKind::Time,
		ValueKind::Duration,
		ValueKind::IdentityId,
		ValueKind::Uuid4,
		ValueKind::Uuid7,
		ValueKind::Blob,
		ValueKind::Int,
		ValueKind::Uint,
		ValueKind::Decimal,
		ValueKind::Any,
		ValueKind::DictionaryId,
		ValueKind::Type,
		ValueKind::List,
		ValueKind::Record,
		ValueKind::Tuple,
	];

	pub const fn byte(self) -> u8 {
		self as u8
	}

	pub fn from_byte(byte: u8) -> Option<ValueKind> {
		if byte < Self::ALL.len() as u8 {
			Some(Self::ALL[byte as usize])
		} else {
			None
		}
	}

	pub fn of_value(value: &Value) -> ValueKind {
		match value {
			Value::None {
				..
			} => ValueKind::None,
			Value::Boolean(_) => ValueKind::Boolean,
			Value::Float4(_) => ValueKind::Float4,
			Value::Float8(_) => ValueKind::Float8,
			Value::Int1(_) => ValueKind::Int1,
			Value::Int2(_) => ValueKind::Int2,
			Value::Int4(_) => ValueKind::Int4,
			Value::Int8(_) => ValueKind::Int8,
			Value::Int16(_) => ValueKind::Int16,
			Value::Utf8(_) => ValueKind::Utf8,
			Value::Uint1(_) => ValueKind::Uint1,
			Value::Uint2(_) => ValueKind::Uint2,
			Value::Uint4(_) => ValueKind::Uint4,
			Value::Uint8(_) => ValueKind::Uint8,
			Value::Uint16(_) => ValueKind::Uint16,
			Value::Date(_) => ValueKind::Date,
			Value::DateTime(_) => ValueKind::DateTime,
			Value::Time(_) => ValueKind::Time,
			Value::Duration(_) => ValueKind::Duration,
			Value::IdentityId(_) => ValueKind::IdentityId,
			Value::Uuid4(_) => ValueKind::Uuid4,
			Value::Uuid7(_) => ValueKind::Uuid7,
			Value::Blob(_) => ValueKind::Blob,
			Value::Int(_) => ValueKind::Int,
			Value::Uint(_) => ValueKind::Uint,
			Value::Decimal(_) => ValueKind::Decimal,
			Value::Any(_) => ValueKind::Any,
			Value::DictionaryId(_) => ValueKind::DictionaryId,
			Value::Type(_) => ValueKind::Type,
			Value::List(_) => ValueKind::List,
			Value::Record(_) => ValueKind::Record,
			Value::Tuple(_) => ValueKind::Tuple,
		}
	}

	pub fn of_type(ty: &ValueType) -> ValueKind {
		match ty {
			ValueType::Option(inner) => Self::of_type(inner),
			ValueType::Boolean => ValueKind::Boolean,
			ValueType::Float4 => ValueKind::Float4,
			ValueType::Float8 => ValueKind::Float8,
			ValueType::Int1 => ValueKind::Int1,
			ValueType::Int2 => ValueKind::Int2,
			ValueType::Int4 => ValueKind::Int4,
			ValueType::Int8 => ValueKind::Int8,
			ValueType::Int16 => ValueKind::Int16,
			ValueType::Utf8 => ValueKind::Utf8,
			ValueType::Uint1 => ValueKind::Uint1,
			ValueType::Uint2 => ValueKind::Uint2,
			ValueType::Uint4 => ValueKind::Uint4,
			ValueType::Uint8 => ValueKind::Uint8,
			ValueType::Uint16 => ValueKind::Uint16,
			ValueType::Date => ValueKind::Date,
			ValueType::DateTime => ValueKind::DateTime,
			ValueType::Time => ValueKind::Time,
			ValueType::Duration => ValueKind::Duration,
			ValueType::IdentityId => ValueKind::IdentityId,
			ValueType::Uuid4 => ValueKind::Uuid4,
			ValueType::Uuid7 => ValueKind::Uuid7,
			ValueType::Blob => ValueKind::Blob,
			ValueType::Int => ValueKind::Int,
			ValueType::Uint => ValueKind::Uint,
			ValueType::Decimal => ValueKind::Decimal,
			ValueType::Any => ValueKind::Any,
			ValueType::DictionaryId => ValueKind::DictionaryId,
			ValueType::List(_) => ValueKind::List,
			ValueType::Record(_) => ValueKind::Record,
			ValueType::Tuple(_) => ValueKind::Tuple,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TypeTag(u8);

impl TypeTag {
	pub fn new(kind: ValueKind, depth: u8) -> Result<TypeTag, EncodeError> {
		if depth > MAX_OPTION_DEPTH {
			return Err(EncodeError::OptionDepthTooDeep {
				depth: depth as u32,
				max: MAX_OPTION_DEPTH,
			});
		}
		Ok(TypeTag((depth << 6) | kind.byte()))
	}

	pub const fn byte(self) -> u8 {
		self.0
	}

	pub const fn depth(self) -> u8 {
		self.0 >> 6
	}

	pub const fn kind_bits(self) -> u8 {
		self.0 & 0x3F
	}

	pub fn kind(self) -> Option<ValueKind> {
		ValueKind::from_byte(self.kind_bits())
	}

	pub fn from_byte(byte: u8) -> Result<TypeTag, DecodeError> {
		let kind_bits = byte & 0x3F;
		if kind_bits == RESERVED_KIND {
			return Err(DecodeError::ReservedTag(byte));
		}
		if ValueKind::from_byte(kind_bits).is_none() {
			return Err(DecodeError::UnknownTypeCode(byte));
		}
		let tag = TypeTag(byte);
		if tag.depth() > 0 && kind_bits == ValueKind::None.byte() {
			return Err(DecodeError::InvalidData(format!(
				"tag byte 0x{byte:02X} wraps the none kind in option depth {}",
				tag.depth()
			)));
		}
		Ok(tag)
	}

	pub fn of_type(ty: &ValueType) -> Result<TypeTag, EncodeError> {
		let (base, depth) = peel_options(ty);
		if depth > MAX_OPTION_DEPTH as u32 {
			return Err(EncodeError::OptionDepthTooDeep {
				depth,
				max: MAX_OPTION_DEPTH,
			});
		}
		Self::new(ValueKind::of_type(base), depth as u8)
	}

	pub fn to_type(self) -> Result<ValueType, DecodeError> {
		let base = match self.kind().ok_or(DecodeError::UnknownTypeCode(self.0))? {
			ValueKind::None | ValueKind::Type => {
				return Err(DecodeError::UnsupportedType(format!(
					"kind {:?} has no standalone value type",
					self.kind()
				)));
			}
			ValueKind::Boolean => ValueType::Boolean,
			ValueKind::Float4 => ValueType::Float4,
			ValueKind::Float8 => ValueType::Float8,
			ValueKind::Int1 => ValueType::Int1,
			ValueKind::Int2 => ValueType::Int2,
			ValueKind::Int4 => ValueType::Int4,
			ValueKind::Int8 => ValueType::Int8,
			ValueKind::Int16 => ValueType::Int16,
			ValueKind::Utf8 => ValueType::Utf8,
			ValueKind::Uint1 => ValueType::Uint1,
			ValueKind::Uint2 => ValueType::Uint2,
			ValueKind::Uint4 => ValueType::Uint4,
			ValueKind::Uint8 => ValueType::Uint8,
			ValueKind::Uint16 => ValueType::Uint16,
			ValueKind::Date => ValueType::Date,
			ValueKind::DateTime => ValueType::DateTime,
			ValueKind::Time => ValueType::Time,
			ValueKind::Duration => ValueType::Duration,
			ValueKind::IdentityId => ValueType::IdentityId,
			ValueKind::Uuid4 => ValueType::Uuid4,
			ValueKind::Uuid7 => ValueType::Uuid7,
			ValueKind::Blob => ValueType::Blob,
			ValueKind::Int => ValueType::Int,
			ValueKind::Uint => ValueType::Uint,
			ValueKind::Decimal => ValueType::Decimal,
			ValueKind::Any => ValueType::Any,
			ValueKind::DictionaryId => ValueType::DictionaryId,
			ValueKind::List => ValueType::List(Box::new(ValueType::Any)),
			ValueKind::Record => ValueType::Record(Vec::new()),
			ValueKind::Tuple => ValueType::Tuple(Vec::new()),
		};
		Ok((0..self.depth()).fold(base, |ty, _| ValueType::Option(Box::new(ty))))
	}
}

pub(crate) fn peel_options(ty: &ValueType) -> (&ValueType, u32) {
	let mut depth = 0u32;
	let mut base = ty;
	while let ValueType::Option(inner) = base {
		depth += 1;
		base = inner;
	}
	(base, depth)
}

pub fn type_tag_byte(ty: &ValueType) -> u8 {
	TypeTag::of_type(ty).expect("option nesting exceeds the type tag capacity").byte()
}

pub fn value_type_from_tag_byte(byte: u8) -> ValueType {
	TypeTag::from_byte(byte).and_then(TypeTag::to_type).expect("invalid persisted type tag byte")
}
