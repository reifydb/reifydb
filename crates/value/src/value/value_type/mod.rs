// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt::{Display, Formatter},
	str::FromStr,
};

use serde::{Deserialize, Serialize};

pub mod get;
pub mod input_types;
pub mod promote;
pub mod super_type;

use std::fmt;

use crate::value::Value;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ValueType {
	Boolean,

	Float4,

	Float8,

	Int1,

	Int2,

	Int4,

	Int8,

	Int16,

	Utf8,

	Uint1,

	Uint2,

	Uint4,

	Uint8,

	Uint16,

	Date,

	DateTime,

	Time,

	Duration,

	IdentityId,

	Uuid4,

	Uuid7,

	Blob,

	Int,

	Uint,

	Decimal,

	Option(Box<ValueType>),

	Any,

	DictionaryId,

	List(Box<ValueType>),

	Record(Vec<(String, ValueType)>),

	Tuple(Vec<ValueType>),
}

impl ValueType {
	pub fn list_of(ty: ValueType) -> Self {
		ValueType::List(Box::new(ty))
	}

	pub fn is_number(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_number(),
			_ => matches!(
				self,
				ValueType::Float4
					| ValueType::Float8 | ValueType::Int1
					| ValueType::Int2 | ValueType::Int4 | ValueType::Int8
					| ValueType::Int16 | ValueType::Uint1
					| ValueType::Uint2 | ValueType::Uint4
					| ValueType::Uint8 | ValueType::Uint16
					| ValueType::Int | ValueType::Uint | ValueType::Decimal
			),
		}
	}

	pub fn is_bool(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_bool(),
			_ => matches!(self, ValueType::Boolean),
		}
	}

	pub fn is_signed_integer(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_signed_integer(),
			_ => matches!(
				self,
				ValueType::Int1
					| ValueType::Int2 | ValueType::Int4 | ValueType::Int8
					| ValueType::Int16 | ValueType::Int
			),
		}
	}

	pub fn is_unsigned_integer(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_unsigned_integer(),
			_ => matches!(
				self,
				ValueType::Uint1
					| ValueType::Uint2 | ValueType::Uint4
					| ValueType::Uint8 | ValueType::Uint16
					| ValueType::Uint
			),
		}
	}

	pub fn is_integer(&self) -> bool {
		self.is_signed_integer() || self.is_unsigned_integer()
	}

	pub fn is_floating_point(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_floating_point(),
			_ => matches!(self, ValueType::Float4 | ValueType::Float8),
		}
	}

	pub fn is_utf8(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_utf8(),
			_ => matches!(self, ValueType::Utf8),
		}
	}

	pub fn is_temporal(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_temporal(),
			_ => matches!(
				self,
				ValueType::Date | ValueType::DateTime | ValueType::Time | ValueType::Duration
			),
		}
	}

	pub fn is_uuid(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_uuid(),
			_ => matches!(self, ValueType::Uuid4 | ValueType::Uuid7),
		}
	}

	pub fn is_blob(&self) -> bool {
		match self {
			ValueType::Option(inner) => inner.is_blob(),
			_ => matches!(self, ValueType::Blob),
		}
	}

	pub fn is_option(&self) -> bool {
		matches!(self, ValueType::Option(_))
	}

	pub fn inner_type(&self) -> &ValueType {
		match self {
			ValueType::Option(inner) => inner,
			other => other,
		}
	}
}

impl ValueType {
	pub fn to_u8(&self) -> u8 {
		match self {
			ValueType::Option(inner) => 0x80 | inner.to_u8(),
			ValueType::Float4 => 1,
			ValueType::Float8 => 2,
			ValueType::Int1 => 3,
			ValueType::Int2 => 4,
			ValueType::Int4 => 5,
			ValueType::Int8 => 6,
			ValueType::Int16 => 7,
			ValueType::Utf8 => 8,
			ValueType::Uint1 => 9,
			ValueType::Uint2 => 10,
			ValueType::Uint4 => 11,
			ValueType::Uint8 => 12,
			ValueType::Uint16 => 13,
			ValueType::Boolean => 14,
			ValueType::Date => 15,
			ValueType::DateTime => 16,
			ValueType::Time => 17,
			ValueType::Duration => 18,
			ValueType::IdentityId => 19,
			ValueType::Uuid4 => 20,
			ValueType::Uuid7 => 21,
			ValueType::Blob => 22,
			ValueType::Int => 23,
			ValueType::Decimal => 24,
			ValueType::Uint => 25,
			ValueType::Any => 26,
			ValueType::DictionaryId => 27,
			ValueType::List(_) => 28,
			ValueType::Record(_) => 29,
			ValueType::Tuple(_) => 30,
		}
	}
}

impl ValueType {
	pub fn from_u8(value: u8) -> Self {
		if value & 0x80 != 0 {
			return ValueType::Option(Box::new(ValueType::from_u8(value & 0x7F)));
		}
		match value {
			1 => ValueType::Float4,
			2 => ValueType::Float8,
			3 => ValueType::Int1,
			4 => ValueType::Int2,
			5 => ValueType::Int4,
			6 => ValueType::Int8,
			7 => ValueType::Int16,
			8 => ValueType::Utf8,
			9 => ValueType::Uint1,
			10 => ValueType::Uint2,
			11 => ValueType::Uint4,
			12 => ValueType::Uint8,
			13 => ValueType::Uint16,
			14 => ValueType::Boolean,
			15 => ValueType::Date,
			16 => ValueType::DateTime,
			17 => ValueType::Time,
			18 => ValueType::Duration,
			19 => ValueType::IdentityId,
			20 => ValueType::Uuid4,
			21 => ValueType::Uuid7,
			22 => ValueType::Blob,
			23 => ValueType::Int,
			24 => ValueType::Decimal,
			25 => ValueType::Uint,
			26 => ValueType::Any,
			27 => ValueType::DictionaryId,
			28 => ValueType::list_of(ValueType::Any),
			29 => ValueType::Record(Vec::new()),
			30 => ValueType::Tuple(Vec::new()),
			_ => unreachable!(),
		}
	}
}

impl ValueType {
	pub fn size(&self) -> usize {
		match self {
			ValueType::Boolean => 1,
			ValueType::Float4 => 4,
			ValueType::Float8 => 8,
			ValueType::Int1 => 1,
			ValueType::Int2 => 2,
			ValueType::Int4 => 4,
			ValueType::Int8 => 8,
			ValueType::Int16 => 16,
			ValueType::Utf8 => 8,
			ValueType::Uint1 => 1,
			ValueType::Uint2 => 2,
			ValueType::Uint4 => 4,
			ValueType::Uint8 => 8,
			ValueType::Uint16 => 16,
			ValueType::Date => 4,
			ValueType::DateTime => 8,
			ValueType::Time => 8,
			ValueType::Duration => 16,
			ValueType::IdentityId => 16,
			ValueType::Uuid4 => 16,
			ValueType::Uuid7 => 16,
			ValueType::Blob => 8,
			ValueType::Int => 16,

			ValueType::Uint => 16,

			ValueType::Decimal => 16,

			ValueType::Option(inner) => inner.size(),
			ValueType::Any => 8,
			ValueType::List(_) => 8,
			ValueType::Record(_) => 8,
			ValueType::Tuple(_) => 8,
			ValueType::DictionaryId => 16,
		}
	}

	pub fn alignment(&self) -> usize {
		match self {
			ValueType::Boolean => 1,
			ValueType::Float4 => 4,
			ValueType::Float8 => 8,
			ValueType::Int1 => 1,
			ValueType::Int2 => 2,
			ValueType::Int4 => 4,
			ValueType::Int8 => 8,
			ValueType::Int16 => 16,
			ValueType::Utf8 => 4,
			ValueType::Uint1 => 1,
			ValueType::Uint2 => 2,
			ValueType::Uint4 => 4,
			ValueType::Uint8 => 8,
			ValueType::Uint16 => 16,
			ValueType::Date => 4,
			ValueType::DateTime => 8,
			ValueType::Time => 8,
			ValueType::Duration => 8,
			ValueType::IdentityId => 8,
			ValueType::Uuid4 => 8,
			ValueType::Uuid7 => 8,
			ValueType::Blob => 4,
			ValueType::Int => 16,

			ValueType::Uint => 16,

			ValueType::Decimal => 16,

			ValueType::Option(inner) => inner.alignment(),
			ValueType::Any => 8,
			ValueType::DictionaryId => 16,
			ValueType::List(_) => 8,
			ValueType::Record(_) => 8,
			ValueType::Tuple(_) => 8,
		}
	}
}

impl Display for ValueType {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ValueType::Boolean => f.write_str("Boolean"),
			ValueType::Float4 => f.write_str("Float4"),
			ValueType::Float8 => f.write_str("Float8"),
			ValueType::Int1 => f.write_str("Int1"),
			ValueType::Int2 => f.write_str("Int2"),
			ValueType::Int4 => f.write_str("Int4"),
			ValueType::Int8 => f.write_str("Int8"),
			ValueType::Int16 => f.write_str("Int16"),
			ValueType::Utf8 => f.write_str("Utf8"),
			ValueType::Uint1 => f.write_str("Uint1"),
			ValueType::Uint2 => f.write_str("Uint2"),
			ValueType::Uint4 => f.write_str("Uint4"),
			ValueType::Uint8 => f.write_str("Uint8"),
			ValueType::Uint16 => f.write_str("Uint16"),
			ValueType::Date => f.write_str("Date"),
			ValueType::DateTime => f.write_str("DateTime"),
			ValueType::Time => f.write_str("Time"),
			ValueType::Duration => f.write_str("Duration"),
			ValueType::IdentityId => f.write_str("IdentityId"),
			ValueType::Uuid4 => f.write_str("Uuid4"),
			ValueType::Uuid7 => f.write_str("Uuid7"),
			ValueType::Blob => f.write_str("Blob"),
			ValueType::Int => f.write_str("Int"),
			ValueType::Uint => f.write_str("Uint"),
			ValueType::Decimal => f.write_str("Decimal"),
			ValueType::Option(inner) => write!(f, "Option({inner})"),
			ValueType::Any => f.write_str("Any"),
			ValueType::DictionaryId => f.write_str("DictionaryId"),
			ValueType::List(inner) => write!(f, "List({inner})"),
			ValueType::Record(fields) => {
				f.write_str("Record(")?;
				for (i, (name, ty)) in fields.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					write!(f, "{}: {}", name, ty)?;
				}
				f.write_str(")")
			}
			ValueType::Tuple(types) => {
				f.write_str("Tuple(")?;
				for (i, ty) in types.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					write!(f, "{}", ty)?;
				}
				f.write_str(")")
			}
		}
	}
}

impl From<&Value> for ValueType {
	fn from(value: &Value) -> Self {
		match value {
			Value::None {
				inner,
			} => ValueType::Option(Box::new(inner.clone())),
			Value::Boolean(_) => ValueType::Boolean,
			Value::Float4(_) => ValueType::Float4,
			Value::Float8(_) => ValueType::Float8,
			Value::Int1(_) => ValueType::Int1,
			Value::Int2(_) => ValueType::Int2,
			Value::Int4(_) => ValueType::Int4,
			Value::Int8(_) => ValueType::Int8,
			Value::Int16(_) => ValueType::Int16,
			Value::Utf8(_) => ValueType::Utf8,
			Value::Uint1(_) => ValueType::Uint1,
			Value::Uint2(_) => ValueType::Uint2,
			Value::Uint4(_) => ValueType::Uint4,
			Value::Uint8(_) => ValueType::Uint8,
			Value::Uint16(_) => ValueType::Uint16,
			Value::Date(_) => ValueType::Date,
			Value::DateTime(_) => ValueType::DateTime,
			Value::Time(_) => ValueType::Time,
			Value::Duration(_) => ValueType::Duration,
			Value::IdentityId(_) => ValueType::IdentityId,
			Value::Uuid4(_) => ValueType::Uuid4,
			Value::Uuid7(_) => ValueType::Uuid7,
			Value::Blob(_) => ValueType::Blob,
			Value::Int(_) => ValueType::Int,
			Value::Uint(_) => ValueType::Uint,
			Value::Decimal(_) => ValueType::Decimal,
			Value::Any(_) => ValueType::Any,
			Value::DictionaryId(_) => ValueType::DictionaryId,
			Value::Type(t) => t.clone(),
			Value::List(items) => {
				let element_type = items.first().map(ValueType::from).unwrap_or(ValueType::Any);
				ValueType::list_of(element_type)
			}
			Value::Record(fields) => {
				ValueType::Record(fields.iter().map(|(k, v)| (k.clone(), ValueType::from(v))).collect())
			}
			Value::Tuple(items) => ValueType::Tuple(items.iter().map(ValueType::from).collect()),
		}
	}
}

impl FromStr for ValueType {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let upper = s.to_uppercase();

		if upper.starts_with("OPTION<") && upper.ends_with('>') {
			let inner_str = &s[7..s.len() - 1];
			let inner = ValueType::from_str(inner_str)?;
			return Ok(ValueType::Option(Box::new(inner)));
		}
		match upper.as_str() {
			"BOOL" | "BOOLEAN" => Ok(ValueType::Boolean),
			"FLOAT4" => Ok(ValueType::Float4),
			"FLOAT8" => Ok(ValueType::Float8),
			"INT1" => Ok(ValueType::Int1),
			"INT2" => Ok(ValueType::Int2),
			"INT4" => Ok(ValueType::Int4),
			"INT8" => Ok(ValueType::Int8),
			"INT16" => Ok(ValueType::Int16),
			"UTF8" | "TEXT" => Ok(ValueType::Utf8),
			"UINT1" => Ok(ValueType::Uint1),
			"UINT2" => Ok(ValueType::Uint2),
			"UINT4" => Ok(ValueType::Uint4),
			"UINT8" => Ok(ValueType::Uint8),
			"UINT16" => Ok(ValueType::Uint16),
			"DATE" => Ok(ValueType::Date),
			"DATETIME" => Ok(ValueType::DateTime),
			"TIME" => Ok(ValueType::Time),
			"DURATION" | "INTERVAL" => Ok(ValueType::Duration),
			"IDENTITYID" | "IDENTITY_ID" => Ok(ValueType::IdentityId),
			"UUID4" => Ok(ValueType::Uuid4),
			"UUID7" => Ok(ValueType::Uuid7),
			"BLOB" => Ok(ValueType::Blob),
			"INT" => Ok(ValueType::Int),
			"UINT" => Ok(ValueType::Uint),
			"DECIMAL" => Ok(ValueType::Decimal),
			"ANY" => Ok(ValueType::Any),
			"DICTIONARYID" | "DICTIONARY_ID" => Ok(ValueType::DictionaryId),
			_ => Err(()),
		}
	}
}
