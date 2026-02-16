// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{
	fmt::{Display, Formatter},
	str::FromStr,
};

use serde::{Deserialize, Serialize};

pub mod get;
pub mod promote;

use crate::value::Value;

/// All possible RQL data types
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Type {
	/// A boolean: true or false.
	Boolean,
	/// A 4-byte floating point
	Float4,
	/// An 8-byte floating point
	Float8,
	/// A 1-byte signed integer
	Int1,
	/// A 2-byte signed integer
	Int2,
	/// A 4-byte signed integer
	Int4,
	/// An 8-byte signed integer
	Int8,
	/// A 16-byte signed integer
	Int16,
	/// A UTF-8 encoded text.
	Utf8,
	/// A 1-byte unsigned integer
	Uint1,
	/// A 2-byte unsigned integer
	Uint2,
	/// A 4-byte unsigned integer
	Uint4,
	/// A 8-byte unsigned integer
	Uint8,
	/// A 16-byte unsigned integer
	Uint16,
	/// A date value (year, month, day)
	Date,
	/// A date and time value with nanosecond precision in SVTC
	DateTime,
	/// A time value (hour, minute, second, nanosecond)
	Time,
	/// A duration representing a duration
	Duration,
	/// An identity identifier (UUID v7)
	IdentityId,
	/// A UUID version 4 (random)
	Uuid4,
	/// A UUID version 7 (timestamp-based)
	Uuid7,
	/// A binary large object (BLOB)
	Blob,
	/// An arbitrary-precision signed integer
	Int,
	/// An arbitrary-precision unsigned integer
	Uint,
	/// An arbitrary-precision decimal with precision and scale
	Decimal,
	/// An optional type that can hold None or a value of the inner type
	Option(Box<Type>),
	/// A container that can hold any value type
	Any,
	/// A dictionary entry identifier
	DictionaryId,
}

impl Type {
	pub fn is_number(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_number(),
			_ => matches!(
				self,
				Type::Float4
					| Type::Float8 | Type::Int1 | Type::Int2
					| Type::Int4 | Type::Int8 | Type::Int16
					| Type::Uint1 | Type::Uint2 | Type::Uint4
					| Type::Uint8 | Type::Uint16 | Type::Int
					| Type::Uint | Type::Decimal
			),
		}
	}

	pub fn is_bool(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_bool(),
			_ => matches!(self, Type::Boolean),
		}
	}

	pub fn is_signed_integer(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_signed_integer(),
			_ => matches!(
				self,
				Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16 | Type::Int
			),
		}
	}

	pub fn is_unsigned_integer(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_unsigned_integer(),
			_ => matches!(
				self,
				Type::Uint1 | Type::Uint2 | Type::Uint4 | Type::Uint8 | Type::Uint16 | Type::Uint
			),
		}
	}

	pub fn is_integer(&self) -> bool {
		self.is_signed_integer() || self.is_unsigned_integer()
	}

	pub fn is_floating_point(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_floating_point(),
			_ => matches!(self, Type::Float4 | Type::Float8),
		}
	}

	pub fn is_utf8(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_utf8(),
			_ => matches!(self, Type::Utf8),
		}
	}

	pub fn is_temporal(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_temporal(),
			_ => matches!(self, Type::Date | Type::DateTime | Type::Time | Type::Duration),
		}
	}

	pub fn is_uuid(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_uuid(),
			_ => matches!(self, Type::Uuid4 | Type::Uuid7),
		}
	}

	pub fn is_blob(&self) -> bool {
		match self {
			Type::Option(inner) => inner.is_blob(),
			_ => matches!(self, Type::Blob),
		}
	}

	pub fn is_option(&self) -> bool {
		matches!(self, Type::Option(_))
	}

	/// Returns the inner type if this is an Option type, otherwise returns self
	pub fn inner_type(&self) -> &Type {
		match self {
			Type::Option(inner) => inner,
			other => other,
		}
	}
}

impl Type {
	pub fn to_u8(&self) -> u8 {
		match self {
			Type::Option(_) => 0,
			Type::Float4 => 1,
			Type::Float8 => 2,
			Type::Int1 => 3,
			Type::Int2 => 4,
			Type::Int4 => 5,
			Type::Int8 => 6,
			Type::Int16 => 7,
			Type::Utf8 => 8,
			Type::Uint1 => 9,
			Type::Uint2 => 10,
			Type::Uint4 => 11,
			Type::Uint8 => 12,
			Type::Uint16 => 13,
			Type::Boolean => 14,
			Type::Date => 15,
			Type::DateTime => 16,
			Type::Time => 17,
			Type::Duration => 18,
			Type::IdentityId => 19,
			Type::Uuid4 => 20,
			Type::Uuid7 => 21,
			Type::Blob => 22,
			Type::Int => 23,
			Type::Decimal {
				..
			} => 24,
			Type::Uint => 25,
			Type::Any => 26,
			Type::DictionaryId => 27,
		}
	}
}

impl Type {
	/// Decode a type from a u8 code. For Option types (code 0), the inner type
	/// code must be provided separately via `from_u8_option`.
	pub fn from_u8(value: u8) -> Self {
		match value {
			0 => Type::Option(Box::new(Type::Boolean)), // placeholder; caller should use from_u8_option
			1 => Type::Float4,
			2 => Type::Float8,
			3 => Type::Int1,
			4 => Type::Int2,
			5 => Type::Int4,
			6 => Type::Int8,
			7 => Type::Int16,
			8 => Type::Utf8,
			9 => Type::Uint1,
			10 => Type::Uint2,
			11 => Type::Uint4,
			12 => Type::Uint8,
			13 => Type::Uint16,
			14 => Type::Boolean,
			15 => Type::Date,
			16 => Type::DateTime,
			17 => Type::Time,
			18 => Type::Duration,
			19 => Type::IdentityId,
			20 => Type::Uuid4,
			21 => Type::Uuid7,
			22 => Type::Blob,
			23 => Type::Int,
			24 => Type::Decimal,
			25 => Type::Uint,
			26 => Type::Any,
			27 => Type::DictionaryId,
			_ => unreachable!(),
		}
	}

	/// Decode an Option type from a u8 code for the inner type.
	pub fn from_u8_option(inner_code: u8) -> Self {
		Type::Option(Box::new(Type::from_u8(inner_code)))
	}
}

impl Type {
	pub fn size(&self) -> usize {
		match self {
			Type::Boolean => 1,
			Type::Float4 => 4,
			Type::Float8 => 8,
			Type::Int1 => 1,
			Type::Int2 => 2,
			Type::Int4 => 4,
			Type::Int8 => 8,
			Type::Int16 => 16,
			Type::Utf8 => 8, // offset: u32 + length: u32
			Type::Uint1 => 1,
			Type::Uint2 => 2,
			Type::Uint4 => 4,
			Type::Uint8 => 8,
			Type::Uint16 => 16,
			Type::Date => 4,
			Type::DateTime => 12, // seconds: i64 + nanos: u32
			Type::Time => 8,
			Type::Duration => 16, // months: i32 + days: i32 +
			// nanos: i64
			Type::IdentityId => 16, // UUID v7 is 16 bytes
			Type::Uuid4 => 16,
			Type::Uuid7 => 16,
			Type::Blob => 8, // offset: u32 + length: u32
			Type::Int => 16, // i128 inline or dynamic
			// storage with offset + length
			Type::Uint => 16, // u128 inline or dynamic
			// storage with offset + length
			Type::Decimal {
				..
			} => 16, // i128 inline or dynamic
			// storage with offset + length
			Type::Option(_) => 0,     // size determined by inner type + bitvec at container level
			Type::Any => 8,           // pointer size on 64-bit systems
			Type::DictionaryId => 16, // max possible; actual size determined by constraint's id_type
		}
	}

	pub fn alignment(&self) -> usize {
		match self {
			Type::Boolean => 1,
			Type::Float4 => 4,
			Type::Float8 => 8,
			Type::Int1 => 1,
			Type::Int2 => 2,
			Type::Int4 => 4,
			Type::Int8 => 8,
			Type::Int16 => 16,
			Type::Utf8 => 4, // u32 alignment
			Type::Uint1 => 1,
			Type::Uint2 => 2,
			Type::Uint4 => 4,
			Type::Uint8 => 8,
			Type::Uint16 => 16,
			Type::Date => 4,
			Type::DateTime => 8,
			Type::Time => 8,
			Type::Duration => 8,
			Type::IdentityId => 8, // Same alignment as UUID
			Type::Uuid4 => 8,
			Type::Uuid7 => 8,
			Type::Blob => 4, // u32 alignment
			Type::Int => 16, // i128 alignment for
			// inline storage
			Type::Uint => 16, // u128 alignment for
			// inline storage
			Type::Decimal {
				..
			} => 16, // i128 alignment for
			// inline storage
			Type::Option(inner) => inner.alignment(),
			Type::Any => 8, // pointer alignment
			Type::DictionaryId => 16,
		}
	}
}

impl Display for Type {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Type::Boolean => f.write_str("Boolean"),
			Type::Float4 => f.write_str("Float4"),
			Type::Float8 => f.write_str("Float8"),
			Type::Int1 => f.write_str("Int1"),
			Type::Int2 => f.write_str("Int2"),
			Type::Int4 => f.write_str("Int4"),
			Type::Int8 => f.write_str("Int8"),
			Type::Int16 => f.write_str("Int16"),
			Type::Utf8 => f.write_str("Utf8"),
			Type::Uint1 => f.write_str("Uint1"),
			Type::Uint2 => f.write_str("Uint2"),
			Type::Uint4 => f.write_str("Uint4"),
			Type::Uint8 => f.write_str("Uint8"),
			Type::Uint16 => f.write_str("Uint16"),
			Type::Date => f.write_str("Date"),
			Type::DateTime => f.write_str("DateTime"),
			Type::Time => f.write_str("Time"),
			Type::Duration => f.write_str("Duration"),
			Type::IdentityId => f.write_str("IdentityId"),
			Type::Uuid4 => f.write_str("Uuid4"),
			Type::Uuid7 => f.write_str("Uuid7"),
			Type::Blob => f.write_str("Blob"),
			Type::Int => f.write_str("Int"),
			Type::Uint => f.write_str("Uint"),
			Type::Decimal => f.write_str("Decimal"),
			Type::Option(inner) => write!(f, "Option<{inner}>"),
			Type::Any => f.write_str("Any"),
			Type::DictionaryId => f.write_str("DictionaryId"),
		}
	}
}

impl From<&Value> for Type {
	fn from(value: &Value) -> Self {
		match value {
			Value::None { inner } => Type::Option(Box::new(inner.clone())),
			Value::Boolean(_) => Type::Boolean,
			Value::Float4(_) => Type::Float4,
			Value::Float8(_) => Type::Float8,
			Value::Int1(_) => Type::Int1,
			Value::Int2(_) => Type::Int2,
			Value::Int4(_) => Type::Int4,
			Value::Int8(_) => Type::Int8,
			Value::Int16(_) => Type::Int16,
			Value::Utf8(_) => Type::Utf8,
			Value::Uint1(_) => Type::Uint1,
			Value::Uint2(_) => Type::Uint2,
			Value::Uint4(_) => Type::Uint4,
			Value::Uint8(_) => Type::Uint8,
			Value::Uint16(_) => Type::Uint16,
			Value::Date(_) => Type::Date,
			Value::DateTime(_) => Type::DateTime,
			Value::Time(_) => Type::Time,
			Value::Duration(_) => Type::Duration,
			Value::IdentityId(_) => Type::IdentityId,
			Value::Uuid4(_) => Type::Uuid4,
			Value::Uuid7(_) => Type::Uuid7,
			Value::Blob(_) => Type::Blob,
			Value::Int(_) => Type::Int,
			Value::Uint(_) => Type::Uint,
			Value::Decimal(_) => Type::Decimal,
			Value::Any(_) => Type::Any,
			Value::DictionaryId(_) => Type::DictionaryId,
			Value::Type(t) => t.clone(),
		}
	}
}

impl FromStr for Type {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let upper = s.to_uppercase();
		// Handle Option<T> syntax
		if upper.starts_with("OPTION<") && upper.ends_with('>') {
			let inner_str = &s[7..s.len() - 1]; // extract between "OPTION<" and ">"
			let inner = Type::from_str(inner_str)?;
			return Ok(Type::Option(Box::new(inner)));
		}
		match upper.as_str() {
			"BOOL" | "BOOLEAN" => Ok(Type::Boolean),
			"FLOAT4" => Ok(Type::Float4),
			"FLOAT8" => Ok(Type::Float8),
			"INT1" => Ok(Type::Int1),
			"INT2" => Ok(Type::Int2),
			"INT4" => Ok(Type::Int4),
			"INT8" => Ok(Type::Int8),
			"INT16" => Ok(Type::Int16),
			"UTF8" | "TEXT" => Ok(Type::Utf8),
			"UINT1" => Ok(Type::Uint1),
			"UINT2" => Ok(Type::Uint2),
			"UINT4" => Ok(Type::Uint4),
			"UINT8" => Ok(Type::Uint8),
			"UINT16" => Ok(Type::Uint16),
			"DATE" => Ok(Type::Date),
			"DATETIME" => Ok(Type::DateTime),
			"TIME" => Ok(Type::Time),
			"DURATION" | "INTERVAL" => Ok(Type::Duration),
			"IDENTITYID" | "IDENTITY_ID" => Ok(Type::IdentityId),
			"UUID4" => Ok(Type::Uuid4),
			"UUID7" => Ok(Type::Uuid7),
			"BLOB" => Ok(Type::Blob),
			"INT" => Ok(Type::Int),
			"UINT" => Ok(Type::Uint),
			"DECIMAL" => Ok(Type::Decimal),
			"ANY" => Ok(Type::Any),
			"DICTIONARYID" | "DICTIONARY_ID" => Ok(Type::DictionaryId),
			_ => Err(()),
		}
	}
}
