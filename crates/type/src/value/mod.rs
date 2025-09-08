// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

mod as_string;
pub mod blob;
pub mod boolean;
pub mod constraint;
mod date;
mod datetime;
pub mod decimal;
mod identity;
mod interval;
mod into;
pub mod is;
pub mod number;
mod ordered_f32;
mod ordered_f64;
pub mod row_number;
pub mod temporal;
mod time;
mod r#type;
pub mod uuid;
pub mod varint;
pub mod varuint;

pub use blob::Blob;
pub use constraint::{Constraint, TypeConstraint};
pub use date::Date;
pub use datetime::DateTime;
pub use decimal::Decimal;
pub use identity::IdentityId;
pub use interval::Interval;
pub use into::IntoValue;
pub use ordered_f32::OrderedF32;
pub use ordered_f64::OrderedF64;
pub use row_number::RowNumber;
pub use time::Time;
pub use r#type::{GetType, Type};
pub use uuid::{Uuid4, Uuid7};
pub use varint::VarInt;
pub use varuint::VarUint;

/// A RQL value, represented as a native Rust type.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
	/// Value is not defined (think null in common programming languages)
	Undefined,
	/// A boolean: true or false.
	Boolean(bool),
	/// A 4-byte floating point
	Float4(OrderedF32),
	/// An 8-byte floating point
	Float8(OrderedF64),
	/// A 1-byte signed integer
	Int1(i8),
	/// A 2-byte signed integer
	Int2(i16),
	/// A 4-byte signed integer
	Int4(i32),
	/// An 8-byte signed integer
	Int8(i64),
	/// A 16-byte signed integer
	Int16(i128),
	/// A UTF-8 encoded text. Maximum 255 bytes
	Utf8(String),
	/// A 1-byte unsigned integer
	Uint1(u8),
	/// A 2-byte unsigned integer
	Uint2(u16),
	/// A 4-byte unsigned integer
	Uint4(u32),
	/// A 8-byte unsigned integer
	Uint8(u64),
	/// A 16-byte unsigned integer
	Uint16(u128),
	/// A date value (year, month, day)
	Date(Date),
	/// A date and time value with nanosecond precision in UTC
	DateTime(DateTime),
	/// A time value (hour, minute, second, nanosecond)
	Time(Time),
	/// An interval representing a duration
	Interval(Interval),
	/// A row number (8-byte unsigned integer)
	RowNumber(RowNumber),
	/// An identity identifier (UUID v7)
	IdentityId(IdentityId),
	/// A UUID version 4 (random)
	Uuid4(Uuid4),
	/// A UUID version 7 (timestamp-based)
	Uuid7(Uuid7),
	/// A binary large object (BLOB)
	Blob(Blob),
	/// An arbitrary-precision signed integer
	VarInt(VarInt),
	/// An arbitrary-precision unsigned integer
	VarUint(VarUint),
	/// An arbitrary-precision decimal
	Decimal(Decimal),
}

impl Value {
	pub fn undefined() -> Self {
		Value::Undefined
	}

	pub fn bool(v: impl Into<bool>) -> Self {
		Value::Boolean(v.into())
	}

	pub fn float4(v: impl Into<f32>) -> Self {
		OrderedF32::try_from(v.into())
			.map(Value::Float4)
			.unwrap_or(Value::Undefined)
	}

	pub fn float8(v: impl Into<f64>) -> Self {
		OrderedF64::try_from(v.into())
			.map(Value::Float8)
			.unwrap_or(Value::Undefined)
	}

	pub fn int1(v: impl Into<i8>) -> Self {
		Value::Int1(v.into())
	}

	pub fn int2(v: impl Into<i16>) -> Self {
		Value::Int2(v.into())
	}

	pub fn int4(v: impl Into<i32>) -> Self {
		Value::Int4(v.into())
	}

	pub fn int8(v: impl Into<i64>) -> Self {
		Value::Int8(v.into())
	}

	pub fn int16(v: impl Into<i128>) -> Self {
		Value::Int16(v.into())
	}

	pub fn utf8(v: impl Into<String>) -> Self {
		Value::Utf8(v.into())
	}

	pub fn uint1(v: impl Into<u8>) -> Self {
		Value::Uint1(v.into())
	}

	pub fn uint2(v: impl Into<u16>) -> Self {
		Value::Uint2(v.into())
	}

	pub fn uint4(v: impl Into<u32>) -> Self {
		Value::Uint4(v.into())
	}

	pub fn uint8(v: impl Into<u64>) -> Self {
		Value::Uint8(v.into())
	}

	pub fn uint16(v: impl Into<u128>) -> Self {
		Value::Uint16(v.into())
	}

	pub fn date(v: impl Into<Date>) -> Self {
		Value::Date(v.into())
	}

	pub fn datetime(v: impl Into<DateTime>) -> Self {
		Value::DateTime(v.into())
	}

	pub fn time(v: impl Into<Time>) -> Self {
		Value::Time(v.into())
	}

	pub fn interval(v: impl Into<Interval>) -> Self {
		Value::Interval(v.into())
	}

	pub fn row_number(v: impl Into<RowNumber>) -> Self {
		Value::RowNumber(v.into())
	}

	pub fn identity_id(v: impl Into<IdentityId>) -> Self {
		Value::IdentityId(v.into())
	}

	pub fn uuid4(v: impl Into<Uuid4>) -> Self {
		Value::Uuid4(v.into())
	}

	pub fn uuid7(v: impl Into<Uuid7>) -> Self {
		Value::Uuid7(v.into())
	}

	pub fn blob(v: impl Into<Blob>) -> Self {
		Value::Blob(v.into())
	}
}

impl PartialOrd for Value {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match (self, other) {
			(Value::Boolean(l), Value::Boolean(r)) => {
				l.partial_cmp(r)
			}
			(Value::Float4(l), Value::Float4(r)) => {
				l.partial_cmp(r)
			}
			(Value::Float8(l), Value::Float8(r)) => {
				l.partial_cmp(r)
			}
			(Value::Int1(l), Value::Int1(r)) => l.partial_cmp(r),
			(Value::Int2(l), Value::Int2(r)) => l.partial_cmp(r),
			(Value::Int4(l), Value::Int4(r)) => l.partial_cmp(r),
			(Value::Int8(l), Value::Int8(r)) => l.partial_cmp(r),
			(Value::Int16(l), Value::Int16(r)) => l.partial_cmp(r),
			(Value::Utf8(l), Value::Utf8(r)) => l.partial_cmp(r),
			(Value::Uint1(l), Value::Uint1(r)) => l.partial_cmp(r),
			(Value::Uint2(l), Value::Uint2(r)) => l.partial_cmp(r),
			(Value::Uint4(l), Value::Uint4(r)) => l.partial_cmp(r),
			(Value::Uint8(l), Value::Uint8(r)) => l.partial_cmp(r),
			(Value::Uint16(l), Value::Uint16(r)) => {
				l.partial_cmp(r)
			}
			(Value::Date(l), Value::Date(r)) => l.partial_cmp(r),
			(Value::DateTime(l), Value::DateTime(r)) => {
				l.partial_cmp(r)
			}
			(Value::Time(l), Value::Time(r)) => l.partial_cmp(r),
			(Value::Interval(l), Value::Interval(r)) => {
				l.partial_cmp(r)
			}
			(Value::RowNumber(l), Value::RowNumber(r)) => {
				l.partial_cmp(r)
			}
			(Value::IdentityId(l), Value::IdentityId(r)) => {
				l.partial_cmp(r)
			}
			(Value::Uuid4(l), Value::Uuid4(r)) => l.partial_cmp(r),
			(Value::Uuid7(l), Value::Uuid7(r)) => l.partial_cmp(r),
			(Value::Blob(l), Value::Blob(r)) => l.partial_cmp(r),
			(Value::VarInt(l), Value::VarInt(r)) => {
				l.partial_cmp(r)
			}
			(Value::VarUint(l), Value::VarUint(r)) => {
				l.partial_cmp(r)
			}
			(Value::Decimal(l), Value::Decimal(r)) => {
				l.partial_cmp(r)
			}
			(Value::Undefined, Value::Undefined) => None,
			(left, right) => {
				unimplemented!("partial cmp {left:?} {right:?}")
			}
		}
	}
}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Value::Boolean(l), Value::Boolean(r)) => l.cmp(r),
			(Value::Float4(l), Value::Float4(r)) => l.cmp(r),
			(Value::Float8(l), Value::Float8(r)) => l.cmp(r),
			(Value::Int1(l), Value::Int1(r)) => l.cmp(r),
			(Value::Int2(l), Value::Int2(r)) => l.cmp(r),
			(Value::Int4(l), Value::Int4(r)) => l.cmp(r),
			(Value::Int8(l), Value::Int8(r)) => l.cmp(r),
			(Value::Int16(l), Value::Int16(r)) => l.cmp(r),
			(Value::Utf8(l), Value::Utf8(r)) => l.cmp(r),
			(Value::Uint1(l), Value::Uint1(r)) => l.cmp(r),
			(Value::Uint2(l), Value::Uint2(r)) => l.cmp(r),
			(Value::Uint4(l), Value::Uint4(r)) => l.cmp(r),
			(Value::Uint8(l), Value::Uint8(r)) => l.cmp(r),
			(Value::Uint16(l), Value::Uint16(r)) => l.cmp(r),
			(Value::Date(l), Value::Date(r)) => l.cmp(r),
			(Value::DateTime(l), Value::DateTime(r)) => l.cmp(r),
			(Value::Time(l), Value::Time(r)) => l.cmp(r),
			(Value::Interval(l), Value::Interval(r)) => l.cmp(r),
			(Value::RowNumber(l), Value::RowNumber(r)) => l.cmp(r),
			(Value::IdentityId(l), Value::IdentityId(r)) => {
				l.cmp(r)
			}
			(Value::Uuid4(l), Value::Uuid4(r)) => l.cmp(r),
			(Value::Uuid7(l), Value::Uuid7(r)) => l.cmp(r),
			(Value::Blob(l), Value::Blob(r)) => l.cmp(r),
			(Value::VarInt(l), Value::VarInt(r)) => l.cmp(r),
			(Value::VarUint(l), Value::VarUint(r)) => l.cmp(r),
			(Value::Decimal(l), Value::Decimal(r)) => l.cmp(r),
			_ => unimplemented!(),
		}
	}
}

impl Display for Value {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Value::Boolean(true) => f.write_str("true"),
			Value::Boolean(false) => f.write_str("false"),
			Value::Float4(value) => Display::fmt(value, f),
			Value::Float8(value) => Display::fmt(value, f),
			Value::Int1(value) => Display::fmt(value, f),
			Value::Int2(value) => Display::fmt(value, f),
			Value::Int4(value) => Display::fmt(value, f),
			Value::Int8(value) => Display::fmt(value, f),
			Value::Int16(value) => Display::fmt(value, f),
			Value::Utf8(value) => Display::fmt(value, f),
			Value::Uint1(value) => Display::fmt(value, f),
			Value::Uint2(value) => Display::fmt(value, f),
			Value::Uint4(value) => Display::fmt(value, f),
			Value::Uint8(value) => Display::fmt(value, f),
			Value::Uint16(value) => Display::fmt(value, f),
			Value::Date(value) => Display::fmt(value, f),
			Value::DateTime(value) => Display::fmt(value, f),
			Value::Time(value) => Display::fmt(value, f),
			Value::Interval(value) => Display::fmt(value, f),
			Value::RowNumber(value) => Display::fmt(value, f),
			Value::IdentityId(value) => Display::fmt(value, f),
			Value::Uuid4(value) => Display::fmt(value, f),
			Value::Uuid7(value) => Display::fmt(value, f),
			Value::Blob(value) => Display::fmt(value, f),
			Value::VarInt(value) => Display::fmt(value, f),
			Value::VarUint(value) => Display::fmt(value, f),
			Value::Decimal(value) => Display::fmt(value, f),
			Value::Undefined => f.write_str("undefined"),
		}
	}
}

impl Value {
	pub fn get_type(&self) -> Type {
		match self {
			Value::Undefined => Type::Undefined,
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
			Value::Interval(_) => Type::Interval,
			Value::RowNumber(_) => Type::RowNumber,
			Value::IdentityId(_) => Type::IdentityId,
			Value::Uuid4(_) => Type::Uuid4,
			Value::Uuid7(_) => Type::Uuid7,
			Value::Blob(_) => Type::Blob,
			Value::VarInt(_) => Type::VarInt,
			Value::VarUint(_) => Type::VarUint,
			Value::Decimal(_) => Type::Decimal,
		}
	}
}
