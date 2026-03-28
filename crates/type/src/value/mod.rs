// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
};

use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};
pub mod as_string;
pub mod blob;
pub mod boolean;
pub mod constraint;
pub mod container;
pub mod date;
pub mod datetime;
pub mod decimal;
pub mod dictionary;
pub mod duration;
pub mod frame;
pub mod identity;
pub mod int;
pub mod into;
pub mod is;
pub mod json;
pub mod number;
pub mod ordered_f32;
pub mod ordered_f64;
pub mod row_number;
pub mod sumtype;
pub mod temporal;
pub mod time;
pub mod to_value;
pub mod try_from;
pub mod r#type;
pub mod uint;
pub mod uuid;

use std::{fmt, hash, mem};

use blob::Blob;
use date::Date;
use datetime::DateTime;
use decimal::Decimal;
use dictionary::DictionaryEntryId;
use duration::Duration;
use identity::IdentityId;
use int::Int;
use ordered_f32::OrderedF32;
use ordered_f64::OrderedF64;
use time::Time;
use r#type::Type;
use uint::Uint;
use uuid::{Uuid4, Uuid7};

/// A RQL value, represented as a native Rust type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
	/// Value is none (think null in common programming languages)
	None {
		#[serde(skip, default = "default_none_inner")]
		inner: Type,
	},
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
	/// A date and time value with nanosecond precision in SVTC
	DateTime(DateTime),
	/// A time value (hour, minute, second, nanosecond)
	Time(Time),
	/// A duration representing a duration
	Duration(Duration),
	/// An identity identifier (UUID v7)
	IdentityId(IdentityId),
	/// A UUID version 4 (random)
	Uuid4(Uuid4),
	/// A UUID version 7 (timestamp-based)
	Uuid7(Uuid7),
	/// A binary large object (BLOB)
	Blob(Blob),
	/// An arbitrary-precision signed integer
	Int(Int),
	/// An arbitrary-precision unsigned integer
	Uint(Uint),
	/// An arbitrary-precision decimal
	Decimal(Decimal),
	/// A container that can hold any value type
	Any(Box<Value>),
	/// A dictionary entry identifier
	DictionaryId(DictionaryEntryId),
	/// A type value (first-class type identifier)
	Type(Type),
	/// An ordered list of values
	List(Vec<Value>),
	/// A record (named fields with values)
	Record(Vec<(String, Value)>),
	/// A tuple of heterogeneous values
	Tuple(Vec<Value>),
}

fn default_none_inner() -> Type {
	Type::Any
}

impl Value {
	pub fn none() -> Self {
		Value::None {
			inner: Type::Any,
		}
	}

	pub fn none_of(ty: Type) -> Self {
		Value::None {
			inner: ty,
		}
	}

	pub fn bool(v: impl Into<bool>) -> Self {
		Value::Boolean(v.into())
	}

	pub fn float4(v: impl Into<f32>) -> Self {
		OrderedF32::try_from(v.into()).map(Value::Float4).unwrap_or(Value::None {
			inner: Type::Float4,
		})
	}

	pub fn float8(v: impl Into<f64>) -> Self {
		OrderedF64::try_from(v.into()).map(Value::Float8).unwrap_or(Value::None {
			inner: Type::Float8,
		})
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

	pub fn duration(v: impl Into<Duration>) -> Self {
		Value::Duration(v.into())
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

	pub fn any(v: impl Into<Value>) -> Self {
		Value::Any(Box::new(v.into()))
	}

	pub fn list(items: Vec<Value>) -> Self {
		Value::List(items)
	}

	pub fn record(fields: Vec<(String, Value)>) -> Self {
		Value::Record(fields)
	}

	pub fn to_usize(&self) -> Option<usize> {
		match self {
			Value::Uint1(v) => Some(*v as usize),
			Value::Uint2(v) => Some(*v as usize),
			Value::Uint4(v) => Some(*v as usize),
			Value::Uint8(v) => usize::try_from(*v).ok(),
			Value::Uint16(v) => usize::try_from(*v).ok(),
			Value::Int1(v) => usize::try_from(*v).ok(),
			Value::Int2(v) => usize::try_from(*v).ok(),
			Value::Int4(v) => usize::try_from(*v).ok(),
			Value::Int8(v) => usize::try_from(*v).ok(),
			Value::Int16(v) => usize::try_from(*v).ok(),
			Value::Float4(v) => {
				let f = v.value();
				if f >= 0.0 {
					Some(f as usize)
				} else {
					None
				}
			}
			Value::Float8(v) => {
				let f = v.value();
				if f >= 0.0 {
					Some(f as usize)
				} else {
					None
				}
			}
			Value::Int(v) => v.0.to_u64().and_then(|n| usize::try_from(n).ok()),
			Value::Uint(v) => v.0.to_u64().and_then(|n| usize::try_from(n).ok()),
			Value::Decimal(v) => v.0.to_u64().and_then(|n| usize::try_from(n).ok()),
			Value::Utf8(s) => {
				let s = s.trim();
				if let Ok(n) = s.parse::<u64>() {
					usize::try_from(n).ok()
				} else if let Ok(f) = s.parse::<f64>() {
					if f >= 0.0 {
						Some(f as usize)
					} else {
						None
					}
				} else {
					None
				}
			}
			_ => None,
		}
	}
}

impl PartialEq for Value {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(
				Value::None {
					..
				},
				Value::None {
					..
				},
			) => true,
			(Value::Boolean(l), Value::Boolean(r)) => l == r,
			(Value::Float4(l), Value::Float4(r)) => l == r,
			(Value::Float8(l), Value::Float8(r)) => l == r,
			(Value::Int1(l), Value::Int1(r)) => l == r,
			(Value::Int2(l), Value::Int2(r)) => l == r,
			(Value::Int4(l), Value::Int4(r)) => l == r,
			(Value::Int8(l), Value::Int8(r)) => l == r,
			(Value::Int16(l), Value::Int16(r)) => l == r,
			(Value::Utf8(l), Value::Utf8(r)) => l == r,
			(Value::Uint1(l), Value::Uint1(r)) => l == r,
			(Value::Uint2(l), Value::Uint2(r)) => l == r,
			(Value::Uint4(l), Value::Uint4(r)) => l == r,
			(Value::Uint8(l), Value::Uint8(r)) => l == r,
			(Value::Uint16(l), Value::Uint16(r)) => l == r,
			(Value::Date(l), Value::Date(r)) => l == r,
			(Value::DateTime(l), Value::DateTime(r)) => l == r,
			(Value::Time(l), Value::Time(r)) => l == r,
			(Value::Duration(l), Value::Duration(r)) => l == r,
			(Value::IdentityId(l), Value::IdentityId(r)) => l == r,
			(Value::Uuid4(l), Value::Uuid4(r)) => l == r,
			(Value::Uuid7(l), Value::Uuid7(r)) => l == r,
			(Value::Blob(l), Value::Blob(r)) => l == r,
			(Value::Int(l), Value::Int(r)) => l == r,
			(Value::Uint(l), Value::Uint(r)) => l == r,
			(Value::Decimal(l), Value::Decimal(r)) => l == r,
			(Value::Any(l), Value::Any(r)) => l == r,
			(Value::DictionaryId(l), Value::DictionaryId(r)) => l == r,
			(Value::Type(l), Value::Type(r)) => l == r,
			(Value::List(l), Value::List(r)) => l == r,
			(Value::Record(l), Value::Record(r)) => l == r,
			(Value::Tuple(l), Value::Tuple(r)) => l == r,
			_ => false,
		}
	}
}

impl Eq for Value {}

impl hash::Hash for Value {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		mem::discriminant(self).hash(state);
		match self {
			Value::None {
				..
			} => {} // All Nones hash identically
			Value::Boolean(v) => v.hash(state),
			Value::Float4(v) => v.hash(state),
			Value::Float8(v) => v.hash(state),
			Value::Int1(v) => v.hash(state),
			Value::Int2(v) => v.hash(state),
			Value::Int4(v) => v.hash(state),
			Value::Int8(v) => v.hash(state),
			Value::Int16(v) => v.hash(state),
			Value::Utf8(v) => v.hash(state),
			Value::Uint1(v) => v.hash(state),
			Value::Uint2(v) => v.hash(state),
			Value::Uint4(v) => v.hash(state),
			Value::Uint8(v) => v.hash(state),
			Value::Uint16(v) => v.hash(state),
			Value::Date(v) => v.hash(state),
			Value::DateTime(v) => v.hash(state),
			Value::Time(v) => v.hash(state),
			Value::Duration(v) => v.hash(state),
			Value::IdentityId(v) => v.hash(state),
			Value::Uuid4(v) => v.hash(state),
			Value::Uuid7(v) => v.hash(state),
			Value::Blob(v) => v.hash(state),
			Value::Int(v) => v.hash(state),
			Value::Uint(v) => v.hash(state),
			Value::Decimal(v) => v.hash(state),
			Value::Any(v) => v.hash(state),
			Value::DictionaryId(v) => v.hash(state),
			Value::Type(v) => v.hash(state),
			Value::List(v) => v.hash(state),
			Value::Record(fields) => {
				for (k, v) in fields {
					k.hash(state);
					v.hash(state);
				}
			}
			Value::Tuple(v) => v.hash(state),
		}
	}
}

impl PartialOrd for Value {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match (self, other) {
			(Value::Boolean(l), Value::Boolean(r)) => l.partial_cmp(r),
			(Value::Float4(l), Value::Float4(r)) => l.partial_cmp(r),
			(Value::Float8(l), Value::Float8(r)) => l.partial_cmp(r),
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
			(Value::Uint16(l), Value::Uint16(r)) => l.partial_cmp(r),
			(Value::Date(l), Value::Date(r)) => l.partial_cmp(r),
			(Value::DateTime(l), Value::DateTime(r)) => l.partial_cmp(r),
			(Value::Time(l), Value::Time(r)) => l.partial_cmp(r),
			(Value::Duration(l), Value::Duration(r)) => l.partial_cmp(r),
			(Value::IdentityId(l), Value::IdentityId(r)) => l.partial_cmp(r),
			(Value::Uuid4(l), Value::Uuid4(r)) => l.partial_cmp(r),
			(Value::Uuid7(l), Value::Uuid7(r)) => l.partial_cmp(r),
			(Value::Blob(l), Value::Blob(r)) => l.partial_cmp(r),
			(Value::Int(l), Value::Int(r)) => l.partial_cmp(r),
			(Value::Uint(l), Value::Uint(r)) => l.partial_cmp(r),
			(Value::Decimal(l), Value::Decimal(r)) => l.partial_cmp(r),
			(Value::DictionaryId(l), Value::DictionaryId(r)) => l.to_u128().partial_cmp(&r.to_u128()),
			(Value::Type(l), Value::Type(r)) => l.partial_cmp(r),
			(Value::List(_), Value::List(_)) => None,     // Lists are not orderable
			(Value::Record(_), Value::Record(_)) => None, // Records are not orderable
			(Value::Tuple(_), Value::Tuple(_)) => None,   // Tuples are not orderable
			(Value::Any(_), Value::Any(_)) => None,       // Any values are not comparable
			(
				Value::None {
					..
				},
				Value::None {
					..
				},
			) => Some(Ordering::Equal),
			// None sorts after all other values (similar to NULL in SQL)
			(
				Value::None {
					..
				},
				_,
			) => Some(Ordering::Greater),
			(
				_,
				Value::None {
					..
				},
			) => Some(Ordering::Less),
			(left, right) => {
				unimplemented!("partial cmp {left:?} {right:?}")
			}
		}
	}
}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(
				Value::None {
					..
				},
				Value::None {
					..
				},
			) => Ordering::Equal,
			(
				Value::None {
					..
				},
				_,
			) => Ordering::Greater,
			(
				_,
				Value::None {
					..
				},
			) => Ordering::Less,
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
			(Value::Duration(l), Value::Duration(r)) => l.cmp(r),
			(Value::IdentityId(l), Value::IdentityId(r)) => l.cmp(r),
			(Value::Uuid4(l), Value::Uuid4(r)) => l.cmp(r),
			(Value::Uuid7(l), Value::Uuid7(r)) => l.cmp(r),
			(Value::Blob(l), Value::Blob(r)) => l.cmp(r),
			(Value::Int(l), Value::Int(r)) => l.cmp(r),
			(Value::Uint(l), Value::Uint(r)) => l.cmp(r),
			(Value::Decimal(l), Value::Decimal(r)) => l.cmp(r),
			(Value::DictionaryId(l), Value::DictionaryId(r)) => l.to_u128().cmp(&r.to_u128()),
			(Value::Type(l), Value::Type(r)) => l.cmp(r),
			(Value::List(_), Value::List(_)) => unreachable!("List values are not orderable"),
			(Value::Record(_), Value::Record(_)) => unreachable!("Record values are not orderable"),
			(Value::Tuple(_), Value::Tuple(_)) => unreachable!("Tuple values are not orderable"),
			(Value::Any(_), Value::Any(_)) => unreachable!("Any values are not orderable"),
			_ => unimplemented!(),
		}
	}
}

impl Display for Value {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
			Value::Duration(value) => Display::fmt(value, f),
			Value::IdentityId(value) => Display::fmt(value, f),
			Value::Uuid4(value) => Display::fmt(value, f),
			Value::Uuid7(value) => Display::fmt(value, f),
			Value::Blob(value) => Display::fmt(value, f),
			Value::Int(value) => Display::fmt(value, f),
			Value::Uint(value) => Display::fmt(value, f),
			Value::Decimal(value) => Display::fmt(value, f),
			Value::Any(value) => Display::fmt(value, f),
			Value::DictionaryId(value) => Display::fmt(value, f),
			Value::Type(value) => Display::fmt(value, f),
			Value::List(items) => {
				f.write_str("[")?;
				for (i, item) in items.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					Display::fmt(item, f)?;
				}
				f.write_str("]")
			}
			Value::Record(fields) => {
				f.write_str("{")?;
				for (i, (key, value)) in fields.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					write!(f, "{}: {}", key, value)?;
				}
				f.write_str("}")
			}
			Value::Tuple(items) => {
				f.write_str("(")?;
				for (i, item) in items.iter().enumerate() {
					if i > 0 {
						f.write_str(", ")?;
					}
					Display::fmt(item, f)?;
				}
				f.write_str(")")
			}
			Value::None {
				..
			} => f.write_str("none"),
		}
	}
}

impl Value {
	pub fn get_type(&self) -> Type {
		match self {
			Value::None {
				inner,
			} => Type::Option(Box::new(inner.clone())),
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
			Value::List(items) => {
				let element_type = items.first().map(|v| v.get_type()).unwrap_or(Type::Any);
				Type::list_of(element_type)
			}
			Value::Record(fields) => {
				Type::Record(fields.iter().map(|(k, v)| (k.clone(), v.get_type())).collect())
			}
			Value::Tuple(items) => Type::Tuple(items.iter().map(|v| v.get_type()).collect()),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use super::*;

	// Happy path — one per numeric type

	#[test]
	fn to_usize_uint1() {
		assert_eq!(Value::uint1(42u8).to_usize(), Some(42));
	}

	#[test]
	fn to_usize_uint2() {
		assert_eq!(Value::uint2(1000u16).to_usize(), Some(1000));
	}

	#[test]
	fn to_usize_uint4() {
		assert_eq!(Value::uint4(100_000u32).to_usize(), Some(100_000));
	}

	#[test]
	fn to_usize_uint8() {
		assert_eq!(Value::uint8(1_000_000u64).to_usize(), Some(1_000_000));
	}

	#[test]
	fn to_usize_uint16() {
		assert_eq!(Value::Uint16(500u128).to_usize(), Some(500));
	}

	#[test]
	fn to_usize_int1() {
		assert_eq!(Value::int1(100i8).to_usize(), Some(100));
	}

	#[test]
	fn to_usize_int2() {
		assert_eq!(Value::int2(5000i16).to_usize(), Some(5000));
	}

	#[test]
	fn to_usize_int4() {
		assert_eq!(Value::int4(50_000i32).to_usize(), Some(50_000));
	}

	#[test]
	fn to_usize_int8() {
		assert_eq!(Value::int8(1_000_000i64).to_usize(), Some(1_000_000));
	}

	#[test]
	fn to_usize_int16() {
		assert_eq!(Value::Int16(999i128).to_usize(), Some(999));
	}

	#[test]
	fn to_usize_float4() {
		assert_eq!(Value::float4(42.0f32).to_usize(), Some(42));
	}

	#[test]
	fn to_usize_float8() {
		assert_eq!(Value::float8(42.0f64).to_usize(), Some(42));
	}

	#[test]
	fn to_usize_int_bigint() {
		assert_eq!(Value::Int(Int::from_i64(42)).to_usize(), Some(42));
	}

	#[test]
	fn to_usize_uint_bigint() {
		assert_eq!(Value::Uint(Uint::from_u64(42)).to_usize(), Some(42));
	}

	#[test]
	fn to_usize_decimal() {
		assert_eq!(Value::Decimal(Decimal::from_i64(42)).to_usize(), Some(42));
	}

	// Edge cases & errors — negative numbers

	#[test]
	fn to_usize_int1_negative() {
		assert_eq!(Value::int1(-1i8).to_usize(), None);
	}

	#[test]
	fn to_usize_int2_negative() {
		assert_eq!(Value::int2(-100i16).to_usize(), None);
	}

	#[test]
	fn to_usize_int4_negative() {
		assert_eq!(Value::int4(-1i32).to_usize(), None);
	}

	#[test]
	fn to_usize_int8_negative() {
		assert_eq!(Value::int8(-1i64).to_usize(), None);
	}

	#[test]
	fn to_usize_int16_negative() {
		assert_eq!(Value::Int16(-1i128).to_usize(), None);
	}

	#[test]
	fn to_usize_float4_negative() {
		assert_eq!(Value::float4(-1.0f32).to_usize(), None);
	}

	#[test]
	fn to_usize_float8_negative() {
		assert_eq!(Value::float8(-1.0f64).to_usize(), None);
	}

	#[test]
	fn to_usize_int_bigint_negative() {
		assert_eq!(Value::Int(Int::from_i64(-5)).to_usize(), None);
	}

	// Edge cases — zero boundary

	#[test]
	fn to_usize_zero() {
		assert_eq!(Value::uint1(0u8).to_usize(), Some(0));
	}

	#[test]
	fn to_usize_int1_zero() {
		assert_eq!(Value::int1(0i8).to_usize(), Some(0));
	}

	#[test]
	fn to_usize_float4_zero() {
		assert_eq!(Value::float4(0.0f32).to_usize(), Some(0));
	}

	// Edge cases — non-numeric types return None

	#[test]
	fn to_usize_boolean_none() {
		assert_eq!(Value::bool(true).to_usize(), None);
	}

	#[test]
	fn to_usize_utf8_integer() {
		assert_eq!(Value::utf8("42").to_usize(), Some(42));
	}

	#[test]
	fn to_usize_utf8_float() {
		assert_eq!(Value::utf8("3.7").to_usize(), Some(3));
	}

	#[test]
	fn to_usize_utf8_negative() {
		assert_eq!(Value::utf8("-5").to_usize(), None);
	}

	#[test]
	fn to_usize_utf8_negative_float() {
		assert_eq!(Value::utf8("-1.5").to_usize(), None);
	}

	#[test]
	fn to_usize_utf8_whitespace() {
		assert_eq!(Value::utf8("  42  ").to_usize(), Some(42));
	}

	#[test]
	fn to_usize_utf8_zero() {
		assert_eq!(Value::utf8("0").to_usize(), Some(0));
	}

	#[test]
	fn to_usize_utf8_non_numeric() {
		assert_eq!(Value::utf8("hello").to_usize(), None);
	}

	#[test]
	fn to_usize_utf8_empty() {
		assert_eq!(Value::utf8("").to_usize(), None);
	}

	#[test]
	fn to_usize_none_none() {
		assert_eq!(Value::none().to_usize(), None);
	}

	// Edge cases — fractional truncation

	#[test]
	fn to_usize_float8_fractional() {
		assert_eq!(Value::float8(3.7f64).to_usize(), Some(3));
	}

	#[test]
	fn to_usize_decimal_fractional() {
		assert_eq!(Value::Decimal(Decimal::from_str("3.7").unwrap()).to_usize(), Some(3));
	}
}
