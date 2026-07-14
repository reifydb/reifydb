// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Workspace-wide value system. Defines the `Value` enum every column carries, the `ValueType` enum that classifies it,
//! the `Constraint` family that narrows a type (max-bytes, precision-scale, optional), and the per-primitive
//! representations - integers, unsigned integers, decimals, floats, blobs, booleans, temporals, UUIDs, JSON,
//! identity ids, and row numbers - that those variants wrap.
//!
//! The variants, their order, and their on-the-wire shape are stable. Adding a variant is a coordinated
//! workspace change that lands together with `wire-format` and the storage encoders; rearranging existing
//! variants silently corrupts persisted data.

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
pub mod iso;
pub mod json;
pub mod number;
pub mod ordered_f32;
pub mod ordered_f64;
pub mod partition;
pub mod row_number;
pub mod sumtype;
pub mod temporal;
pub mod time;
pub mod to_value;
pub mod try_from;
pub mod uint;
pub mod uuid;
pub mod value_type;
pub mod vector;

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
use uint::Uint;
use uuid::{Uuid4, Uuid7};
use value_type::ValueType;
use vector::VectorValue;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
	None {
		inner: ValueType,
	},

	Boolean(bool),

	Float4(OrderedF32),

	Float8(OrderedF64),

	Int1(i8),

	Int2(i16),

	Int4(i32),

	Int8(i64),

	Int16(i128),

	Utf8(String),

	Uint1(u8),

	Uint2(u16),

	Uint4(u32),

	Uint8(u64),

	Uint16(u128),

	Date(Date),

	DateTime(DateTime),

	Time(Time),

	Duration(Duration),

	IdentityId(IdentityId),

	Uuid4(Uuid4),

	Uuid7(Uuid7),

	Blob(Blob),

	Int(Int),

	Uint(Uint),

	Decimal(Decimal),

	Any(Box<Value>),

	DictionaryId(DictionaryEntryId),

	Type(ValueType),

	List(Vec<Value>),

	Record(Vec<(String, Value)>),

	Tuple(Vec<Value>),

	Vector(VectorValue),
}

impl Value {
	pub fn none() -> Self {
		Value::None {
			inner: ValueType::Any,
		}
	}

	pub fn none_of(ty: ValueType) -> Self {
		Value::None {
			inner: ty,
		}
	}

	pub fn bool(v: impl Into<bool>) -> Self {
		Value::Boolean(v.into())
	}

	pub fn float4(v: impl Into<f32>) -> Self {
		OrderedF32::try_from(v.into()).map(Value::Float4).unwrap_or(Value::None {
			inner: ValueType::Float4,
		})
	}

	pub fn float8(v: impl Into<f64>) -> Self {
		OrderedF64::try_from(v.into()).map(Value::Float8).unwrap_or(Value::None {
			inner: ValueType::Float8,
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

	pub fn duration_nanoseconds(nanoseconds: i64) -> Self {
		Value::Duration(Duration::from_nanoseconds_const(nanoseconds))
	}

	pub fn duration_microseconds(microseconds: i64) -> Self {
		Value::Duration(Duration::from_microseconds_const(microseconds))
	}

	pub fn duration_milliseconds(milliseconds: i64) -> Self {
		Value::Duration(Duration::from_milliseconds_const(milliseconds))
	}

	pub fn duration_seconds(seconds: i64) -> Self {
		Value::Duration(Duration::from_seconds_const(seconds))
	}

	pub fn duration_minutes(minutes: i64) -> Self {
		Value::Duration(Duration::from_minutes_const(minutes))
	}

	pub fn duration_hours(hours: i64) -> Self {
		Value::Duration(Duration::from_hours_const(hours))
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

	pub fn vector(v: impl Into<VectorValue>) -> Self {
		Value::Vector(v.into())
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
		match self {
			Value::None {
				inner: l,
			} => matches!(other, Value::None { inner: r } if l == r),
			Value::Boolean(l) => matches!(other, Value::Boolean(r) if l == r),
			Value::Float4(l) => matches!(other, Value::Float4(r) if l == r),
			Value::Float8(l) => matches!(other, Value::Float8(r) if l == r),
			Value::Int1(l) => matches!(other, Value::Int1(r) if l == r),
			Value::Int2(l) => matches!(other, Value::Int2(r) if l == r),
			Value::Int4(l) => matches!(other, Value::Int4(r) if l == r),
			Value::Int8(l) => matches!(other, Value::Int8(r) if l == r),
			Value::Int16(l) => matches!(other, Value::Int16(r) if l == r),
			Value::Utf8(l) => matches!(other, Value::Utf8(r) if l == r),
			Value::Uint1(l) => matches!(other, Value::Uint1(r) if l == r),
			Value::Uint2(l) => matches!(other, Value::Uint2(r) if l == r),
			Value::Uint4(l) => matches!(other, Value::Uint4(r) if l == r),
			Value::Uint8(l) => matches!(other, Value::Uint8(r) if l == r),
			Value::Uint16(l) => matches!(other, Value::Uint16(r) if l == r),
			Value::Date(l) => matches!(other, Value::Date(r) if l == r),
			Value::DateTime(l) => matches!(other, Value::DateTime(r) if l == r),
			Value::Time(l) => matches!(other, Value::Time(r) if l == r),
			Value::Duration(l) => matches!(other, Value::Duration(r) if l == r),
			Value::IdentityId(l) => matches!(other, Value::IdentityId(r) if l == r),
			Value::Uuid4(l) => matches!(other, Value::Uuid4(r) if l == r),
			Value::Uuid7(l) => matches!(other, Value::Uuid7(r) if l == r),
			Value::Blob(l) => matches!(other, Value::Blob(r) if l == r),
			Value::Int(l) => matches!(other, Value::Int(r) if l == r),
			Value::Uint(l) => matches!(other, Value::Uint(r) if l == r),
			Value::Decimal(l) => matches!(other, Value::Decimal(r) if l == r),
			Value::Any(l) => matches!(other, Value::Any(r) if l == r),
			Value::DictionaryId(l) => matches!(other, Value::DictionaryId(r) if l == r),
			Value::Type(l) => matches!(other, Value::Type(r) if l == r),
			Value::List(l) => matches!(other, Value::List(r) if l == r),
			Value::Record(l) => matches!(other, Value::Record(r) if l == r),
			Value::Tuple(l) => matches!(other, Value::Tuple(r) if l == r),
			Value::Vector(l) => matches!(other, Value::Vector(r) if l == r),
		}
	}
}

impl Eq for Value {}

#[cfg(reifydb_assertions)]
pub fn assert_equal_with_tolerance(left: &[Value], right: &[Value]) {
	const REL_EPS: f64 = 1e-9;
	const ABS_EPS: f64 = 1e-6;
	fn close(x: f64, y: f64) -> bool {
		if x.is_nan() && y.is_nan() {
			return true;
		}
		(x - y).abs() <= ABS_EPS.max(REL_EPS * x.abs().max(y.abs()))
	}
	fn matches(a: &Value, b: &Value) -> bool {
		match (a, b) {
			(Value::Float8(x), Value::Float8(y)) => close(x.value(), y.value()),
			(Value::Float4(x), Value::Float4(y)) => close(x.value() as f64, y.value() as f64),
			_ => a == b,
		}
	}
	assert_eq!(left.len(), right.len(), "value count diverges beyond tolerance: {} vs {}", left.len(), right.len());
	for (i, (l, r)) in left.iter().zip(right).enumerate() {
		assert!(matches(l, r), "value {i} diverges beyond tolerance: {l:?} vs {r:?}");
	}
}

impl hash::Hash for Value {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		mem::discriminant(self).hash(state);
		match self {
			Value::None {
				..
			} => {}
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
			Value::Vector(v) => v.hash(state),
		}
	}
}

impl PartialOrd for Value {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Value {
	fn cmp(&self, other: &Self) -> Ordering {
		match self {
			Value::None {
				..
			} => match other {
				Value::None {
					..
				} => Ordering::Equal,
				_ => Ordering::Greater,
			},
			Value::Boolean(l) => match other {
				Value::Boolean(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Float4(l) => match other {
				Value::Float4(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Float8(l) => match other {
				Value::Float8(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Int1(l) => match other {
				Value::Int1(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Int2(l) => match other {
				Value::Int2(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Int4(l) => match other {
				Value::Int4(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Int8(l) => match other {
				Value::Int8(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Int16(l) => match other {
				Value::Int16(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Utf8(l) => match other {
				Value::Utf8(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uint1(l) => match other {
				Value::Uint1(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uint2(l) => match other {
				Value::Uint2(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uint4(l) => match other {
				Value::Uint4(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uint8(l) => match other {
				Value::Uint8(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uint16(l) => match other {
				Value::Uint16(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Date(l) => match other {
				Value::Date(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::DateTime(l) => match other {
				Value::DateTime(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Time(l) => match other {
				Value::Time(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Duration(l) => match other {
				Value::Duration(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::IdentityId(l) => match other {
				Value::IdentityId(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uuid4(l) => match other {
				Value::Uuid4(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uuid7(l) => match other {
				Value::Uuid7(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Blob(l) => match other {
				Value::Blob(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Int(l) => match other {
				Value::Int(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Uint(l) => match other {
				Value::Uint(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Decimal(l) => match other {
				Value::Decimal(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::DictionaryId(l) => match other {
				Value::DictionaryId(r) => l.to_u128().cmp(&r.to_u128()),
				other => order_against_non_matching(other),
			},
			Value::Type(l) => match other {
				Value::Type(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::Vector(l) => match other {
				Value::Vector(r) => l.cmp(r),
				other => order_against_non_matching(other),
			},
			Value::List(_) => match other {
				Value::List(_) => unreachable!("List values are not orderable"),
				other => order_against_non_matching(other),
			},
			Value::Record(_) => match other {
				Value::Record(_) => unreachable!("Record values are not orderable"),
				other => order_against_non_matching(other),
			},
			Value::Tuple(_) => match other {
				Value::Tuple(_) => unreachable!("Tuple values are not orderable"),
				other => order_against_non_matching(other),
			},
			Value::Any(_) => match other {
				Value::Any(_) => unreachable!("Any values are not orderable"),
				other => order_against_non_matching(other),
			},
		}
	}
}

fn order_against_non_matching(other: &Value) -> Ordering {
	match other {
		Value::None {
			..
		} => Ordering::Less,
		_ => unimplemented!("values of different types are not orderable"),
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
			Value::Vector(value) => Display::fmt(value, f),
			Value::None {
				..
			} => f.write_str("none"),
		}
	}
}

impl Value {
	pub fn get_type(&self) -> ValueType {
		match self {
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
				let element_type = items.first().map(|v| v.get_type()).unwrap_or(ValueType::Any);
				ValueType::list_of(element_type)
			}
			Value::Record(fields) => {
				ValueType::Record(fields.iter().map(|(k, v)| (k.clone(), v.get_type())).collect())
			}
			Value::Tuple(items) => ValueType::Tuple(items.iter().map(|v| v.get_type()).collect()),
			Value::Vector(v) => ValueType::Vector(v.dims() as u32),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use super::*;

	// Happy path - one per numeric type

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

	// Edge cases & errors - negative numbers

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

	// Edge cases - zero boundary

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

	// Edge cases - non-numeric types return None

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

	// Edge cases - fractional truncation

	#[test]
	fn to_usize_float8_fractional() {
		assert_eq!(Value::float8(3.7f64).to_usize(), Some(3));
	}

	#[test]
	fn to_usize_decimal_fractional() {
		assert_eq!(Value::Decimal(Decimal::from_str("3.7").unwrap()).to_usize(), Some(3));
	}

	// Value::PartialEq currently treats every Value::None { .. } as equal regardless of `inner`
	// (see the `(Value::None { .. }, Value::None { .. }) => true` arm above). That is wrong:
	// Option<Duration>::None and Option<Boolean>::None are different values, and Option<Duration>::None
	// is different again from Option<Option<Duration>>::None. Code that round-trips a None through
	// Any encoding (or compares config values) needs `==` to catch a lost/changed inner type instead
	// of silently treating it as "the same None".

	#[test]
	fn test_none_with_same_inner_type_are_equal() {
		assert_eq!(Value::none_of(ValueType::Duration), Value::none_of(ValueType::Duration));
	}

	#[test]
	fn test_none_with_different_inner_type_are_not_equal() {
		assert_ne!(Value::none_of(ValueType::Duration), Value::none_of(ValueType::Boolean));
	}

	#[test]
	fn test_none_with_different_nesting_depth_are_not_equal() {
		let option_duration = Value::none_of(ValueType::Option(Box::new(ValueType::Duration)));
		let duration = Value::none_of(ValueType::Duration);
		assert_ne!(option_duration, duration);
	}

	#[test]
	fn test_none_any_is_not_equal_to_none_of_concrete_type() {
		assert_ne!(Value::none(), Value::none_of(ValueType::Duration));
	}

	// Value::PartialEq ends in `_ => false` and Value::Ord ends in `_ => unimplemented!()`, so a
	// missing arm for a new variant compiles clean and fails only at runtime: equal values compare
	// unequal, and any sort panics. Exact KNN sorts on distance and compares vectors, so both
	// arms are load-bearing.

	#[test]
	fn test_vector_equals_itself() {
		assert_eq!(Value::vector(vec![0.1, 0.2]), Value::vector(vec![0.1, 0.2]));
		assert_ne!(Value::vector(vec![0.1, 0.2]), Value::vector(vec![0.1, 0.3]));
	}

	#[test]
	fn test_vector_values_are_orderable() {
		let mut values = vec![
			Value::vector(vec![2.0, 0.0]),
			Value::vector(vec![1.0, 0.0]),
			Value::vector(vec![1.5, 0.0]),
		];
		values.sort();
		assert_eq!(
			values,
			vec![
				Value::vector(vec![1.0, 0.0]),
				Value::vector(vec![1.5, 0.0]),
				Value::vector(vec![2.0, 0.0])
			]
		);
	}
}
