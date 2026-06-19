// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error,
	fmt::{self, Display, Formatter},
};

use crate::{
	fragment::Fragment,
	value::{
		Value,
		blob::Blob,
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
		value_type::ValueType,
	},
};

#[derive(Debug, Clone, PartialEq)]
pub enum FromValueError {
	TypeMismatch {
		expected: ValueType,
		found: ValueType,
	},

	OutOfRange {
		value: String,
		target_type: &'static str,
	},
}

impl Display for FromValueError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			FromValueError::TypeMismatch {
				expected,
				found,
			} => {
				write!(f, "type mismatch: expected {:?}, found {:?}", expected, found)
			}
			FromValueError::OutOfRange {
				value,
				target_type,
			} => {
				write!(f, "value {} out of range for type {}", value, target_type)
			}
		}
	}
}

impl error::Error for FromValueError {}

pub trait TryFromValue: Sized {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError>;

	fn from_value(value: &Value) -> Option<Self> {
		match value {
			Value::None {
				..
			} => None,
			v => Self::try_from_value(v).ok(),
		}
	}
}

pub trait TryFromValueCoerce: Sized {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError>;

	fn from_value_coerce(value: &Value) -> Option<Self> {
		match value {
			Value::None {
				..
			} => None,
			v => Self::try_from_value_coerce(v).ok(),
		}
	}
}

impl TryFromValue for Value {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		Ok(value.clone())
	}
}

impl TryFromValue for bool {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Boolean(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Boolean,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for i8 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int1(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Int1,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for i16 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int2(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Int2,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for i32 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int4(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Int4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for i64 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int8(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Int8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for i128 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int16(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Int16,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for u8 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint1(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uint1,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for u16 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint2(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uint2,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for u32 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint4(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uint4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for u64 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint8(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uint8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for u128 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint16(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uint16,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for f32 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Float4(v) => Ok(v.value()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Float4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for f64 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Float8(v) => Ok(v.value()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Float8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for String {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Utf8(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Utf8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for OrderedF32 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Float4(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Float4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for OrderedF64 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Float8(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Float8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Blob {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Blob(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Blob,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Uuid4 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uuid4(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uuid4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Uuid7 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uuid7(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uuid7,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Date {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Date(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Date,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for DateTime {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::DateTime(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::DateTime,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Time {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Time(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Time,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Duration {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Duration(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Duration,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for IdentityId {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::IdentityId(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::IdentityId,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Int {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Int,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Uint {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Uint,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Decimal {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Decimal(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Decimal,
				found: value.get_type(),
			}),
		}
	}
}

macro_rules! coerce_temporal {
	($t:ty, $variant:ident, $parse:path, $expected:expr) => {
		impl TryFromValueCoerce for $t {
			fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
				match value {
					Value::$variant(v) => Ok(*v),
					Value::Utf8(s) => $parse(Fragment::internal(s)).map_err(|_| {
						FromValueError::TypeMismatch {
							expected: $expected,
							found: ValueType::Utf8,
						}
					}),
					_ => Err(FromValueError::TypeMismatch {
						expected: $expected,
						found: value.get_type(),
					}),
				}
			}
		}
	};
}

coerce_temporal!(Date, Date, parse_date, ValueType::Date);
coerce_temporal!(DateTime, DateTime, parse_datetime, ValueType::DateTime);
coerce_temporal!(Time, Time, parse_time, ValueType::Time);
coerce_temporal!(Duration, Duration, parse_duration, ValueType::Duration);

macro_rules! coerce_int {
	($t:ty, $expected:expr, $name:literal) => {
		impl TryFromValueCoerce for $t {
			fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
				let out_of_range = |repr: String| FromValueError::OutOfRange {
					value: repr,
					target_type: $name,
				};
				match value {
					Value::Int1(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Int2(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Int4(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Int8(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Int16(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Uint1(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Uint2(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Uint4(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Uint8(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					Value::Uint16(v) => <$t>::try_from(*v).map_err(|_| out_of_range(v.to_string())),
					_ => Err(FromValueError::TypeMismatch {
						expected: $expected,
						found: value.get_type(),
					}),
				}
			}
		}
	};
}

coerce_int!(i8, ValueType::Int1, "i8");
coerce_int!(i16, ValueType::Int2, "i16");
coerce_int!(i32, ValueType::Int4, "i32");
coerce_int!(i64, ValueType::Int8, "i64");
coerce_int!(i128, ValueType::Int16, "i128");
coerce_int!(u8, ValueType::Uint1, "u8");
coerce_int!(u16, ValueType::Uint2, "u16");
coerce_int!(u32, ValueType::Uint4, "u32");
coerce_int!(u64, ValueType::Uint8, "u64");
coerce_int!(u128, ValueType::Uint16, "u128");
coerce_int!(usize, ValueType::Uint8, "usize");

impl TryFromValueCoerce for f32 {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Float4(v) => Ok(v.value()),
			Value::Float8(v) => Ok(v.value() as f32),

			Value::Int1(v) => Ok(*v as f32),
			Value::Int2(v) => Ok(*v as f32),
			Value::Int4(v) => Ok(*v as f32),
			Value::Int8(v) => Ok(*v as f32),
			Value::Int16(v) => Ok(*v as f32),
			Value::Uint1(v) => Ok(*v as f32),
			Value::Uint2(v) => Ok(*v as f32),
			Value::Uint4(v) => Ok(*v as f32),
			Value::Uint8(v) => Ok(*v as f32),
			Value::Uint16(v) => Ok(*v as f32),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Float4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValueCoerce for f64 {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Float4(v) => Ok(v.value() as f64),
			Value::Float8(v) => Ok(v.value()),

			Value::Int1(v) => Ok(*v as f64),
			Value::Int2(v) => Ok(*v as f64),
			Value::Int4(v) => Ok(*v as f64),
			Value::Int8(v) => Ok(*v as f64),
			Value::Int16(v) => Ok(*v as f64),
			Value::Uint1(v) => Ok(*v as f64),
			Value::Uint2(v) => Ok(*v as f64),
			Value::Uint4(v) => Ok(*v as f64),
			Value::Uint8(v) => Ok(*v as f64),
			Value::Uint16(v) => Ok(*v as f64),
			_ => Err(FromValueError::TypeMismatch {
				expected: ValueType::Float8,
				found: value.get_type(),
			}),
		}
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use super::*;
	use crate::value::{ordered_f32::OrderedF32, ordered_f64::OrderedF64};

	#[test]
	fn test_try_from_value_primitives() {
		// Test boolean
		assert_eq!(bool::try_from_value(&Value::Boolean(true)), Ok(true));
		assert_eq!(bool::try_from_value(&Value::Boolean(false)), Ok(false));
		assert!(bool::try_from_value(&Value::Int4(42)).is_err());

		// Test integers
		assert_eq!(i8::try_from_value(&Value::Int1(42)), Ok(42i8));
		assert_eq!(i16::try_from_value(&Value::Int2(1234)), Ok(1234i16));
		assert_eq!(i32::try_from_value(&Value::Int4(123456)), Ok(123456i32));
		assert_eq!(i64::try_from_value(&Value::Int8(1234567890)), Ok(1234567890i64));

		// Test unsigned integers
		assert_eq!(u8::try_from_value(&Value::Uint1(42)), Ok(42u8));
		assert_eq!(u16::try_from_value(&Value::Uint2(1234)), Ok(1234u16));
		assert_eq!(u32::try_from_value(&Value::Uint4(123456)), Ok(123456u32));
		assert_eq!(u64::try_from_value(&Value::Uint8(1234567890)), Ok(1234567890u64));

		// Test string
		assert_eq!(String::try_from_value(&Value::Utf8("hello".to_string())), Ok("hello".to_string()));
	}

	#[test]
	fn test_from_value_undefined() {
		// from_value should return None for Undefined
		assert_eq!(bool::from_value(&Value::none()), None);
		assert_eq!(i32::from_value(&Value::none()), None);
		assert_eq!(String::from_value(&Value::none()), None);

		// from_value should return None for type mismatch
		assert_eq!(bool::from_value(&Value::Int4(42)), None);
		assert_eq!(i32::from_value(&Value::Boolean(true)), None);
	}

	#[test]
	fn test_try_from_value_coerce_i64() {
		// Accepts every integer width whose value fits in i64, regardless of signedness
		assert_eq!(i64::try_from_value_coerce(&Value::Int1(42)), Ok(42i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Int2(1234)), Ok(1234i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Int4(123456)), Ok(123456i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Int8(1234567890)), Ok(1234567890i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Uint4(42)), Ok(42i64));

		// Rejects values that overflow i64 (range-checked) and non-integer types
		assert!(i64::try_from_value_coerce(&Value::Uint8(u64::MAX)).is_err());
		assert!(i64::try_from_value_coerce(&Value::Boolean(true)).is_err());
	}

	#[test]
	fn test_try_from_value_coerce_u64() {
		// Should accept all unsigned integer types
		assert_eq!(u64::try_from_value_coerce(&Value::Uint1(42)), Ok(42u64));
		assert_eq!(u64::try_from_value_coerce(&Value::Uint2(1234)), Ok(1234u64));
		assert_eq!(u64::try_from_value_coerce(&Value::Uint4(123456)), Ok(123456u64));
		assert_eq!(u64::try_from_value_coerce(&Value::Uint8(1234567890)), Ok(1234567890u64));

		// Should accept non-negative signed integers
		assert_eq!(u64::try_from_value_coerce(&Value::Int4(42)), Ok(42u64));

		// Should reject negative signed integers
		assert!(u64::try_from_value_coerce(&Value::Int4(-42)).is_err());
	}

	#[test]
	fn test_try_from_value_coerce_f64() {
		// Should accept float types
		let f4 = OrderedF32::try_from(3.14f32).unwrap();
		let f8 = OrderedF64::try_from(3.14159f64).unwrap();
		assert!((f64::try_from_value_coerce(&Value::Float4(f4)).unwrap() - 3.14).abs() < 0.01);
		assert!((f64::try_from_value_coerce(&Value::Float8(f8)).unwrap() - 3.14159).abs() < 0.00001);

		// Should accept integer types
		assert_eq!(f64::try_from_value_coerce(&Value::Int4(42)), Ok(42.0f64));
		assert_eq!(f64::try_from_value_coerce(&Value::Uint4(42)), Ok(42.0f64));
	}

	#[test]
	fn test_from_value_coerce_undefined() {
		// from_value_coerce should return None for Undefined
		assert_eq!(i64::from_value_coerce(&Value::none()), None);
		assert_eq!(u64::from_value_coerce(&Value::none()), None);
		assert_eq!(f64::from_value_coerce(&Value::none()), None);
	}

	#[test]
	fn test_try_from_value_coerce_temporal_parses_strings() {
		// A duration literal string coerces; sub-minute must survive intact.
		assert_eq!(
			Duration::try_from_value_coerce(&Value::Utf8("1s".to_string())),
			Ok(Duration::from_seconds(1).unwrap())
		);
		assert_eq!(
			Duration::try_from_value_coerce(&Value::Utf8("5m".to_string())),
			Ok(Duration::from_minutes(5).unwrap())
		);
		assert_eq!(
			Duration::try_from_value_coerce(&Value::Utf8("PT1M".to_string())),
			Ok(Duration::from_minutes(1).unwrap())
		);

		// The native variant passes straight through.
		let d = Duration::from_seconds(60).unwrap();
		assert_eq!(Duration::try_from_value_coerce(&Value::Duration(d)), Ok(d));

		// Date / DateTime / Time literals coerce as well.
		assert_eq!(
			Date::try_from_value_coerce(&Value::Utf8("2024-01-15".to_string())),
			Ok(Date::new(2024, 1, 15).unwrap())
		);
		assert_eq!(
			DateTime::try_from_value_coerce(&Value::Utf8("2024-01-15T10:30:00".to_string())),
			Ok(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap())
		);
		assert_eq!(
			Time::try_from_value_coerce(&Value::Utf8("10:30:00".to_string())),
			Ok(Time::new(10, 30, 0, 0).unwrap())
		);
	}

	#[test]
	fn test_try_from_value_coerce_temporal_rejects_non_literals() {
		// A bare integer must NOT be silently treated as a duration (no implied unit).
		assert!(Duration::try_from_value_coerce(&Value::Uint8(60)).is_err());
		assert!(Duration::try_from_value_coerce(&Value::Int4(1)).is_err());

		// An unparseable string and a foreign temporal variant are rejected.
		assert!(Duration::try_from_value_coerce(&Value::Utf8("notaduration".to_string())).is_err());
		assert!(Duration::try_from_value_coerce(&Value::Time(Time::new(1, 0, 0, 0).unwrap())).is_err());
	}
}
