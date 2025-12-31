// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use crate::{
	Blob, Date, DateTime, Decimal, Duration, IdentityId, Int, OrderedF32, OrderedF64, Time, Type, Uint, Uuid4,
	Uuid7, Value,
};

/// Error type for Value extraction failures
#[derive(Debug, Clone, PartialEq)]
pub enum FromValueError {
	/// The Value variant doesn't match the expected type
	TypeMismatch {
		expected: Type,
		found: Type,
	},
	/// Numeric value out of range for target type
	OutOfRange {
		value: String,
		target_type: &'static str,
	},
}

impl Display for FromValueError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl std::error::Error for FromValueError {}

/// Trait for strict extraction of Rust types from Value.
///
/// This is the inverse of `IntoValue`. Each implementation only accepts
/// the exact matching Value variant (e.g., `i64` only accepts `Value::Int8`).
///
/// For Undefined values or type mismatches, use `from_value()` which returns
/// `Option<Self>` for convenience.
pub trait TryFromValue: Sized {
	/// Attempt to extract a value of this type from a Value.
	///
	/// Returns an error if the Value variant doesn't match the expected type.
	/// Note: This does NOT handle Undefined - use `from_value()` for that.
	fn try_from_value(value: &Value) -> Result<Self, FromValueError>;

	/// Extract from Value, returning None for Undefined or type mismatch.
	///
	/// This is the recommended method for most use cases as it handles
	/// Undefined values gracefully.
	fn from_value(value: &Value) -> Option<Self> {
		match value {
			Value::Undefined => None,
			v => Self::try_from_value(v).ok(),
		}
	}
}

/// Trait for widening extraction of Rust types from Value.
///
/// Unlike `TryFromValue`, this allows compatible type conversions:
/// - `i64` can be extracted from `Int1`, `Int2`, `Int4`, or `Int8`
/// - `u64` can be extracted from `Uint1`, `Uint2`, `Uint4`, or `Uint8`
/// - `f64` can be extracted from `Float4`, `Float8`, or any integer type
pub trait TryFromValueCoerce: Sized {
	/// Attempt to extract with widening conversion.
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError>;

	/// Extract with coercion, returning None for Undefined or incompatible types.
	fn from_value_coerce(value: &Value) -> Option<Self> {
		match value {
			Value::Undefined => None,
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
				expected: Type::Boolean,
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
				expected: Type::Int1,
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
				expected: Type::Int2,
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
				expected: Type::Int4,
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
				expected: Type::Int8,
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
				expected: Type::Int16,
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
				expected: Type::Uint1,
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
				expected: Type::Uint2,
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
				expected: Type::Uint4,
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
				expected: Type::Uint8,
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
				expected: Type::Uint16,
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
				expected: Type::Float4,
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
				expected: Type::Float8,
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
				expected: Type::Utf8,
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
				expected: Type::Float4,
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
				expected: Type::Float8,
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
				expected: Type::Blob,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Uuid4 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uuid4(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Uuid4,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Uuid7 {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uuid7(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Uuid7,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Date {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Date(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Date,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for DateTime {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::DateTime(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::DateTime,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Time {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Time(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Time,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for Duration {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Duration(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Duration,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValue for IdentityId {
	fn try_from_value(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::IdentityId(v) => Ok(v.clone()),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::IdentityId,
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
				expected: Type::Int,
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
				expected: Type::Uint,
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
				expected: Type::Decimal,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValueCoerce for i64 {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int1(v) => Ok(*v as i64),
			Value::Int2(v) => Ok(*v as i64),
			Value::Int4(v) => Ok(*v as i64),
			Value::Int8(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Int8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValueCoerce for i128 {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Int1(v) => Ok(*v as i128),
			Value::Int2(v) => Ok(*v as i128),
			Value::Int4(v) => Ok(*v as i128),
			Value::Int8(v) => Ok(*v as i128),
			Value::Int16(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Int16,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValueCoerce for u64 {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint1(v) => Ok(*v as u64),
			Value::Uint2(v) => Ok(*v as u64),
			Value::Uint4(v) => Ok(*v as u64),
			Value::Uint8(v) => Ok(*v),
			// Also allow signed integers if non-negative
			Value::Int1(v) if *v >= 0 => Ok(*v as u64),
			Value::Int2(v) if *v >= 0 => Ok(*v as u64),
			Value::Int4(v) if *v >= 0 => Ok(*v as u64),
			Value::Int8(v) if *v >= 0 => Ok(*v as u64),
			Value::Int1(v) => Err(FromValueError::OutOfRange {
				value: v.to_string(),
				target_type: "u64",
			}),
			Value::Int2(v) => Err(FromValueError::OutOfRange {
				value: v.to_string(),
				target_type: "u64",
			}),
			Value::Int4(v) => Err(FromValueError::OutOfRange {
				value: v.to_string(),
				target_type: "u64",
			}),
			Value::Int8(v) => Err(FromValueError::OutOfRange {
				value: v.to_string(),
				target_type: "u64",
			}),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Uint8,
				found: value.get_type(),
			}),
		}
	}
}

impl TryFromValueCoerce for u128 {
	fn try_from_value_coerce(value: &Value) -> Result<Self, FromValueError> {
		match value {
			Value::Uint1(v) => Ok(*v as u128),
			Value::Uint2(v) => Ok(*v as u128),
			Value::Uint4(v) => Ok(*v as u128),
			Value::Uint8(v) => Ok(*v as u128),
			Value::Uint16(v) => Ok(*v),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Uint16,
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
			// Allow integer to float conversion (may lose precision for large values)
			Value::Int1(v) => Ok(*v as f64),
			Value::Int2(v) => Ok(*v as f64),
			Value::Int4(v) => Ok(*v as f64),
			Value::Int8(v) => Ok(*v as f64),
			Value::Uint1(v) => Ok(*v as f64),
			Value::Uint2(v) => Ok(*v as f64),
			Value::Uint4(v) => Ok(*v as f64),
			Value::Uint8(v) => Ok(*v as f64),
			_ => Err(FromValueError::TypeMismatch {
				expected: Type::Float8,
				found: value.get_type(),
			}),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

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
		assert_eq!(bool::from_value(&Value::Undefined), None);
		assert_eq!(i32::from_value(&Value::Undefined), None);
		assert_eq!(String::from_value(&Value::Undefined), None);

		// from_value should return None for type mismatch
		assert_eq!(bool::from_value(&Value::Int4(42)), None);
		assert_eq!(i32::from_value(&Value::Boolean(true)), None);
	}

	#[test]
	fn test_try_from_value_coerce_i64() {
		// Should accept all signed integer types
		assert_eq!(i64::try_from_value_coerce(&Value::Int1(42)), Ok(42i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Int2(1234)), Ok(1234i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Int4(123456)), Ok(123456i64));
		assert_eq!(i64::try_from_value_coerce(&Value::Int8(1234567890)), Ok(1234567890i64));

		// Should reject other types
		assert!(i64::try_from_value_coerce(&Value::Uint4(42)).is_err());
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
		assert_eq!(i64::from_value_coerce(&Value::Undefined), None);
		assert_eq!(u64::from_value_coerce(&Value::Undefined), None);
		assert_eq!(f64::from_value_coerce(&Value::Undefined), None);
	}
}
