// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Blob, Date, DateTime, IdentityId, Interval, OrderedF32, OrderedF64,
	RowId, Time, Uuid4, Uuid7, Value, util::CowVec,
};

pub trait IntoValue {
	fn into_value(self) -> Value;
}

impl IntoValue for Value {
	fn into_value(self) -> Value {
		self
	}
}

impl IntoValue for bool {
	fn into_value(self) -> Value {
		Value::Bool(self)
	}
}

impl IntoValue for i8 {
	fn into_value(self) -> Value {
		Value::Int1(self)
	}
}

impl IntoValue for i16 {
	fn into_value(self) -> Value {
		Value::Int2(self)
	}
}

impl IntoValue for i32 {
	fn into_value(self) -> Value {
		Value::Int4(self)
	}
}

impl IntoValue for i64 {
	fn into_value(self) -> Value {
		Value::Int8(self)
	}
}

impl IntoValue for i128 {
	fn into_value(self) -> Value {
		Value::Int16(self)
	}
}

impl IntoValue for u8 {
	fn into_value(self) -> Value {
		Value::Uint1(self)
	}
}

impl IntoValue for u16 {
	fn into_value(self) -> Value {
		Value::Uint2(self)
	}
}

impl IntoValue for u32 {
	fn into_value(self) -> Value {
		Value::Uint4(self)
	}
}

impl IntoValue for u64 {
	fn into_value(self) -> Value {
		Value::Uint8(self)
	}
}

impl IntoValue for u128 {
	fn into_value(self) -> Value {
		Value::Uint16(self)
	}
}

impl IntoValue for f32 {
	fn into_value(self) -> Value {
		OrderedF32::try_from(self)
			.map(|v| Value::Float4(v))
			.unwrap_or(Value::Undefined)
	}
}

impl IntoValue for f64 {
	fn into_value(self) -> Value {
		OrderedF64::try_from(self)
			.map(|v| Value::Float8(v))
			.unwrap_or(Value::Undefined)
	}
}

impl IntoValue for String {
	fn into_value(self) -> Value {
		Value::Utf8(self)
	}
}

impl IntoValue for &str {
	fn into_value(self) -> Value {
		Value::Utf8(self.to_string())
	}
}

impl IntoValue for OrderedF32 {
	fn into_value(self) -> Value {
		Value::Float4(self)
	}
}

impl IntoValue for OrderedF64 {
	fn into_value(self) -> Value {
		Value::Float8(self)
	}
}

impl IntoValue for Blob {
	fn into_value(self) -> Value {
		Value::Blob(self)
	}
}

impl IntoValue for Uuid4 {
	fn into_value(self) -> Value {
		Value::Uuid4(self)
	}
}

impl IntoValue for Uuid7 {
	fn into_value(self) -> Value {
		Value::Uuid7(self)
	}
}

impl IntoValue for Date {
	fn into_value(self) -> Value {
		Value::Date(self)
	}
}

impl IntoValue for DateTime {
	fn into_value(self) -> Value {
		Value::DateTime(self)
	}
}

impl IntoValue for Time {
	fn into_value(self) -> Value {
		Value::Time(self)
	}
}

impl IntoValue for Interval {
	fn into_value(self) -> Value {
		Value::Interval(self)
	}
}

impl IntoValue for RowId {
	fn into_value(self) -> Value {
		Value::RowId(self)
	}
}

impl IntoValue for IdentityId {
	fn into_value(self) -> Value {
		Value::IdentityId(self)
	}
}

impl<T: IntoValue> IntoValue for Option<T> {
	fn into_value(self) -> Value {
		match self {
			Some(v) => v.into_value(),
			None => Value::Undefined,
		}
	}
}

impl IntoValue for Vec<u8> {
	fn into_value(self) -> Value {
		Value::Blob(Blob::new(self))
	}
}

impl IntoValue for &[u8] {
	fn into_value(self) -> Value {
		Value::Blob(Blob::from_slice(self))
	}
}

impl IntoValue for CowVec<u8> {
	fn into_value(self) -> Value {
		Value::Blob(Blob::new(self.to_vec()))
	}
}

impl<const N: usize> IntoValue for [u8; N] {
	fn into_value(self) -> Value {
		Value::Blob(Blob::from_slice(&self))
	}
}

impl<const N: usize> IntoValue for &[u8; N] {
	fn into_value(self) -> Value {
		Value::Blob(Blob::from_slice(self))
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
	use std::f64::consts::PI;

	use crate::{
		Blob, CowVec,
		value::{IntoValue, OrderedF32, OrderedF64, Value},
	};

	#[test]
	fn test_into_value_primitives() {
		// Test boolean
		assert_eq!(true.into_value(), Value::Bool(true));
		assert_eq!(false.into_value(), Value::Bool(false));

		// Test integers
		assert_eq!(42i8.into_value(), Value::Int1(42));
		assert_eq!(1234i16.into_value(), Value::Int2(1234));
		assert_eq!(123456i32.into_value(), Value::Int4(123456));
		assert_eq!(1234567890i64.into_value(), Value::Int8(1234567890));
		assert_eq!(
			12345678901234567890i128.into_value(),
			Value::Int16(12345678901234567890)
		);

		// Test unsigned integers
		assert_eq!(42u8.into_value(), Value::Uint1(42));
		assert_eq!(1234u16.into_value(), Value::Uint2(1234));
		assert_eq!(123456u32.into_value(), Value::Uint4(123456));
		assert_eq!(
			1234567890u64.into_value(),
			Value::Uint8(1234567890)
		);
		assert_eq!(
			12345678901234567890u128.into_value(),
			Value::Uint16(12345678901234567890)
		);

		// Test floats
		assert_eq!(
			3.14f32.into_value(),
			Value::Float4(OrderedF32::try_from(3.14f32).unwrap())
		);
		assert_eq!(
			PI.into_value(),
			Value::Float8(OrderedF64::try_from(PI).unwrap())
		);

		// Test NaN handling

		assert_eq!(f32::NAN.into_value(), Value::Undefined);
		assert_eq!(f64::NAN.into_value(), Value::Undefined);
	}

	#[test]
	fn test_into_value_strings() {
		assert_eq!(
			"hello".into_value(),
			Value::Utf8("hello".to_string())
		);
		assert_eq!(
			"world".to_string().into_value(),
			Value::Utf8("world".to_string())
		);
	}

	#[test]
	fn test_into_value_option() {
		assert_eq!(Some(42i32).into_value(), Value::Int4(42));
		assert_eq!(None::<i32>.into_value(), Value::Undefined);
		assert_eq!(
			Some("hello").into_value(),
			Value::Utf8("hello".to_string())
		);
		assert_eq!(None::<&str>.into_value(), Value::Undefined);
	}

	#[test]
	fn test_into_value_bytes() {
		// Test Vec<u8>
		let vec_bytes = vec![1u8, 2, 3, 4];
		assert_eq!(
			vec_bytes.clone().into_value(),
			Value::Blob(Blob::new(vec![1, 2, 3, 4]))
		);

		// Test &[u8]
		let slice_bytes: &[u8] = &[5, 6, 7, 8];
		assert_eq!(
			slice_bytes.into_value(),
			Value::Blob(Blob::from_slice(&[5, 6, 7, 8]))
		);

		// Test [u8; N]
		let array_bytes: [u8; 4] = [9, 10, 11, 12];
		assert_eq!(
			array_bytes.into_value(),
			Value::Blob(Blob::from_slice(&[9, 10, 11, 12]))
		);

		// Test &[u8; N]
		let array_ref: &[u8; 3] = &[13, 14, 15];
		assert_eq!(
			array_ref.into_value(),
			Value::Blob(Blob::from_slice(&[13, 14, 15]))
		);

		// Test CowVec<u8>
		let cow_vec = CowVec::new(vec![16, 17, 18]);
		assert_eq!(
			cow_vec.into_value(),
			Value::Blob(Blob::new(vec![16, 17, 18]))
		);
	}
}
