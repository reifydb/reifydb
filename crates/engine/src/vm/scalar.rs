// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	error,
	error::diagnostic,
	fragment::Fragment,
	value::{Value, try_from::TryFromValueCoerce, r#type::Type as ValueType},
};

/// Convert a Value to the given target type (widening/promotion).
pub fn convert_to(value: Value, target: ValueType) -> crate::Result<Value> {
	use Value::*;
	if value.get_type() == target {
		return Ok(value);
	}
	match (&value, &target) {
		(Value::None { .. }, _) => Ok(Value::none_of(target)),
		// To Float8
		(_, ValueType::Float8) => {
			let f = f64::try_from_value_coerce(&value).map_err(|_| {
				error!(diagnostic::cast::unsupported_cast(
					Fragment::internal(""),
					value.get_type(),
					target.clone(),
				))
			})?;
			Ok(Value::float8(f))
		}
		// To Int2
		(Int1(v), ValueType::Int2) => Ok(Int2(*v as i16)),
		(Uint1(v), ValueType::Int2) => Ok(Int2(*v as i16)),
		// To Int4
		(Int1(v), ValueType::Int4) => Ok(Int4(*v as i32)),
		(Int2(v), ValueType::Int4) => Ok(Int4(*v as i32)),
		(Uint1(v), ValueType::Int4) => Ok(Int4(*v as i32)),
		(Uint2(v), ValueType::Int4) => Ok(Int4(*v as i32)),
		// To Int8
		(Int1(v), ValueType::Int8) => Ok(Int8(*v as i64)),
		(Int2(v), ValueType::Int8) => Ok(Int8(*v as i64)),
		(Int4(v), ValueType::Int8) => Ok(Int8(*v as i64)),
		(Uint1(v), ValueType::Int8) => Ok(Int8(*v as i64)),
		(Uint2(v), ValueType::Int8) => Ok(Int8(*v as i64)),
		(Uint4(v), ValueType::Int8) => Ok(Int8(*v as i64)),
		// To Int16
		(Int1(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Int2(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Int4(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Int8(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Uint1(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Uint2(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Uint4(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		(Uint8(v), ValueType::Int16) => Ok(Int16(*v as i128)),
		// To Uint2
		(Uint1(v), ValueType::Uint2) => Ok(Uint2(*v as u16)),
		// To Uint4
		(Uint1(v), ValueType::Uint4) => Ok(Uint4(*v as u32)),
		(Uint2(v), ValueType::Uint4) => Ok(Uint4(*v as u32)),
		// To Uint8
		(Uint1(v), ValueType::Uint8) => Ok(Uint8(*v as u64)),
		(Uint2(v), ValueType::Uint8) => Ok(Uint8(*v as u64)),
		(Uint4(v), ValueType::Uint8) => Ok(Uint8(*v as u64)),
		// To Uint16
		(Uint1(v), ValueType::Uint16) => Ok(Uint16(*v as u128)),
		(Uint2(v), ValueType::Uint16) => Ok(Uint16(*v as u128)),
		(Uint4(v), ValueType::Uint16) => Ok(Uint16(*v as u128)),
		(Uint8(v), ValueType::Uint16) => Ok(Uint16(*v as u128)),
		// To Utf8
		(_, ValueType::Utf8) => Ok(Utf8(format!("{}", value))),
		// To Boolean
		(_, ValueType::Boolean) => Ok(Boolean(value_is_truthy(&value))),
		_ => Err(error!(diagnostic::cast::unsupported_cast(Fragment::internal(""), value.get_type(), target,))),
	}
}

/// Arithmetic: add two scalar values.
pub fn scalar_add(left: Value, right: Value) -> crate::Result<Value> {
	use Value::*;
	match (&left, &right) {
		(Value::None { inner }, _) | (_, Value::None { inner }) => return Ok(Value::none_of(inner.clone())),
		// String concatenation
		(Utf8(l), Utf8(r)) => return Ok(Utf8(format!("{}{}", l, r))),
		_ => {}
	}
	let target = ValueType::promote(left.get_type(), right.get_type());
	let l = convert_to(left, target.clone())?;
	let r = convert_to(right, target)?;
	Ok(match (l, r) {
		(Int1(a), Int1(b)) => Int4(a as i32 + b as i32),
		(Int2(a), Int2(b)) => Int4(a as i32 + b as i32),
		(Int4(a), Int4(b)) => Int8(a as i64 + b as i64),
		(Int8(a), Int8(b)) => Int16(a as i128 + b as i128),
		(Int16(a), Int16(b)) => Int16(a.wrapping_add(b)),
		(Uint1(a), Uint1(b)) => Uint4(a as u32 + b as u32),
		(Uint2(a), Uint2(b)) => Uint4(a as u32 + b as u32),
		(Uint4(a), Uint4(b)) => Uint8(a as u64 + b as u64),
		(Uint8(a), Uint8(b)) => Uint16(a as u128 + b as u128),
		(Uint16(a), Uint16(b)) => Uint16(a.wrapping_add(b)),
		(Float8(a), Float8(b)) => Value::float8(a.value() + b.value()),
		(Boolean(a), Boolean(b)) => Boolean(a || b),
		(Utf8(a), Utf8(b)) => Utf8(format!("{}{}", a, b)),
		_ => Value::none(),
	})
}

/// Arithmetic: subtract two scalar values.
pub fn scalar_sub(left: Value, right: Value) -> crate::Result<Value> {
	use Value::*;
	match (&left, &right) {
		(Value::None { inner }, _) | (_, Value::None { inner }) => return Ok(Value::none_of(inner.clone())),
		_ => {}
	}
	let target = ValueType::promote(left.get_type(), right.get_type());
	let l = convert_to(left, target.clone())?;
	let r = convert_to(right, target)?;
	Ok(match (l, r) {
		(Int1(a), Int1(b)) => Int4(a as i32 - b as i32),
		(Int2(a), Int2(b)) => Int4(a as i32 - b as i32),
		(Int4(a), Int4(b)) => Int8(a as i64 - b as i64),
		(Int8(a), Int8(b)) => Int16(a as i128 - b as i128),
		(Int16(a), Int16(b)) => Int16(a.wrapping_sub(b)),
		(Uint1(a), Uint1(b)) => Int4(a as i32 - b as i32),
		(Uint2(a), Uint2(b)) => Int4(a as i32 - b as i32),
		(Uint4(a), Uint4(b)) => Int8(a as i64 - b as i64),
		(Uint8(a), Uint8(b)) => Int16(a as i128 - b as i128),
		(Uint16(a), Uint16(b)) => Int16(a as i128 - b as i128),
		(Float8(a), Float8(b)) => Value::float8(a.value() - b.value()),
		_ => Value::none(),
	})
}

/// Arithmetic: multiply two scalar values.
pub fn scalar_mul(left: Value, right: Value) -> crate::Result<Value> {
	use Value::*;
	match (&left, &right) {
		(Value::None { inner }, _) | (_, Value::None { inner }) => return Ok(Value::none_of(inner.clone())),
		_ => {}
	}
	let target = ValueType::promote(left.get_type(), right.get_type());
	let l = convert_to(left, target.clone())?;
	let r = convert_to(right, target)?;
	Ok(match (l, r) {
		(Int1(a), Int1(b)) => Int4(a as i32 * b as i32),
		(Int2(a), Int2(b)) => Int4(a as i32 * b as i32),
		(Int4(a), Int4(b)) => Int8(a as i64 * b as i64),
		(Int8(a), Int8(b)) => Int16(a as i128 * b as i128),
		(Int16(a), Int16(b)) => Int16(a.wrapping_mul(b)),
		(Uint1(a), Uint1(b)) => Uint4(a as u32 * b as u32),
		(Uint2(a), Uint2(b)) => Uint4(a as u32 * b as u32),
		(Uint4(a), Uint4(b)) => Uint8(a as u64 * b as u64),
		(Uint8(a), Uint8(b)) => Uint16(a as u128 * b as u128),
		(Uint16(a), Uint16(b)) => Uint16(a.wrapping_mul(b)),
		(Float8(a), Float8(b)) => Value::float8(a.value() * b.value()),
		(Boolean(a), Boolean(b)) => Boolean(a && b),
		_ => Value::none(),
	})
}

/// Arithmetic: divide two scalar values.
pub fn scalar_div(left: Value, right: Value) -> crate::Result<Value> {
	use Value::*;
	match (&left, &right) {
		(Value::None { inner }, _) | (_, Value::None { inner }) => return Ok(Value::none_of(inner.clone())),
		_ => {}
	}
	let lt = left.get_type();
	let rt = right.get_type();
	if lt.is_integer() && rt.is_integer() {
		let target = ValueType::promote(lt, rt);
		let l = convert_to(left, target.clone())?;
		let r = convert_to(right, target)?;
		return match (&l, &r) {
			(Int1(a), Int1(b)) if *b != 0 => Ok(Int1(a / b)),
			(Int2(a), Int2(b)) if *b != 0 => Ok(Int2(a / b)),
			(Int4(a), Int4(b)) if *b != 0 => Ok(Int4(a / b)),
			(Int8(a), Int8(b)) if *b != 0 => Ok(Int8(a / b)),
			(Int16(a), Int16(b)) if *b != 0 => Ok(Int16(a / b)),
			(Uint1(a), Uint1(b)) if *b != 0 => Ok(Uint1(a / b)),
			(Uint2(a), Uint2(b)) if *b != 0 => Ok(Uint2(a / b)),
			(Uint4(a), Uint4(b)) if *b != 0 => Ok(Uint4(a / b)),
			(Uint8(a), Uint8(b)) if *b != 0 => Ok(Uint8(a / b)),
			(Uint16(a), Uint16(b)) if *b != 0 => Ok(Uint16(a / b)),
			_ => Ok(Value::none()),
		};
	}
	let lf = f64::try_from_value_coerce(&left).unwrap_or(0.0);
	let rf = f64::try_from_value_coerce(&right).unwrap_or(0.0);
	if rf == 0.0 {
		return Ok(Value::none());
	}
	Ok(Value::float8(lf / rf))
}

/// Arithmetic: remainder of two scalar values.
pub fn scalar_rem(left: Value, right: Value) -> crate::Result<Value> {
	use Value::*;
	match (&left, &right) {
		(Value::None { inner }, _) | (_, Value::None { inner }) => return Ok(Value::none_of(inner.clone())),
		_ => {}
	}
	let target = ValueType::promote(left.get_type(), right.get_type());
	let l = convert_to(left, target.clone())?;
	let r = convert_to(right, target)?;
	Ok(match (l, r) {
		(Int1(a), Int1(b)) if b != 0 => Int1(a % b),
		(Int2(a), Int2(b)) if b != 0 => Int2(a % b),
		(Int4(a), Int4(b)) if b != 0 => Int4(a % b),
		(Int8(a), Int8(b)) if b != 0 => Int8(a % b),
		(Int16(a), Int16(b)) if b != 0 => Int16(a % b),
		(Uint1(a), Uint1(b)) if b != 0 => Uint1(a % b),
		(Uint2(a), Uint2(b)) if b != 0 => Uint2(a % b),
		(Uint4(a), Uint4(b)) if b != 0 => Uint4(a % b),
		(Uint8(a), Uint8(b)) if b != 0 => Uint8(a % b),
		(Uint16(a), Uint16(b)) if b != 0 => Uint16(a % b),
		(Float8(a), Float8(b)) if b.value() != 0.0 => Value::float8(a.value() % b.value()),
		_ => Value::none(),
	})
}

/// Unary negate.
pub fn scalar_negate(value: Value) -> crate::Result<Value> {
	use Value::*;
	Ok(match value {
		Value::None { inner } => Value::none_of(inner),
		Int1(v) => Int4(-(v as i32)),
		Int2(v) => Int4(-(v as i32)),
		Int4(v) => Int8(-(v as i64)),
		Int8(v) => Int16(-(v as i128)),
		Int16(v) => Int16(-v),
		Uint1(v) => Int4(-(v as i32)),
		Uint2(v) => Int4(-(v as i32)),
		Uint4(v) => Int8(-(v as i64)),
		Uint8(v) => Int16(-(v as i128)),
		Float4(v) => Value::float8(-(v.value() as f64)),
		Float8(v) => Value::float8(-v.value()),
		_ => {
			return Err(error!(diagnostic::cast::unsupported_cast(
				Fragment::internal(""),
				value.get_type(),
				ValueType::Float8,
			)));
		}
	})
}

/// Comparison: equality.
pub fn scalar_eq(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => {
			let lt = left.get_type();
			let rt = right.get_type();
			if lt == rt {
				return Value::Boolean(left == right);
			}
			let target = ValueType::promote(lt, rt);
			let l = convert_to(left.clone(), target.clone()).unwrap_or(Value::none());
			let r = convert_to(right.clone(), target).unwrap_or(Value::none());
			Value::Boolean(l == r)
		}
	}
}

/// Comparison: not equal.
pub fn scalar_ne(left: &Value, right: &Value) -> Value {
	match scalar_eq(left, right) {
		Value::Boolean(b) => Value::Boolean(!b),
		other => other,
	}
}

/// Comparison: less than.
pub fn scalar_lt(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => {
			let lt = left.get_type();
			let rt = right.get_type();
			if lt == rt {
				return Value::Boolean(left < right);
			}
			let target = ValueType::promote(lt, rt);
			let l = convert_to(left.clone(), target.clone()).unwrap_or(Value::none());
			let r = convert_to(right.clone(), target).unwrap_or(Value::none());
			Value::Boolean(l < r)
		}
	}
}

/// Comparison: less than or equal.
pub fn scalar_le(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => {
			let lt = left.get_type();
			let rt = right.get_type();
			if lt == rt {
				return Value::Boolean(left <= right);
			}
			let target = ValueType::promote(lt, rt);
			let l = convert_to(left.clone(), target.clone()).unwrap_or(Value::none());
			let r = convert_to(right.clone(), target).unwrap_or(Value::none());
			Value::Boolean(l <= r)
		}
	}
}

/// Comparison: greater than.
pub fn scalar_gt(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => {
			let lt = left.get_type();
			let rt = right.get_type();
			if lt == rt {
				return Value::Boolean(left > right);
			}
			let target = ValueType::promote(lt, rt);
			let l = convert_to(left.clone(), target.clone()).unwrap_or(Value::none());
			let r = convert_to(right.clone(), target).unwrap_or(Value::none());
			Value::Boolean(l > r)
		}
	}
}

/// Comparison: greater than or equal.
pub fn scalar_ge(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => {
			let lt = left.get_type();
			let rt = right.get_type();
			if lt == rt {
				return Value::Boolean(left >= right);
			}
			let target = ValueType::promote(lt, rt);
			let l = convert_to(left.clone(), target.clone()).unwrap_or(Value::none());
			let r = convert_to(right.clone(), target).unwrap_or(Value::none());
			Value::Boolean(l >= r)
		}
	}
}

/// Truthiness test (extracted from old evaluate_condition logic).
pub fn value_is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::None { .. } => false,
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		Value::Utf8(s) => !s.is_empty(),
		_ => true,
	}
}

pub fn scalar_not(value: &Value) -> Value {
	match value {
		Value::None { .. } => Value::none_of(ValueType::Boolean),
		v => Value::Boolean(!value_is_truthy(v)),
	}
}

pub fn scalar_xor(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => Value::Boolean(value_is_truthy(left) ^ value_is_truthy(right)),
	}
}

pub fn scalar_or(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => Value::Boolean(value_is_truthy(left) || value_is_truthy(right)),
	}
}

pub fn scalar_and(left: &Value, right: &Value) -> Value {
	match (left, right) {
		(Value::None { .. }, _) | (_, Value::None { .. }) => Value::none_of(ValueType::Boolean),
		_ => Value::Boolean(value_is_truthy(left) && value_is_truthy(right)),
	}
}

/// Cast a scalar value to the given target type.
pub fn scalar_cast(value: Value, target: ValueType) -> crate::Result<Value> {
	use Value::*;
	if value.get_type() == target {
		return Ok(value);
	}
	match (&value, &target) {
		(Value::None { .. }, _) => Ok(Value::none_of(target)),
		// To Boolean
		(_, ValueType::Boolean) => Ok(Boolean(value_is_truthy(&value))),
		// To Utf8
		(_, ValueType::Utf8) => Ok(Utf8(format!("{}", value))),
		// Number conversions
		(_, t) if t.is_number() => convert_to(value, target),
		_ => convert_to(value, target),
	}
}
