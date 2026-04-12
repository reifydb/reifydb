// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	fragment::Fragment,
	value::{
		Value::{self, *},
		r#type::Type as ValueType,
	},
};

use crate::{
	Result,
	error::CastError,
	expression::{cast::cast_column_data, context::EvalContext},
};

// Type conversion

/// Convert a Value to the given target type.
///
/// TODO(perf): This delegates to cast_column_data by wrapping the scalar Value into a
/// single-element ColumnData and extracting it back. This is wasteful and should be replaced
/// with a shared scalar-level cast using SafeConvert directly on Value primitives.
pub fn convert_to(value: Value, target: ValueType) -> Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}
	match (&value, &target) {
		(
			Value::None {
				..
			},
			_,
		) => Ok(Value::none_of(target)),
		(_, ValueType::Utf8) => Ok(Utf8(format!("{}", value))),
		(_, ValueType::Boolean) => Ok(Boolean(value_is_truthy(&value))),
		_ => {
			let from_type = value.get_type();
			let data = ColumnData::from(value);
			let ctx = EvalContext::testing();
			let result = cast_column_data(&ctx, &data, target.clone(), Fragment::internal("")).map_err(
				|_| CastError::UnsupportedCast {
					fragment: Fragment::internal(""),
					from_type,
					to_type: target,
				},
			)?;
			Ok(result.get_value(0))
		}
	}
}

// Comparison operations

/// Generic comparison with None propagation and type promotion.
fn scalar_cmp(left: &Value, right: &Value, op: fn(&Value, &Value) -> bool) -> Value {
	match (left, right) {
		(
			Value::None {
				..
			},
			_,
		)
		| (
			_,
			Value::None {
				..
			},
		) => Value::none_of(ValueType::Boolean),
		_ => {
			let lt = left.get_type();
			let rt = right.get_type();
			if lt == rt {
				return Value::Boolean(op(left, right));
			}
			let target = ValueType::promote(lt, rt);
			let l = convert_to(left.clone(), target.clone()).unwrap_or(Value::none());
			let r = convert_to(right.clone(), target).unwrap_or(Value::none());
			Value::Boolean(op(&l, &r))
		}
	}
}

pub fn scalar_eq(left: &Value, right: &Value) -> Value {
	scalar_cmp(left, right, PartialEq::eq)
}
pub fn scalar_le(left: &Value, right: &Value) -> Value {
	scalar_cmp(left, right, PartialOrd::le)
}
pub fn scalar_ge(left: &Value, right: &Value) -> Value {
	scalar_cmp(left, right, PartialOrd::ge)
}

// Truthiness and logic operations

pub fn value_is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::None {
			..
		} => false,
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		Value::Utf8(s) => !s.is_empty(),
		_ => true,
	}
}

/// Generic logic binary operation with None propagation.
fn logic_binop(left: &Value, right: &Value, op: fn(bool, bool) -> bool) -> Value {
	match (left, right) {
		(
			Value::None {
				..
			},
			_,
		)
		| (
			_,
			Value::None {
				..
			},
		) => Value::none_of(ValueType::Boolean),
		_ => Value::Boolean(op(value_is_truthy(left), value_is_truthy(right))),
	}
}

pub fn scalar_and(left: &Value, right: &Value) -> Value {
	logic_binop(left, right, |a, b| a && b)
}
pub fn scalar_or(left: &Value, right: &Value) -> Value {
	logic_binop(left, right, |a, b| a || b)
}
pub fn scalar_xor(left: &Value, right: &Value) -> Value {
	logic_binop(left, right, |a, b| a ^ b)
}

// Cast

pub fn scalar_cast(value: Value, target: ValueType) -> Result<Value> {
	if value.get_type() == target {
		return Ok(value);
	}
	match (&value, &target) {
		(
			Value::None {
				..
			},
			_,
		) => Ok(Value::none_of(target)),
		(_, ValueType::Boolean) => Ok(Boolean(value_is_truthy(&value))),
		(_, ValueType::Utf8) => Ok(Utf8(format!("{}", value))),
		_ => convert_to(value, target),
	}
}
