// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	cast::{cast_column_data, convert::TargetConvert},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{Value, value_type::ValueType},
};

fn cast_any(value: Value, target: ValueType) -> Result<ColumnBuffer> {
	let column = ColumnBuffer::any(vec![value]);
	cast_column_data(
		TargetConvert {
			target: None,
		},
		&column,
		target,
		|| Fragment::internal("payload"),
	)
}

fn scalar_targets() -> Vec<ValueType> {
	vec![
		ValueType::Utf8,
		ValueType::Int4,
		ValueType::Float8,
		ValueType::DECIMAL,
		ValueType::Boolean,
		ValueType::Blob,
		ValueType::Date,
		ValueType::Duration,
		ValueType::Uuid4,
		ValueType::IdentityId,
		ValueType::Option(Box::new(ValueType::Utf8)),
	]
}

fn assert_unsupported_for_every_scalar_target(value: Value) {
	for target in scalar_targets() {
		let err = cast_any(value.clone(), target.clone())
			.expect_err(&format!("casting {value} to {target:?} must be refused"));
		assert_eq!(err.0.code, "CAST_001", "casting {value} to {target:?} must be an unsupported cast");
		assert_eq!(err.0.fragment.text(), "payload", "the cast error must point at the cast expression");
	}
}

#[test]
fn a_list_in_an_any_column_is_an_unsupported_cast_for_every_scalar_target() {
	// A list re-wraps into an Any column, so the cast must refuse instead of recursing until the stack overflows.
	assert_unsupported_for_every_scalar_target(Value::List(vec![Value::Int4(1), Value::Int4(2)]));
}

#[test]
fn a_record_in_an_any_column_is_an_unsupported_cast_for_every_scalar_target() {
	// A record re-wraps into an Any column exactly like a list, so it must never re-enter the Any cast.
	assert_unsupported_for_every_scalar_target(Value::Record(vec![("a".to_string(), Value::Int4(1))]));
}

#[test]
fn a_tuple_in_an_any_column_is_an_unsupported_cast_for_every_scalar_target() {
	// A tuple re-wraps into an Any column exactly like a list, so it must never re-enter the Any cast.
	assert_unsupported_for_every_scalar_target(Value::Tuple(vec![Value::Int4(1), Value::Utf8("a".to_string())]));
}

#[test]
fn a_type_value_in_an_any_column_is_an_unsupported_cast_for_every_scalar_target() {
	// A type value re-wraps into an Any column, so casting it must refuse instead of overflowing the stack.
	assert_unsupported_for_every_scalar_target(Value::Type(ValueType::Int4));
}

#[test]
fn a_container_value_cast_to_a_container_target_returns() {
	// Container targets also route through the Any cast, so every pair must return rather than recurse.
	let values = [
		Value::List(vec![Value::Int4(1)]),
		Value::Record(vec![("a".to_string(), Value::Int4(1))]),
		Value::Tuple(vec![Value::Int4(1)]),
		Value::Type(ValueType::Int4),
	];
	let targets = [
		ValueType::list_of(ValueType::Int4),
		ValueType::Record(vec![("a".to_string(), ValueType::Int4)]),
		ValueType::Tuple(vec![ValueType::Int4]),
	];
	for value in &values {
		for target in &targets {
			let _ = cast_any(value.clone(), target.clone());
		}
	}
}

#[test]
fn a_list_nested_in_any_is_refused_like_a_bare_list() {
	// An Any wrapper around a list must not hide the list from the refusal and restart the recursion.
	assert_unsupported_for_every_scalar_target(Value::Any(Box::new(Value::List(vec![Value::Int4(1)]))));
}

#[test]
fn a_scalar_nested_in_any_still_casts() {
	// Refusing containers must not refuse scalars that sit behind an extra Any wrapper.
	let cast = cast_any(Value::Any(Box::new(Value::Int4(7))), ValueType::Utf8).expect("an int4 casts to utf8");
	assert_eq!(cast.get_value(0), Value::Utf8("7".to_string()));
}

#[test]
fn a_none_entry_next_to_a_list_does_not_mask_the_refusal() {
	// The refusal must come from the list row even when an earlier row is none.
	let column = ColumnBuffer::any_with_bitvec(
		vec![Value::none(), Value::List(vec![Value::Int4(1)])],
		vec![false, true],
	);
	let err = cast_column_data(
		TargetConvert {
			target: None,
		},
		&column,
		ValueType::Utf8,
		|| Fragment::internal("payload"),
	)
	.expect_err("the list row must be refused");
	assert_eq!(err.0.code, "CAST_001");
}
