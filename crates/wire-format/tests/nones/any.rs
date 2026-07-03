// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	Value, blob::Blob, container::any::AnyContainer, date::Date, frame::data::FrameColumnData,
	ordered_f64::OrderedF64, uuid::Uuid4, value_type::ValueType,
};

fn make(v: Vec<Value>) -> FrameColumnData {
	FrameColumnData::Any(AnyContainer::new(v.into_iter().map(Box::new).collect()))
}

crate::nones_tests! {
	values: vec![
		Value::Int4(42),
		Value::Utf8("hello".to_string()),
		Value::Boolean(true),
		Value::Float8(OrderedF64::try_from(3.14).unwrap()),
		Value::Int8(i64::MIN),
	],
	inner_type: ValueType::Any,
}

// The tests above cover column-level absence (the outer FrameColumnData::Option bitvec says a row
// isn't present). This covers the different case of an Any cell that IS present but whose payload
// is itself a None sentinel for some concrete type, e.g. an optional config value stored through
// the Any escape hatch. That value flows through encode_any_value/decode_any_value directly,
// bypassing the outer bitvec entirely.

#[test]
fn any_cell_holding_none_of_duration_round_trips() {
	crate::utils::round_trip_column(
		"c",
		FrameColumnData::Any(AnyContainer::new(vec![
			Box::new(Value::none_of(ValueType::Duration)),
			Box::new(Value::Int4(5)),
		])),
	);
}

#[test]
fn any_cell_holding_none_of_nested_option_duration_round_trips() {
	crate::utils::round_trip_column(
		"c",
		FrameColumnData::Any(AnyContainer::new(vec![Box::new(Value::none_of(ValueType::Option(Box::new(
			ValueType::Duration,
		))))])),
	);
}

#[test]
fn any_cell_holding_none_of_triple_nested_option_round_trips() {
	let inner_ty = ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Duration))));
	crate::utils::round_trip_column(
		"c",
		FrameColumnData::Any(AnyContainer::new(vec![Box::new(Value::none_of(inner_ty))])),
	);
}

#[test]
fn any_cell_holding_bare_none_round_trips() {
	// Value::none() defaults to inner: Any, distinct from Value::none_of(ValueType::Any) only in
	// how it is constructed, not in the encoded bytes.
	crate::utils::round_trip_column("c", FrameColumnData::Any(AnyContainer::new(vec![Box::new(Value::none())])));
}

#[test]
fn any_cell_holding_none_of_various_inner_types_round_trips() {
	let cases: &[ValueType] = &[
		ValueType::Boolean,
		ValueType::Int4,
		ValueType::Uint8,
		ValueType::Utf8,
		ValueType::Blob,
		ValueType::Date,
		ValueType::Uuid4,
	];

	for ty in cases {
		crate::utils::round_trip_column(
			"c",
			FrameColumnData::Any(AnyContainer::new(vec![Box::new(Value::none_of(ty.clone()))])),
		);
	}
}

#[test]
fn any_cell_mixed_concrete_and_none_of_different_types_round_trips() {
	crate::utils::round_trip_column(
		"c",
		FrameColumnData::Any(AnyContainer::new(vec![
			Box::new(Value::Int4(1)),
			Box::new(Value::none_of(ValueType::Duration)),
			Box::new(Value::Utf8("hello".to_string())),
			Box::new(Value::none_of(ValueType::Boolean)),
			Box::new(Value::Blob(Blob::new(vec![1, 2, 3]))),
			Box::new(Value::none()),
			Box::new(Value::Date(Date::from_days_since_epoch(0).unwrap())),
			Box::new(Value::Uuid4(Uuid4(uuid::Uuid::nil()))),
		])),
	);
}
