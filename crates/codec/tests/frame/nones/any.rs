// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::PI;

use reifydb_value::value::{
	Value,
	blob::Blob,
	container::any_array::{any_array, any_array_optional},
	date::Date,
	ordered_f64::OrderedF64,
	uuid::Uuid4,
	value_type::ValueType,
};

use crate::common::{ColumnData, data, optional};

fn make(v: Vec<Value>) -> ColumnData {
	data(ValueType::Any, any_array(v))
}

crate::nones_tests! {
	values: vec![
		Value::Int4(42),
		Value::Utf8("hello".to_string()),
		Value::Boolean(true),
		Value::Float8(OrderedF64::try_from(PI).unwrap()),
		Value::Int8(i64::MIN),
	],
	inner_type: ValueType::Any,
}

#[test]
fn any_cell_holding_none_of_duration_round_trips() {
	// A none row next to a value must come back as a null row, never as an encoded none cell.
	crate::common::round_trip_column(
		"c",
		optional(data(ValueType::Any, any_array_optional([None, Some(Value::Int4(5))])), &[false, true]),
	);
}

#[test]
fn any_cell_mixed_concrete_and_none_of_different_types_round_trips() {
	crate::common::round_trip_column(
		"c",
		data(
			ValueType::Any,
			any_array(vec![
				Value::Int4(1),
				Value::List(vec![Value::none_of(ValueType::Duration)]),
				Value::Utf8("hello".to_string()),
				Value::List(vec![Value::none_of(ValueType::Boolean)]),
				Value::Blob(Blob::new(vec![1, 2, 3])),
				Value::List(vec![Value::none()]),
				Value::Date(Date::from_days_since_epoch(0).unwrap()),
				Value::Uuid4(Uuid4(uuid::Uuid::nil())),
			]),
		),
	);
}
