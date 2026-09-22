// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_codec::json::{
	from::{frames_from_json, parse_json_value},
	to::convert_frames,
};
use reifydb_value::value::{
	Value,
	container::any::AnyContainer,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};
use serde_json::json;

fn list_int4() -> ValueType {
	ValueType::list_of(ValueType::Int4)
}

fn record_region() -> ValueType {
	ValueType::Record(vec![("name".to_string(), ValueType::Utf8), ("interval".to_string(), ValueType::Int4)])
}

#[test]
fn a_json_array_parses_as_a_list_of_the_declared_element_type() {
	let value = parse_json_value(&list_int4(), &json!(["1", "2", "3"])).unwrap();
	assert_eq!(value, Value::List(vec![Value::Int4(1), Value::Int4(2), Value::Int4(3)]));
}

#[test]
fn a_json_object_parses_as_a_record_of_the_declared_fields() {
	let value = parse_json_value(&record_region(), &json!({"name": "eu", "interval": "30"})).unwrap();
	assert_eq!(
		value,
		Value::Record(vec![
			("name".to_string(), Value::Utf8("eu".to_string())),
			("interval".to_string(), Value::Int4(30)),
		])
	);
}

#[test]
fn a_record_missing_a_declared_field_is_a_decode_error() {
	let err = parse_json_value(&record_region(), &json!({"name": "eu"})).unwrap_err().to_string();
	assert!(err.contains("interval"), "{err}");
}

#[test]
fn a_scalar_json_string_where_a_list_is_declared_is_a_decode_error() {
	let err = parse_json_value(&list_int4(), &json!("not a list")).unwrap_err().to_string();
	assert!(err.contains("List"), "{err}");
}

#[test]
fn a_list_of_records_round_trips_as_real_json_structure_not_an_escaped_string() {
	let regions = vec![
		Value::Record(vec![
			("name".to_string(), Value::Utf8("eu".to_string())),
			("interval".to_string(), Value::Int4(30)),
		]),
		Value::Record(vec![
			("name".to_string(), Value::Utf8("us".to_string())),
			("interval".to_string(), Value::Int4(60)),
		]),
	];
	let declared = ValueType::list_of(record_region());
	let column = FrameColumnData::Any(
		AnyContainer::from_vec(vec![Value::List(regions.clone())]).with_declared_type(declared),
	);
	let frame = Frame::new(vec![FrameColumn {
		name: "regions".to_string(),
		data: column,
	}]);

	let response = convert_frames(&[frame]);
	let payload = &response[0].columns[0].payload[0];
	// a list-of-records cell must be a real JSON array of objects, never an escaped JSON string
	assert_eq!(payload, &json!([{"name": "eu", "interval": "30"}, {"name": "us", "interval": "60"}]));

	let json = serde_json::to_string(&response).unwrap();
	let decoded = frames_from_json(&json).unwrap();
	assert_eq!(decoded[0].columns[0].data.get_value(0), Value::List(regions));
}

#[test]
fn a_none_list_column_still_renders_as_the_none_marker() {
	let column = FrameColumnData::Option {
		inner: Box::new(FrameColumnData::Any(
			AnyContainer::from_vec(vec![Value::List(vec![Value::Int4(1)])]).with_declared_type(list_int4()),
		)),
		bitvec: BooleanBuffer::from(vec![false]),
	};
	let frame = Frame::new(vec![FrameColumn {
		name: "maybe_regions".to_string(),
		data: column,
	}]);

	let response = convert_frames(&[frame]);
	assert_eq!(response[0].columns[0].payload[0], json!("⟪none⟫"));

	let json = serde_json::to_string(&response).unwrap();
	let decoded = frames_from_json(&json).unwrap();
	assert_eq!(decoded[0].columns[0].data.get_value(0), Value::none_of(list_int4()));
}
