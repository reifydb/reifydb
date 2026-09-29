// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, make_array};
use arrow_buffer::NullBuffer;
use arrow_schema::Schema;
use reifydb_codec::json::{
	from::{frames_from_json, parse_json_value},
	to::convert_frames,
};
use reifydb_value::value::{
	Value,
	column_view::ColumnView,
	container::any_array::any_array,
	frame::frame::Frame,
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};
use serde_json::json;

fn list_int4() -> ValueType {
	ValueType::list_of(ValueType::Int4)
}

fn record_region() -> ValueType {
	ValueType::Record(vec![("name".to_string(), ValueType::Utf8), ("interval".to_string(), ValueType::Int4)])
}

fn declared(value_type: ValueType, declared_type: ValueType) -> FieldType {
	FieldType {
		value_type: Some(value_type),
		declared_type: Some(declared_type),
		..FieldType::default()
	}
}

fn frame(name: &str, field_type: FieldType, array: ArrayRef) -> Frame {
	let (field, array) = named(name, field_type, array);
	Frame::from(RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap())
}

fn first_value(frame: &Frame) -> Value {
	ColumnView::try_from((frame.batch.column(0), frame.batch.schema_ref().field(0))).unwrap().get_value(0)
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
	let declared_type = ValueType::list_of(record_region());
	let frame = frame(
		"regions",
		declared(declared_type.clone(), declared_type),
		Arc::new(any_array(vec![Value::List(regions.clone())])),
	);

	let response = convert_frames(&[frame]).unwrap();
	let payload = &response[0].columns[0].payload[0];
	// a list-of-records cell must be a real JSON array of objects, never an escaped JSON string
	assert_eq!(payload, &json!([{"name": "eu", "interval": "30"}, {"name": "us", "interval": "60"}]));

	let json = serde_json::to_string(&response).unwrap();
	let decoded = frames_from_json(&json).unwrap();
	assert_eq!(first_value(&decoded[0]), Value::List(regions));
}

#[test]
fn a_none_list_column_still_renders_as_the_none_marker() {
	let masked = any_array(vec![Value::List(vec![Value::Int4(1)])])
		.into_data()
		.into_builder()
		.nulls(Some(NullBuffer::from(vec![false])))
		.build()
		.unwrap();
	let frame = frame(
		"maybe_regions",
		declared(ValueType::Option(Box::new(list_int4())), list_int4()),
		make_array(masked),
	);

	let response = convert_frames(&[frame]).unwrap();
	assert_eq!(response[0].columns[0].payload[0], json!("⟪none⟫"));

	let json = serde_json::to_string(&response).unwrap();
	let decoded = frames_from_json(&json).unwrap();
	assert_eq!(first_value(&decoded[0]), Value::none_of(list_int4()));
}
