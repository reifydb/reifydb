// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{
	ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
	LargeStringArray, RecordBatch, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{FieldRef, Schema};
use reifydb_value::value::{
	container::{
		temporal_array::{date_array, datetime_array, time_array},
		uuid_array::{uuid4_array, uuid7_array},
		wide_int_array::wide_array,
	},
	date::Date,
	datetime::DateTime,
	frame::frame::Frame,
	time::Time,
	uuid::{Uuid4, Uuid7},
	value_type::{
		ValueType,
		field::{FieldType, named},
	},
};

fn column(name: &str, ty: ValueType, array: ArrayRef) -> (FieldRef, ArrayRef) {
	named(name, FieldType::from(ty), array)
}

pub fn int8_column(name: &str, values: Vec<i64>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Int8, Arc::new(Int64Array::from(values)))
}

pub fn int4_column(name: &str, values: Vec<i32>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Int4, Arc::new(Int32Array::from(values)))
}

pub fn int2_column(name: &str, values: Vec<i16>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Int2, Arc::new(Int16Array::from(values)))
}

pub fn int1_column(name: &str, values: Vec<i8>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Int1, Arc::new(Int8Array::from(values)))
}

pub fn int16_column(name: &str, values: Vec<i128>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Int16, Arc::new(wide_array(values)))
}

pub fn uint8_column(name: &str, values: Vec<u64>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uint8, Arc::new(UInt64Array::from(values)))
}

pub fn uint4_column(name: &str, values: Vec<u32>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uint4, Arc::new(UInt32Array::from(values)))
}

pub fn uint2_column(name: &str, values: Vec<u16>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uint2, Arc::new(UInt16Array::from(values)))
}

pub fn uint1_column(name: &str, values: Vec<u8>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uint1, Arc::new(UInt8Array::from(values)))
}

pub fn uint16_column(name: &str, values: Vec<u128>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uint16, Arc::new(wide_array(values)))
}

pub fn float8_column(name: &str, values: Vec<f64>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Float8, Arc::new(Float64Array::from(values)))
}

pub fn float4_column(name: &str, values: Vec<f32>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Float4, Arc::new(Float32Array::from(values)))
}

pub fn bool_column(name: &str, values: Vec<bool>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Boolean, Arc::new(BooleanArray::from(values)))
}

pub fn utf8_column(name: &str, values: Vec<&str>) -> (FieldRef, ArrayRef) {
	column(
		name,
		ValueType::Utf8,
		Arc::new(LargeStringArray::from(values.into_iter().map(|s| s.to_string()).collect::<Vec<String>>())),
	)
}

pub fn utf8_column_owned(name: &str, values: Vec<String>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Utf8, Arc::new(LargeStringArray::from(values)))
}

pub fn date_column(name: &str, values: Vec<Date>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Date, Arc::new(date_array(values)))
}

pub fn datetime_column(name: &str, values: Vec<DateTime>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::DateTime, Arc::new(datetime_array(values)))
}

pub fn time_column(name: &str, values: Vec<Time>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Time, Arc::new(time_array(values)))
}

pub fn uuid4_column(name: &str, values: Vec<Uuid4>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uuid4, Arc::new(uuid4_array(values)))
}

pub fn uuid7_column(name: &str, values: Vec<Uuid7>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Uuid7, Arc::new(uuid7_array(values)))
}

pub fn optional_int8_column(name: &str, values: Vec<Option<i64>>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Option(Box::new(ValueType::Int8)), Arc::new(Int64Array::from(values)))
}

pub fn optional_utf8_column(name: &str, values: Vec<Option<&str>>) -> (FieldRef, ArrayRef) {
	column(name, ValueType::Option(Box::new(ValueType::Utf8)), Arc::new(LargeStringArray::from(values)))
}

pub fn frame(columns: Vec<(FieldRef, ArrayRef)>) -> Frame {
	let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
	Frame::from(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("the test columns form a batch"))
}
