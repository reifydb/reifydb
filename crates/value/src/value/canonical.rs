// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{
	Array, Float32Array, Float64Array, RecordBatch,
	cast::AsArray,
	types::{Float32Type, Float64Type},
};
use arrow_schema::DataType;

use crate::value::{ordered_f32::OrderedF32, ordered_f64::OrderedF64};

pub fn assert_canonical_floats(batch: &RecordBatch, boundary: &str) {
	for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
		match column.data_type() {
			DataType::Float32 => check_f32(field.name(), column.as_primitive::<Float32Type>(), boundary),
			DataType::Float64 => check_f64(field.name(), column.as_primitive::<Float64Type>(), boundary),
			_ => {}
		}
	}
}

fn check_f32(name: &str, array: &Float32Array, boundary: &str) {
	for i in 0..array.len() {
		if array.is_null(i) {
			continue;
		}
		let v = array.value(i);
		assert!(
			v.to_bits() == OrderedF32::canonical(v).to_bits(),
			"{boundary}: column {name} row {i} float {v:?} is not canonical"
		);
	}
}

fn check_f64(name: &str, array: &Float64Array, boundary: &str) {
	for i in 0..array.len() {
		if array.is_null(i) {
			continue;
		}
		let v = array.value(i);
		assert!(
			v.to_bits() == OrderedF64::canonical(v).to_bits(),
			"{boundary}: column {name} row {i} float {v:?} is not canonical"
		);
	}
}
