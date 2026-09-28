// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f32::consts::PI;

use arrow_array::Float32Array;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<f32>) -> FrameColumnData {
	FrameColumnData::Float4(Float32Array::from(v))
}

crate::nones_tests! {
	values: vec![0.0f32, 1.5, -PI, 1e10, f32::MAX],
	inner_type: ValueType::Float4,
}
