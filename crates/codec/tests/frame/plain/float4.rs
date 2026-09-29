// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f32::consts::PI;

use arrow_array::Float32Array;
use reifydb_value::value::value_type::ValueType;

use crate::common::{ColumnData, data};

fn make(v: Vec<f32>) -> ColumnData {
	data(ValueType::Float4, Float32Array::from(v))
}

crate::plain_tests! {
	typical: vec![0.0f32, 1.5, -PI, 1e10],
	boundary: vec![f32::MIN, -0.0, 0.0, f32::EPSILON, f32::MAX],
	single: 0.0f32,
}
