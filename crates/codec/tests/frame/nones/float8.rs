// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::E;

use arrow_array::Float64Array;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<f64>) -> FrameColumnData {
	FrameColumnData::Float8(Float64Array::from(v))
}

crate::nones_tests! {
	values: vec![0.0f64, 1.5e100, -E, 42.0, f64::MAX],
	inner_type: ValueType::Float8,
}
