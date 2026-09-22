// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int64Array;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<i64>) -> FrameColumnData {
	FrameColumnData::Int8(Int64Array::from(v))
}

crate::nones_tests! {
	values: vec![-9_000_000_000i64, 0, 42, 9_000_000_000, i64::MAX],
	inner_type: ValueType::Int8,
}
