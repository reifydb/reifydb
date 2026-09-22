// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int8Array;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<i8>) -> FrameColumnData {
	FrameColumnData::Int1(Int8Array::from(v))
}

crate::nones_tests! {
	values: vec![-10i8, 0, 42, 100, i8::MIN],
	inner_type: ValueType::Int1,
}
