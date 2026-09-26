// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::wide_int_array::wide_array, frame::data::FrameColumnData, value_type::ValueType,
};

fn make(v: Vec<i128>) -> FrameColumnData {
	FrameColumnData::Int16(wide_array(v))
}

crate::nones_tests! {
	values: vec![-1_000_000_000_000i128, 0, 42, 1_000_000_000_000, i128::MAX],
	inner_type: ValueType::Int16,
}
