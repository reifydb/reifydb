// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::BooleanArray;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<bool>) -> FrameColumnData {
	FrameColumnData::Bool(BooleanArray::from(v))
}

crate::nones_tests! {
	values: vec![true, false, true, true, false],
	inner_type: ValueType::Boolean,
}
