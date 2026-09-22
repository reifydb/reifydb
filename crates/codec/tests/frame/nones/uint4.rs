// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt32Array;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<u32>) -> FrameColumnData {
	FrameColumnData::Uint4(UInt32Array::from(v))
}

crate::nones_tests! {
	values: vec![0u32, 1, 42, 3_000_000_000, u32::MAX],
	inner_type: ValueType::Uint4,
}
