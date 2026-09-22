// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt16Array;
use reifydb_value::value::{frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<u16>) -> FrameColumnData {
	FrameColumnData::Uint2(UInt16Array::from(v))
}

crate::nones_tests! {
	values: vec![0u16, 1, 42, 40_000, u16::MAX],
	inner_type: ValueType::Uint2,
}
