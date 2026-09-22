// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(Int32Array::from(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500i32).collect::<Vec<_>>(),
	descending_stride: (1..=500i32).rev().collect::<Vec<_>>(),
}
