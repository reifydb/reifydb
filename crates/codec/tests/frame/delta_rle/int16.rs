// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::wide_int_array::wide_array, frame::data::FrameColumnData};

fn make(v: Vec<i128>) -> FrameColumnData {
	FrameColumnData::Int16(wide_array(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500i128).collect::<Vec<_>>(),
	descending_stride: (1..=500i128).rev().collect::<Vec<_>>(),
}
