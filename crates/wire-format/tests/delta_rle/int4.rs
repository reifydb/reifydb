// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500i32).collect::<Vec<_>>(),
	descending_stride: (1..=500i32).rev().collect::<Vec<_>>(),
}
