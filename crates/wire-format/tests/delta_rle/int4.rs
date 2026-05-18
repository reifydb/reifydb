// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500i32).collect::<Vec<_>>(),
	descending_stride: (1..=500i32).rev().collect::<Vec<_>>(),
}
