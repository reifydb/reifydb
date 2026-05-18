// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![-2_000_000i32, 0, 42, 2_000_000],
	boundary: vec![i32::MIN, -1, 0, 1, i32::MAX],
	single: 0i32,
}
