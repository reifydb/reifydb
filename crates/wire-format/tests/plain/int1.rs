// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i8>) -> FrameColumnData {
	FrameColumnData::Int1(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![-10i8, 0, 42, 100],
	boundary: vec![i8::MIN, -1, 0, 1, i8::MAX],
	single: 0i8,
}
