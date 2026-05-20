// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i16>) -> FrameColumnData {
	FrameColumnData::Int2(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![-1000i16, 0, 1000, 32000],
	boundary: vec![i16::MIN, -1, 0, 1, i16::MAX],
	single: 0i16,
}
