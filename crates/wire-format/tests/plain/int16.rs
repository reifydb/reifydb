// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i128>) -> FrameColumnData {
	FrameColumnData::Int16(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![-1_000_000_000_000i128, 0, 42, 1_000_000_000_000],
	boundary: vec![i128::MIN, -1, 0, 1, i128::MAX],
	single: 0i128,
}
