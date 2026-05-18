// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i64>) -> FrameColumnData {
	FrameColumnData::Int8(NumberContainer::new(v))
}

crate::delta_tests! {
	ascending: (0..500i64).collect::<Vec<_>>(),
	descending: (0..500i64).rev().collect::<Vec<_>>(),
	unsorted: (0..500).map(|i| ((i * 7 + 13) % 97) as i64).collect::<Vec<_>>(),
}
