// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(v))
}

crate::delta_tests! {
	ascending: (0..200i32).collect::<Vec<_>>(),
	descending: (0..200i32).rev().collect::<Vec<_>>(),
	unsorted: (0..200).map(|i| ((i * 7 + 13) % 97) as i32).collect::<Vec<_>>(),
}
