// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(NumberContainer::new(v))
}

crate::delta_tests! {
	ascending: (0..200u128).collect::<Vec<_>>(),
	descending: (0..200u128).rev().collect::<Vec<_>>(),
	unsorted: (0..200).map(|i| ((i * 7 + 13) % 97) as u128).collect::<Vec<_>>(),
}
