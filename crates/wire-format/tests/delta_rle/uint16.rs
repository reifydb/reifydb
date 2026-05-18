// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(NumberContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500u128).collect::<Vec<_>>(),
	descending_stride: (1..=500u128).rev().collect::<Vec<_>>(),
}
