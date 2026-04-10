// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(NumberContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500u64).map(|i| i * 1000).collect::<Vec<_>>(),
	descending_stride: (1..=500u64).rev().map(|i| i * 1000).collect::<Vec<_>>(),
}
