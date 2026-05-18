// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u32>) -> FrameColumnData {
	FrameColumnData::Uint4(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![0u32, 1, 1_000_000, u32::MAX - 1],
	boundary: vec![u32::MIN, 1, u32::MAX],
	single: 0u32,
}
