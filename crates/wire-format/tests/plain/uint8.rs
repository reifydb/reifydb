// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![0u64, 1, 1_000_000_000, u64::MAX - 1],
	boundary: vec![u64::MIN, 1, u64::MAX],
	single: 0u64,
}
