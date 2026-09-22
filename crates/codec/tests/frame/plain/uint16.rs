// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::decimal_array::uint16_array, frame::data::FrameColumnData};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(uint16_array(v))
}

crate::plain_tests! {
	typical: vec![0u128, 1, 1_000_000_000_000, u128::MAX - 1],
	boundary: vec![u128::MIN, 1, u128::MAX],
	single: 0u128,
}
