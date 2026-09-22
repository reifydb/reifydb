// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt64Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(UInt64Array::from(v))
}

crate::plain_tests! {
	typical: vec![0u64, 1, 1_000_000_000, u64::MAX - 1],
	boundary: vec![u64::MIN, 1, u64::MAX],
	single: 0u64,
}
