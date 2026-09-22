// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt16Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<u16>) -> FrameColumnData {
	FrameColumnData::Uint2(UInt16Array::from(v))
}

crate::plain_tests! {
	typical: vec![0u16, 1, 1000, 65000],
	boundary: vec![u16::MIN, 1, u16::MAX],
	single: 0u16,
}
