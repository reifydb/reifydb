// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::UInt8Array;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<u8>) -> FrameColumnData {
	FrameColumnData::Uint1(UInt8Array::from(v))
}

crate::plain_tests! {
	typical: vec![0u8, 1, 128, 255],
	boundary: vec![u8::MIN, 1, u8::MAX],
	single: 0u8,
}
