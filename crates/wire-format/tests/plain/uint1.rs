// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u8>) -> FrameColumnData {
	FrameColumnData::Uint1(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![0u8, 1, 128, 255],
	boundary: vec![u8::MIN, 1, u8::MAX],
	single: 0u8,
}
