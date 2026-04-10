// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u16>) -> FrameColumnData {
	FrameColumnData::Uint2(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![0u16, 1, 1000, 65000],
	boundary: vec![u16::MIN, 1, u16::MAX],
	single: 0u16,
}
