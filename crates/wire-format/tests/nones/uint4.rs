// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<u32>) -> FrameColumnData {
	FrameColumnData::Uint4(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u32, 1, 42, 3_000_000_000, u32::MAX],
	inner_type: ValueType::Uint4,
}
