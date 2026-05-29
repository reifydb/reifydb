// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u128, 1, 1_000_000_000_000, u128::MAX - 1, u128::MAX],
	inner_type: ValueType::Uint16,
}
