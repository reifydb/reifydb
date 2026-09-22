// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::decimal_array::uint16_array, frame::data::FrameColumnData, value_type::ValueType,
};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(uint16_array(v))
}

crate::nones_tests! {
	values: vec![0u128, 1, 1_000_000_000_000, u128::MAX - 1, u128::MAX],
	inner_type: ValueType::Uint16,
}
