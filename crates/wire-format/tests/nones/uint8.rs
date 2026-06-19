// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<u64>) -> FrameColumnData {
	FrameColumnData::Uint8(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u64, 1, 1_000_000_000, u64::MAX - 1, u64::MAX],
	inner_type: ValueType::Uint8,
}
