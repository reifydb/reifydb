// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<i16>) -> FrameColumnData {
	FrameColumnData::Int2(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-1000i16, 0, 42, 12345, i16::MAX],
	inner_type: ValueType::Int2,
}
