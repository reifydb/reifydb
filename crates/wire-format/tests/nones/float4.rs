// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<f32>) -> FrameColumnData {
	FrameColumnData::Float4(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0.0f32, 1.5, -3.14, 1e10, f32::MAX],
	inner_type: ValueType::Float4,
}
