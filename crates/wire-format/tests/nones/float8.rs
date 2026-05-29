// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<f64>) -> FrameColumnData {
	FrameColumnData::Float8(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0.0f64, 1.5e100, -2.718281828, 42.0, f64::MAX],
	inner_type: ValueType::Float8,
}
