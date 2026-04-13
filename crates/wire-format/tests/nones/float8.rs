// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<f64>) -> FrameColumnData {
	FrameColumnData::Float8(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0.0f64, 1.5e100, -2.718281828, 42.0, f64::MAX],
	inner_type: Type::Float8,
}
