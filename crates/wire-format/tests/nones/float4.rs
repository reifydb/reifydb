// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<f32>) -> FrameColumnData {
	FrameColumnData::Float4(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0.0f32, 1.5, -3.14, 1e10, f32::MAX],
	inner_type: Type::Float4,
}
