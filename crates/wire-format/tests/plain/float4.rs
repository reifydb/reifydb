// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<f32>) -> FrameColumnData {
	FrameColumnData::Float4(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![0.0f32, 1.5, -3.14, 1e10],
	boundary: vec![f32::MIN, -0.0, 0.0, f32::EPSILON, f32::MAX],
	single: 0.0f32,
}
