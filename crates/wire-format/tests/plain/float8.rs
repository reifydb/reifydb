// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<f64>) -> FrameColumnData {
	FrameColumnData::Float8(NumberContainer::new(v))
}

crate::plain_tests! {
	typical: vec![0.0f64, 1.5e100, -2.718281828, 42.0],
	boundary: vec![f64::MIN, -0.0, 0.0, f64::EPSILON, f64::MAX],
	single: 0.0f64,
}
