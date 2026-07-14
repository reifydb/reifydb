// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::vector::VectorContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<Vec<f32>>) -> FrameColumnData {
	let dims = v.first().map(|row| row.len()).unwrap_or(4) as u32;
	let mut container = VectorContainer::with_capacity(dims, v.len());
	for row in &v {
		container.push(row);
	}
	FrameColumnData::Vector(container)
}

crate::nones_tests! {
	values: vec![
		vec![0.1, 0.2, 0.3, 0.4],
		vec![1.0, 0.0, -1.0, 0.5],
		vec![0.0, 0.0, 0.0, 0.0],
		vec![-1.5, 2.5, -3.5, 4.5],
		vec![0.25, 0.25, 0.25, 0.25],
	],
	inner_type: ValueType::Vector(4),
}
