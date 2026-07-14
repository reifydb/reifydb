// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::vector::VectorContainer, frame::data::FrameColumnData};

fn make(v: Vec<Vec<f32>>) -> FrameColumnData {
	let dims = v.first().map(|row| row.len()).unwrap_or(4) as u32;
	let mut container = VectorContainer::with_capacity(dims, v.len());
	for row in &v {
		container.push(row);
	}
	FrameColumnData::Vector(container)
}

crate::plain_tests! {
	typical: vec![
		vec![0.1, 0.2, 0.3, 0.4],
		vec![1.0, 0.0, -1.0, 0.5],
		vec![0.25, 0.25, 0.25, 0.25],
	],
	boundary: vec![
		vec![f32::MIN, f32::MAX, f32::MIN_POSITIVE, -0.0],
		vec![0.0, 1.0, -1.0, 0.5],
	],
	single: vec![42.0, -42.0, 0.5, 0.25],
}

#[test]
fn high_dimension_round_trip() {
	let row: Vec<f32> = (0..1536).map(|i| i as f32 * 0.001).collect();
	crate::utils::round_trip_column("test", make(vec![row.clone(), row]));
}

#[test]
fn single_dimension_round_trip() {
	crate::utils::round_trip_column("test", make(vec![vec![1.0], vec![-1.0], vec![0.0]]));
}
