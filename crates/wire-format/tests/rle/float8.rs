// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<f64>) -> FrameColumnData {
	FrameColumnData::Float8(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [1.0f64, 2.0, 3.0, 4.0, 5.0] {
			v.extend(std::iter::repeat(val).take(100));
		}
		v
	},
	unique: (0..100).map(|i| i as f64 * 0.7 + 0.13).collect::<Vec<_>>(),
}
