// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i16>) -> FrameColumnData {
	FrameColumnData::Int2(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [100i16, 200, 300, 400, 500] {
			v.extend(std::iter::repeat(val).take(100));
		}
		v
	},
	unique: (0..100).map(|i| i as i16 * 7 + 13).collect::<Vec<_>>(),
}
