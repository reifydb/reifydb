// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<i8>) -> FrameColumnData {
	FrameColumnData::Int1(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [10i8, 20, 30, 40, 50] {
			v.extend(std::iter::repeat(val).take(100));
		}
		v
	},
	unique: (0..100).map(|i| (i % 127) as i8).collect::<Vec<_>>(),
}
