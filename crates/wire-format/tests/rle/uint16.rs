// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData};

fn make(v: Vec<u128>) -> FrameColumnData {
	FrameColumnData::Uint16(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [1_000_000_000_000u128, 2_000_000_000_000, 3_000_000_000_000, 4_000_000_000_000, 5_000_000_000_000] {
			v.extend(std::iter::repeat(val).take(100));
		}
		v
	},
	unique: (0..100).map(|i| i as u128 * 7 + 13).collect::<Vec<_>>(),
}
