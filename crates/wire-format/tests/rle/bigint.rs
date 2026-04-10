// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::BigInt;
use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, int::Int};

fn make(v: Vec<Int>) -> FrameColumnData {
	FrameColumnData::Int(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [42i64, -1, 1_000_000_000_000, 0, 999] {
			v.extend(std::iter::repeat(Int(BigInt::from(val))).take(100));
		}
		v
	},
	unique: (0..100i64).map(|i| Int(BigInt::from(i * 997))).collect::<Vec<_>>(),
}
