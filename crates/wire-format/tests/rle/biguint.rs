// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::BigInt;
use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, uint::Uint};

fn make(v: Vec<Uint>) -> FrameColumnData {
	FrameColumnData::Uint(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for val in [0u64, 42, 1_000_000_000_000, u64::MAX, 999] {
			v.extend(std::iter::repeat(Uint(BigInt::from(val))).take(100));
		}
		v
	},
	unique: (0..100u64).map(|i| Uint(BigInt::from(i * 997))).collect::<Vec<_>>(),
}
