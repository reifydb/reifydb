// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::BigInt;
use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, int::Int};

fn make(v: Vec<Int>) -> FrameColumnData {
	FrameColumnData::Int(NumberContainer::new(v))
}

crate::dict_tests! {
	low_cardinality: {
		let mut v = Vec::new();
		for _ in 0..100 {
			v.push(Int(BigInt::from(42)));
			v.push(Int(BigInt::from(-1)));
			v.push(Int(BigInt::from(1_000_000_000_000i64)));
		}
		v
	},
	high_cardinality: (0..100i64).map(|i| Int(BigInt::from(i * 997))).collect::<Vec<_>>(),
}
