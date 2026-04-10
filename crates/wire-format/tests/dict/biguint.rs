// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_bigint::BigInt;
use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, uint::Uint};

fn make(v: Vec<Uint>) -> FrameColumnData {
	FrameColumnData::Uint(NumberContainer::new(v))
}

crate::dict_tests! {
	low_cardinality: {
		let mut v = Vec::new();
		for _ in 0..100 {
			v.push(Uint(BigInt::from(0u64)));
			v.push(Uint(BigInt::from(u64::MAX)));
			v.push(Uint(BigInt::from(42u64)));
		}
		v
	},
	high_cardinality: (0..100u64).map(|i| Uint(BigInt::from(i * 997))).collect::<Vec<_>>(),
}
