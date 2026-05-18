// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, decimal::Decimal, frame::data::FrameColumnData};

fn make(v: Vec<Decimal>) -> FrameColumnData {
	FrameColumnData::Decimal(NumberContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for s in ["0.00", "99.99", "-123.456", "1000000.001", "0.000001"] {
			let d = Decimal::new(s.parse().unwrap());
			v.extend(std::iter::repeat(d).take(100));
		}
		v
	},
	unique: (0..100i64).map(|i| {
		Decimal::new(format!("{}.{}", i * 7, i % 100).parse().unwrap())
	}).collect::<Vec<_>>(),
}
