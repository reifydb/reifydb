// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, decimal::Decimal, frame::data::FrameColumnData};

fn make(v: Vec<Decimal>) -> FrameColumnData {
	FrameColumnData::Decimal(NumberContainer::new(v))
}

crate::dict_tests! {
	low_cardinality: {
		let mut v = Vec::new();
		for _ in 0..100 {
			v.push(Decimal::new("0.00".parse().unwrap()));
			v.push(Decimal::new("99.99".parse().unwrap()));
			v.push(Decimal::new("-123.456".parse().unwrap()));
		}
		v
	},
	high_cardinality: (0..100i64).map(|i| {
		Decimal::new(format!("{}.{}", i * 7, i % 100).parse().unwrap())
	}).collect::<Vec<_>>(),
}
