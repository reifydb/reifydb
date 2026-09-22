// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::LargeStringArray;
use reifydb_value::value::frame::data::FrameColumnData;

fn make(v: Vec<String>) -> FrameColumnData {
	FrameColumnData::Utf8(LargeStringArray::from(v))
}

crate::dict_tests! {
	low_cardinality: {
		let mut v = Vec::new();
		for _ in 0..100 {
			v.push("active".to_string());
			v.push("inactive".to_string());
			v.push("pending".to_string());
		}
		v
	},
	high_cardinality: (0..100).map(|i| format!("unique_{}", i)).collect::<Vec<_>>(),
}
