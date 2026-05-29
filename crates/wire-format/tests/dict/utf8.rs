// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::utf8::Utf8Container, frame::data::FrameColumnData};

fn make(v: Vec<String>) -> FrameColumnData {
	FrameColumnData::Utf8(Utf8Container::new(v))
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
