// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::utf8::Utf8Container, frame::data::FrameColumnData};

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
