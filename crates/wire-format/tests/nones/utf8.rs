// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::utf8::Utf8Container, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<String>) -> FrameColumnData {
	FrameColumnData::Utf8(Utf8Container::new(v))
}

crate::nones_tests! {
	values: vec![
		"hello".to_string(),
		"world".to_string(),
		"".to_string(),
		"unicode: 日本語".to_string(),
		"last".to_string(),
	],
	inner_type: ValueType::Utf8,
}
