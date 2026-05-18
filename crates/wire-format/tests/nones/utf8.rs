// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::utf8::Utf8Container, frame::data::FrameColumnData, r#type::Type};

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
	inner_type: Type::Utf8,
}
