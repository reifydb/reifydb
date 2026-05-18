// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::utf8::Utf8Container, frame::data::FrameColumnData};

fn make(v: Vec<String>) -> FrameColumnData {
	FrameColumnData::Utf8(Utf8Container::new(v))
}

crate::plain_tests! {
	typical: vec!["hello".to_string(), "world".to_string(), "".to_string(), "unicode: 日本語".to_string()],
	boundary: vec!["".to_string(), "a".to_string(), "x".repeat(255)],
	single: "test".to_string(),
}
