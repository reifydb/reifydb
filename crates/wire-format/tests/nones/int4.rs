// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-2_000_000i32, 0, 42, 2_000_000, i32::MAX],
	inner_type: Type::Int4,
}
