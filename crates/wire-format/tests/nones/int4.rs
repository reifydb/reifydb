// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<i32>) -> FrameColumnData {
	FrameColumnData::Int4(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-2_000_000i32, 0, 42, 2_000_000, i32::MAX],
	inner_type: Type::Int4,
}
