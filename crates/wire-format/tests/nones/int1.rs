// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<i8>) -> FrameColumnData {
	FrameColumnData::Int1(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-10i8, 0, 42, 100, i8::MIN],
	inner_type: Type::Int1,
}
