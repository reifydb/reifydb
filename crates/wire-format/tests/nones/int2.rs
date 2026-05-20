// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<i16>) -> FrameColumnData {
	FrameColumnData::Int2(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-1000i16, 0, 42, 12345, i16::MAX],
	inner_type: Type::Int2,
}
