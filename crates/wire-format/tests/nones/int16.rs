// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<i128>) -> FrameColumnData {
	FrameColumnData::Int16(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-1_000_000_000_000i128, 0, 42, 1_000_000_000_000, i128::MAX],
	inner_type: Type::Int16,
}
