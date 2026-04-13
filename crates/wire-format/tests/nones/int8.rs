// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<i64>) -> FrameColumnData {
	FrameColumnData::Int8(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![-9_000_000_000i64, 0, 42, 9_000_000_000, i64::MAX],
	inner_type: Type::Int8,
}
