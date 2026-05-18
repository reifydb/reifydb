// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<u32>) -> FrameColumnData {
	FrameColumnData::Uint4(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u32, 1, 42, 3_000_000_000, u32::MAX],
	inner_type: Type::Uint4,
}
