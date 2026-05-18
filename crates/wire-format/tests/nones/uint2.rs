// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<u16>) -> FrameColumnData {
	FrameColumnData::Uint2(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u16, 1, 42, 40_000, u16::MAX],
	inner_type: Type::Uint2,
}
