// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::number::NumberContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<u8>) -> FrameColumnData {
	FrameColumnData::Uint1(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u8, 1, 42, 200, u8::MAX],
	inner_type: Type::Uint1,
}
