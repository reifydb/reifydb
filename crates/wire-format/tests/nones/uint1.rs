// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::number::NumberContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<u8>) -> FrameColumnData {
	FrameColumnData::Uint1(NumberContainer::new(v))
}

crate::nones_tests! {
	values: vec![0u8, 1, 42, 200, u8::MAX],
	inner_type: ValueType::Uint1,
}
