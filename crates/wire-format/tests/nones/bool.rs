// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::bool::BoolContainer, frame::data::FrameColumnData, value_type::ValueType};

fn make(v: Vec<bool>) -> FrameColumnData {
	FrameColumnData::Bool(BoolContainer::new(v))
}

crate::nones_tests! {
	values: vec![true, false, true, true, false],
	inner_type: ValueType::Boolean,
}
