// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::bool::BoolContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<bool>) -> FrameColumnData {
	FrameColumnData::Bool(BoolContainer::new(v))
}

crate::nones_tests! {
	values: vec![true, false, true, true, false],
	inner_type: Type::Boolean,
}
