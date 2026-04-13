// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::bool::BoolContainer, frame::data::FrameColumnData, r#type::Type};

fn make(v: Vec<bool>) -> FrameColumnData {
	FrameColumnData::Bool(BoolContainer::new(v))
}

crate::nones_tests! {
	values: vec![true, false, true, true, false],
	inner_type: Type::Boolean,
}
