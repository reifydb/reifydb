// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_type::value::{container::bool::BoolContainer, frame::data::FrameColumnData};

fn make(v: Vec<bool>) -> FrameColumnData {
	FrameColumnData::Bool(BoolContainer::new(v))
}

crate::plain_tests! {
	typical: vec![true, false, true, true, false],
	boundary: vec![true, false],
	single: true,
}
