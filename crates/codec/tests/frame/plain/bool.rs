// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::bool::BoolContainer, frame::data::FrameColumnData};

fn make(v: Vec<bool>) -> FrameColumnData {
	FrameColumnData::Bool(BoolContainer::new(v))
}

crate::plain_tests! {
	typical: vec![true, false, true, true, false],
	boundary: vec![true, false],
	single: true,
}
