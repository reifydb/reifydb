// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal::TemporalContainer, duration::Duration, frame::data::FrameColumnData};

fn make(v: Vec<Duration>) -> FrameColumnData {
	FrameColumnData::Duration(TemporalContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		Duration::new(0, 0, 0).unwrap(),
		Duration::new(1, 2, 3_000_000_000).unwrap(),
		Duration::new(-1, -2, -3_000_000_000).unwrap(),
	],
	boundary: vec![
		Duration::new(0, 0, 0).unwrap(),
		Duration::new(12, 365, 86_400_000_000_000).unwrap(),
		Duration::new(-12, -365, -86_400_000_000_000).unwrap(),
	],
	single: Duration::new(0, 0, 0).unwrap(),
}
