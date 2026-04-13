// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{
	container::temporal::TemporalContainer, duration::Duration, frame::data::FrameColumnData, r#type::Type,
};

fn make(v: Vec<Duration>) -> FrameColumnData {
	FrameColumnData::Duration(TemporalContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Duration::new(0, 0, 0).unwrap(),
		Duration::new(1, 2, 3_000_000_000).unwrap(),
		Duration::new(-1, -2, -3_000_000_000).unwrap(),
		Duration::new(12, 365, 86_400_000_000_000).unwrap(),
		Duration::new(-12, -365, -86_400_000_000_000).unwrap(),
	],
	inner_type: Type::Duration,
}
