// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal::TemporalContainer, frame::data::FrameColumnData, time::Time};

fn make(v: Vec<Time>) -> FrameColumnData {
	FrameColumnData::Time(TemporalContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		Time::from_nanos_since_midnight(0).unwrap(),
		Time::from_nanos_since_midnight(43_200_000_000_000).unwrap(),
	],
	boundary: vec![
		Time::from_nanos_since_midnight(0).unwrap(),
		Time::from_nanos_since_midnight(86_399_999_999_999).unwrap(),
	],
	single: Time::from_nanos_since_midnight(0).unwrap(),
}
