// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, frame::data::FrameColumnData, time::Time};

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
