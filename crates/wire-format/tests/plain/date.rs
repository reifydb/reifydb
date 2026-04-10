// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, frame::data::FrameColumnData};

fn make(v: Vec<Date>) -> FrameColumnData {
	FrameColumnData::Date(TemporalContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		Date::from_days_since_epoch(0).unwrap(),
		Date::from_days_since_epoch(18000).unwrap(),
		Date::from_days_since_epoch(-1000).unwrap(),
	],
	boundary: vec![
		Date::from_days_since_epoch(-100_000).unwrap(),
		Date::from_days_since_epoch(0).unwrap(),
		Date::from_days_since_epoch(100_000).unwrap(),
	],
	single: Date::from_days_since_epoch(0).unwrap(),
}
