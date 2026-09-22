// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal_array::date_array, date::Date, frame::data::FrameColumnData};

fn make(v: Vec<Date>) -> FrameColumnData {
	FrameColumnData::Date(date_array(v))
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
