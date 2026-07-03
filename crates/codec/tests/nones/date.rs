// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::temporal::TemporalContainer, date::Date, frame::data::FrameColumnData, value_type::ValueType,
};

fn make(v: Vec<Date>) -> FrameColumnData {
	FrameColumnData::Date(TemporalContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		Date::from_days_since_epoch(0).unwrap(),
		Date::from_days_since_epoch(18000).unwrap(),
		Date::from_days_since_epoch(-1000).unwrap(),
		Date::from_days_since_epoch(100_000).unwrap(),
		Date::from_days_since_epoch(-100_000).unwrap(),
	],
	inner_type: ValueType::Date,
}
