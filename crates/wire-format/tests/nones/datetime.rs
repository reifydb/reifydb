// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::temporal::TemporalContainer, datetime::DateTime, frame::data::FrameColumnData, value_type::ValueType,
};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(TemporalContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		DateTime::from_nanos(0),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		DateTime::from_nanos(1_000_000),
		DateTime::from_nanos(u64::MAX / 2),
		DateTime::from_nanos(u64::MAX),
	],
	inner_type: ValueType::DateTime,
}
