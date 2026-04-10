// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, frame::data::FrameColumnData};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(TemporalContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		DateTime::from_nanos(0),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		DateTime::from_nanos(1_000_000),
	],
	boundary: vec![
		DateTime::from_nanos(0),
		DateTime::from_nanos(u64::MAX),
	],
	single: DateTime::from_nanos(0),
}
