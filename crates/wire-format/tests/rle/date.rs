// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, frame::data::FrameColumnData};

fn make(v: Vec<Date>) -> FrameColumnData {
	FrameColumnData::Date(TemporalContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for days in [0i32, 1000, 2000, 3000, 4000] {
			let d = Date::from_days_since_epoch(days).unwrap();
			v.extend(std::iter::repeat(d).take(100));
		}
		v
	},
	unique: (0..100).map(|i| Date::from_days_since_epoch(i * 7).unwrap()).collect::<Vec<_>>(),
}
