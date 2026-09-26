// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use reifydb_value::value::{
	container::temporal_array::datetime_array, datetime::DateTime, frame::data::FrameColumnData,
};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(datetime_array(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		let base = 1_700_000_000_000_000_000i64;
		for offset in [0i64, 1_000_000_000, 2_000_000_000, 3_000_000_000, 4_000_000_000] {
			let dt = DateTime::from_nanos(base + offset);
			v.extend(repeat_n(dt, 100));
		}
		v
	},
	unique: (0..100).map(|i| DateTime::from_nanos(1_700_000_000_000_000_000 + i * 7_000_000_000)).collect::<Vec<_>>(),
}
