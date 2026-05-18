// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, frame::data::FrameColumnData};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(TemporalContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		let base = 1_700_000_000_000_000_000u64;
		for offset in [0u64, 1_000_000_000, 2_000_000_000, 3_000_000_000, 4_000_000_000] {
			let dt = DateTime::from_nanos(base + offset);
			v.extend(std::iter::repeat(dt).take(100));
		}
		v
	},
	unique: (0..100).map(|i| DateTime::from_nanos(1_700_000_000_000_000_000 + i * 7_000_000_000)).collect::<Vec<_>>(),
}
