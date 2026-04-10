// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, frame::data::FrameColumnData, time::Time};

fn make(v: Vec<Time>) -> FrameColumnData {
	FrameColumnData::Time(TemporalContainer::new(v))
}

crate::rle_tests! {
	repeated: {
		let mut v = Vec::new();
		for nanos in [0u64, 10_000_000_000, 20_000_000_000, 30_000_000_000, 40_000_000_000] {
			let t = Time::from_nanos_since_midnight(nanos).unwrap();
			v.extend(std::iter::repeat(t).take(100));
		}
		v
	},
	unique: (0..100).map(|i| Time::from_nanos_since_midnight(i * 800_000_000).unwrap()).collect::<Vec<_>>(),
}
