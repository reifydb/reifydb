// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{container::temporal_array::time_array, frame::data::FrameColumnData, time::Time};

fn make(v: Vec<Time>) -> FrameColumnData {
	FrameColumnData::Time(time_array(v))
}

crate::delta_rle_tests! {
	constant_stride: (1..=500u64)
		.map(|i| Time::from_nanos_since_midnight(i * 1000).unwrap())
		.collect::<Vec<_>>(),
	descending_stride: (1..=500u64)
		.rev()
		.map(|i| Time::from_nanos_since_midnight(i * 1000).unwrap())
		.collect::<Vec<_>>(),
}
