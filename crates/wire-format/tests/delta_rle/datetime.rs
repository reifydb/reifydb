// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, frame::data::FrameColumnData};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(TemporalContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: {
		let base = 1_700_000_000_000_000_000u64;
		(0..500).map(|i| DateTime::from_nanos(base + i * 1_000_000_000)).collect::<Vec<_>>()
	},
	descending_stride: {
		let base = 1_700_000_000_000_000_000u64;
		(0..500).rev().map(|i| DateTime::from_nanos(base + i * 1_000_000_000)).collect::<Vec<_>>()
	},
}
