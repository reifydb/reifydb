// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, frame::data::FrameColumnData};

fn make(v: Vec<DateTime>) -> FrameColumnData {
	FrameColumnData::DateTime(TemporalContainer::new(v))
}

crate::delta_tests! {
	ascending: {
		let base = 1_700_000_000_000_000_000u64;
		(0..200).map(|i| DateTime::from_nanos(base + i * 1_000_000_000)).collect::<Vec<_>>()
	},
	descending: {
		let base = 1_700_000_000_000_000_000u64;
		(0..200).rev().map(|i| DateTime::from_nanos(base + i * 1_000_000_000)).collect::<Vec<_>>()
	},
	unsorted: {
		let base = 1_700_000_000_000_000_000u64;
		(0..200).map(|i| DateTime::from_nanos(base + ((i * 7 + 13) % 200) * 1_000_000_000)).collect::<Vec<_>>()
	},
}
