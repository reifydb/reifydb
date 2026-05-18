// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, frame::data::FrameColumnData, time::Time};

fn make(v: Vec<Time>) -> FrameColumnData {
	FrameColumnData::Time(TemporalContainer::new(v))
}

crate::delta_tests! {
	ascending: (0..500u64).map(|i| Time::from_nanos_since_midnight(i * 100_000_000).unwrap()).collect::<Vec<_>>(),
	descending: (0..500u64).rev().map(|i| Time::from_nanos_since_midnight(i * 100_000_000).unwrap()).collect::<Vec<_>>(),
	unsorted: (0..500u64).map(|i| Time::from_nanos_since_midnight(((i * 7 + 13) % 500) * 100_000_000).unwrap()).collect::<Vec<_>>(),
}
