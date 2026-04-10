// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, frame::data::FrameColumnData};

fn make(v: Vec<Date>) -> FrameColumnData {
	FrameColumnData::Date(TemporalContainer::new(v))
}

crate::delta_tests! {
	ascending: (0..200).map(|i| Date::from_days_since_epoch(18000 + i).unwrap()).collect::<Vec<_>>(),
	descending: (0..200).rev().map(|i| Date::from_days_since_epoch(18000 + i).unwrap()).collect::<Vec<_>>(),
	unsorted: {
		let mut v: Vec<i32> = (0..200).collect::<Vec<_>>();
		// Simple deterministic shuffle
		for i in 0..200 {
			let j = (i * 7 + 13) % 200;
			v.swap(i as usize, j as usize);
		}
		v.into_iter().map(|d| Date::from_days_since_epoch(18000 + d).unwrap()).collect::<Vec<_>>()
	},
}
