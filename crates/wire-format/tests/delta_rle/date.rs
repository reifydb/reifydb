// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, frame::data::FrameColumnData};

fn make(v: Vec<Date>) -> FrameColumnData {
	FrameColumnData::Date(TemporalContainer::new(v))
}

crate::delta_rle_tests! {
	constant_stride: (0..500).map(|i| Date::from_days_since_epoch(18000 + i).unwrap()).collect::<Vec<_>>(),
	descending_stride: (0..500).rev().map(|i| Date::from_days_since_epoch(18000 + i).unwrap()).collect::<Vec<_>>(),
}
