// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_flow::window::{
	coord::{EventCoord, EventTime, Ordinal, OrdinalCoord},
	kind::sliding::SlidingKind,
	span::WindowCoord,
};
use reifydb_value::value::datetime::DateTime;

use super::operator::WindowOperator;

const UNRESOLVED: fn() -> Vec<u64> = || vec![0];

impl WindowOperator {
	pub fn sliding_window_anchors(&self, timestamp_or_row_index: u64) -> Vec<u64> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Duration(size),
				slide: WindowSize::Duration(slide),
				..
			} => SlidingKind::<EventTime>::by_duration(*size, *slide)
				.map(|kind| {
					kind.anchors(EventCoord::of(&<DateTime as WindowCoord>::from_order(
						timestamp_or_row_index,
					)))
				})
				.unwrap_or_else(UNRESOLVED),
			WindowKind::Sliding {
				size: WindowSize::Count(size),
				slide: WindowSize::Count(slide),
				..
			} => SlidingKind::<Ordinal>::by_count(*size, *slide)
				.map(|kind| kind.anchors(OrdinalCoord::from_arrival_counter(timestamp_or_row_index)))
				.unwrap_or_else(UNRESOLVED),
			_ => UNRESOLVED(),
		}
	}
}
