// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_flow::window::{
	coord::{EventCoord, EventTime, Ordinal, OrdinalCoord},
	kind::{ordinal_window_span, sliding::SlidingKind, tumbling::TumblingOverRows},
	span::{WindowCoord, WindowSpan},
};
use reifydb_value::value::datetime::DateTime;

use super::operator::WindowOperator;

const UNRESOLVED: fn() -> Vec<u64> = || vec![0];

impl WindowOperator {
	fn sliding_over_time(&self) -> Option<SlidingKind<EventTime>> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Duration(size),
				slide: WindowSize::Duration(slide),
				..
			} => SlidingKind::by_duration(*size, *slide),
			_ => None,
		}
	}

	fn sliding_over_rows(&self) -> Option<SlidingKind<Ordinal>> {
		match &self.kind {
			WindowKind::Sliding {
				size: WindowSize::Count(size),
				slide: WindowSize::Count(slide),
				..
			} => SlidingKind::by_count(*size, *slide),
			_ => None,
		}
	}

	pub fn sliding_window_anchors(&self, timestamp_or_row_index: u64) -> Vec<u64> {
		if let Some(kind) = self.sliding_over_time() {
			let instant = <DateTime as WindowCoord>::from_order(timestamp_or_row_index);
			return kind.anchors(EventCoord::of(&instant));
		}
		if let Some(kind) = self.sliding_over_rows() {
			return kind.anchors(OrdinalCoord::from_arrival_counter(timestamp_or_row_index));
		}
		UNRESOLVED()
	}

	pub(super) fn sliding_window_span(&self, anchor: u64) -> WindowSpan<DateTime> {
		if self.is_count_based() {
			return ordinal_window_span(anchor);
		}
		self.sliding_over_time().map_or_else(
			|| TumblingOverRows::holding(1).span(OrdinalCoord::from_arrival_counter(anchor)),
			|kind| kind.span(anchor),
		)
	}
}
