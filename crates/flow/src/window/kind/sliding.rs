// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{
	state::seal::coord::Coord,
	window::{
		coord::{EventCoord, OrdinalCoord, RowSpan},
		kind::ordinal_window_span,
		span::WindowSpan,
	},
};

fn fits(size: u64, slide: u64) -> Option<(u64, u64)> {
	(size > 0 && slide > 0 && slide < size).then_some((size, slide))
}

fn fits_span(size: Duration, slide: Duration) -> Option<(Duration, Duration)> {
	(size.is_positive() && slide.is_positive() && slide < size).then_some((size, slide))
}

pub struct SlidingOverTime {
	size: Duration,
	slide: Duration,
}

impl SlidingOverTime {
	pub fn by_duration(size: Duration, slide: Duration) -> Option<Self> {
		let (size, slide) = fits_span(size, slide)?;
		Some(Self {
			size,
			slide,
		})
	}

	pub fn span(&self, anchor: u64) -> WindowSpan<DateTime> {
		let start = <DateTime as Coord>::from_order(anchor);
		WindowSpan::new(start, start.saturating_add(self.size))
	}

	pub fn anchors(&self, coord: EventCoord) -> Vec<u64> {
		let instant = coord.at();
		let mut start = instant.saturating_sub(self.size).floor_to(self.slide);
		let mut anchors = Vec::new();
		while start <= instant {
			if instant < start.saturating_add(self.size) {
				anchors.push(start.to_order());
			}
			start = start.saturating_add(self.slide);
		}
		anchors
	}
}

pub struct SlidingOverRows {
	size: RowSpan,
	slide: RowSpan,
}

impl SlidingOverRows {
	pub fn by_count(size: RowSpan, slide: RowSpan) -> Option<Self> {
		let (size, slide) = fits(size.rows(), slide.rows())?;
		Some(Self {
			size: RowSpan::of(size),
			slide: RowSpan::of(slide),
		})
	}

	pub fn span(&self, anchor: u64) -> WindowSpan<DateTime> {
		ordinal_window_span(anchor)
	}

	pub fn anchors(&self, coord: OrdinalCoord) -> Vec<u64> {
		let row = coord.value() + 1;
		let size = self.size.rows();
		let slide = self.slide.rows();
		let lowest = if row > size {
			(row - size) / slide
		} else {
			0
		};
		let highest = (row - 1) / slide;
		(lowest..=highest)
			.filter(|window| {
				let first = window * slide + 1;
				row >= first && row < first + size
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::factory::coord::event_coord_at_millis;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn order(millis: u64) -> u64 {
		DateTime::from_millis(millis).to_order()
	}

	fn timed() -> SlidingOverTime {
		SlidingOverTime::by_duration(ms(1_000), ms(250)).expect("a 250ms slide fits inside a 1000ms window")
	}

	fn counted() -> SlidingOverRows {
		SlidingOverRows::by_count(RowSpan::of(4), RowSpan::of(2))
			.expect("a slide of 2 fits inside a window of 4")
	}

	#[test]
	fn a_zero_slide_cannot_be_constructed_at_all() {
		// Every anchor path divides by the slide. RQL rejects `slide >= size`, which lets `slide: 0`
		// through, so a zero slide reaches the arithmetic and panics the whole flow from a plain
		// user query.
		assert!(SlidingOverTime::by_duration(ms(1_000), ms(0)).is_none());
		assert!(SlidingOverRows::by_count(RowSpan::of(4), RowSpan::of(0)).is_none());
	}

	#[test]
	fn a_slide_that_does_not_fit_inside_the_window_is_refused() {
		// A slide at or above the size makes the windows disjoint or gapped - tumbling at best,
		// row-dropping at worst. RQL rejects matched pairs; refusing here closes every other route.
		assert!(SlidingOverTime::by_duration(ms(1_000), ms(1_000)).is_none());
		assert!(SlidingOverRows::by_count(RowSpan::of(4), RowSpan::of(9)).is_none());
		assert!(SlidingOverRows::by_count(RowSpan::of(0), RowSpan::of(0)).is_none());
	}

	#[test]
	fn a_time_coordinate_lands_in_every_window_whose_span_still_covers_it() {
		// One row contributes to several overlapping windows, and missing one under-counts that
		// window forever. Size 1000 with slide 250 covers an instant with exactly four windows.
		assert_eq!(
			timed().anchors(event_coord_at_millis(5_000)),
			vec![order(4_250), order(4_500), order(4_750), order(5_000)]
		);
	}

	#[test]
	fn the_earliest_instants_do_not_produce_windows_that_start_before_zero() {
		// The low bound saturates because an instant inside the first window would otherwise
		// underflow to near u64::MAX and iterate a range the size of the address space. The epoch is
		// a real coordinate here: unstamped rows sit at exactly DateTime::default().
		assert_eq!(timed().anchors(event_coord_at_millis(0)), vec![order(0)]);
		assert_eq!(timed().anchors(event_coord_at_millis(250)), vec![order(0), order(250)]);
	}

	#[test]
	fn a_row_ordinal_lands_in_every_window_still_accepting_rows() {
		// The count domain is 1-based where the time domain is 0-based: window 0 holds rows 1..=size,
		// so ordinal 0 is row 1. That offset shifts every count window by one row for the operator's
		// whole life, and nothing downstream can tell.
		assert_eq!(counted().anchors(OrdinalCoord::from_arrival_counter(0)), vec![0]);
		assert_eq!(counted().anchors(OrdinalCoord::from_arrival_counter(3)), vec![0, 1]);
		assert_eq!(counted().anchors(OrdinalCoord::from_arrival_counter(4)), vec![1, 2]);
	}

	#[test]
	fn no_coordinate_ever_lands_in_zero_windows() {
		// A row that maps to no anchor is silently dropped - it reaches no accumulator and is absent
		// from every aggregate with nothing logged.
		for instant in (0..4_000).step_by(37) {
			assert!(
				!timed().anchors(event_coord_at_millis(instant)).is_empty(),
				"instant {instant} joined no window"
			);
		}
		for ordinal in 0..500 {
			assert!(
				!counted().anchors(OrdinalCoord::from_arrival_counter(ordinal)).is_empty(),
				"ordinal {ordinal} joined no window"
			);
		}
	}

	#[test]
	fn a_time_window_span_covers_exactly_the_size_it_was_built_with() {
		// The span the engine keys by must agree with the anchors() filter that decided membership
		// (`instant < start + size`). A span one unit off keys the window under a boundary no row
		// was admitted against, and the seal timer armed from it closes a different window.
		let span = timed().span(order(4_250));

		assert_eq!(span.start, DateTime::from_millis(4_250));
		assert_eq!(span.end, DateTime::from_millis(5_250));
	}
}
