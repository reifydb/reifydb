// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub fn fits_slide<S: Ord + Default>(size: S, slide: S) -> bool {
	S::default() < slide && slide < size
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::{
		factory::coord::event_coord_at_millis,
		window::{coord::DateTimeCoord, span::WindowSpan},
	};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn starts(coord: DateTimeCoord) -> Vec<DateTimeCoord> {
		WindowSpan::covering(coord, ms(1_000), ms(250))
			.expect("a coordinate near the epoch has every window end before the maximum")
			.into_iter()
			.map(|span| span.start)
			.collect()
	}

	fn at(millis: i64) -> DateTimeCoord {
		DateTimeCoord(DateTime::from_millis(millis))
	}

	#[test]
	fn a_zero_slide_cannot_be_constructed_at_all() {
		// A zero slide reaching the window arithmetic panics the whole flow from a plain user query.
		assert!(!fits_slide(ms(1_000), ms(0)));
	}

	#[test]
	fn a_slide_that_does_not_fit_inside_the_window_is_refused() {
		// A slide at or above the size makes the windows disjoint or gapped, so rows would be dropped.
		assert!(!fits_slide(ms(1_000), ms(1_000)));
	}

	#[test]
	fn a_time_coordinate_lands_in_every_window_whose_span_still_covers_it() {
		// Missing one overlapping window under-counts it forever; size 1000 slide 250 must give exactly four.
		assert_eq!(starts(event_coord_at_millis(5_000)), vec![at(4_250), at(4_500), at(4_750), at(5_000)]);
	}

	#[test]
	fn the_earliest_instants_produce_windows_that_start_before_the_epoch() {
		// Wrapping below the epoch would iterate a range the size of the address space.
		assert_eq!(starts(event_coord_at_millis(0)), vec![at(-750), at(-500), at(-250), at(0)]);
		assert_eq!(starts(event_coord_at_millis(250)), vec![at(-500), at(-250), at(0), at(250)]);
	}

	#[test]
	fn no_coordinate_ever_lands_in_zero_windows() {
		// A row that maps to no window reaches no accumulator and vanishes with nothing logged.
		for instant in (0..4_000).step_by(37) {
			assert!(
				!starts(event_coord_at_millis(instant)).is_empty(),
				"instant {instant} joined no window"
			);
		}
	}

	#[test]
	fn a_time_window_span_covers_exactly_the_size_it_was_built_with() {
		// A span one unit off keys the window under a boundary no row was admitted against.
		let spans = WindowSpan::covering(event_coord_at_millis(4_250), ms(1_000), ms(250))
			.expect("a coordinate near the epoch has every window end before the maximum");
		let span = spans
			.iter()
			.find(|span| span.start == at(4_250))
			.expect("the window starting at the coordinate");

		assert_eq!(span.start, DateTimeCoord(DateTime::from_millis(4_250)));
		assert_eq!(span.end, DateTimeCoord(DateTime::from_millis(5_250)));
	}
}
