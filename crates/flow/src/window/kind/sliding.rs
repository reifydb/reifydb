// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::marker::PhantomData;

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::{
	coord::{EventCoord, EventTime, Ordinal, OrdinalCoord, WindowDomain},
	kind::ordinal_window_span,
	span::{WindowCoord, WindowSpan},
};

pub struct SlidingKind<D: WindowDomain> {
	size: u64,
	slide: u64,
	domain: PhantomData<D>,
}

impl<D: WindowDomain> SlidingKind<D> {
	fn build(size: u64, slide: u64) -> Option<Self> {
		(size > 0 && slide > 0 && slide < size).then_some(Self {
			size,
			slide,
			domain: PhantomData,
		})
	}

	pub fn size(&self) -> u64 {
		self.size
	}

	pub fn slide(&self) -> u64 {
		self.slide
	}
}

impl SlidingKind<EventTime> {
	pub fn by_duration(size: Duration, slide: Duration) -> Option<Self> {
		Self::build(
			<DateTime as WindowCoord>::span_millis(size)?,
			<DateTime as WindowCoord>::span_millis(slide)?,
		)
	}

	pub fn span(&self, anchor: u64) -> WindowSpan<DateTime> {
		WindowSpan::new(
			<DateTime as WindowCoord>::from_order(anchor),
			<DateTime as WindowCoord>::from_order(anchor.saturating_add(self.size)),
		)
	}

	pub fn anchors(&self, coord: EventCoord) -> Vec<u64> {
		let instant = coord.at().to_order();
		let lowest = instant.saturating_sub(self.size.saturating_sub(1)) / self.slide;
		let highest = instant / self.slide;
		(lowest..=highest)
			.map(|window| window * self.slide)
			.filter(|start| instant >= *start && instant < start + self.size)
			.collect()
	}
}

impl SlidingKind<Ordinal> {
	pub fn by_count(size: u64, slide: u64) -> Option<Self> {
		Self::build(size, slide)
	}

	pub fn span(&self, anchor: u64) -> WindowSpan<DateTime> {
		ordinal_window_span(anchor)
	}

	pub fn anchors(&self, coord: OrdinalCoord) -> Vec<u64> {
		let row = coord.value() + 1;
		let lowest = if row > self.size {
			(row - self.size) / self.slide
		} else {
			0
		};
		let highest = (row - 1) / self.slide;
		(lowest..=highest)
			.filter(|window| {
				let first = window * self.slide + 1;
				row >= first && row <= first + self.size - 1
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn timed() -> SlidingKind<EventTime> {
		SlidingKind::by_duration(ms(1_000), ms(250)).expect("a 250ms slide fits inside a 1000ms window")
	}

	fn counted() -> SlidingKind<Ordinal> {
		SlidingKind::by_count(4, 2).expect("a slide of 2 fits inside a window of 4")
	}

	fn at(millis: u64) -> EventCoord {
		EventCoord::of(&DateTime::from_millis(millis))
	}

	#[test]
	fn a_zero_slide_cannot_be_constructed_at_all() {
		// Every anchor path divides by the slide. RQL rejects `slide >= size`, which
		// lets `slide: 0` through untouched - 0 >= size is false for any real window - so a zero
		// slide reaches the arithmetic and divides by zero. A panic inside an operator takes the
		// whole flow down, and it is reachable from a plain user query today.
		assert!(SlidingKind::<EventTime>::by_duration(ms(1_000), ms(0)).is_none());
		assert!(SlidingKind::<Ordinal>::by_count(4, 0).is_none());
	}

	#[test]
	fn a_slide_that_does_not_fit_inside_the_window_is_refused() {
		// A slide at or above the size makes the windows disjoint or gapped, which is a tumbling
		// window at best and a row-dropping window at worst. RQL rejects this for matched pairs;
		// refusing it here too means the shell cannot be handed one through any other route.
		assert!(SlidingKind::<EventTime>::by_duration(ms(1_000), ms(1_000)).is_none());
		assert!(SlidingKind::<Ordinal>::by_count(4, 9).is_none());
		assert!(SlidingKind::<Ordinal>::by_count(0, 0).is_none());
	}

	#[test]
	fn a_time_coordinate_lands_in_every_window_whose_span_still_covers_it() {
		// This is the whole point of a sliding window - one row contributes to several
		// overlapping windows, and missing one under-counts that window forever. With size 1000
		// and slide 250, an instant is covered by exactly four windows, and the anchors are the
		// slide multiples at or below it.
		assert_eq!(timed().anchors(at(5_000)), vec![4_250, 4_500, 4_750, 5_000]);
	}

	#[test]
	fn the_earliest_instants_do_not_produce_windows_that_start_before_zero() {
		// The low bound is a saturating subtraction because an instant inside the first
		// window would otherwise underflow to near u64::MAX and iterate a range the size of the
		// address space. The epoch is a real coordinate here - P2b leaves unstamped rows at
		// exactly DateTime::default() - so this is the common path, not an edge case.
		assert_eq!(timed().anchors(at(0)), vec![0]);
		assert_eq!(timed().anchors(at(250)), vec![0, 250]);
	}

	#[test]
	fn a_row_ordinal_lands_in_every_window_still_accepting_rows() {
		// The count domain is 1-BASED where the time domain is 0-based - window 0 holds
		// rows 1..=size, so the first row (ordinal 0) is row 1. Getting that offset wrong shifts
		// every count window by one row for the operator's whole life, and nothing downstream
		// can tell.
		assert_eq!(counted().anchors(OrdinalCoord::from_arrival_counter(0)), vec![0]);
		assert_eq!(counted().anchors(OrdinalCoord::from_arrival_counter(3)), vec![0, 1]);
		assert_eq!(counted().anchors(OrdinalCoord::from_arrival_counter(4)), vec![1, 2]);
	}

	#[test]
	fn no_coordinate_ever_lands_in_zero_windows() {
		// A row that maps to no anchor is silently dropped - it reaches no accumulator,
		// so it is absent from every aggregate with nothing logged. That is the failure mode the
		// old untyped `_ => vec![0]` fallback was papering over, so it must be impossible rather
		// than merely unlikely.
		for instant in (0..4_000).step_by(37) {
			assert!(!timed().anchors(at(instant)).is_empty(), "instant {instant} joined no window");
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
		// The span the engine keys by must agree with the anchors() filter that decided
		// membership - `instant < start + size` there, so `end == start + size` here. A span one
		// unit off would key a window under a boundary no row was ever admitted against, and the
		// seal timer armed from that boundary would close a different window.
		let span = timed().span(4_250);

		assert_eq!(span.start, DateTime::from_millis(4_250));
		assert_eq!(span.end, DateTime::from_millis(5_250));
	}

	#[test]
	fn every_window_a_coordinate_joins_really_does_contain_it() {
		// The mirror of the test above. Over-reporting is just as silent as
		// under-reporting - the row is added to a window whose span does not cover it, so that
		// window's aggregate is wrong and the retraction path will later subtract it from a
		// window it was never in.
		for instant in (0..4_000).step_by(37) {
			for start in timed().anchors(at(instant)) {
				assert!(
					instant >= start && instant < start + timed().size(),
					"instant {instant} was placed in window [{start}, {})",
					start + timed().size()
				);
			}
		}
	}
}
