// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::{key::encoded::EncodedKey, row::operator::state::StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::value::datetime::DateTime;

use crate::{
	operator::state::seal::coord::{Coord, IsZero},
	window::coord::{DateTimeCoord, U8Coord},
};

mod sealed {
	pub trait Sealed {}
}

impl sealed::Sealed for DateTimeCoord {}

impl sealed::Sealed for U8Coord {}

pub trait WindowAnchor: sealed::Sealed + Slot<Coord = Self> + Coord {
	fn as_time(self) -> Option<DateTime>;
}

impl WindowAnchor for DateTimeCoord {
	fn as_time(self) -> Option<DateTime> {
		Some(self.0)
	}
}

impl WindowAnchor for U8Coord {
	fn as_time(self) -> Option<DateTime> {
		None
	}
}

pub type SlotSpan<S> = <<S as Slot>::Coord as Coord>::Span;

pub trait Slot: Copy + Ord + Debug + StateCodec {
	type Coord: Coord;

	fn order_key(&self) -> Self::Coord;

	fn from_order_key(coord: Self::Coord) -> Self;
}

impl Slot for DateTimeCoord {
	type Coord = DateTimeCoord;

	fn order_key(&self) -> DateTimeCoord {
		*self
	}

	fn from_order_key(coord: DateTimeCoord) -> Self {
		coord
	}
}

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WindowSpan<T> {
	pub start: T,
	pub end: T,
}

impl<T: HeapSize> HeapSize for WindowSpan<T> {
	fn heap_size(&self) -> usize {
		self.start.heap_size() + self.end.heap_size()
	}
}

impl<C> WindowSpan<C>
where
	C: Coord,
{
	#[inline]
	pub fn for_coord(coord: C, span: C::Span) -> Self {
		assert!(!span.is_zero(), "WindowSpan::for_coord: span must be > 0");
		let start = coord.floor_to(span);
		Self {
			start,
			end: start.add_span(span),
		}
	}

	pub fn covering(coord: C, size: C::Span, slide: C::Span) -> Vec<Self> {
		assert!(!size.is_zero(), "WindowSpan::covering: size must be > 0");
		assert!(!slide.is_zero(), "WindowSpan::covering: slide must be > 0");
		assert!(slide < size, "WindowSpan::covering: slide must be below size");
		let mut spans = Vec::new();
		let mut start = coord.saturating_sub_span(size).floor_to(slide);
		while start <= coord {
			let end = start.add_span(size);
			if coord < end {
				spans.push(Self {
					start,
					end,
				});
			}
			start = start.add_span(slide);
		}
		spans
	}

	#[inline]
	pub fn new(start: C, end: C) -> Self {
		assert!(start < end, "WindowSpan::new: start ({start:?}) must be < end ({end:?})");
		Self {
			start,
			end,
		}
	}

	#[inline]
	pub fn duration(&self) -> C::Span {
		self.end.span_since(self.start)
	}

	#[inline]
	pub fn contains(&self, coord: C) -> bool {
		coord >= self.start && coord < self.end
	}

	#[inline]
	pub fn next(&self) -> Self {
		let span = self.duration();
		Self {
			start: self.end,
			end: self.end.add_span(span),
		}
	}
}

pub fn window_row_key<C: Coord>(group: EncodedKey, start: C) -> EncodedKey {
	start.extend_key(EncodedKey::builder().raw(group.as_slice())).build()
}

#[cfg(test)]
mod tests {
	use reifydb_value::{factory::time::millis, value::duration::Duration};

	use super::*;
	use crate::{
		factory::coord::event_coord_at_millis,
		operator::state::seal::rule::{is_sealed, seal_horizon},
		window::coord::RowSpan,
	};

	#[test]
	fn for_coord_aligns_datetime_to_span() {
		let coord = DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap());
		let one_second = Duration::from_seconds(1).unwrap();
		let one_minute = Duration::from_seconds(60).unwrap();

		// A sub-minute (1s) window must stay 1s, not round up to a minute.
		let sec = WindowSpan::for_coord(coord, one_second);
		assert_eq!(sec.start, DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap()));
		assert_eq!(sec.end, DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 26).unwrap()));
		assert_eq!(sec.duration(), one_second);

		// A 1m window aligns the coord down to the minute boundary.
		let min = WindowSpan::for_coord(coord, one_minute);
		assert_eq!(min.start, DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap()));
		assert_eq!(min.end, DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap()));
		assert!(min.contains(coord));
		assert!(!min.contains(min.end));
	}

	#[test]
	fn contains_is_half_open() {
		let span = WindowSpan::new(event_coord_at_millis(100), event_coord_at_millis(200));
		assert!(span.contains(event_coord_at_millis(100)));
		assert!(span.contains(event_coord_at_millis(199)));
		assert!(!span.contains(event_coord_at_millis(200)));
		assert!(!span.contains(event_coord_at_millis(99)));
	}

	#[test]
	fn boundary_coord_belongs_to_next_window() {
		// An event at exactly window_end must not be claimed by the current window.
		let cur = WindowSpan::for_coord(event_coord_at_millis(60), millis(60));
		let nxt = cur.next();
		assert!(!cur.contains(event_coord_at_millis(120)));
		assert!(nxt.contains(event_coord_at_millis(120)));
		assert_eq!(nxt, WindowSpan::new(event_coord_at_millis(120), event_coord_at_millis(180)));
	}

	#[test]
	#[should_panic(expected = "span must be > 0")]
	fn zero_duration_panics() {
		WindowSpan::for_coord(event_coord_at_millis(10), Duration::zero());
	}

	#[test]
	#[should_panic(expected = "must be <")]
	fn empty_span_panics() {
		WindowSpan::new(event_coord_at_millis(100), event_coord_at_millis(100));
	}

	#[test]
	fn a_time_coordinate_can_only_have_a_duration_subtracted_from_it() {
		// A seal horizon is watermark - lateness; with both sides a bare u64 nothing stopped a
		// millisecond span reaching a nanosecond coordinate, yielding a horizon a million times too
		// small. Pairing a coordinate with its own Span makes the wrong subtraction fail to compile.
		let watermark = DateTimeCoord(DateTime::from_epoch_millis(6_060_000).expect("representable instant"));
		let one_minute = Duration::from_seconds(60).expect("representable span");

		assert_eq!(
			watermark.saturating_sub_span(one_minute),
			DateTimeCoord(DateTime::from_epoch_millis(6_000_000).expect("representable")),
			"a minute behind the watermark is a minute, not a million times less"
		);
	}

	#[test]
	fn a_coordinate_survives_the_round_trip_through_its_storage_encoding() {
		// to_order/from_order are the persisted expiry-index encoding. They must be exact inverses:
		// a lossy round trip would move a window's anchor and either seal it early or strand it.
		let coord = DateTimeCoord(DateTime::from_epoch_millis(1_234_567).expect("representable"));
		assert_eq!(<DateTimeCoord as Coord>::from_order(coord.to_order()), coord);
	}

	#[test]
	fn a_seal_horizon_leaves_the_window_exactly_at_the_boundary_admissible() {
		// The boundary is load-bearing in both directions. A window whose start sits exactly one lateness
		// span behind the watermark is still reachable by a late event, so sealing it would discard a
		// legitimate retraction; sealing nothing would let state grow without bound.
		let watermark = DateTimeCoord(DateTime::from_epoch_millis(6_060_000).expect("representable"));
		let horizon = seal_horizon(watermark, Duration::from_seconds(60).expect("representable"));

		let at_boundary = DateTimeCoord(DateTime::from_epoch_millis(6_000_000).expect("representable"));
		let before_boundary = DateTimeCoord(DateTime::from_epoch_millis(5_999_999).expect("representable"));

		assert!(!is_sealed(at_boundary, horizon), "the boundary window is still live");
		assert!(is_sealed(before_boundary, horizon), "anything older is sealed");
	}

	#[test]
	fn window_row_key_puts_the_group_before_the_coordinate() {
		// A coordinate ahead of the group would interleave every group's windows in one key range.
		let start = DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap());
		let group = EncodedKey::builder().u32(3u32).u32(9u32).build();

		let expected = EncodedKey::builder().u32(3u32).u32(9u32).datetime(&start.0).build();
		assert_eq!(window_row_key(group.clone(), start), expected);

		assert!(window_row_key(group.clone(), start).as_slice().starts_with(group.as_slice()));
	}

	#[test]
	fn covering_returns_every_window_that_holds_the_coord() {
		// stepping by the size instead of the slide would skip the overlapping windows
		let spans = WindowSpan::covering(event_coord_at_millis(5_000), millis(1_000), millis(250));

		assert_eq!(
			spans,
			vec![
				WindowSpan::new(event_coord_at_millis(4_250), event_coord_at_millis(5_250)),
				WindowSpan::new(event_coord_at_millis(4_500), event_coord_at_millis(5_500)),
				WindowSpan::new(event_coord_at_millis(4_750), event_coord_at_millis(5_750)),
				WindowSpan::new(event_coord_at_millis(5_000), event_coord_at_millis(6_000)),
			]
		);
	}

	#[test]
	fn covering_excludes_the_window_that_ends_at_the_coord() {
		// a closed end would put the coord in the window that ended at it
		let spans = WindowSpan::covering(event_coord_at_millis(5_000), millis(1_000), millis(250));

		assert!(!spans.contains(&WindowSpan::new(event_coord_at_millis(4_000), event_coord_at_millis(5_000))));
	}

	#[test]
	fn covering_on_an_ordinal_coord_counts_slots() {
		// flooring the coord by the size would start the windows off the slide grid
		let spans = WindowSpan::covering(U8Coord(23), RowSpan::of(10), RowSpan::of(4));

		assert_eq!(
			spans,
			vec![WindowSpan::new(U8Coord(16), U8Coord(26)), WindowSpan::new(U8Coord(20), U8Coord(30)),]
		);
	}

	#[test]
	#[should_panic(expected = "slide must be below size")]
	fn covering_refuses_a_slide_that_is_not_below_the_size() {
		// a slide past the size leaves gaps where rows vanish with no error
		let _ = WindowSpan::covering(event_coord_at_millis(5_000), millis(1_000), millis(1_500));
	}
}
