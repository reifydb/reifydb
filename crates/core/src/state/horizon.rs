// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{common::WindowKind, state::group::ActivityBuckets};

const BUCKETS_PER_HORIZON: u64 = 16;

pub const DEFAULT_VERSION_BUCKET_WIDTH: u64 = 1 << 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position(pub DateTime);

impl Position {
	pub fn instant(&self) -> DateTime {
		self.0
	}

	pub fn raw(&self) -> u64 {
		self.0.to_nanos()
	}

	pub fn from_raw(raw: u64) -> Self {
		Self(DateTime::from_nanos(raw))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cutoff(pub DateTime);

impl Cutoff {
	pub fn instant(&self) -> DateTime {
		self.0
	}

	pub fn raw(&self) -> u64 {
		self.0.to_nanos()
	}
}

pub fn usable_scale(scale: Option<Duration>) -> Option<Duration> {
	scale.filter(|span| span.as_nanos().is_ok_and(|nanos| nanos >= 0))
}

pub fn activity_buckets(scale: Option<Duration>) -> ActivityBuckets {
	match usable_scale(scale) {
		None => ActivityBuckets::undeclared(u64::MAX),
		Some(span) => ActivityBuckets::event(fraction_of(span, BUCKETS_PER_HORIZON)),
	}
}

fn fraction_of(span: Duration, parts: u64) -> Duration {
	let nanos = span.as_nanos().unwrap_or(i64::MAX);
	let divisor = i64::try_from(parts).unwrap_or(1).max(1);
	Duration::from_nanoseconds((nanos / divisor).max(1)).unwrap_or(span)
}

pub fn window_retention_scale(kind: &WindowKind, grace: Duration) -> Option<Duration> {
	usable_scale(window_span(kind).and_then(|span| span.try_add(grace).ok()))
}

fn window_span(kind: &WindowKind) -> Option<Duration> {
	match kind {
		WindowKind::Tumbling {
			size,
			..
		}
		| WindowKind::Sliding {
			size,
			..
		} => size.as_duration(),
		WindowKind::Rolling {
			size,
			lag,
			..
		} => {
			let size = size.as_duration()?;
			match lag {
				Some(lag) => size.try_add(*lag).ok(),
				None => Some(size),
			}
		}
		WindowKind::Session {
			gap,
			..
		} => Some(*gap),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::common::WindowSize;

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	fn tumbling(size: Duration) -> WindowKind {
		WindowKind::Tumbling {
			size: WindowSize::Duration(size),
		}
	}

	#[test]
	fn a_windowed_group_is_reclaimable_only_after_span_and_grace_have_both_elapsed() {
		// An event admitted at the watermark may land in a window that started `span` ago and may
		// arrive `grace` out of order. Dropping either term reclaims an accumulator a still-admissible
		// event is about to update, corrupting the emitted aggregate rather than merely losing state.
		assert_eq!(window_retention_scale(&tumbling(ms(60_000)), ms(5_000)), Some(ms(65_000)));
	}

	#[test]
	fn a_rolling_window_retains_its_lag_as_well_as_its_span() {
		// A rolling window's lookback starts `lag` behind the coordinate, so its oldest reachable
		// contribution is span + lag old. Omitting lag reclaims the tail the next admissible event
		// reads back over, and the operator emits over a truncated window without any error.
		let with_lag = window_retention_scale(
			&WindowKind::Rolling {
				size: WindowSize::Duration(ms(60_000)),
				lag: Some(ms(15_000)),
			},
			ms(0),
		);
		let without_lag = window_retention_scale(
			&WindowKind::Rolling {
				size: WindowSize::Duration(ms(60_000)),
				lag: None,
			},
			ms(0),
		);

		assert_eq!(with_lag, Some(ms(75_000)));
		assert_eq!(without_lag, Some(ms(60_000)));
	}

	#[test]
	fn a_session_window_measures_its_scale_from_the_gap() {
		// A session stays open until `gap` of silence closes it, so the gap is the longest a group's
		// state can sit untouched and still be extended by the next event.
		assert_eq!(
			window_retention_scale(
				&WindowKind::Session {
					gap: ms(120_000),
				},
				ms(1_000)
			),
			Some(ms(121_000))
		);
	}

	#[test]
	fn a_count_based_window_has_no_time_span_and_is_therefore_perpetual() {
		// A count window seals after N contributions, which no clock can predict, so no elapsed time
		// makes a further contribution inadmissible. Any finite scale would reclaim a window still
		// waiting for its Nth row.
		assert_eq!(
			window_retention_scale(
				&WindowKind::Tumbling {
					size: WindowSize::Count(100),
				},
				ms(5_000)
			),
			None
		);
	}

	#[test]
	fn a_negative_scale_retains_rather_than_reclaiming_from_the_future() {
		// Durations are signed, so a misdeclared negative ttl reaches this arithmetic; converted
		// naively it produces a cutoff ABOVE the watermark and reclaims groups still being written.
		assert_eq!(usable_scale(Some(ms(-1))), None);
		assert_eq!(usable_scale(Some(ms(1))), Some(ms(1)));
		assert_eq!(usable_scale(None), None);
	}

	#[test]
	fn the_bucket_width_keeps_reclamation_lag_within_a_fraction_of_the_scale() {
		// Buckets trade index churn for reclamation latency, so the delay must stay proportional to
		// the scale: a wide bucket on a short scale holds state for many multiples of its lifetime.
		for span in [1_000i64, 60_000, 3_600_000] {
			let buckets = activity_buckets(Some(ms(span)));
			let width = buckets.event_grid().expect("a declared scale buckets in event time").width();
			let width_nanos = width.as_nanos().unwrap();
			let span_nanos = ms(span).as_nanos().unwrap();

			assert!(width_nanos >= 1, "a zero width would divide by zero in the event grid");
			assert!(
				width_nanos <= span_nanos / BUCKETS_PER_HORIZON as i64 + 1,
				"span {span}ms: bucket width {width_nanos}ns lets a group outlive its scale by too much"
			);
		}
	}

	#[test]
	fn an_undeclared_scale_parks_every_group_in_one_bucket_that_never_retires() {
		// A operator that never reclaims should not pay for the activity index: the widest bucket
		// collapses every reachable position into bucket 0, so a group writes one index entry ever.
		// Event-time coordinates stay within i64 range, so only u64::MAX reaches a second bucket.
		let buckets = activity_buckets(None);

		assert!(buckets.event_grid().is_none(), "an undeclared scale has no event grid to bucket in");
		assert_eq!(buckets.of(Position(DateTime::from_nanos(0))), 0);
		assert_eq!(
			buckets.of(Position(DateTime::from_nanos(i64::MAX as u64))),
			0,
			"every representable position shares one bucket"
		);
		assert_eq!(
			buckets.first_live(Cutoff(DateTime::from_nanos(i64::MAX as u64))),
			0,
			"bucket 0 must never be reported due"
		);
	}
}
