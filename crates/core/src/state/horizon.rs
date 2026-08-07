// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::common::WindowKind;

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
	use reifydb_value::value::duration::Duration;

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
}
