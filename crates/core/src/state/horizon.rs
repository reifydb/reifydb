// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{
	common::WindowKind,
	row::{JoinTtl, OperatorSettings, OperatorTtl},
	state::group::ActivityBuckets,
};

const BUCKETS_PER_HORIZON: u64 = 16;

pub const DEFAULT_VERSION_BUCKET_WIDTH: u64 = 1 << 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Domain {
	Event,
	Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
	Event(DateTime),
	Version(u64),
}

impl Position {
	pub fn domain(&self) -> Domain {
		match self {
			Self::Event(_) => Domain::Event,
			Self::Version(_) => Domain::Version,
		}
	}

	pub fn event(&self) -> Option<DateTime> {
		match self {
			Self::Event(instant) => Some(*instant),
			Self::Version(_) => None,
		}
	}

	pub fn version(&self) -> Option<u64> {
		match self {
			Self::Version(version) => Some(*version),
			Self::Event(_) => None,
		}
	}

	pub fn raw(&self) -> u64 {
		match self {
			Self::Event(instant) => instant.to_nanos(),
			Self::Version(version) => *version,
		}
	}

	pub fn from_raw(domain: Option<Domain>, raw: u64) -> Self {
		match domain {
			Some(Domain::Event) => Self::Event(DateTime::from_nanos(raw)),
			_ => Self::Version(raw),
		}
	}

	pub fn matches(&self, horizon: Horizon) -> bool {
		horizon.domain().is_none_or(|domain| domain == self.domain())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cutoff {
	Event(DateTime),
	Version(u64),
}

impl Cutoff {
	pub fn domain(&self) -> Domain {
		match self {
			Self::Event(_) => Domain::Event,
			Self::Version(_) => Domain::Version,
		}
	}

	pub fn raw(&self) -> u64 {
		match self {
			Self::Event(instant) => instant.to_nanos(),
			Self::Version(version) => *version,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Horizon {
	Perpetual,
	Seal {
		span: Duration,
	},
	Idle {
		span: Duration,
	},
}

impl Horizon {
	pub fn seal(span: Duration) -> Self {
		Self::Seal {
			span,
		}
	}

	pub fn idle(span: Duration) -> Self {
		Self::Idle {
			span,
		}
	}

	pub fn from_ttl(ttl: Option<&OperatorTtl>) -> Self {
		ttl.map(|ttl| Self::idle(ttl.duration)).unwrap_or(Self::Perpetual)
	}

	pub fn span(&self) -> Option<Duration> {
		match self {
			Self::Perpetual => None,
			Self::Seal {
				span,
			}
			| Self::Idle {
				span,
			} => Some(*span),
		}
	}

	pub fn usable_span(&self) -> Option<Duration> {
		self.span().filter(|span| span.as_nanos().is_ok_and(|nanos| nanos >= 0))
	}

	pub fn span_ms(&self) -> Option<u64> {
		self.usable_span().and_then(|span| span.milliseconds().ok()).and_then(|ms| u64::try_from(ms).ok())
	}

	pub fn reclaims(&self) -> bool {
		self.usable_span().is_some()
	}

	pub fn seal_cutoff(&self, watermark: DateTime) -> Option<DateTime> {
		match self {
			Self::Seal {
				..
			} => self.usable_span().map(|span| watermark.saturating_sub(span)),
			_ => None,
		}
	}

	pub fn idle_span(&self) -> Option<Duration> {
		match self {
			Self::Idle {
				span,
			} if self.reclaims() => Some(*span),
			_ => None,
		}
	}

	pub fn domain(&self) -> Option<Domain> {
		match self {
			Self::Perpetual => None,
			Self::Seal {
				..
			} => Some(Domain::Event),
			Self::Idle {
				..
			} => Some(Domain::Version),
		}
	}

	pub fn buckets(&self) -> ActivityBuckets {
		match self.usable_span() {
			None => ActivityBuckets::undeclared(u64::MAX),
			Some(span) => match self {
				Self::Seal {
					..
				} => ActivityBuckets::event(fraction_of(span, BUCKETS_PER_HORIZON)),
				_ => ActivityBuckets::version(DEFAULT_VERSION_BUCKET_WIDTH),
			},
		}
	}

	pub fn later_of(self, other: Self) -> Self {
		let (Some(left), Some(right)) = (self.usable_span(), other.usable_span()) else {
			return Self::Perpetual;
		};
		match (&self, &other) {
			(
				Self::Seal {
					..
				},
				Self::Seal {
					..
				},
			)
			| (
				Self::Idle {
					..
				},
				Self::Idle {
					..
				},
			) => {
				if left >= right {
					self
				} else {
					other
				}
			}
			_ => Self::Perpetual,
		}
	}
}

fn fraction_of(span: Duration, parts: u64) -> Duration {
	let nanos = span.as_nanos().unwrap_or(i64::MAX);
	let divisor = i64::try_from(parts).unwrap_or(1).max(1);
	Duration::from_nanoseconds((nanos / divisor).max(1)).unwrap_or(span)
}

pub fn window_horizon(kind: &WindowKind, grace: Duration, lateness: Duration) -> Horizon {
	let Some(span) = window_span(kind) else {
		return Horizon::Perpetual;
	};
	match span.try_add(grace).and_then(|total| total.try_add(lateness)) {
		Ok(total) => Horizon::seal(total),
		Err(_) => Horizon::Perpetual,
	}
}

pub fn keyed_horizon(settings: Option<&OperatorSettings>) -> Horizon {
	let Some(settings) = settings else {
		return Horizon::Perpetual;
	};
	match &settings.join {
		Some(join) => join_horizon(join),
		None => Horizon::from_ttl(settings.ttl.as_ref()),
	}
}

fn join_horizon(join: &JoinTtl) -> Horizon {
	Horizon::from_ttl(join.left.as_ref()).later_of(Horizon::from_ttl(join.right.as_ref()))
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

	#[test]
	fn a_position_is_not_interchangeable_across_domains() {
		// The same integer means a millisecond of event time in one domain and a commit version in
		// the other, and the two have no exchange rate. Comparing them by value is the mistake the
		// type exists to prevent, so equality must see the domain, not just the number.
		assert_ne!(Position::Event(DateTime::from_nanos(1_000)), Position::Version(1_000));
		assert_eq!(Position::Event(DateTime::from_nanos(1_000)).raw(), Position::Version(1_000).raw());
		assert_eq!(Position::Event(DateTime::from_nanos(1_000)).domain(), Domain::Event);
		assert_eq!(Position::Version(1_000).domain(), Domain::Version);
	}

	#[test]
	fn a_position_must_be_measured_in_its_nodes_own_domain() {
		// A windowed node stamps an event-time watermark; everything else stamps a commit version.
		// Stamping the wrong one does not error anywhere downstream: the bucket arithmetic still
		// runs and simply produces buckets that never come due, or come due instantly. matches() is
		// what lets the interner refuse the mismatch at the point of stamping instead.
		let seal = Horizon::seal(ms(60_000));
		let idle = Horizon::idle(ms(60_000));

		let event = Position::Event(DateTime::from_nanos(1));

		assert!(event.matches(seal));
		assert!(!Position::Version(1).matches(seal));
		assert!(Position::Version(1).matches(idle));
		assert!(!event.matches(idle));

		// Perpetual names no domain, so it constrains nothing and must not reject either form.
		assert!(event.matches(Horizon::Perpetual));
		assert!(Position::Version(1).matches(Horizon::Perpetual));
		assert_eq!(Horizon::Perpetual.domain(), None);
	}

	fn ttl(milliseconds: i64) -> OperatorTtl {
		OperatorTtl {
			duration: ms(milliseconds),
		}
	}

	fn tumbling(size: Duration) -> WindowKind {
		WindowKind::Tumbling {
			size: WindowSize::Duration(size),
		}
	}

	#[test]
	fn a_windowed_group_is_sealed_only_after_span_grace_and_lateness_have_all_elapsed() {
		// The seal horizon is the whole reason windowed reclamation is safe: an event admitted at the
		// watermark may still land in a window that started `span` ago, may arrive `grace` out of
		// order, and may be accepted `lateness` after that. Dropping any of the three terms would
		// reclaim an accumulator a still-admissible event is about to update, silently corrupting the
		// emitted aggregate rather than merely losing state.
		let horizon = window_horizon(&tumbling(ms(60_000)), ms(5_000), ms(30_000));

		assert_eq!(horizon.span_ms(), Some(95_000));
		assert_eq!(
			horizon.seal_cutoff(DateTime::from_millis(1_000_000)),
			Some(DateTime::from_millis(905_000)),
			"a coordinate at or above the cutoff is still reachable by an admissible event"
		);
	}

	#[test]
	fn a_rolling_window_retains_its_lag_as_well_as_its_span() {
		// A rolling window's lookback starts `lag` behind the coordinate, so its oldest reachable
		// contribution is span + lag old, not span. Omitting lag would reclaim exactly the tail the
		// next admissible event reads back over, and the operator would emit an aggregate over a
		// truncated window without any error.
		let with_lag = window_horizon(
			&WindowKind::Rolling {
				size: WindowSize::Duration(ms(60_000)),
				lag: Some(ms(15_000)),
			},
			ms(0),
			ms(0),
		);
		let without_lag = window_horizon(
			&WindowKind::Rolling {
				size: WindowSize::Duration(ms(60_000)),
				lag: None,
			},
			ms(0),
			ms(0),
		);

		assert_eq!(with_lag.span_ms(), Some(75_000));
		assert_eq!(without_lag.span_ms(), Some(60_000));
	}

	#[test]
	fn a_session_window_measures_its_horizon_from_the_gap() {
		// A session has no fixed span - it stays open until `gap` of silence closes it. The gap is
		// therefore the longest a group's state can sit untouched and still be extended by the next
		// event, which makes it the correct span term for sealing.
		let horizon = window_horizon(
			&WindowKind::Session {
				gap: ms(120_000),
			},
			ms(1_000),
			ms(0),
		);

		assert_eq!(horizon.span_ms(), Some(121_000));
	}

	#[test]
	fn a_count_based_window_has_no_time_span_and_is_therefore_perpetual() {
		// A count window seals after N contributions, an event that no clock can predict: there is no
		// elapsed time after which a further contribution becomes inadmissible. Deriving any finite
		// time horizon here would reclaim a window still waiting for its Nth row, so the only correct
		// answer is to retain and let the step-8 report name it.
		let horizon = window_horizon(
			&WindowKind::Tumbling {
				size: WindowSize::Count(100),
			},
			ms(5_000),
			ms(5_000),
		);

		assert_eq!(horizon, Horizon::Perpetual);
		assert!(!horizon.reclaims());
		assert_eq!(
			horizon.seal_cutoff(DateTime::from_millis(1_000_000)),
			None,
			"a perpetual horizon must not produce a cutoff"
		);
	}

	#[test]
	fn a_negative_span_retains_rather_than_reclaiming_from_the_future() {
		// Durations are signed, so a misdeclared negative ttl reaches this arithmetic. Converted
		// naively it would produce a cutoff ABOVE the watermark and reclaim groups that are still
		// being written. The conversion refuses it and the group stays.
		let horizon = Horizon::idle(ms(-1));

		assert_eq!(horizon.span_ms(), None);
		assert!(!horizon.reclaims());
		assert_eq!(horizon.idle_span(), None, "an unusable span must not be handed to the epoch either");
	}

	#[test]
	fn a_join_group_survives_until_the_longer_lived_side_is_idle() {
		// Both join sides share one group range, so phase 1 erases them together. Taking the shorter
		// of the two ttls would delete the longer side's rows while they can still be probed, turning
		// a hot-key read into a silent non-match. The group horizon is therefore the max, never the
		// min and never an average.
		let horizon = keyed_horizon(Some(&OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(ttl(60_000)),
				right: Some(ttl(600_000)),
			}),
		}));

		assert_eq!(horizon.span_ms(), Some(600_000));
	}

	#[test]
	fn a_join_side_with_no_ttl_keeps_the_whole_group() {
		// An undeclared side never expires. Since the sides cannot be reclaimed independently, one
		// undeclared side makes the entire group perpetual - the same rule the existing operator-ttl
		// scan applies when it skips a join with a missing side.
		let horizon = keyed_horizon(Some(&OperatorSettings {
			ttl: None,
			join: Some(JoinTtl {
				left: Some(ttl(60_000)),
				right: None,
			}),
		}));

		assert_eq!(horizon, Horizon::Perpetual);
	}

	#[test]
	fn an_operator_that_declared_nothing_is_perpetual() {
		// No settings row means no declaration, and the substrate cannot infer what a custom
		// operator's state means. Guessing a horizon here would reclaim arbitrary extension state on
		// a schedule its author never agreed to.
		assert_eq!(keyed_horizon(None), Horizon::Perpetual);
		assert_eq!(
			keyed_horizon(Some(&OperatorSettings {
				ttl: None,
				join: None,
			})),
			Horizon::Perpetual
		);
	}

	#[test]
	fn a_declared_ttl_becomes_the_idle_horizon_but_never_a_seal_cutoff() {
		// Idle spans are wall-clock and must be resolved through the epoch into a commit version
		// before they mean anything; seal spans are already in coordinate units. Handing an idle span
		// to seal_cutoff would compare milliseconds against a version number and produce a cutoff
		// that is arbitrarily wrong in either direction, so the type refuses it.
		let horizon = keyed_horizon(Some(&OperatorSettings {
			ttl: Some(ttl(3_600_000)),
			join: None,
		}));

		assert_eq!(horizon.idle_span(), Some(ms(3_600_000)));
		assert_eq!(
			horizon.seal_cutoff(DateTime::from_millis(1_000_000)),
			None,
			"an idle span is not a coordinate"
		);
	}

	#[test]
	fn horizons_from_different_domains_cannot_be_merged_and_retain_instead() {
		// A seal span counts window coordinates; an idle span counts wall-clock. There is no exchange
		// rate between them, so "the later of the two" is not computable. Picking either would be a
		// guess that can reclaim early, so the combination degrades to retaining.
		let merged = Horizon::seal(ms(60_000)).later_of(Horizon::idle(ms(10)));

		assert_eq!(merged, Horizon::Perpetual);
	}

	#[test]
	fn merging_with_a_perpetual_horizon_always_retains() {
		// Perpetual is the absorbing element: if any component of a group is undeclared, no finite
		// horizon covers the whole group.
		let seal = Horizon::seal(ms(60_000));

		assert_eq!(seal.later_of(Horizon::Perpetual), Horizon::Perpetual);
		assert_eq!(Horizon::Perpetual.later_of(seal), Horizon::Perpetual);
	}

	#[test]
	fn a_seal_cutoff_saturates_while_the_watermark_is_younger_than_the_horizon() {
		// Early in a node's life the watermark is below the horizon. Wrapping would put the cutoff
		// near u64::MAX and make every group due on the first tick, wiping state before a single
		// window ever closed.
		let horizon = Horizon::seal(ms(60_000));

		assert_eq!(horizon.seal_cutoff(DateTime::from_millis(1_000)), Some(DateTime::EPOCH));
		assert_eq!(horizon.seal_cutoff(DateTime::from_millis(60_000)), Some(DateTime::EPOCH));
		assert_eq!(horizon.seal_cutoff(DateTime::from_millis(61_000)), Some(DateTime::from_millis(1_000)));
	}

	#[test]
	fn the_bucket_width_keeps_reclamation_lag_within_a_fraction_of_the_horizon() {
		// Buckets trade index churn for reclamation latency, and coarse buckets only ever delay. What
		// must hold is that the delay stays proportional to the horizon: a wide bucket on a short
		// horizon would hold state for many multiples of its declared lifetime, which is the leak the
		// whole plane exists to close.
		for span in [1_000i64, 60_000, 3_600_000] {
			let horizon = Horizon::seal(ms(span));
			let buckets = horizon.buckets();
			let width = buckets.event_grid().expect("a seal horizon buckets in event time").width();
			let width_nanos = width.as_nanos().unwrap();
			let span_nanos = ms(span).as_nanos().unwrap();

			assert_eq!(buckets.domain(), Some(Domain::Event), "a seal horizon buckets in event time");
			assert!(width_nanos >= 1, "a zero width would divide by zero in the event grid");
			assert!(
				width_nanos <= span_nanos / BUCKETS_PER_HORIZON as i64 + 1,
				"span {span}ms: bucket width {width_nanos}ns lets a group outlive its horizon by too much"
			);
		}
	}

	#[test]
	fn a_perpetual_horizon_parks_every_group_in_one_bucket_that_never_retires() {
		// A node that never reclaims should not pay for the activity index at all. The widest
		// possible bucket collapses every reachable position into bucket 0, so each group writes one
		// index entry ever instead of rewriting it on every bucket transition. Positions are either
		// millisecond coordinates or commit versions, both of which stay within i64 range; only the
		// arithmetically unreachable u64::MAX itself lands in a second bucket.
		let buckets = Horizon::Perpetual.buckets();

		assert_eq!(buckets.domain(), None, "a perpetual horizon names no domain to bucket in");
		assert_eq!(buckets.of(Position::Version(0)), 0);
		assert_eq!(
			buckets.of(Position::Version(i64::MAX as u64)),
			0,
			"every representable position shares one bucket"
		);
		assert_eq!(
			buckets.first_live(Cutoff::Version(i64::MAX as u64)),
			0,
			"bucket 0 must never be reported due"
		);
	}
}
