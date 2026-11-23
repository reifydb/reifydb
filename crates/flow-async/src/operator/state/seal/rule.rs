// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::{WindowKind, WindowSize};
use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{
	operator::state::seal::coord::Coord,
	window::{
		coord::ByDateTime,
		kind::{rolling::RollingOverTime, session::SessionKind},
	},
};

pub const SEAL_GATE_STEP: Duration = Duration::from_milliseconds_const(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SealSpanDuration(Duration);

impl SealSpanDuration {
	pub fn duration(self) -> Duration {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SealInstant(DateTime);

impl SealInstant {
	pub fn at(self) -> DateTime {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvictionInstant(DateTime);

impl EvictionInstant {
	pub fn at(self) -> DateTime {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SealedThrough(DateTime);

impl SealedThrough {
	pub fn from_order(order: u64) -> Self {
		Self(DateTime::from_order(order))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealRule {
	seal_span: SealSpanDuration,
}

impl SealRule {
	pub fn tumbling(size: Duration, drop_span: Duration) -> Self {
		Self::extended_by_seal(size, drop_span)
	}

	pub fn sliding(size: Duration, drop_span: Duration) -> Self {
		Self::extended_by_seal(size, drop_span)
	}

	pub fn session(gap: Duration, drop_span: Duration) -> Self {
		Self::extended_by_seal(gap, drop_span)
	}

	pub fn rolling(span: Duration, drop_span: Duration) -> Self {
		Self::extended_by_seal(span, drop_span)
	}

	pub fn of(seal_span: Duration) -> Self {
		Self {
			seal_span: SealSpanDuration(seal_span),
		}
	}

	pub fn for_window(kind: &WindowKind, drop_span: Duration) -> Option<Self> {
		match kind {
			WindowKind::Tumbling {
				size: WindowSize::Duration(size),
			} => Some(Self::tumbling(*size, drop_span)),
			WindowKind::Sliding {
				size: WindowSize::Duration(size),
				..
			} => Some(Self::sliding(*size, drop_span)),
			WindowKind::Rolling {
				size: WindowSize::Duration(size),
				lag,
				..
			} => Some(RollingOverTime::new(*size, lag.unwrap_or_default()).seal_rule(drop_span)),
			WindowKind::Session {
				gap,
			} => Some(SessionKind::with_gap(*gap).seal_rule(drop_span)),
			_ => None,
		}
	}

	fn extended_by_seal(base: Duration, drop_span: Duration) -> Self {
		Self {
			seal_span: SealSpanDuration(base.try_add(drop_span).unwrap_or(base)),
		}
	}

	pub fn seal_span(self) -> SealSpanDuration {
		self.seal_span
	}

	pub fn seal_instant(self, event: DateTime) -> SealInstant {
		SealInstant(event.saturating_add(self.seal_span.0).saturating_add(SEAL_GATE_STEP))
	}

	pub fn seal_instant_from_order(self, event_order: u64) -> SealInstant {
		self.seal_instant(DateTime::from_order(event_order))
	}

	pub fn last_sealed(self, at: ByDateTime) -> Option<ByDateTime> {
		at.0.checked_sub(self.seal_span.0)
			.and_then(|sealed_below| sealed_below.checked_sub(SEAL_GATE_STEP))
			.map(ByDateTime)
	}
}

pub fn output_watermark<C: Coord>(operator_watermark: C, drop_span: C::Span) -> C {
	operator_watermark.saturating_sub_span(drop_span)
}

pub fn is_sealed<C: Coord>(event: C, first_admitted: C) -> bool {
	event < first_admitted
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionRule {
	span: Duration,
}

impl EvictionRule {
	pub fn rolling(span: Duration) -> Self {
		Self {
			span,
		}
	}

	pub fn eviction_instant(self, event: DateTime) -> EvictionInstant {
		EvictionInstant(event.saturating_add(self.span))
	}

	pub fn eviction_instant_from_order(self, event_order: u64) -> EvictionInstant {
		self.eviction_instant(DateTime::from_order(event_order))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::OnEmpty;
	use reifydb_value::factory::time::at_millis;

	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	#[test]
	fn a_seal_instant_is_one_past_the_seal_span() {
		// The wheel fires inclusively (`at <= operator watermark`) but the seal gate is strict - a window
		// closes once the operator watermark has passed its whole seal span, not on reaching it. The +1
		// is what converts one into the other.
		let rule = SealRule::tumbling(ms(1_000), ms(200));

		assert_eq!(rule.seal_span().duration(), ms(1_200));
		assert_eq!(rule.seal_instant(at_millis(5_000)).at(), at_millis(6_201));
	}

	#[test]
	fn the_output_watermark_trails_the_ledger_by_the_whole_seal_span() {
		// The ledger holds the instant a seal timer fired, a whole seal span ahead of the
		// newest window that timer actually sealed. Treating the ledger itself as the immutable
		// operator watermark erases the accumulator of a window that is still open and still taking rows.
		let rule = SealRule::tumbling(ms(30_000), ms(45_000));
		let ledger = at_millis(358_262);

		let last_sealed = rule.last_sealed(ByDateTime(ledger)).expect("the ledger is past one seal span");

		assert_eq!(last_sealed.0, at_millis(283_261), "ledger - (size + drop) - 1");
		assert!(
			last_sealed.0 < ledger,
			"an operator watermark at or past the ledger forgets windows that have not sealed"
		);
		assert_eq!(
			rule.seal_instant(last_sealed.0).at(),
			ledger,
			"and it is the exact inverse of arming, so a window at this last sealed instant sealed at precisely \
			 this ledger rather than one millisecond either side of it"
		);
	}

	#[test]
	fn a_ledger_short_of_one_seal_span_has_a_real_pre_epoch_output_watermark() {
		// Wrapping through u64 would put the last sealed instant near u64::MAX and report every window sealed
		let rule = SealRule::tumbling(ms(30_000), ms(45_000));

		assert_eq!(rule.last_sealed(ByDateTime(at_millis(0))), Some(ByDateTime(at_millis(-75_001))));
		assert_eq!(
			rule.last_sealed(ByDateTime(at_millis(75_000))),
			Some(ByDateTime(at_millis(-1))),
			"the last sealed instant is 0 - 1, not 0"
		);
		assert_eq!(rule.last_sealed(ByDateTime(at_millis(75_001))), Some(ByDateTime(at_millis(0))));
	}

	#[test]
	fn rolling_admission_carries_drop_and_rolling_eviction_does_not() {
		// Rolling admits a late event inside the drop but evicts on the bare span, which is why
		// SealInstant and EvictionInstant are separate types. An eviction that also waited out the
		// drop keeps every rolling window one drop-period too wide, inflating every aggregate.
		let admission = SealRule::rolling(ms(1_000), ms(200));
		let eviction = EvictionRule::rolling(ms(1_000));

		assert_eq!(admission.seal_instant(at_millis(5_000)).at(), at_millis(6_201));
		assert_eq!(eviction.eviction_instant(at_millis(5_000)).at(), at_millis(6_000));
	}

	#[test]
	fn an_eviction_instant_never_carries_the_strict_gate_plus_one() {
		// The +1 belongs to the seal gate alone. Eviction is a frame boundary, not a gate, so
		// carrying the +1 there keeps one millisecond too much on every rolling window, forever.
		let eviction = EvictionRule::rolling(ms(0));

		assert_eq!(eviction.eviction_instant(at_millis(7_000)).at(), at_millis(7_000));
	}

	#[test]
	fn every_kind_admits_its_own_base_span_plus_drop() {
		// Tumbling and sliding admit size + drop, session admits gap + drop, rolling
		// admits span + drop. A divergence here is a behaviour change, not a refactor.
		assert_eq!(SealRule::tumbling(ms(1_000), ms(50)).seal_span().duration(), ms(1_050));
		assert_eq!(SealRule::sliding(ms(1_000), ms(50)).seal_span().duration(), ms(1_050));
		assert_eq!(SealRule::session(ms(300), ms(50)).seal_span().duration(), ms(350));
		assert_eq!(SealRule::rolling(ms(2_000), ms(50)).seal_span().duration(), ms(2_050));
	}

	#[test]
	fn no_drop_can_make_the_seal_span_shorter_than_the_window() {
		// the seal span must never fall below the window size, or a live window seals on arrival
		let enormous = Duration::from_nanoseconds_const(i64::MAX);

		for drop_span in [ms(0), ms(1), enormous] {
			let rule = SealRule::tumbling(ms(1_000), drop_span);
			assert!(
				rule.seal_span().duration() >= ms(1_000),
				"seal span {:?} fell below the 1000ms window for drop {drop_span:?}",
				rule.seal_span().duration()
			);
		}
	}

	#[test]
	fn for_window_matches_each_kinds_own_rule() {
		// for_window must reproduce each kind's own constructor exactly, not approximate it.
		let drop_span = ms(50);

		let tumbling = WindowKind::Tumbling {
			size: WindowSize::Duration(ms(1_000)),
		};
		assert_eq!(SealRule::for_window(&tumbling, drop_span), Some(SealRule::tumbling(ms(1_000), drop_span)));

		let sliding = WindowKind::Sliding {
			size: WindowSize::Duration(ms(1_000)),
			slide: WindowSize::Duration(ms(500)),
		};
		assert_eq!(SealRule::for_window(&sliding, drop_span), Some(SealRule::sliding(ms(1_000), drop_span)));

		let rolling = WindowKind::Rolling {
			size: WindowSize::Duration(ms(2_000)),
			lag: Some(ms(300)),
			pane: None,
			on_empty: OnEmpty::Remove,
		};
		assert_eq!(
			SealRule::for_window(&rolling, drop_span),
			Some(RollingOverTime::new(ms(2_000), ms(300)).seal_rule(drop_span))
		);

		let session = WindowKind::Session {
			gap: ms(300),
		};
		assert_eq!(
			SealRule::for_window(&session, drop_span),
			Some(SessionKind::with_gap(ms(300)).seal_rule(drop_span))
		);
	}

	#[test]
	fn for_window_gives_no_rule_for_a_slots_size() {
		// A count size seals in a row coordinate the engine has no duration for, so it gets no rule.
		let drop_span = ms(50);

		let kinds = [
			WindowKind::Tumbling {
				size: WindowSize::Slots(10),
			},
			WindowKind::Sliding {
				size: WindowSize::Slots(10),
				slide: WindowSize::Slots(5),
			},
			WindowKind::Rolling {
				size: WindowSize::Slots(10),
				lag: None,
				pane: None,
				on_empty: OnEmpty::Remove,
			},
		];
		for kind in kinds {
			assert_eq!(SealRule::for_window(&kind, drop_span), None);
		}
	}
}
