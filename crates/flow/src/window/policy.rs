// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::span::WindowCoord;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdmissibleSpan(Duration);

impl AdmissibleSpan {
	pub fn duration(self) -> Duration {
		self.0
	}

	pub fn millis(self) -> u64 {
		<DateTime as WindowCoord>::span_millis(self.0).unwrap_or(0)
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
		Self(<DateTime as WindowCoord>::from_order(order))
	}

	pub fn at(self) -> DateTime {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealPolicy {
	admissible: AdmissibleSpan,
}

impl SealPolicy {
	pub fn tumbling(size: Duration, grace: Duration) -> Self {
		Self::extended_by_grace(size, grace)
	}

	pub fn sliding(size: Duration, grace: Duration) -> Self {
		Self::extended_by_grace(size, grace)
	}

	pub fn session(gap: Duration, grace: Duration) -> Self {
		Self::extended_by_grace(gap, grace)
	}

	pub fn rolling(span: Duration, grace: Duration) -> Self {
		Self::extended_by_grace(span, grace)
	}

	pub fn of(admissible: Duration) -> Self {
		Self {
			admissible: AdmissibleSpan(admissible),
		}
	}

	fn extended_by_grace(base: Duration, grace: Duration) -> Self {
		Self {
			admissible: AdmissibleSpan(base.try_add(grace).unwrap_or(base)),
		}
	}

	pub fn admissible(self) -> AdmissibleSpan {
		self.admissible
	}

	pub fn is_inert(self) -> bool {
		self.admissible.0.is_zero()
	}

	pub fn seal_instant(self, anchor: DateTime) -> SealInstant {
		SealInstant(<DateTime as WindowCoord>::from_order(
			anchor.to_order().saturating_add(self.admissible.millis()).saturating_add(1),
		))
	}

	pub fn seal_instant_from_order(self, anchor_order: u64) -> SealInstant {
		self.seal_instant(<DateTime as WindowCoord>::from_order(anchor_order))
	}

	pub fn sealed_anchor(self, at: DateTime) -> Option<DateTime> {
		at.to_order()
			.checked_sub(self.admissible.millis())
			.and_then(|anchor| anchor.checked_sub(1))
			.map(<DateTime as WindowCoord>::from_order)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionPolicy {
	span: Duration,
}

impl EvictionPolicy {
	pub fn rolling(span: Duration) -> Self {
		Self {
			span,
		}
	}

	pub fn eviction_instant(self, anchor: DateTime) -> EvictionInstant {
		EvictionInstant(<DateTime as WindowCoord>::from_order(
			anchor.to_order()
				.saturating_add(<DateTime as WindowCoord>::span_millis(self.span).unwrap_or(0)),
		))
	}

	pub fn eviction_instant_from_order(self, anchor_order: u64) -> EvictionInstant {
		self.eviction_instant(<DateTime as WindowCoord>::from_order(anchor_order))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	#[test]
	fn a_seal_instant_is_one_past_the_admissible_span() {
		// The wheel fires INCLUSIVELY (`at <= watermark`), but the gate the seal
		// implements is STRICT - a window closes once the watermark has passed its whole
		// admissible span, not on reaching it. The +1 is what converts one into the
		// other, and it is the single arithmetic fact the host's five scattered cutoff
		// sites all encoded by hand.
		let policy = SealPolicy::tumbling(ms(1_000), ms(200));

		assert_eq!(policy.admissible().millis(), 1_200);
		assert_eq!(policy.seal_instant(at(5_000)).at(), at(6_201));
	}

	#[test]
	fn the_sealed_anchor_trails_the_ledger_by_the_whole_admissible_span() {
		// The seal ledger holds the instant a seal TIMER fired, which is a whole admissible span
		// ahead of the newest window that timer actually sealed. Treating the ledger itself as the
		// immutable frontier - which reclamation briefly did - erases the accumulator of a window
		// that is still open and still taking rows, and the operator then publishes against state
		// that is gone. That is the exact failure the seal clamp was introduced to prevent, so the
		// gap between the two instants has to be a fact this file owns rather than a subtraction
		// each caller repeats.
		let policy = SealPolicy::tumbling(ms(30_000), ms(45_000));
		let ledger = at(358_262);

		let anchor = policy.sealed_anchor(ledger).expect("the ledger is past one admissible span");

		assert_eq!(anchor, at(283_261), "ledger - (size + grace) - 1");
		assert!(anchor < ledger, "a frontier at or past the ledger reclaims windows that have not sealed");
		assert_eq!(
			policy.seal_instant(anchor).at(),
			ledger,
			"and it is the exact inverse of arming, so a window anchored here sealed at precisely \
			 this ledger rather than one millisecond either side of it"
		);
	}

	#[test]
	fn a_ledger_short_of_one_admissible_span_has_sealed_nothing() {
		// Early in a node's life the ledger sits below its own span. Wrapping through u64 there
		// would put the anchor near u64::MAX and report every window sealed, which reclaims the
		// whole node in one sweep. None means "nothing has sealed yet", and nothing is reclaimable.
		let policy = SealPolicy::tumbling(ms(30_000), ms(45_000));

		assert_eq!(policy.sealed_anchor(at(0)), None);
		assert_eq!(policy.sealed_anchor(at(75_000)), None, "the anchor would be 0 - 1, not 0");
		assert_eq!(policy.sealed_anchor(at(75_001)), Some(at(0)));
	}

	#[test]
	fn rolling_admission_carries_grace_and_rolling_eviction_does_not() {
		// The one asymmetry in the host's arithmetic, and the reason SealInstant
		// and EvictionInstant are separate types. Rolling ADMITS a late event inside the
		// grace, but EVICTS on the bare span - an eviction that also waited out the grace
		// would keep every rolling window one grace-period too wide, silently inflating
		// every aggregate it publishes.
		let admission = SealPolicy::rolling(ms(1_000), ms(200));
		let eviction = EvictionPolicy::rolling(ms(1_000));

		assert_eq!(admission.seal_instant(at(5_000)).at(), at(6_201));
		assert_eq!(eviction.eviction_instant(at(5_000)).at(), at(6_000));
	}

	#[test]
	fn an_eviction_instant_never_carries_the_strict_gate_plus_one() {
		// The +1 belongs to the seal gate alone. Eviction is a retention boundary, not a
		// gate, so carrying the +1 there would retain one millisecond too much on every
		// rolling window - invisible per window, unbounded across a long-running flow.
		let eviction = EvictionPolicy::rolling(ms(0));

		assert_eq!(eviction.eviction_instant(at(7_000)).at(), at(7_000));
	}

	#[test]
	fn every_kind_admits_its_own_base_span_plus_grace() {
		// Tumbling and sliding admit size + grace, session admits gap + grace, rolling
		// admits span + grace. A divergence here is a behaviour change, not a refactor.
		assert_eq!(SealPolicy::tumbling(ms(1_000), ms(50)).admissible().millis(), 1_050);
		assert_eq!(SealPolicy::sliding(ms(1_000), ms(50)).admissible().millis(), 1_050);
		assert_eq!(SealPolicy::session(ms(300), ms(50)).admissible().millis(), 350);
		assert_eq!(SealPolicy::rolling(ms(2_000), ms(50)).admissible().millis(), 2_050);
	}

	#[test]
	fn no_grace_can_make_the_admissible_span_shorter_than_the_window() {
		// The invariant the host's `try_add(grace).unwrap_or(base)` exists to hold.
		// An admissible span BELOW the window size seals live windows on arrival, which is
		// silent data loss and looks like nothing at runtime. Two ways to break it, both
		// tested here: the sum failing and falling back to something smaller than the
		// base, and `span_millis` answering none for a Duration carrying months or days
		// and collapsing to 0 through `unwrap_or(0)`. A grace of i64::MAX nanoseconds
		// normalises into ~106751 DAYS, so it exercises the second path specifically.
		let enormous = Duration::from_nanoseconds_const(i64::MAX);

		for grace in [ms(0), ms(1), enormous] {
			let policy = SealPolicy::tumbling(ms(1_000), grace);
			assert!(
				policy.admissible().millis() >= 1_000,
				"admissible {} fell below the 1000ms window for grace {grace:?}",
				policy.admissible().millis()
			);
		}
	}
}
