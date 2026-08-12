// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::seal::coord::Coord;

pub const SEAL_GATE_STEP: Duration = Duration::from_milliseconds_const(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdmissibleSpan(Duration);

impl AdmissibleSpan {
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
		Self(<DateTime as Coord>::from_order(order))
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
	pub fn tumbling(size: Duration, seal: Duration) -> Self {
		Self::extended_by_seal(size, seal)
	}

	pub fn sliding(size: Duration, seal: Duration) -> Self {
		Self::extended_by_seal(size, seal)
	}

	pub fn session(gap: Duration, seal: Duration) -> Self {
		Self::extended_by_seal(gap, seal)
	}

	pub fn rolling(span: Duration, seal: Duration) -> Self {
		Self::extended_by_seal(span, seal)
	}

	pub fn of(admissible: Duration) -> Self {
		Self {
			admissible: AdmissibleSpan(admissible),
		}
	}

	fn extended_by_seal(base: Duration, seal: Duration) -> Self {
		Self {
			admissible: AdmissibleSpan(base.try_add(seal).unwrap_or(base)),
		}
	}

	pub fn admissible(self) -> AdmissibleSpan {
		self.admissible
	}

	pub fn is_inert(self) -> bool {
		self.admissible.0.is_zero()
	}

	pub fn seal_instant(self, anchor: DateTime) -> SealInstant {
		SealInstant(anchor.saturating_add(self.admissible.0).saturating_add(SEAL_GATE_STEP))
	}

	pub fn seal_instant_from_order(self, anchor_order: u64) -> SealInstant {
		self.seal_instant(<DateTime as Coord>::from_order(anchor_order))
	}

	pub fn sealed_anchor(self, at: DateTime) -> Option<DateTime> {
		at.checked_sub(self.admissible.0).and_then(|anchor| anchor.checked_sub(SEAL_GATE_STEP))
	}
}

pub fn seal_horizon<C: Coord>(watermark: C, seal_after: C::Span) -> C {
	watermark.saturating_sub_span(seal_after)
}

pub fn is_sealed<C: Coord>(anchor: C, horizon: C) -> bool {
	anchor < horizon
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
		EvictionInstant(anchor.saturating_add(self.span))
	}

	pub fn eviction_instant_from_order(self, anchor_order: u64) -> EvictionInstant {
		self.eviction_instant(<DateTime as Coord>::from_order(anchor_order))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::at_millis;

	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	#[test]
	fn a_seal_instant_is_one_past_the_admissible_span() {
		// The wheel fires inclusively (`at <= watermark`) but the seal gate is strict - a window
		// closes once the watermark has passed its whole admissible span, not on reaching it. The +1
		// is what converts one into the other.
		let policy = SealPolicy::tumbling(ms(1_000), ms(200));

		assert_eq!(policy.admissible().duration(), ms(1_200));
		assert_eq!(policy.seal_instant(at_millis(5_000)).at(), at_millis(6_201));
	}

	#[test]
	fn the_sealed_anchor_trails_the_ledger_by_the_whole_admissible_span() {
		// The ledger holds the instant a seal timer fired, a whole admissible span ahead of the
		// newest window that timer actually sealed. Treating the ledger itself as the immutable
		// frontier erases the accumulator of a window that is still open and still taking rows.
		let policy = SealPolicy::tumbling(ms(30_000), ms(45_000));
		let ledger = at_millis(358_262);

		let anchor = policy.sealed_anchor(ledger).expect("the ledger is past one admissible span");

		assert_eq!(anchor, at_millis(283_261), "ledger - (size + grace) - 1");
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
		// Early in a operator's life the ledger sits below its own span. Wrapping through u64 would put
		// the anchor near u64::MAX and report every window sealed, reclaiming the operator in one sweep.
		let policy = SealPolicy::tumbling(ms(30_000), ms(45_000));

		assert_eq!(policy.sealed_anchor(at_millis(0)), None);
		assert_eq!(policy.sealed_anchor(at_millis(75_000)), None, "the anchor would be 0 - 1, not 0");
		assert_eq!(policy.sealed_anchor(at_millis(75_001)), Some(at_millis(0)));
	}

	#[test]
	fn rolling_admission_carries_grace_and_rolling_eviction_does_not() {
		// Rolling admits a late event inside the grace but evicts on the bare span, which is why
		// SealInstant and EvictionInstant are separate types. An eviction that also waited out the
		// grace keeps every rolling window one grace-period too wide, inflating every aggregate.
		let admission = SealPolicy::rolling(ms(1_000), ms(200));
		let eviction = EvictionPolicy::rolling(ms(1_000));

		assert_eq!(admission.seal_instant(at_millis(5_000)).at(), at_millis(6_201));
		assert_eq!(eviction.eviction_instant(at_millis(5_000)).at(), at_millis(6_000));
	}

	#[test]
	fn an_eviction_instant_never_carries_the_strict_gate_plus_one() {
		// The +1 belongs to the seal gate alone. Eviction is a retention boundary, not a gate, so
		// carrying the +1 there retains one millisecond too much on every rolling window, forever.
		let eviction = EvictionPolicy::rolling(ms(0));

		assert_eq!(eviction.eviction_instant(at_millis(7_000)).at(), at_millis(7_000));
	}

	#[test]
	fn every_kind_admits_its_own_base_span_plus_grace() {
		// Tumbling and sliding admit size + grace, session admits gap + grace, rolling
		// admits span + grace. A divergence here is a behaviour change, not a refactor.
		assert_eq!(SealPolicy::tumbling(ms(1_000), ms(50)).admissible().duration(), ms(1_050));
		assert_eq!(SealPolicy::sliding(ms(1_000), ms(50)).admissible().duration(), ms(1_050));
		assert_eq!(SealPolicy::session(ms(300), ms(50)).admissible().duration(), ms(350));
		assert_eq!(SealPolicy::rolling(ms(2_000), ms(50)).admissible().duration(), ms(2_050));
	}

	#[test]
	fn no_grace_can_make_the_admissible_span_shorter_than_the_window() {
		// An admissible span below the window size seals live windows on arrival - silent data loss.
		// Two ways to break it: the sum failing back to something smaller than the base, and
		// span_millis answering none for a months/days Duration, which i64::MAX nanoseconds becomes.
		let enormous = Duration::from_nanoseconds_const(i64::MAX);

		for grace in [ms(0), ms(1), enormous] {
			let policy = SealPolicy::tumbling(ms(1_000), grace);
			assert!(
				policy.admissible().duration() >= ms(1_000),
				"admissible {:?} fell below the 1000ms window for grace {grace:?}",
				policy.admissible().duration()
			);
		}
	}
}
