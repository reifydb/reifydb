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
	pub(crate) fn from_order(order: u64) -> Self {
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

	fn extended_by_grace(base: Duration, grace: Duration) -> Self {
		Self {
			admissible: AdmissibleSpan(base.try_add(grace).unwrap_or(base)),
		}
	}

	pub fn admissible(self) -> AdmissibleSpan {
		self.admissible
	}

	pub fn seal_instant(self, anchor: DateTime) -> SealInstant {
		SealInstant(<DateTime as WindowCoord>::from_order(
			anchor.to_order().saturating_add(self.admissible.millis()).saturating_add(1),
		))
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
		// Intent: the wheel fires INCLUSIVELY (`at <= watermark`), but the gate the seal
		// implements is STRICT - a window closes once the watermark has passed its whole
		// admissible span, not on reaching it. The +1 is what converts one into the
		// other, and it is the single arithmetic fact the host's five scattered cutoff
		// sites all encoded by hand.
		// Mutation: drop the +1 and a window seals a millisecond early, dropping any
		// event that lands exactly on its last admissible instant.
		let policy = SealPolicy::tumbling(ms(1_000), ms(200));

		assert_eq!(policy.admissible().millis(), 1_200);
		assert_eq!(policy.seal_instant(at(5_000)).at(), at(6_201));
	}

	#[test]
	fn rolling_admission_carries_grace_and_rolling_eviction_does_not() {
		// Intent: the one asymmetry in the host's arithmetic, and the reason SealInstant
		// and EvictionInstant are separate types. Rolling ADMITS a late event inside the
		// grace, but EVICTS on the bare span - an eviction that also waited out the grace
		// would keep every rolling window one grace-period too wide, silently inflating
		// every aggregate it publishes.
		// Mutation: build the eviction instant from SealPolicy::rolling and this returns
		// 6_201 instead of 6_000; there is no way to make that mistake now, because
		// EvictionPolicy has no grace parameter to pass.
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
		// Intent: pins the four host cutoffs that P7 relocates, so the relocation is a
		// move rather than a rewrite. Tumbling and sliding admit size + grace, session
		// admits gap + grace, rolling admits span + grace. These are the D7 reference
		// values; a divergence here is a behaviour change, not a refactor.
		assert_eq!(SealPolicy::tumbling(ms(1_000), ms(50)).admissible().millis(), 1_050);
		assert_eq!(SealPolicy::sliding(ms(1_000), ms(50)).admissible().millis(), 1_050);
		assert_eq!(SealPolicy::session(ms(300), ms(50)).admissible().millis(), 350);
		assert_eq!(SealPolicy::rolling(ms(2_000), ms(50)).admissible().millis(), 2_050);
	}

	#[test]
	fn no_grace_can_make_the_admissible_span_shorter_than_the_window() {
		// Intent: the invariant the host's `try_add(grace).unwrap_or(base)` exists to hold.
		// An admissible span BELOW the window size seals live windows on arrival, which is
		// silent data loss and looks like nothing at runtime. Two ways to break it, both
		// tested here: the sum failing and falling back to something smaller than the
		// base, and `span_millis` answering none for a Duration carrying months or days
		// and collapsing to 0 through `unwrap_or(0)`. A grace of i64::MAX nanoseconds
		// normalises into ~106751 DAYS, so it exercises the second path specifically.
		// Mutation: change `unwrap_or(base)` to `unwrap_or_default()`, or `unwrap_or(0)`
		// in millis(), and this fails.
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
