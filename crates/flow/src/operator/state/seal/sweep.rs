// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::operator::state::seal::{ledger::FiredAt, policy::SealPolicy};

pub struct SealSweep {
	policy: SealPolicy,
}

impl SealSweep {
	pub fn new(policy: SealPolicy) -> Self {
		Self {
			policy,
		}
	}

	pub fn horizon(&self, fired: FiredAt) -> Option<DateTime> {
		self.policy.sealed_anchor(fired.at())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::state::timer::TimerKind;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::{operator::state::seal::coord::Coord, timer::Timer};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn order(millis: u64) -> u64 {
		DateTime::from_millis(millis).to_order()
	}

	fn fired(order: u64) -> FiredAt {
		FiredAt::of(&Timer {
			due: <DateTime as Coord>::from_order(order),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		})
	}

	#[test]
	fn the_sweep_horizon_inverts_the_seal_instant_exactly() {
		// Arming computes `anchor + admissible + 1`; the sweep must invert it exactly or a window
		// seals its neighbour instead of itself. The two halves live in separate files and have
		// drifted apart before.
		let policy = SealPolicy::tumbling(ms(1_000), ms(200));
		let sweep = SealSweep::new(policy);

		let anchor = order(5_000);
		let instant = policy.seal_instant_from_order(anchor).at();

		assert_eq!(sweep.horizon(fired(instant.to_order())), Some(DateTime::from_millis(5_000)));
	}

	#[test]
	fn a_timer_that_fires_before_its_own_span_has_elapsed_sweeps_nothing() {
		// A cold-restart wheel can present an instant earlier than the admissible span. Wrapping
		// through u64 there yields a horizon near u64::MAX and seals every window the operator owns in
		// one tick.
		let sweep = SealSweep::new(SealPolicy::tumbling(ms(1_000), ms(200)));

		assert!(sweep.horizon(fired(order(0))).is_none());
		assert!(sweep.horizon(fired(order(1_200))).is_none(), "the anchor would be 0 - 1, not 0");
		assert_eq!(sweep.horizon(fired(order(1_201))), Some(DateTime::from_millis(0)));
	}

	#[test]
	fn an_inert_policy_still_sweeps_by_the_fired_instant_alone() {
		// Callers gate on is_inert() before sweeping; this pins the arithmetic for one that does
		// not, so the horizon lands one millisecond behind the fired instant rather than wrapping.
		let sweep = SealSweep::new(SealPolicy::tumbling(ms(0), ms(0)));

		assert_eq!(sweep.horizon(fired(order(5_000))), Some(DateTime::from_millis(4_999)));
	}
}
