// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::window::{ledger::FiredAt, policy::SealPolicy};

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
	use reifydb_abi::operator::timer::TimerKind;
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::{timer::Timer, window::span::WindowCoord};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn fired(millis: u64) -> FiredAt {
		FiredAt::of(&Timer {
			at: DateTime::from_millis(millis),
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

		let anchor = 5_000u64;
		let instant = policy.seal_instant_from_order(anchor).at();

		assert_eq!(sweep.horizon(fired(instant.to_order())), Some(DateTime::from_millis(anchor)));
	}

	#[test]
	fn a_timer_that_fires_before_its_own_span_has_elapsed_sweeps_nothing() {
		// A cold-restart wheel can present an instant earlier than the admissible span. Wrapping
		// through u64 there yields a horizon near u64::MAX and seals every window the operator owns in
		// one tick.
		let sweep = SealSweep::new(SealPolicy::tumbling(ms(1_000), ms(200)));

		assert!(sweep.horizon(fired(0)).is_none());
		assert!(sweep.horizon(fired(1_200)).is_none(), "the anchor would be 0 - 1, not 0");
		assert_eq!(sweep.horizon(fired(1_201)), Some(DateTime::from_millis(0)));
	}

	#[test]
	fn an_inert_policy_still_sweeps_by_the_fired_instant_alone() {
		// Callers gate on is_inert() before sweeping; this pins the arithmetic for one that does
		// not, so the horizon lands one millisecond behind the fired instant rather than wrapping.
		let sweep = SealSweep::new(SealPolicy::tumbling(ms(0), ms(0)));

		assert_eq!(sweep.horizon(fired(5_000)), Some(DateTime::from_millis(4_999)));
	}
}
