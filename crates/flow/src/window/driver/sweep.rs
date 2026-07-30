// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::window::{ledger::FiredAt, policy::SealPolicy, span::WindowCoord};

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
		fired.at()
			.to_order()
			.checked_sub(self.policy.admissible().millis())
			.and_then(|anchor| anchor.checked_sub(1))
			.map(<DateTime as WindowCoord>::from_order)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::operator::timer::TimerKind;
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::timer::Timer;

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
		// Arming computes `anchor + admissible + 1` and the sweep must recover `anchor`
		// from the instant that fired, or a window seals its neighbour instead of itself. These
		// are the two halves of one equation living in two files, which is precisely how the
		// host and guest shells drifted apart.
		let policy = SealPolicy::tumbling(ms(1_000), ms(200));
		let sweep = SealSweep::new(policy);

		let anchor = 5_000u64;
		let instant = policy.seal_instant_from_order(anchor).at();

		assert_eq!(sweep.horizon(fired(instant.to_order())), Some(DateTime::from_millis(anchor)));
	}

	#[test]
	fn a_timer_that_fires_before_its_own_span_has_elapsed_sweeps_nothing() {
		// A wheel restored from a cold restart, or a manually advanced test watermark,
		// can present an instant earlier than the operator's admissible span. Wrapping through
		// u64 there would produce a horizon near u64::MAX and seal every window the node owns in
		// one tick - total, silent data loss.
		let sweep = SealSweep::new(SealPolicy::tumbling(ms(1_000), ms(200)));

		assert!(sweep.horizon(fired(0)).is_none());
		assert!(sweep.horizon(fired(1_200)).is_none(), "the anchor would be 0 - 1, not 0");
		assert_eq!(sweep.horizon(fired(1_201)), Some(DateTime::from_millis(0)));
	}

	#[test]
	fn an_inert_policy_still_sweeps_by_the_fired_instant_alone() {
		// A zero span admits nothing beyond the instant itself, so the horizon is the fired
		// instant minus the strict-gate millisecond and nothing else. Callers gate on
		// is_inert() before sweeping; this pins what the arithmetic does if one ever does not,
		// so the answer is a narrow sweep rather than an unbounded one.
		let sweep = SealSweep::new(SealPolicy::tumbling(ms(0), ms(0)));

		assert_eq!(sweep.horizon(fired(5_000)), Some(DateTime::from_millis(4_999)));
	}
}
