// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::state::timer::{TimerKind, TimerStore};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::operator::state::seal::rule::{EvictionRule, SealRule, SealedThrough};

pub struct SealGate {
	rule: SealRule,
	frontier: DateTime,
}

impl SealGate {
	pub fn new(rule: SealRule, ledger: Option<SealedThrough>, watermark: Option<DateTime>) -> Self {
		let ledger = ledger.map_or_else(DateTime::default, SealedThrough::at);
		Self {
			rule,
			frontier: watermark.map_or(ledger, |watermark| ledger.max(watermark)),
		}
	}

	pub fn admits(&self, horizon: u64) -> bool {
		self.rule.seal_instant_from_order(horizon).at() > self.frontier
	}

	pub fn arm(
		&self,
		store: &mut dyn TimerStore,
		key: &EncodedKey,
		prior_horizon: Option<u64>,
		horizon: u64,
	) -> Result<()> {
		let at = self.rule.seal_instant_from_order(horizon);
		if let Some(prior_horizon) = prior_horizon {
			let prior_at = self.rule.seal_instant_from_order(prior_horizon);
			if prior_at != at {
				store.disarm_timer(prior_at.at(), TimerKind::Seal, key)?;
			}
		}
		store.arm_timer(at.at(), TimerKind::Seal, key)
	}
}

pub fn disarm_seal(store: &mut dyn TimerStore, rule: SealRule, key: &EncodedKey, horizon: u64) -> Result<()> {
	store.disarm_timer(rule.seal_instant_from_order(horizon).at(), TimerKind::Seal, key)
}

pub struct EvictionGate {
	rule: EvictionRule,
}

impl EvictionGate {
	pub fn new(span: Duration) -> Self {
		Self {
			rule: EvictionRule::rolling(span),
		}
	}

	pub fn rearm(
		&self,
		store: &mut dyn TimerStore,
		key: &EncodedKey,
		before: Option<u64>,
		after: Option<u64>,
	) -> Result<()> {
		if before == after {
			return Ok(());
		}
		if let Some(before) = before {
			store.disarm_timer(self.rule.eviction_instant_from_order(before).at(), TimerKind::Seal, key)?;
		}
		if let Some(after) = after {
			store.arm_timer(self.rule.eviction_instant_from_order(after).at(), TimerKind::Seal, key)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::at_millis;

	use super::*;
	use crate::operator::state::{
		mock::{MockStore, RecordedTimer},
		seal::coord::Coord,
	};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn key() -> EncodedKey {
		EncodedKey::new(b"window".as_slice())
	}

	fn rule() -> SealRule {
		SealRule::tumbling(ms(1_000), ms(200))
	}

	fn order(millis: u64) -> u64 {
		at_millis(millis).to_order()
	}

	fn sealed_through(millis: u64) -> SealedThrough {
		SealedThrough::from_order(order(millis))
	}

	#[test]
	fn a_window_is_admitted_until_the_frontier_passes_its_whole_admissible_span() {
		// The gate is strict: a window whose seal instant lands exactly on the frontier is still
		// open, because the wheel fires inclusively at that instant and has not fired yet. Sealing
		// one millisecond early drops rows that were still legitimately late.
		let gate = SealGate::new(rule(), Some(sealed_through(6_201)), None);

		assert!(!gate.admits(order(5_000)), "5_000 + 1_200 + 1 == the frontier, so it is sealed");
		assert!(gate.admits(order(5_001)), "one millisecond later the seal instant is past the frontier");
	}

	#[test]
	fn arming_a_moved_horizon_disarms_the_instant_it_replaces() {
		// The wheel holds one entry per (instant, kind, key). Re-arming an advanced horizon without
		// disarming the old instant leaves a stale timer that fires early and seals a window still
		// taking rows.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(rule(), None, None);

		gate.arm(&mut store, &key(), Some(order(4_000)), order(5_000)).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::disarmed(at_millis(5_201), TimerKind::Seal, key()),
				RecordedTimer::armed(at_millis(6_201), TimerKind::Seal, key()),
			]
		);
	}

	#[test]
	fn arming_an_unmoved_horizon_leaves_the_existing_timer_alone() {
		// Disarming and re-arming the same instant is not a no-op on a wheel that dedups by entry -
		// the disarm removes it and a failure between the two loses the seal. Comparing resolved
		// instants rather than horizons is what makes it safe, since two horizons can share one.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(rule(), None, None);

		gate.arm(&mut store, &key(), Some(order(5_000)), order(5_000)).unwrap();

		assert_eq!(store.timers(), &[RecordedTimer::armed(at_millis(6_201), TimerKind::Seal, key())]);
	}

	#[test]
	fn a_window_seen_for_the_first_time_arms_without_a_disarm() {
		// A brand-new window has no prior instant, and issuing a disarm for one would remove
		// whatever unrelated entry happens to sit at that instant for the same key.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(rule(), None, None);

		gate.arm(&mut store, &key(), None, order(5_000)).unwrap();

		assert_eq!(store.timers(), &[RecordedTimer::armed(at_millis(6_201), TimerKind::Seal, key())]);
	}

	#[test]
	fn eviction_rearms_on_the_bare_span_and_never_on_the_seal_instant() {
		// Rolling eviction is a retention boundary, not a gate, so it carries neither the seal nor
		// the strict-gate +1. Both mistakes silently keep too much state on every group, forever.
		let mut store = MockStore::recording_timers();
		let gate = EvictionGate::new(ms(1_000));

		gate.rearm(&mut store, &key(), Some(order(4_000)), Some(order(5_000))).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::disarmed(at_millis(5_000), TimerKind::Seal, key()),
				RecordedTimer::armed(at_millis(6_000), TimerKind::Seal, key()),
			]
		);
	}

	#[test]
	fn an_unchanged_oldest_coordinate_touches_no_timer_at_all() {
		// Rolling re-derives its earliest expiry every batch and most batches do not move it. A
		// disarm/arm pair each time rewrites the wheel entry for groups whose expiry is unchanged,
		// turning one seal timer into per-batch wheel churn.
		let mut store = MockStore::recording_timers();
		let gate = EvictionGate::new(ms(1_000));

		gate.rearm(&mut store, &key(), Some(order(4_000)), Some(order(4_000))).unwrap();
		gate.rearm(&mut store, &key(), None, None).unwrap();

		assert!(store.timers().is_empty());
	}

	#[test]
	fn a_group_that_empties_disarms_without_arming_anything_new() {
		// When the last coordinate leaves a rolling group there is no next expiry, so the standing
		// timer must come down or it seals an empty group forever - on a keyed wheel, one live
		// entry per group that ever drained.
		let mut store = MockStore::recording_timers();
		let gate = EvictionGate::new(ms(1_000));

		gate.rearm(&mut store, &key(), Some(order(4_000)), None).unwrap();

		assert_eq!(store.timers(), &[RecordedTimer::disarmed(at_millis(5_000), TimerKind::Seal, key())]);
	}

	#[test]
	fn disarming_targets_the_seal_instant_the_horizon_resolves_to() {
		// A session that closes early disarms by horizon, and the instant it computes must be
		// byte-identical to the one arm() wrote or the entry is orphaned. A hand-rolled
		// `horizon + span` at the call site would miss the +1 and leave the real timer armed.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(rule(), None, None);

		gate.arm(&mut store, &key(), None, order(5_000)).unwrap();
		disarm_seal(&mut store, rule(), &key(), order(5_000)).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::armed(at_millis(6_201), TimerKind::Seal, key()),
				RecordedTimer::disarmed(at_millis(6_201), TimerKind::Seal, key()),
			]
		);
	}
}
