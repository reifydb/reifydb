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
}

pub fn rearm_seal(
	store: &mut dyn TimerStore,
	rule: SealRule,
	key: &EncodedKey,
	before: Option<u64>,
	after: Option<u64>,
) -> Result<()> {
	if before == after {
		return Ok(());
	}
	if let Some(before) = before {
		store.disarm_timer(rule.seal_instant_from_order(before).at(), TimerKind::Seal, key)?;
	}
	if let Some(after) = after {
		store.arm_timer(rule.seal_instant_from_order(after).at(), TimerKind::Seal, key)?;
	}
	Ok(())
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
	fn a_moved_earliest_expiry_disarms_the_instant_it_replaces() {
		// Without the disarm the wheel keeps a stale entry for every batch that moves the minimum.
		let mut store = MockStore::recording_timers();

		rearm_seal(&mut store, rule(), &key(), Some(order(4_000)), Some(order(5_000))).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::disarmed(at_millis(5_201), TimerKind::Seal, key()),
				RecordedTimer::armed(at_millis(6_201), TimerKind::Seal, key()),
			]
		);
	}

	#[test]
	fn an_unmoved_earliest_expiry_touches_no_timer_at_all() {
		// A disarm/arm pair on one instant is never a no-op; a failure between the two loses the seal.
		let mut store = MockStore::recording_timers();

		rearm_seal(&mut store, rule(), &key(), Some(order(5_000)), Some(order(5_000))).unwrap();
		rearm_seal(&mut store, rule(), &key(), None, None).unwrap();

		assert!(store.timers().is_empty());
	}

	#[test]
	fn a_first_indexed_window_arms_without_a_disarm() {
		// An operator holding nothing has no prior instant, and a disarm would remove an unrelated entry.
		let mut store = MockStore::recording_timers();

		rearm_seal(&mut store, rule(), &key(), None, Some(order(5_000))).unwrap();

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
	fn an_index_that_empties_disarms_without_arming_anything_new() {
		// A hand-rolled `horizon + span` would miss the seal step and orphan the entry arming wrote.
		let mut store = MockStore::recording_timers();

		rearm_seal(&mut store, rule(), &key(), None, Some(order(5_000))).unwrap();
		rearm_seal(&mut store, rule(), &key(), Some(order(5_000)), None).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::armed(at_millis(6_201), TimerKind::Seal, key()),
				RecordedTimer::disarmed(at_millis(6_201), TimerKind::Seal, key()),
			]
		);
	}
}
