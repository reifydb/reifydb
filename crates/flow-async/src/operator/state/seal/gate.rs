// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::state::timer::{TimerKind, TimerMove, TimerStore};
use reifydb_value::{Result, value::duration::Duration};

use crate::operator::state::seal::rule::{EvictionRule, SealRule};

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

pub fn seal_move(rule: SealRule, key: EncodedKey, before: Option<u64>, after: Option<u64>) -> TimerMove {
	TimerMove {
		key,
		before: before.map(|order| rule.seal_instant_from_order(order).at()),
		after: after.map(|order| rule.seal_instant_from_order(order).at()),
	}
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
	use crate::operator::state::mock::{MockStore, RecordedTimer};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn key() -> EncodedKey {
		EncodedKey::new(b"window".as_slice())
	}

	fn rule() -> SealRule {
		SealRule::tumbling(ms(1_000), ms(200))
	}

	fn order(millis: i64) -> u64 {
		at_millis(millis).to_order()
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
	fn forget_rearms_on_the_bare_span_and_never_on_the_seal_instant() {
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
