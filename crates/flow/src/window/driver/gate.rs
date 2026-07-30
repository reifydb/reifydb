// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::state::store::StateStore;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::window::policy::{EvictionPolicy, SealPolicy, SealedThrough};

pub struct SealGate {
	policy: SealPolicy,
	frontier: DateTime,
}

impl SealGate {
	pub fn new(policy: SealPolicy, ledger: Option<SealedThrough>, watermark: Option<DateTime>) -> Self {
		let ledger = ledger.map_or_else(DateTime::default, SealedThrough::at);
		Self {
			policy,
			frontier: watermark.map_or(ledger, |watermark| ledger.max(watermark)),
		}
	}

	pub fn policy(&self) -> SealPolicy {
		self.policy
	}

	pub fn frontier(&self) -> DateTime {
		self.frontier
	}

	pub fn admits(&self, horizon: u64) -> bool {
		self.policy.seal_instant_from_order(horizon).at() > self.frontier
	}

	pub fn arm<S: StateStore>(
		&self,
		store: &mut S,
		key: &EncodedKey,
		prior_horizon: Option<u64>,
		horizon: u64,
	) -> Result<()> {
		let at = self.policy.seal_instant_from_order(horizon);
		if let Some(prior_horizon) = prior_horizon {
			let prior_at = self.policy.seal_instant_from_order(prior_horizon);
			if prior_at != at {
				store.disarm_timer(prior_at.at(), TimerKind::Seal, key)?;
			}
		}
		store.arm_timer(at.at(), TimerKind::Seal, key)
	}
}

pub fn disarm_seal<S: StateStore>(store: &mut S, policy: SealPolicy, key: &EncodedKey, horizon: u64) -> Result<()> {
	store.disarm_timer(policy.seal_instant_from_order(horizon).at(), TimerKind::Seal, key)
}

pub struct EvictionGate {
	policy: EvictionPolicy,
}

impl EvictionGate {
	pub fn new(span: Duration) -> Self {
		Self {
			policy: EvictionPolicy::rolling(span),
		}
	}

	pub fn rearm<S: StateStore>(
		&self,
		store: &mut S,
		key: &EncodedKey,
		before: Option<u64>,
		after: Option<u64>,
	) -> Result<()> {
		if before == after {
			return Ok(());
		}
		if let Some(before) = before {
			store.disarm_timer(self.policy.eviction_instant_from_order(before).at(), TimerKind::Seal, key)?;
		}
		if let Some(after) = after {
			store.arm_timer(self.policy.eviction_instant_from_order(after).at(), TimerKind::Seal, key)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::window::engine::test_support::{MockStore, RecordedTimer};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	fn key() -> EncodedKey {
		EncodedKey::new(b"window".as_slice())
	}

	fn policy() -> SealPolicy {
		SealPolicy::tumbling(ms(1_000), ms(200))
	}

	fn sealed_through(millis: u64) -> SealedThrough {
		SealedThrough::from_order(millis)
	}

	#[test]
	fn the_frontier_is_the_ledger_and_the_watermark_merged_upward() {
		// The frontier is what a window is measured against, and it must never move
		// backwards when one of its two inputs lags. The flow watermark is a min-merge across
		// sources, so a newly attached source drags it DOWN; the ledger only moves forward.
		// Taking the max is what stops a lagging watermark from re-admitting rows into windows
		// this node has already sealed and emitted.
		let lagging = SealGate::new(policy(), Some(sealed_through(9_000)), Some(at(3_000)));
		let leading = SealGate::new(policy(), Some(sealed_through(3_000)), Some(at(9_000)));

		assert_eq!(lagging.frontier(), at(9_000));
		assert_eq!(leading.frontier(), at(9_000));
	}

	#[test]
	fn an_empty_ledger_and_no_watermark_leave_the_frontier_at_the_epoch() {
		// A node that has never fired a timer and sits under a flow with no watermark must
		// admit everything. The epoch is the only frontier that does that, and it is reached
		// through none on BOTH inputs - which is why neither may default to "now".
		let gate = SealGate::new(policy(), None, None);

		assert_eq!(gate.frontier(), DateTime::default());
		assert!(gate.admits(0));
	}

	#[test]
	fn the_guest_reaches_the_same_frontier_as_the_host_from_the_same_two_inputs() {
		// The built-in operator and an SDK operator must seal identically, so both build
		// the frontier from the SAME two inputs - the node's own seal ledger and the flow
		// watermark - merged upward. The host reads the watermark off its FlowTransaction; the
		// guest reads it through the flow_watermark ABI callback added for exactly this. Before
		// that callback existed the guest had no watermark at all and derived its frontier from
		// the MAX COORDINATE IN THE ARRIVING BATCH, which is why a guest window whose feed
		// stopped never sealed: with no arrivals there was nothing to advance it.
		let ledger = sealed_through(9_000);
		let watermark = at(3_000);

		let host_side = SealGate::new(policy(), Some(ledger), Some(watermark));
		let guest_side = SealGate::new(policy(), Some(ledger), Some(watermark));

		assert_eq!(host_side.frontier(), guest_side.frontier());
		assert_eq!(host_side.admits(5_000), guest_side.admits(5_000));
	}

	#[test]
	fn a_window_is_admitted_until_the_frontier_passes_its_whole_admissible_span() {
		// The gate is STRICT. A window whose seal instant lands exactly on the frontier
		// is still open, because the wheel fires inclusively at that instant and has not fired
		// yet. Sealing one millisecond early drops rows that were still legitimately late.
		let gate = SealGate::new(policy(), Some(sealed_through(6_201)), None);

		assert!(!gate.admits(5_000), "5_000 + 1_200 + 1 == the frontier, so it is sealed");
		assert!(gate.admits(5_001), "one millisecond later the seal instant is past the frontier");
	}

	#[test]
	fn arming_a_moved_horizon_disarms_the_instant_it_replaces() {
		// The wheel holds one entry per (instant, kind, key). Re-arming a window whose
		// horizon advanced without disarming the old instant leaves a stale timer that fires
		// early and seals a window still taking rows. This is the single reason arm() takes the
		// prior horizon at all.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(policy(), None, None);

		gate.arm(&mut store, &key(), Some(4_000), 5_000).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::disarmed(at(5_201), TimerKind::Seal, key()),
				RecordedTimer::armed(at(6_201), TimerKind::Seal, key()),
			]
		);
	}

	#[test]
	fn arming_an_unmoved_horizon_leaves_the_existing_timer_alone() {
		// Batches routinely touch a window without advancing its horizon. Disarming and
		// re-arming the SAME instant is not a no-op on a wheel that dedups by entry - the disarm
		// removes it and a failure between the two loses the seal entirely. Comparing the
		// resolved INSTANTS rather than the horizons is what makes this safe, because two
		// distinct horizons inside one millisecond resolve to one instant.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(policy(), None, None);

		gate.arm(&mut store, &key(), Some(5_000), 5_000).unwrap();

		assert_eq!(store.timers(), &[RecordedTimer::armed(at(6_201), TimerKind::Seal, key())]);
	}

	#[test]
	fn a_window_seen_for_the_first_time_arms_without_a_disarm() {
		// A brand-new window has no prior instant, and issuing a disarm for one would remove
		// whatever unrelated entry happens to sit at that instant for the same key.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(policy(), None, None);

		gate.arm(&mut store, &key(), None, 5_000).unwrap();

		assert_eq!(store.timers(), &[RecordedTimer::armed(at(6_201), TimerKind::Seal, key())]);
	}

	#[test]
	fn eviction_rearms_on_the_bare_span_and_never_on_the_seal_instant() {
		// Rolling eviction is a retention boundary, not a gate, so it carries neither
		// the grace nor the strict-gate +1. Both mistakes are silent: the window simply keeps
		// one grace-period-plus-a-millisecond too much state, forever, on every group.
		let mut store = MockStore::recording_timers();
		let gate = EvictionGate::new(ms(1_000));

		gate.rearm(&mut store, &key(), Some(4_000), Some(5_000)).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::disarmed(at(5_000), TimerKind::Seal, key()),
				RecordedTimer::armed(at(6_000), TimerKind::Seal, key()),
			]
		);
	}

	#[test]
	fn an_unchanged_oldest_coordinate_touches_no_timer_at_all() {
		// Rolling re-derives its earliest expiry on every batch, and most batches do not
		// move it. Emitting a disarm/arm pair each time would rewrite the wheel entry on every
		// change for every group - and, worse, would do it for groups whose expiry is unchanged,
		// which is how a hot flow turns a single seal timer into per-batch wheel churn.
		let mut store = MockStore::recording_timers();
		let gate = EvictionGate::new(ms(1_000));

		gate.rearm(&mut store, &key(), Some(4_000), Some(4_000)).unwrap();
		gate.rearm(&mut store, &key(), None, None).unwrap();

		assert!(store.timers().is_empty());
	}

	#[test]
	fn a_group_that_empties_disarms_without_arming_anything_new() {
		// When the last coordinate leaves a rolling group there is no next expiry, so the
		// standing timer must come down. Leaving it armed fires a seal against an empty group
		// forever, and on a keyed wheel that is one live entry per group that ever drained.
		let mut store = MockStore::recording_timers();
		let gate = EvictionGate::new(ms(1_000));

		gate.rearm(&mut store, &key(), Some(4_000), None).unwrap();

		assert_eq!(store.timers(), &[RecordedTimer::disarmed(at(5_000), TimerKind::Seal, key())]);
	}

	#[test]
	fn disarming_targets_the_seal_instant_the_horizon_resolves_to() {
		// A session that closes early disarms by horizon, and the instant it computes
		// must be byte-identical to the one arm() wrote or the entry is orphaned. Sharing the
		// SealPolicy is what guarantees that; a hand-rolled `horizon + span` at the call site
		// would miss the +1 and leave the real timer armed.
		let mut store = MockStore::recording_timers();
		let gate = SealGate::new(policy(), None, None);

		gate.arm(&mut store, &key(), None, 5_000).unwrap();
		disarm_seal(&mut store, policy(), &key(), 5_000).unwrap();

		assert_eq!(
			store.timers(),
			&[
				RecordedTimer::armed(at(6_201), TimerKind::Seal, key()),
				RecordedTimer::disarmed(at(6_201), TimerKind::Seal, key()),
			]
		);
	}
}
