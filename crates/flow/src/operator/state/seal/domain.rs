// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::state::timer::{StateStore, TimerKind, TimerStore};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::operator::state::seal::{coord::Coord, ledger::SealLedger, policy::SEAL_GATE_STEP};

pub trait SealDomain: Coord {
	type Lateness: Copy + Debug + Send + Sync;

	fn arms_timer() -> bool;

	fn lateness_duration(lateness: Self::Lateness) -> Option<Duration>;

	fn observe(store: &mut (impl StateStore + TimerStore), newest: Self, lateness: Self::Lateness) -> Result<()>;

	fn frontier(store: &mut (impl StateStore + TimerStore)) -> Result<Self>;

	fn horizon(frontier: Self, lateness: Self::Lateness) -> Self;
}

impl SealDomain for DateTime {
	type Lateness = Duration;

	fn arms_timer() -> bool {
		true
	}

	fn lateness_duration(lateness: Duration) -> Option<Duration> {
		Some(lateness)
	}

	fn observe(store: &mut (impl StateStore + TimerStore), newest: Self, lateness: Duration) -> Result<()> {
		let at = newest.saturating_add(lateness).saturating_add(SEAL_GATE_STEP);
		store.arm_timer(at, TimerKind::Seal, &EncodedKey::new(Vec::new()))
	}

	fn frontier(store: &mut (impl StateStore + TimerStore)) -> Result<Self> {
		let ledger = SealLedger::read_order(store)?.unwrap_or(0);
		let watermark = store.flow_watermark()?.map_or(0, |at| at.to_order());
		Ok(<DateTime as Coord>::from_order(ledger.max(watermark)))
	}

	fn horizon(frontier: Self, lateness: Duration) -> Self {
		frontier.saturating_sub(lateness)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::at_millis;

	use super::*;
	use crate::{
		operator::state::{
			mock::{MockStore, RecordedTimer},
			seal::ledger::{FiredAt, seal_ledger_key},
		},
		timer::Timer,
	};

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn timer(millis: u64) -> Timer {
		Timer {
			due: DateTime::from_millis(millis),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		}
	}

	#[test]
	fn observing_a_wall_clock_batch_arms_one_past_the_admissible_span() {
		// the bare sum seals a window still taking late rows, so the arm must carry the strict gate step
		let mut store = MockStore::recording_timers();

		DateTime::observe(&mut store, at_millis(5_000), ms(200)).unwrap();

		assert_eq!(
			store.timers(),
			&[RecordedTimer::armed(at_millis(5_201), TimerKind::Seal, EncodedKey::new(Vec::new()))]
		);
	}

	#[test]
	fn the_wall_clock_frontier_merges_the_ledger_and_the_watermark_upward() {
		// without the max a newly attached source drags the watermark down and re-admits sealed rows
		let mut lagging_watermark = MockStore::default().with_flow_watermark(at_millis(3_000));
		SealLedger::advance(&mut lagging_watermark, FiredAt::of(&timer(9_000))).unwrap();

		let mut lagging_ledger = MockStore::default().with_flow_watermark(at_millis(9_000));
		SealLedger::advance(&mut lagging_ledger, FiredAt::of(&timer(3_000))).unwrap();

		assert_eq!(DateTime::frontier(&mut lagging_watermark).unwrap(), at_millis(9_000));
		assert_eq!(DateTime::frontier(&mut lagging_ledger).unwrap(), at_millis(9_000));
	}

	#[test]
	fn an_untouched_wall_clock_operator_has_its_frontier_at_the_epoch() {
		// neither input may default to "now", or a operator that never fired a timer drops every row
		let mut store = MockStore::default();

		assert_eq!(DateTime::frontier(&mut store).unwrap(), DateTime::default());
		assert!(store.state_get(&seal_ledger_key()).unwrap().is_none());
	}

	#[test]
	fn a_wall_clock_horizon_is_the_frontier_less_the_lateness_and_never_wraps() {
		// a horizon below the epoch must clamp, or it wraps high and reports every window sealed
		assert_eq!(DateTime::horizon(at_millis(6_060_000), ms(60_000)), at_millis(6_000_000));
		assert_eq!(DateTime::horizon(at_millis(1_000), ms(60_000)), DateTime::default());
	}

	#[test]
	fn the_wall_clock_domain_seals_on_the_wheel_and_declares_its_lateness_to_the_flow() {
		// the flow frontier reads this as a wall-clock span, so none would stop holding the watermark back
		assert!(DateTime::arms_timer());
		assert_eq!(DateTime::lateness_duration(ms(65_000)), Some(ms(65_000)));
	}
}
