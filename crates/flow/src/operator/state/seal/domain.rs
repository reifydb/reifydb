// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	error::CoreError,
	operator_with::{ApplyWith, WindowSealing},
	state::timer::{StateStore, TimerKind, TimerStore},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{
	operator::state::seal::{
		coord::Coord,
		ledger::SealLedger,
		rule::{SEAL_GATE_STEP, SealRule},
	},
	window::settings::WindowSettings,
};

pub trait SealDomain: Coord {
	type SealSpan: Copy + Debug + Send + Sync;

	fn arms_timer() -> bool;

	fn seal_span_of(with: &ApplyWith) -> Result<Option<Self::SealSpan>>;

	fn window_settings_of(with: &ApplyWith) -> Result<WindowSettings<Self>>;

	fn observe(store: &mut (impl StateStore + TimerStore), newest: Self, seal_span: Self::SealSpan) -> Result<()>;

	fn frontier(store: &mut (impl StateStore + TimerStore)) -> Result<Self>;

	fn horizon(frontier: Self, seal_span: Self::SealSpan) -> Self;
}

impl SealDomain for DateTime {
	type SealSpan = Duration;

	fn arms_timer() -> bool {
		true
	}

	fn seal_span_of(with: &ApplyWith) -> Result<Option<Duration>> {
		let Some(kind) = &with.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		if let Some(WindowSize::Count(count)) = kind.size() {
			return Err(CoreError::OperatorWithWindowSizeCount {
				count: *count,
			}
			.into());
		}
		let lateness = with.lateness_duration()?.unwrap_or_else(Duration::zero);
		Ok(SealRule::for_window(kind, lateness).map(|rule| rule.admissible().duration()))
	}

	fn window_settings_of(with: &ApplyWith) -> Result<WindowSettings<Self>> {
		let Some(kind) = &with.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		let size = with.window_duration()?;
		let sealing = WindowSealing::from_operator_with(with)?;
		let pane = match kind {
			WindowKind::Rolling {
				pane,
				..
			} => *pane,
			_ => None,
		};
		Ok(WindowSettings {
			kind: kind.clone(),
			size,
			pane,
			lateness: sealing.lateness.unwrap_or_else(Duration::zero),
			immutable: sealing.immutable,
		})
	}

	fn observe(store: &mut (impl StateStore + TimerStore), newest: Self, seal_span: Duration) -> Result<()> {
		let at = newest.saturating_add(seal_span).saturating_add(SEAL_GATE_STEP);
		store.arm_timer(at, TimerKind::Seal, &EncodedKey::new(Vec::new()))
	}

	fn frontier(store: &mut (impl StateStore + TimerStore)) -> Result<Self> {
		let ledger = SealLedger::read_order(store)?.unwrap_or(0);
		let watermark = store.flow_watermark()?.map_or(0, |at| at.to_order());
		Ok(<DateTime as Coord>::from_order(ledger.max(watermark)))
	}

	fn horizon(frontier: Self, seal_span: Duration) -> Self {
		frontier.saturating_sub(seal_span)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::WindowSize, operator_with::WithSpan};
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
	fn the_wall_clock_domain_seals_on_the_wheel() {
		// the flow frontier reads this domain's span in wall-clock time, so it must arm off the timer wheel
		assert!(DateTime::arms_timer());
	}

	#[test]
	fn the_wall_clock_domain_rejects_a_count_size() {
		// a count-sized window has no wall-clock span, so the wheel domain must refuse rather than guess one
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
		};

		assert!(DateTime::seal_span_of(&with).is_err());
	}

	#[test]
	fn the_wall_clock_settings_carry_the_rolling_pane_and_none_for_a_tumbling_window() {
		// the pane lives only on a rolling window; a tumbling one that reported a pane would make the engine
		// merge panes it never built
		let rolling = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(Duration::from_seconds(3600).unwrap()),
				lag: None,
				pane: Some(Duration::from_seconds(1).unwrap()),
			}),
			lateness: Some(WithSpan::Duration(Duration::from_seconds(20).unwrap())),
			immutable: Some(WithSpan::Duration(Duration::from_seconds(15).unwrap())),
		};
		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
			}),
			lateness: None,
			immutable: None,
		};

		let rolling = DateTime::window_settings_of(&rolling).unwrap();
		let tumbling = DateTime::window_settings_of(&tumbling).unwrap();

		assert_eq!(rolling.size, Duration::from_seconds(3600).unwrap());
		assert_eq!(rolling.pane, Some(Duration::from_seconds(1).unwrap()));
		assert_eq!(rolling.lateness, Duration::from_seconds(20).unwrap());
		assert_eq!(rolling.immutable, Some(Duration::from_seconds(15).unwrap()));
		assert_eq!(tumbling.pane, None);
		assert_eq!(tumbling.lateness, Duration::zero());
		assert_eq!(tumbling.immutable, None);
	}

	#[test]
	fn the_wall_clock_settings_refuse_a_missing_window_and_a_count_size() {
		// without a window there is nothing to size, and a count has no wall-clock span; both must fail loud
		let count = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
		};

		assert!(DateTime::window_settings_of(&ApplyWith::default()).is_err());
		assert!(DateTime::window_settings_of(&count).is_err());
	}
}
