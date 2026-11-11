// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{WindowKind, WindowSize, WindowSizeDomain},
	error::CoreError,
	operator_with::{ApplyWith, WindowKnobs},
	state::timer::TimerKind,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{
	operator::state::seal::{
		coord::Coord,
		gate::{EvictionGate, rearm_seal},
		ledger::SealLedger,
		rule::{SEAL_GATE_STEP, SealRule},
	},
	window::{
		coord::DateTimeCoord, driver::store::WindowStore, kind::rolling::RollingOverTime,
		settings::WindowSettings,
	},
};

pub trait SealDomain: Coord {
	type SealSpan: Copy + Debug + Send + Sync;

	const SIZE_DOMAIN: WindowSizeDomain;

	fn arms_timer() -> bool;

	fn seal_span_of(with: &ApplyWith) -> Result<Option<Self::SealSpan>>;

	fn window_settings_of(with: &ApplyWith) -> Result<WindowSettings<Self>>;

	fn observe(store: &mut dyn WindowStore, newest: Self, seal_span: Self::SealSpan) -> Result<()>;

	fn frontier(store: &mut dyn WindowStore) -> Result<Self>;

	fn horizon(frontier: Self, seal_span: Self::SealSpan) -> Self;

	fn rearm_decay(
		store: &mut dyn WindowStore,
		settings: &WindowSettings<Self>,
		before: Option<u64>,
		after: Option<u64>,
	) -> Result<()>;

	fn rearm_seal(
		store: &mut dyn WindowStore,
		seal_span: Self::SealSpan,
		before: Option<u64>,
		after: Option<u64>,
	) -> Result<()>;

	fn arm_reap(store: &mut dyn WindowStore, fired: DateTime, seal_span: Self::SealSpan) -> Result<()>;

	fn idle_retention_of(with: &ApplyWith) -> Result<Option<Self::Span>> {
		with.reject_retention()?;
		Ok(None)
	}
}

const ROLLING_DECAY_TIMER_KEY: &[u8] = b"rolling-decay";

impl SealDomain for DateTimeCoord {
	type SealSpan = Duration;

	const SIZE_DOMAIN: WindowSizeDomain = WindowSizeDomain::Time;

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
		Ok(SealRule::for_window(kind, lateness).map(|rule| rule.seal_span().duration()))
	}

	fn idle_retention_of(with: &ApplyWith) -> Result<Option<Duration>> {
		with.check_retention()?;
		Ok(with.retention)
	}

	fn window_settings_of(with: &ApplyWith) -> Result<WindowSettings<Self>> {
		let Some(kind) = &with.window else {
			return Err(CoreError::OperatorWithWindowMissing.into());
		};
		let (size, gap) = match with.window_session_gap() {
			Some(gap) => (None, Some(gap)),
			None => (Some(with.window_duration()?), None),
		};
		let knobs = WindowKnobs::from_operator_with(with)?;
		let (pane, lag) = match kind {
			WindowKind::Rolling {
				pane,
				lag,
				..
			} => (*pane, *lag),
			_ => (None, None),
		};
		Ok(WindowSettings {
			kind: kind.clone(),
			size,
			pane,
			lag,
			slide: with.window_slide_duration()?,
			gap,
			lateness: knobs.lateness.unwrap_or_else(Duration::zero),
			immutable: knobs.immutable,
		})
	}

	fn observe(store: &mut dyn WindowStore, newest: Self, seal_span: Duration) -> Result<()> {
		let at = newest.0.saturating_add(seal_span).saturating_add(SEAL_GATE_STEP);
		store.arm_timer(at, TimerKind::Seal, &EncodedKey::new(Vec::new()))
	}

	fn frontier(store: &mut dyn WindowStore) -> Result<Self> {
		let ledger = SealLedger::read_order(store)?.unwrap_or(0);
		let watermark = store.flow_watermark()?.map_or(0, |at| at.to_order());
		Ok(Self::from_order(ledger.max(watermark)))
	}

	fn horizon(frontier: Self, seal_span: Duration) -> Self {
		Self(SealRule::of(seal_span)
			.horizon_at(frontier)
			.map_or(DateTime::MIN, |horizon| horizon.0.saturating_add(Duration::from_nanoseconds_const(1))))
	}

	fn rearm_decay(
		store: &mut dyn WindowStore,
		settings: &WindowSettings<Self>,
		before: Option<u64>,
		after: Option<u64>,
	) -> Result<()> {
		let span = RollingOverTime::new(settings.fixed_size(), settings.lag.unwrap_or_default()).span();
		EvictionGate::new(span).rearm(store, &EncodedKey::new(ROLLING_DECAY_TIMER_KEY), before, after)
	}

	fn rearm_seal(
		store: &mut dyn WindowStore,
		seal_span: Duration,
		before: Option<u64>,
		after: Option<u64>,
	) -> Result<()> {
		rearm_seal(store, SealRule::of(seal_span), &EncodedKey::new(Vec::new()), before, after)
	}

	fn arm_reap(store: &mut dyn WindowStore, fired: DateTime, seal_span: Duration) -> Result<()> {
		store.arm_timer(fired.saturating_add(seal_span), TimerKind::Maintenance, &EncodedKey::new(Vec::new()))
	}
}

#[cfg(test)]
mod tests {
	use std::slice::from_ref;

	use reifydb_core::{
		common::{OnEmpty, WindowSize},
		operator_with::WithSpan,
		state::timer::StateStore,
	};
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

	fn timer(millis: i64) -> Timer {
		Timer {
			due: DateTime::from_millis(millis),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		}
	}

	fn decay_due(lag: Option<Duration>) -> DateTime {
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(Duration::from_seconds(5).unwrap()),
				lag,
				pane: Some(Duration::from_seconds(1).unwrap()),
				on_empty: OnEmpty::Remove,
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};
		let settings = DateTimeCoord::window_settings_of(&with).unwrap();
		let mut store = MockStore::recording_timers();
		let oldest = DateTimeCoord(at_millis(1_000)).to_order();
		DateTimeCoord::rearm_decay(&mut store, &settings, None, Some(oldest)).unwrap();
		match store.timers() {
			[RecordedTimer::Armed(timer)] => timer.due,
			other => panic!("expected one armed decay timer, got {other:?}"),
		}
	}

	#[test]
	fn a_lagged_rolling_decay_timer_fires_lag_later_than_an_unlagged_one() {
		// A decay armed at size alone frees a lagged group while its lagged window still holds panes.
		let lag = Duration::from_seconds(2).unwrap();

		assert_eq!(decay_due(Some(lag)), decay_due(None).saturating_add(lag));
	}

	#[test]
	fn observing_a_wall_clock_batch_arms_one_past_the_seal_span() {
		// the bare sum seals a window still taking late rows, so the arm must carry the strict gate step
		let mut store = MockStore::recording_timers();

		DateTimeCoord::observe(&mut store, DateTimeCoord(at_millis(5_000)), ms(200)).unwrap();

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

		assert_eq!(DateTimeCoord::frontier(&mut lagging_watermark).unwrap(), DateTimeCoord(at_millis(9_000)));
		assert_eq!(DateTimeCoord::frontier(&mut lagging_ledger).unwrap(), DateTimeCoord(at_millis(9_000)));
	}

	#[test]
	fn an_untouched_wall_clock_operator_has_its_frontier_at_datetime_min() {
		// neither input may default to "now", or a operator that never fired a timer drops every row
		let mut store = MockStore::default();

		assert_eq!(DateTimeCoord::frontier(&mut store).unwrap(), DateTimeCoord(DateTime::MIN));
		assert!(store.state_get(from_ref(&seal_ledger_key())).unwrap().pop().flatten().is_none());
	}

	#[test]
	fn a_wall_clock_horizon_is_the_frontier_less_the_lateness_and_the_gate_step_and_may_go_pre_epoch() {
		// wrapping high instead of going negative would report every window sealed
		let step_past =
			|millis| DateTimeCoord(at_millis(millis).saturating_add(Duration::from_nanoseconds_const(1)));
		assert_eq!(
			DateTimeCoord::horizon(DateTimeCoord(at_millis(6_060_000)), ms(60_000)),
			step_past(5_999_999)
		);
		assert_eq!(DateTimeCoord::horizon(DateTimeCoord(at_millis(1_000)), ms(60_000)), step_past(-59_001));
	}

	#[test]
	fn the_wall_clock_domain_seals_on_the_wheel() {
		// the flow frontier reads this domain's span in wall-clock time, so it must arm off the timer wheel
		assert!(DateTimeCoord::arms_timer());
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
			retention: None,
		};

		assert!(DateTimeCoord::seal_span_of(&with).is_err());
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
				on_empty: OnEmpty::Remove,
			}),
			lateness: Some(WithSpan::Duration(Duration::from_seconds(20).unwrap())),
			immutable: Some(WithSpan::Duration(Duration::from_seconds(15).unwrap())),
			retention: None,
		};
		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		let rolling = DateTimeCoord::window_settings_of(&rolling).unwrap();
		let tumbling = DateTimeCoord::window_settings_of(&tumbling).unwrap();

		assert_eq!(rolling.size, Some(Duration::from_seconds(3600).unwrap()));
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
			retention: None,
		};

		assert!(DateTimeCoord::window_settings_of(&ApplyWith::default()).is_err());
		assert!(DateTimeCoord::window_settings_of(&count).is_err());
	}

	#[test]
	fn the_wall_clock_settings_carry_the_slide_of_a_sliding_window() {
		// a slide that came back as none would make create panic on every sliding view
		let sliding = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Duration(Duration::from_seconds(600).unwrap()),
				slide: WindowSize::Duration(Duration::from_seconds(300).unwrap()),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		let settings = DateTimeCoord::window_settings_of(&sliding).unwrap();

		assert_eq!(settings.size, Some(Duration::from_seconds(600).unwrap()));
		assert_eq!(settings.slide, Some(Duration::from_seconds(300).unwrap()));
	}

	#[test]
	fn the_wall_clock_settings_carry_the_gap_of_a_session_window() {
		// A session read as a sized window would fail create or seal on a size it does not have.
		let session = ApplyWith {
			window: Some(WindowKind::Session {
				gap: Duration::from_seconds(30).unwrap(),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		let settings = DateTimeCoord::window_settings_of(&session).unwrap();

		assert_eq!(settings.gap, Some(Duration::from_seconds(30).unwrap()));
		assert_eq!(settings.size, None, "a session has no fixed size");
		assert_eq!(settings.slide, None);
	}
}
