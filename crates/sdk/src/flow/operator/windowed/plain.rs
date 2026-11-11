// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::StateCodec,
};
use reifydb_core::{
	common::{OnEmpty, WindowKind, WindowRequirements, WindowSizeDomain},
	error::CoreError,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::GroupId,
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
	state::timer::TimerKind,
};
use reifydb_flow_async::{
	operator::state::seal::{coord::Coord, domain::SealDomain, ledger::FiredAt},
	window::{
		driver::{
			rolling::{RollingRunner, RoutedPanes, RowHooks, route_rolling},
			route::Routed,
			runner::WindowRunner,
			session::route_session,
			store::WindowStore,
		},
		engine::{
			pane::{PaneEngine, PaneSpans, RollingEmit},
			tumbling::TumblingEngine,
		},
		kind::sliding::fits_slide,
		meta::NoKey,
		settings::WindowSettings,
		span::window_row_key,
	},
};
use reifydb_value::{
	Result as ValueResult,
	config::ExtensionParams,
	error::Error,
	util::hash::xxh3_128,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{operator::OperatorColumn, row::Row},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::ChangeView,
		windowed::{
			fired_at,
			guest_as_host::GuestAsHost,
			hooks::SdkHooks,
			operator::{Contribution, Contributions, Emit, KindSet, WindowedOperator},
			publish::{Emitted, UpdateRow},
			rolling_engine, rolling_output,
			rows::SdkRows,
			window_engine_config,
		},
	},
};

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
struct RollingMode<A: Emit> {
	engine: PaneEngine<A::GroupKey, A::Coord, A::Accumulator>,
	runner: RollingRunner<A::Coord>,
	pane: <A::Coord as Coord>::Span,
	on_empty: OnEmpty,
}

enum RollingInput<A: WindowedOperator> {
	Batch(RoutedPanes<A::GroupKey, A::Coord, Contribution<A>>),
	Timer(TimerKind, FiredAt),
}

type RollingEmits<A> = Vec<RollingEmit<<A as WindowedOperator>::GroupKey, NoKey, <A as WindowedOperator>::Output>>;

pub struct PlainDriver<A>
where
	A: Emit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
	rolling: Option<RollingMode<A>>,
	runner: Option<WindowRunner<A::Coord>>,
	sessions: bool,
	seal_span: Option<SealSpan<A>>,
	settings: WindowSettings<A::Coord>,
}

impl<A> PlainDriver<A>
where
	A: Emit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn row_key(group: &A::GroupKey, window_start: A::Coord) -> EncodedKey {
		window_row_key(group.into_encoded_key(), window_start)
	}
}

impl<A> PlainDriver<A>
where
	A: Emit + Send + Sync + 'static,
	A::Output: Row,
	A::Output: Clone + StateCodec + HeapSize,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync,
	Contribution<A>: Send + Sync + StateCodec + HeapSize,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn rolling_emitted(emits: RollingEmits<A>) -> Emitted<A> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		for emit in emits {
			match (emit.prior, emit.post) {
				(None, Some(post)) => {
					emitted.0.push((RowNumber(post.row_number), post.first_time, post.last_value))
				}
				(Some(prior), Some(post)) => emitted.1.push((
					RowNumber(post.row_number),
					post.first_time,
					Some(prior.last_value),
					post.last_value,
				)),
				(Some(prior), None) => emitted.2.push((
					RowNumber(prior.row_number),
					prior.first_time,
					prior.last_value,
				)),
				(None, None) => {}
			}
		}
		emitted
	}

	fn run_rolling(
		aggregator: &A,
		mode: &mut RollingMode<A>,
		settings: &WindowSettings<A::Coord>,
		store: &mut dyn WindowStore,
		input: RollingInput<A>,
	) -> Result<Emitted<A>> {
		let pane = mode.pane;
		let new_accumulator = || aggregator.new_accumulator(settings);
		let finalize = |group: &A::GroupKey, newest: A::Coord, accumulator: &A::Accumulator| {
			rolling_output(aggregator, settings, pane, group, newest, accumulator)
		};
		let empty = |prior: &A::Output| -> ValueResult<Option<A::Output>> { Ok(aggregator.carry_empty(prior)) };
		let key = |_: &A::GroupKey| -> ValueResult<NoKey> { Ok(NoKey) };
		let mut hooks = RowHooks {
			new_accumulator: &new_accumulator,
			key: Some(&key),
			finalize: &finalize,
			on_empty: mode.on_empty,
			empty: &empty,
		};
		let emits = match input {
			RollingInput::Batch(routed) => {
				mode.runner.apply(&mut mode.engine, store, routed, &mut hooks)?
			}
			RollingInput::Timer(kind, fired) => {
				hooks.key = None;
				mode.runner.on_timer(&mut mode.engine, store, kind, fired, &mut hooks)?
			}
		};
		Ok(Self::rolling_emitted(emits))
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_plain_tumbling<C: GuestContext, V: ChangeView>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		runner: &mut WindowRunner<A::Coord>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		change: &V,
	) -> Result<Emitted<A>> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		let mut rows = SdkRows::new(aggregator, ctx, change);
		let routed: Routed<A::GroupKey, A::Coord, Contributions<A>> = runner.route(&mut rows)?;
		drop(rows);
		if routed.buckets.is_empty() {
			return Ok(emitted);
		}
		let emits =
			runner.apply(engine, &mut GuestAsHost(ctx), routed, &mut SdkHooks::new(aggregator, settings))?;
		emitted.0 = emits.inserts;
		emitted.1 = emits
			.updates
			.into_iter()
			.map(|(number, time, pre, post)| (number, time, Some(pre), post))
			.collect();
		emitted.2 = emits.removes;
		Ok(emitted)
	}

	fn apply_plain_session<C: GuestContext, V: ChangeView>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		runner: &mut WindowRunner<A::Coord>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		change: &V,
	) -> Result<Emitted<A>> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		let mut rows = SdkRows::new(aggregator, ctx, change);
		let routed = route_session(&mut rows)?;
		drop(rows);
		if routed.ops.is_empty() && routed.refused == 0 {
			return Ok(emitted);
		}
		let emits = runner.apply_session(
			engine,
			&mut GuestAsHost(ctx),
			routed,
			&mut SdkHooks::new(aggregator, settings),
		)?;
		emitted.0 = emits.inserts;
		emitted.1 = emits
			.updates
			.into_iter()
			.map(|(number, time, pre, post)| (number, time, Some(pre), post))
			.collect();
		emitted.2 = emits.removes;
		Ok(emitted)
	}

	#[inline]
	fn emit_batches(
		&self,
		ctx: &mut impl GuestContext,
		inserts: &[(RowNumber, DateTime, A::Output)],
		updates: &[UpdateRow<A>],
		removes: &[(RowNumber, DateTime, A::Output)],
	) -> Result<()> {
		let row_numbers: Vec<RowNumber> = inserts.iter().map(|(rn, ..)| *rn).collect();
		let times: Vec<DateTime> = inserts.iter().map(|(_, time, _)| *time).collect();
		ctx.emit_insert(inserts.iter().map(|(.., data)| data), &row_numbers, &times)?;
		let row_numbers: Vec<RowNumber> = updates.iter().map(|(rn, ..)| *rn).collect();
		let times: Vec<DateTime> = updates.iter().map(|(_, time, ..)| *time).collect();
		ctx.emit_update(
			updates.iter().map(|(.., pre, post)| pre.as_ref().unwrap_or(post)),
			updates.iter().map(|(.., post)| post),
			&row_numbers,
			&times,
		)?;
		let row_numbers: Vec<RowNumber> = removes.iter().map(|(rn, ..)| *rn).collect();
		let times: Vec<DateTime> = removes.iter().map(|(_, time, _)| *time).collect();
		ctx.emit_remove(removes.iter().map(|(.., data)| data), &row_numbers, &times)?;
		Ok(())
	}
}

impl<A> OperatorMetadata for PlainDriver<A>
where
	A: Emit + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = A::CAPABILITIES;
}

impl<A> MountedOperator for PlainDriver<A>
where
	A: Emit + Send + Sync + 'static,
	A::Output: Row,
	A::Output: Clone + StateCodec + HeapSize,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	Contribution<A>: Send + Sync + StateCodec + HeapSize,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	type Class = Windowed;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: match (
			<A::Kinds as KindSet<A>>::ROLLING,
			matches!(<A::Coord as SealDomain>::SIZE_DOMAIN, WindowSizeDomain::Time),
		) {
			(true, true) => &["tumbling", "sliding", "session", "rolling"],
			(true, false) => &["tumbling", "sliding", "rolling"],
			(false, true) => &["tumbling", "sliding", "session"],
			(false, false) => &["tumbling", "sliding"],
		},
		domain: <A::Coord as SealDomain>::SIZE_DOMAIN,
		needs_pane: <A::Kinds as KindSet<A>>::ROLLING,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = None;

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.reject_retention()?;
		let rolls = <A::Kinds as KindSet<A>>::ROLLING
			&& with.window.as_ref().is_some_and(|kind| kind.name() == "rolling");
		let slides = with.window.as_ref().is_some_and(|kind| kind.name() == "sliding");
		let sessions = with.window.as_ref().is_some_and(|kind| kind.name() == "session");
		if !rolls && !slides && !sessions {
			with.require_window("tumbling")?;
		}
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		with.check_session_window()?;
		let aggregator = A::create(operator_id, params, with)?;
		let rolling = if rolls {
			let Some(pane) = settings.pane else {
				return Err(Error::from(CoreError::OperatorWithPaneMissing).into());
			};
			if !<A::Kinds as KindSet<A>>::compacts(&aggregator.new_accumulator(&settings)) {
				return Err(Error::from(CoreError::OperatorRollingUnbounded {
					operator: A::NAME,
				})
				.into());
			}
			let spans = PaneSpans {
				size: settings.fixed_size(),
				lag: settings.lag.unwrap_or_default(),
				lateness: settings.lateness,
				retract: settings.immutable.unwrap_or(settings.lateness),
			};
			let carries = matches!(
				settings.kind,
				WindowKind::Rolling {
					on_empty: OnEmpty::Carry,
					..
				}
			);
			if carries && !A::CARRIES_EMPTY {
				return Err(Error::from(CoreError::OperatorRollingCarryUnsupported {
					operator: A::NAME,
				})
				.into());
			}
			Some(RollingMode {
				engine: rolling_engine::<A>(window_engine_config(params), spans),
				runner: RollingRunner::new(operator_id, spans.size, spans.lag, Some(pane)),
				pane,
				on_empty: if carries {
					OnEmpty::Carry
				} else {
					OnEmpty::Remove
				},
			})
		} else {
			None
		};
		let slide = if slides {
			let Some(slide) = settings.slide else {
				panic!("{}: a sliding window reached create without a slide", A::NAME);
			};
			if !fits_slide(settings.fixed_size(), slide) {
				return Err(SdkError::OperatorError(format!(
					"{}: a sliding window needs a slide above zero and below its size",
					A::NAME
				)));
			}
			Some(slide)
		} else {
			None
		};
		let runner = match (rolls, sessions) {
			(true, _) => None,
			(false, true) => {
				let Some(gap) = settings.gap else {
					panic!("{}: a session window reached create without a gap", A::NAME);
				};
				Some(WindowRunner::sessions(operator_id, gap, seal_span, &window_engine_config(params)))
			}
			(false, false) => Some(WindowRunner::new(
				operator_id,
				settings.fixed_size(),
				slide,
				seal_span,
				&window_engine_config(params),
			)),
		};
		Ok(Self {
			aggregator,
			engine: TumblingEngine::new(window_engine_config(params)),
			rolling,
			runner,
			sessions,
			seal_span,
			settings,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		if self.seal_span.is_none() || !<A::Coord as SealDomain>::arms_timer() {
			return Ok(());
		}
		let Self {
			aggregator,
			engine,
			rolling,
			runner,
			sessions,
			settings,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		let fired = fired_at(&timer);
		let (inserts, updates, removes) = match (timer.kind, rolling, *sessions, runner) {
			(TimerKind::Seal, Some(mode), _, _) => Self::run_rolling(
				aggregator,
				mode,
				settings,
				&mut store,
				RollingInput::Timer(timer.kind, fired),
			)?,
			(TimerKind::Seal, None, true, Some(runner)) => {
				runner.on_session_timers(engine, &mut store, from_ref(&timer.key), fired)?;
				(Vec::new(), Vec::new(), Vec::new())
			}
			(kind, None, _, Some(runner)) => {
				runner.on_timer(engine, &mut store, kind, fired)?;
				(Vec::new(), Vec::new(), Vec::new())
			}
			(kind, ..) => {
				return Err(SdkError::OperatorError(format!(
					"{}: a {:?} timer fired that this window never arms",
					A::NAME,
					kind
				)));
			}
		};
		self.emit_batches(ctx, &inserts, &updates, &removes)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		if self.sessions {
			let (inserts, updates, removes) = {
				let Self {
					aggregator,
					engine,
					runner: Some(runner),
					settings,
					..
				} = &mut *self
				else {
					unreachable!("create builds a runner for every session window")
				};
				Self::apply_plain_session(aggregator, engine, runner, settings, ctx, &change)?
			};
			return self.emit_batches(ctx, &inserts, &updates, &removes);
		}
		if let Some(pane) = self.rolling.as_ref().map(|mode| mode.pane) {
			let routed = route_rolling(
				&mut SdkRows::new(&self.aggregator, ctx, &change),
				Some(pane),
				&|group| GroupId::window(xxh3_128(group.into_encoded_key().as_slice()), 0),
			)?;
			if routed.is_empty() {
				return Ok(());
			}
			let Self {
				aggregator,
				rolling: Some(mode),
				settings,
				..
			} = &mut *self
			else {
				unreachable!("the rolling mode was present a line above")
			};
			let (inserts, updates, removes) = Self::run_rolling(
				aggregator,
				mode,
				settings,
				&mut GuestAsHost(ctx),
				RollingInput::Batch(routed),
			)?;
			return self.emit_batches(ctx, &inserts, &updates, &removes);
		}
		let (inserts, updates, removes) = {
			let Self {
				aggregator,
				engine,
				runner: Some(runner),
				settings,
				..
			} = &mut *self
			else {
				unreachable!("create builds a runner for every tumbling or sliding window")
			};
			Self::apply_plain_tumbling(aggregator, engine, runner, settings, ctx, &change)?
		};
		self.emit_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::marker::PhantomData;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		common::{OnEmpty, WindowKind, WindowSize, WindowSizeDomain},
		operator_with::WithSpan,
	};
	use reifydb_flow_async::window::{
		accumulator::{
			MergeAccumulator, UnmergeAccumulator, WindowAccumulator, invertible::last_value::LastValue,
		},
		coord::{DateTimeCoord, RowSpan, U8Coord},
		driver::rolling::rolling_cutoff,
		engine::pane::PaneFold,
		span::WindowSpan,
	};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::{
		flow::operator::{view::ColumnsView, windowed::operator::Invertible},
		row,
	};

	#[reifydb_macro::operator_state]
	#[derive(Clone, HeapSize)]
	struct Out {
		v: i64,
	}

	row!(Out {
		v: i64
	});

	struct Probe<C, G> {
		settings_seen: Mutex<Vec<Option<C>>>,
		_group: PhantomData<fn() -> G>,
	}

	macro_rules! probe {
		($coord:ty, $group:ty, $immutable:ty, $seen:expr) => {
			impl OperatorMetadata for Probe<$immutable, $group> {
				const NAME: &'static str = "probe";
				const VERSION: &'static str = "0";
				const DESCRIPTION: &'static str = "probe";
				const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
				const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
				const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
			}

			impl WindowedOperator for Probe<$immutable, $group> {
				type Coord = $coord;
				type GroupKey = $group;
				type Accumulator = LastValue<i64>;
				type Output = Out;

				fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
					Ok(Self {
						settings_seen: Mutex::new(Vec::new()),
						_group: PhantomData,
					})
				}

				fn coord(
					&self,
					cols: &impl crate::flow::operator::view::ColumnsView,
					rows: &[usize],
				) -> Result<Vec<Option<$coord>>> {
					rows.iter()
						.map(|&i| {
							cols.row(i)
								.as_ref()
								.map(|_| -> Result<Option<$coord>> { Ok(None) })
								.transpose()
								.map(Option::flatten)
						})
						.collect()
				}

				fn extract(
					&self,
					_: &mut impl GuestContext<Windowed>,
					cols: &impl crate::flow::operator::view::ColumnsView,
					rows: &[usize],
				) -> Result<Vec<Option<($group, i64)>>> {
					rows.iter()
						.map(|&i| {
							cols.row(i)
								.as_ref()
								.map(|_| -> Result<Option<($group, i64)>> { Ok(None) })
								.transpose()
								.map(Option::flatten)
						})
						.collect()
				}

				fn new_accumulator(&self, settings: &WindowSettings<$coord>) -> LastValue<i64> {
					self.settings_seen.lock().push($seen(settings));
					LastValue::default()
				}
			}

			impl Emit for Probe<$immutable, $group> {
				type Kinds = crate::flow::operator::windowed::operator::NoRolling;

				fn build_output(&self, _: &$group, _: WindowSpan<$coord>, _: &i64) -> Option<Out> {
					None
				}
			}
		};
	}

	probe!(DateTimeCoord, u32, Duration, |s: &WindowSettings<DateTimeCoord>| s.immutable);
	probe!(U8Coord, u64, RowSpan, |s: &WindowSettings<U8Coord>| s.immutable);

	type TimeProbe = Probe<Duration, u32>;
	type SlotProbe = Probe<RowSpan, u64>;

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	fn params() -> ExtensionParams {
		ExtensionParams::new("probe", Default::default())
	}

	fn tumbling_time(size: i64, lateness: Option<i64>, immutable: Option<i64>) -> ApplyWith {
		ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(size)),
			}),
			lateness: lateness.map(|n| WithSpan::Duration(secs(n))),
			immutable: immutable.map(|n| WithSpan::Duration(secs(n))),
			retention: None,
		}
	}

	fn time_driver(with: &ApplyWith) -> PlainDriver<TimeProbe> {
		PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), with).unwrap()
	}

	#[test]
	fn the_row_key_is_the_group_bytes_then_the_coordinate() {
		// The driver owns the row key: a group bytes prefix keeps one group's windows together, and the
		// coordinate must follow it, or two windows of one group collide or two groups interleave.
		let start = DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap());

		let key = PlainDriver::<TimeProbe>::row_key(&7u32, start);

		assert_eq!(key, EncodedKey::builder().u32(7u32).datetime(&start.0).build());
		assert_ne!(key, PlainDriver::<TimeProbe>::row_key(&8u32, start));
	}

	#[test]
	fn the_span_is_the_declared_size_aligned_down_from_the_coordinate() {
		// The engine builds the span from the size in the with clause; a hard-coded or ignored size would give
		// every operator the same window.
		let coord = DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap());
		let minute = time_driver(&tumbling_time(60, None, None));
		let five_minutes = time_driver(&tumbling_time(300, None, None));

		assert_eq!(
			WindowSpan::for_coord(coord, minute.settings.fixed_size()),
			WindowSpan::new(
				DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap()),
				DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap())
			)
		);
		assert_eq!(
			WindowSpan::for_coord(coord, five_minutes.settings.fixed_size()),
			WindowSpan::new(
				DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap()),
				DateTimeCoord(DateTime::from_ymd_hms(2024, 1, 15, 10, 35, 0).unwrap())
			)
		);
	}

	#[test]
	fn the_accumulator_receives_immutable_from_with_and_none_when_it_is_omitted() {
		// A view that declares immutable must hand it to the accumulator, and one that omits it must not invent
		// one, or a plain window would start refusing retractions.
		let declared = time_driver(&tumbling_time(60, Some(20), Some(15)));
		let omitted = time_driver(&tumbling_time(60, Some(20), None));

		declared.aggregator.new_accumulator(&declared.settings);
		omitted.aggregator.new_accumulator(&omitted.settings);

		assert_eq!(*declared.aggregator.settings_seen.lock(), vec![Some(secs(15))]);
		assert_eq!(*omitted.aggregator.settings_seen.lock(), vec![None]);
		assert_eq!(declared.settings.lateness, secs(20));
	}

	#[test]
	fn a_slot_row_key_carries_the_group_slot_and_the_coordinate() {
		// The slot operators keep the slot in the group key and in the coordinate; the row key must hold both,
		// so two slots never share a key.
		let key = PlainDriver::<SlotProbe>::row_key(&5u64, U8Coord(5));

		assert_eq!(key, EncodedKey::builder().u64(5u64).u64(5u64).build());
		assert_ne!(key, PlainDriver::<SlotProbe>::row_key(&6u64, U8Coord(6)));
	}

	#[test]
	fn a_count_sized_slot_driver_takes_its_span_and_immutable_from_the_counts() {
		// Slot-domain views declare counts; the settings must read them as row spans, or the window is sized
		// in the wrong unit.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: Some(WithSpan::Count(4)),
			immutable: Some(WithSpan::Count(2)),
			retention: None,
		};

		let driver = PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).unwrap();

		assert_eq!(driver.settings.size, Some(RowSpan::of(10)));
		assert_eq!(driver.settings.lateness, RowSpan::of(4));
		assert_eq!(driver.settings.immutable, Some(RowSpan::of(2)));
		assert_eq!(
			WindowSpan::for_coord(U8Coord(23), driver.settings.fixed_size()),
			WindowSpan::new(U8Coord(20), U8Coord(30))
		);
	}

	#[test]
	fn a_count_sliding_window_slides_by_slots() {
		// a slide swapped for the size or read in the wrong unit would put each slot in the wrong windows
		let with = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Count(10),
				slide: WindowSize::Count(4),
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		let driver = PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).unwrap();
		let slide = driver.settings.slide.expect("a sliding view keeps its slide");

		assert_eq!(driver.settings.slide, Some(RowSpan::of(4)));
		assert_eq!(
			WindowSpan::covering(U8Coord(23), driver.settings.fixed_size(), slide)
				.expect("every window around slot 23 ends before the maximum"),
			vec![WindowSpan::new(U8Coord(16), U8Coord(26)), WindowSpan::new(U8Coord(20), U8Coord(30)),]
		);
	}

	#[test]
	fn create_refuses_a_window_that_is_not_tumbling() {
		// a NoRolling operator must refuse a rolling window, and no window at all must be refused too
		let rolling = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: None,
				pane: None,
				on_empty: OnEmpty::Remove,
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};

		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &rolling).is_err());
		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &ApplyWith::default()).is_err());
	}

	#[test]
	fn a_plain_operator_with_no_rolling_kinds_publishes_tumbling_sliding_and_session() {
		// a NoRolling operator must publish sliding but never rolling, or the create check admits the wrong
		// views
		assert_eq!(
			<PlainDriver<TimeProbe> as MountedOperator>::WINDOW,
			WindowRequirements {
				takes_window: true,
				kinds: &["tumbling", "sliding", "session"],
				domain: WindowSizeDomain::Time,
				needs_pane: false,
			}
		);
	}

	#[test]
	fn a_slot_coordinate_plain_operator_does_not_publish_session() {
		// a session needs a time gap; publishing it for slots lets CREATE pass and the flow fail at start
		assert_eq!(<PlainDriver<SlotProbe> as MountedOperator>::WINDOW.kinds, &["tumbling", "sliding"]);
	}

	#[test]
	fn a_slot_coordinate_operator_publishes_slots_and_a_time_one_publishes_time() {
		// a wrong domain makes the create check read the window size in the wrong unit
		assert_eq!(<PlainDriver<SlotProbe> as MountedOperator>::WINDOW.domain, WindowSizeDomain::Slots);
		assert_eq!(<PlainDriver<TimeProbe> as MountedOperator>::WINDOW.domain, WindowSizeDomain::Time);
	}

	struct SpanProbe {
		spans: Mutex<Vec<WindowSpan<DateTimeCoord>>>,
	}

	impl OperatorMetadata for SpanProbe {
		const NAME: &'static str = "span_probe";
		const VERSION: &'static str = "0";
		const DESCRIPTION: &'static str = "span probe";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl WindowedOperator for SpanProbe {
		type Coord = DateTimeCoord;
		type GroupKey = u32;
		type Accumulator = LastValue<i64>;
		type Output = Out;

		fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
			Ok(Self {
				spans: Mutex::new(Vec::new()),
			})
		}

		fn coord(&self, _: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTimeCoord>>> {
			Ok(vec![None; rows.len()])
		}

		fn extract(
			&self,
			_: &mut impl GuestContext<Windowed>,
			_: &impl ColumnsView,
			rows: &[usize],
		) -> Result<Vec<Option<(u32, i64)>>> {
			Ok(vec![None; rows.len()])
		}

		fn new_accumulator(&self, _: &WindowSettings<DateTimeCoord>) -> LastValue<i64> {
			LastValue::default()
		}
	}

	impl Emit for SpanProbe {
		type Kinds = crate::flow::operator::windowed::operator::NoRolling;

		fn build_output(&self, _: &u32, span: WindowSpan<DateTimeCoord>, _: &i64) -> Option<Out> {
			self.spans.lock().push(span);
			None
		}
	}

	fn rolling_lagged(size: i64, pane: i64, lag: Option<i64>) -> WindowSettings<DateTimeCoord> {
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(size)),
				lag: lag.map(secs),
				pane: Some(secs(pane)),
				on_empty: OnEmpty::Remove,
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};
		<DateTimeCoord as SealDomain>::window_settings_of(&with).unwrap()
	}

	fn at(seconds: i64) -> DateTimeCoord {
		DateTimeCoord(DateTime::from_millis(seconds * 1_000))
	}

	#[test]
	fn a_lagged_rolling_cutoff_reaches_back_size_plus_lag() {
		// A guest cutoff of clock - size evicts panes a lagged native window still holds, so the two sums
		// differ.
		let lagged = rolling_lagged(5, 1, Some(2));
		let unlagged = rolling_lagged(5, 1, None);

		assert_eq!(
			rolling_cutoff(at(20), lagged.fixed_size(), lagged.lag.unwrap_or_default(), Some(secs(1))),
			Some(at(13))
		);
		assert_eq!(
			rolling_cutoff(at(20), unlagged.fixed_size(), unlagged.lag.unwrap_or_default(), Some(secs(1))),
			Some(at(15))
		);
	}

	#[test]
	fn a_lagged_rolling_output_span_ends_lag_before_the_newest_pane() {
		// A span ending at the newest pane would label the previous window as the current one.

		let probe = SpanProbe::create(OperatorId(1), &params(), &ApplyWith::default()).unwrap();
		let mut accumulator = LastValue::default();
		accumulator.add(&[7]);

		rolling_output(&probe, &rolling_lagged(5, 1, Some(2)), secs(1), &1, at(20), &accumulator);
		rolling_output(&probe, &rolling_lagged(5, 1, None), secs(1), &1, at(20), &accumulator);

		assert_eq!(
			*probe.spans.lock(),
			vec![WindowSpan::new(at(14), at(19)), WindowSpan::new(at(16), at(21))],
			"the lagged span must end at newest - lag + pane, the unlagged one at newest + pane"
		);
	}

	#[reifydb_macro::operator_state]
	#[derive(Clone, Debug, Default, HeapSize)]
	struct Tally {
		total: i64,
		rows: u64,
	}

	impl WindowAccumulator for Tally {
		type Contributions = Vec<i64>;
		type Output = i64;

		fn add(&mut self, contributions: &[i64]) {
			self.total += contributions.iter().sum::<i64>();
			self.rows += contributions.len() as u64;
		}

		fn remove(&mut self, contributions: &[i64]) {
			self.total -= contributions.iter().sum::<i64>();
			self.rows -= contributions.len() as u64;
		}

		fn finalize(&self) -> Option<i64> {
			(self.rows > 0).then_some(self.total)
		}
	}

	impl MergeAccumulator for Tally {
		fn merge(&mut self, other: &Self) {
			self.total += other.total;
			self.rows += other.rows;
		}
	}

	impl UnmergeAccumulator for Tally {
		fn unmerge(&mut self, other: &Self) {
			self.total -= other.total;
			self.rows -= other.rows;
		}
	}

	struct TallyProbe;

	impl OperatorMetadata for TallyProbe {
		const NAME: &'static str = "tally";
		const VERSION: &'static str = "0";
		const DESCRIPTION: &'static str = "tally";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
	}

	impl WindowedOperator for TallyProbe {
		type Coord = DateTimeCoord;
		type GroupKey = u32;
		type Accumulator = Tally;
		type Output = Out;

		fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
			Ok(Self)
		}

		fn coord(&self, _: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTimeCoord>>> {
			Ok(vec![None; rows.len()])
		}

		fn extract(
			&self,
			_: &mut impl GuestContext<Windowed>,
			_: &impl ColumnsView,
			rows: &[usize],
		) -> Result<Vec<Option<(u32, i64)>>> {
			Ok(vec![None; rows.len()])
		}

		fn new_accumulator(&self, _: &WindowSettings<DateTimeCoord>) -> Tally {
			Tally::default()
		}
	}

	impl Emit for TallyProbe {
		type Kinds = Invertible;

		fn build_output(&self, _: &u32, _: WindowSpan<DateTimeCoord>, total: &i64) -> Option<Out> {
			Some(Out {
				v: *total,
			})
		}
	}

	#[test]
	fn an_invertible_rolling_operator_runs_on_a_running_total() {
		// An invertible operator left on the queue fold re-merges every pane for each output.
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(secs(60)),
				lag: None,
				pane: Some(secs(10)),
				on_empty: OnEmpty::Remove,
			}),
			lateness: None,
			immutable: None,
			retention: None,
		};
		let driver = PlainDriver::<TallyProbe>::create(OperatorId(1), &params(), &with).unwrap();

		assert_eq!(
			driver.rolling.as_ref().map(|mode| mode.engine.fold()),
			Some(PaneFold::Running),
			"an invertible operator must fold its rolling panes on a running total"
		);
	}
}
