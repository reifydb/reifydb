// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_core::{
	error::CoreError,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
	state::timer::StateStore,
};
#[cfg(reifydb_assertions)]
use reifydb_flow::operator::state::reaper::queued;
use reifydb_flow::{
	operator::state::{
		reaper::{drain, drain_groups, enqueue},
		seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind,
			rolling::{RollingBuckets, RollingBuffer, RollingEngine, RollingEviction},
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		settings::WindowSettings,
		span::{WindowSpan, window_row_key},
	},
};
use reifydb_value::{
	config::ExtensionParams,
	error::Error,
	reifydb_assertions,
	value::{diff_type::DiffType, row_number::RowNumber},
};
use tracing::debug;

use crate::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			group_of,
			guest_as_host::GuestAsHost,
			intern_window_groups, observe_batch,
			operator::{Contribution, Emit, KindSet, WindowedOperator},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

const SEAL_REAP_BATCH: usize = 256;

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Buckets<A> = TumblingBuckets<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, Contribution<A>>;
type WindowOrder<A> = Vec<(<A as WindowedOperator>::GroupKey, WindowSpan<<A as WindowedOperator>::Coord>)>;
type Rows<A> = Vec<(RowNumber, <A as WindowedOperator>::Output)>;
type Emitted<A> = (Rows<A>, Rows<A>, Rows<A>);

struct RollingMode<A: Emit> {
	engine: RollingEngine<A::GroupKey, A::Coord, A::Accumulator>,
	pane: <A::Coord as Coord>::Span,
}

pub struct PlainDriver<A>
where
	A: Emit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
	rolling: Option<RollingMode<A>>,
	reap_queue_empty: bool,
	seal_span: Option<SealSpan<A>>,
	settings: WindowSettings<A::Coord>,
}

impl<A> PlainDriver<A>
where
	A: Emit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn window_span(&self, coord: A::Coord) -> WindowSpan<A::Coord> {
		WindowSpan::for_coord(coord, self.settings.size)
	}

	fn bucket_span(&self, coord: A::Coord) -> WindowSpan<A::Coord> {
		match &self.rolling {
			Some(mode) => WindowSpan::for_coord(coord, mode.pane),
			None => self.window_span(coord),
		}
	}

	fn row_key(group: &A::GroupKey, window_start: A::Coord) -> EncodedKey {
		window_row_key(group.into_encoded_key(), window_start)
	}

	fn route(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> Buckets<A> {
		let mut buckets: Buckets<A> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						self.push_all(ctx, &cols, &mut buckets, true);
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						self.push_all(ctx, &pre, &mut buckets, false);
						self.push_all(ctx, &post, &mut buckets, true);
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						self.push_all(ctx, &cols, &mut buckets, false);
					}
				}
			}
		}
		buckets
	}

	fn push_all<C: ColumnsView>(
		&self,
		ctx: &mut impl GuestContext,
		cols: &C,
		buckets: &mut Buckets<A>,
		is_add: bool,
	) {
		for i in 0..cols.row_count() {
			let Some(row) = cols.row(i) else {
				continue;
			};
			let Some(coord) = self.aggregator.coord(&row) else {
				continue;
			};
			let Some((group, contribution)) = self.aggregator.extract(ctx, &row) else {
				continue;
			};
			let span = self.bucket_span(coord);
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, span)).or_default().push(event);
		}
	}
}

impl<A> PlainDriver<A>
where
	A: Emit + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync,
	Contribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: GuestContext>(
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		store: &mut GuestAsHost<'_, C>,
		frontier: A::Coord,
		seal_span: SealSpan<A>,
	) -> Result<()> {
		let horizon = <A::Coord as SealDomain>::horizon(frontier, seal_span);
		if horizon <= <A::Coord as Coord>::from_order(0) {
			return Ok(());
		}
		let store_queue_empty = *reap_queue_empty;
		let mut expired = Vec::new();
		for window in engine.expire(store, horizon.to_order().saturating_sub(1))? {
			enqueue(store, window.group_id)?;
			expired.push(window.group_id);
			*reap_queue_empty = false;
		}
		engine.expire_meta(store, horizon.to_order())?;
		if *reap_queue_empty {
			reifydb_assertions! {
				let pending = queued(store, 1)?;
				assert!(
					pending.groups.is_empty(),
					"the reap queue still holds {:?} while the driver believes it drained the \
					 queue; skipping the drain here leaves that group's state behind for good",
					pending.groups
				);
			}
			return Ok(());
		}
		let drained = match store_queue_empty {
			true => drain_groups(store, &expired, engine, SEAL_REAP_BATCH)?,
			false => drain(store, engine, SEAL_REAP_BATCH)?,
		};
		*reap_queue_empty = drained.queue_is_empty();
		if !*reap_queue_empty {
			observe_batch(store, frontier, seal_span)?;
		}
		Ok(())
	}

	fn expire_rolling<C: GuestContext>(
		engine: &mut RollingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		store: &mut GuestAsHost<'_, C>,
		horizon: A::Coord,
	) -> Result<()> {
		if horizon > <A::Coord as Coord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}

	fn combine_panes(
		aggregator: &A,
		settings: &WindowSettings<A::Coord>,
		pane: <A::Coord as Coord>::Span,
		group: &A::GroupKey,
		buffer: &RollingBuffer<A::Coord, A::Accumulator>,
	) -> Option<A::Output> {
		let value = <A::Kinds as KindSet<A>>::merge_panes(buffer).finalize()?;
		let (newest, _) = buffer.last_key_value()?;
		let end = newest.add_span(pane);
		let span = WindowSpan::new(end.saturating_sub_span(settings.size), end);
		aggregator.build_output(group, span, &value)
	}

	fn apply_rolling<C: GuestContext>(
		aggregator: &A,
		mode: &mut RollingMode<A>,
		seal_span: Option<SealSpan<A>>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		windows: Buckets<A>,
	) -> Result<Emitted<A>> {
		let mut buckets: RollingBuckets<A::GroupKey, A::Coord, Contribution<A>> =
			windows.into_iter().map(|((group, span), events)| ((group, span.start), events)).collect();

		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, coord)| *coord).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, seal_span)?;
			}
			let watermark = seal_frontier::<A::Coord>(&mut store)?;
			let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
			Self::expire_rolling(&mut mode.engine, &mut store, horizon)?;
			let mut dropped = 0u64;
			buckets.retain(|(_, coord), events| {
				if is_sealed(*coord, horizon) {
					dropped += events.len() as u64;
					false
				} else {
					true
				}
			});
			if dropped > 0 {
				debug!(operator = A::NAME, dropped, "mutations targeting sealed panes were dropped");
			}
			if buckets.is_empty() {
				return Ok((Vec::new(), Vec::new(), Vec::new()));
			}
		}

		let groups = intern_window_groups(
			buckets.keys().map(|(group, _)| group.clone()).collect::<BTreeSet<_>>().into_iter().map(
				|group| {
					let key = (&group).into_encoded_key();
					((group, ()), key)
				},
			),
		);

		let pane = mode.pane;
		let results = {
			let mut store = GuestAsHost(ctx);
			mode.engine.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Span(settings.size),
				|group| (group_of(&groups, group, ()), group.into_encoded_key()),
				|| aggregator.new_accumulator(settings),
				|group, buffer| Self::combine_panes(aggregator, settings, pane, group, buffer),
			)?
		};

		let mut inserts: Rows<A> = Vec::new();
		let mut updates: Rows<A> = Vec::new();
		let mut removes: Rows<A> = Vec::new();
		let mut removed_groups: Vec<A::GroupKey> = Vec::new();
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, r.value)),
				EmitKind::Remove => {
					removed_groups.push(r.group);
					removes.push((r.row_number, r.value));
				}
			}
		}

		if !removed_groups.is_empty() {
			let mut store = GuestAsHost(ctx);
			for group in &removed_groups {
				store.remove_row_number(group_of(&groups, group, ()), &group.into_encoded_key())?;
			}
		}

		Ok((inserts, updates, removes))
	}

	fn apply_tumbling<C: GuestContext>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		mut buckets: Buckets<A>,
	) -> Result<Emitted<A>> {
		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, span)| span.start).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, seal_span)?;
			}
			let watermark = seal_frontier::<A::Coord>(&mut store)?;
			let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
			let mut dropped = 0u64;
			buckets.retain(|(_, span), events| {
				if is_sealed(span.start, horizon) {
					dropped += events.len() as u64;
					false
				} else {
					true
				}
			});
			if dropped > 0 {
				debug!(operator = A::NAME, dropped, "mutations targeting sealed windows were dropped");
			}
			Self::expire_through(engine, reap_queue_empty, &mut store, watermark, seal_span)?;
			if buckets.is_empty() {
				return Ok((Vec::new(), Vec::new(), Vec::new()));
			}
		}

		let groups = intern_window_groups(
			buckets.keys()
				.map(|(group, span)| ((group.clone(), span.start), Self::row_key(group, span.start))),
		);

		let results = {
			let mut store = GuestAsHost(ctx);
			let order: WindowOrder<A> = buckets.keys().cloned().collect();
			engine.apply(
				&mut store,
				buckets,
				&order,
				|group, window_start| {
					(group_of(&groups, group, window_start), Self::row_key(group, window_start))
				},
				|| aggregator.new_accumulator(settings),
			)?
		};

		if seal_span.is_some() {
			let mut store = GuestAsHost(ctx);
			for r in &results {
				if r.kind == EmitKind::Insert {
					let group = group_of(&groups, &r.group, r.span.start);
					engine.reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						group,
						&Self::row_key(&r.group, r.span.start),
						None,
						Some(r.span.start.to_order()),
					)?;
				}
			}
		}

		let mut inserts: Rows<A> = Vec::new();
		let mut updates: Rows<A> = Vec::new();
		let mut removes: Rows<A> = Vec::new();
		for r in results {
			let Some(out) = aggregator.build_output(&r.group, r.span, &r.value) else {
				continue;
			};
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, out)),
				EmitKind::Update => updates.push((r.row_number, out)),
				EmitKind::Remove => removes.push((r.row_number, out)),
			}
		}
		Ok((inserts, updates, removes))
	}

	#[inline]
	fn emit_batches(
		&self,
		ctx: &mut impl GuestContext,
		inserts: &[(RowNumber, A::Output)],
		updates: &[(RowNumber, A::Output)],
		removes: &[(RowNumber, A::Output)],
	) -> Result<()> {
		if !inserts.is_empty() {
			let mut batch = InsertBatch::<A::Output, _>::new(ctx, inserts.len())?;
			for (rn, data) in inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<A::Output, _>::new(ctx, updates.len())?;
			for (rn, data) in updates {
				batch.push(*rn, data, data)?;
			}
			batch.finish()?;
		}
		if !removes.is_empty() {
			let mut batch = RemoveBatch::<A::Output, _>::new(ctx, removes.len())?;
			for (rn, data) in removes {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
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
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	Contribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	type Class = Windowed;

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		let rolls = <A::Kinds as KindSet<A>>::ROLLING
			&& with.window.as_ref().is_some_and(|kind| kind.name() == "rolling");
		if !rolls {
			with.require_window("tumbling")?;
		}
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		let aggregator = A::create(operator_id, params, with)?;
		let rolling = if rolls {
			let Some(pane) = settings.pane else {
				return Err(Error::from(CoreError::OperatorWithPaneMissing).into());
			};
			Some(RollingMode {
				engine: RollingEngine::new(window_engine_config(params)),
				pane,
			})
		} else {
			None
		};
		Ok(Self {
			aggregator,
			engine: TumblingEngine::new(window_engine_config(params)),
			rolling,
			reap_queue_empty: false,
			seal_span,
			settings,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_span) = self.seal_span else {
			return Ok(());
		};
		let Self {
			engine,
			rolling,
			reap_queue_empty,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		let Some(frontier) = timer_frontier::<A::Coord>(&mut store, timer)? else {
			return Ok(());
		};
		match rolling {
			Some(mode) => Self::expire_rolling(
				&mut mode.engine,
				&mut store,
				<A::Coord as SealDomain>::horizon(frontier, seal_span),
			),
			None => Self::expire_through(engine, reap_queue_empty, &mut store, frontier, seal_span),
		}
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		let buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let (inserts, updates, removes) = {
			let Self {
				aggregator,
				engine,
				rolling,
				reap_queue_empty,
				seal_span,
				settings,
			} = &mut *self;
			match rolling {
				Some(mode) => {
					Self::apply_rolling(aggregator, mode, *seal_span, settings, ctx, buckets)?
				}
				None => Self::apply_tumbling(
					aggregator,
					engine,
					reap_queue_empty,
					*seal_span,
					settings,
					ctx,
					buckets,
				)?,
			}
		};
		self.emit_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{marker::PhantomData, sync::Mutex};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		common::{WindowKind, WindowSize},
		operator_with::WithSpan,
	};
	use reifydb_flow::window::{
		accumulator::invertible::last_value::LastValue,
		coord::{OrdinalCoord, RowSpan},
	};
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::row;

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

				fn coord(&self, _: &impl crate::flow::operator::view::RowView) -> Option<$coord> {
					None
				}

				fn extract(
					&self,
					_: &mut impl GuestContext<Windowed>,
					_: &impl crate::flow::operator::view::RowView,
				) -> Option<($group, i64)> {
					None
				}

				fn new_accumulator(&self, settings: &WindowSettings<$coord>) -> LastValue<i64> {
					self.settings_seen.lock().unwrap().push($seen(settings));
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

	probe!(DateTime, u32, Duration, |s: &WindowSettings<DateTime>| s.immutable);
	probe!(OrdinalCoord, u64, RowSpan, |s: &WindowSettings<OrdinalCoord>| s.immutable);

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
		}
	}

	fn time_driver(with: &ApplyWith) -> PlainDriver<TimeProbe> {
		PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), with).unwrap()
	}

	#[test]
	fn the_row_key_is_the_group_bytes_then_the_coordinate() {
		// The driver owns the row key: a group bytes prefix keeps one group's windows together, and the
		// coordinate must follow it, or two windows of one group collide or two groups interleave.
		let start = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap();

		let key = PlainDriver::<TimeProbe>::row_key(&7u32, start);

		assert_eq!(key, EncodedKey::builder().u32(7u32).datetime(&start).build());
		assert_ne!(key, PlainDriver::<TimeProbe>::row_key(&8u32, start));
	}

	#[test]
	fn the_span_is_the_declared_size_aligned_down_from_the_coordinate() {
		// The engine builds the span from the size in the with clause; a hard-coded or ignored size would give
		// every operator the same window.
		let coord = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();
		let minute = time_driver(&tumbling_time(60, None, None));
		let five_minutes = time_driver(&tumbling_time(300, None, None));

		assert_eq!(
			minute.window_span(coord),
			WindowSpan::new(
				DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap(),
				DateTime::from_ymd_hms(2024, 1, 15, 10, 31, 0).unwrap()
			)
		);
		assert_eq!(
			five_minutes.window_span(coord),
			WindowSpan::new(
				DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 0).unwrap(),
				DateTime::from_ymd_hms(2024, 1, 15, 10, 35, 0).unwrap()
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

		assert_eq!(*declared.aggregator.settings_seen.lock().unwrap(), vec![Some(secs(15))]);
		assert_eq!(*omitted.aggregator.settings_seen.lock().unwrap(), vec![None]);
		assert_eq!(declared.settings.lateness, secs(20));
	}

	#[test]
	fn a_slot_row_key_carries_the_group_slot_and_the_coordinate() {
		// The slot operators keep the slot in the group key and in the coordinate; the row key must hold both,
		// so two slots never share a key.
		let key = PlainDriver::<SlotProbe>::row_key(&5u64, OrdinalCoord::from_arrival_counter(5));

		assert_eq!(key, EncodedKey::builder().u64(5u64).u64(5u64).build());
		assert_ne!(key, PlainDriver::<SlotProbe>::row_key(&6u64, OrdinalCoord::from_arrival_counter(6)));
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
		};

		let driver = PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).unwrap();

		assert_eq!(driver.settings.size, RowSpan::of(10));
		assert_eq!(driver.settings.lateness, RowSpan::of(4));
		assert_eq!(driver.settings.immutable, Some(RowSpan::of(2)));
		assert_eq!(
			driver.window_span(OrdinalCoord::from_arrival_counter(23)),
			WindowSpan::new(OrdinalCoord::from_arrival_counter(20), OrdinalCoord::from_arrival_counter(30))
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
			}),
			lateness: None,
			immutable: None,
		};

		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &rolling).is_err());
		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &ApplyWith::default()).is_err());
	}
}
