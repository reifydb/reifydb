// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	error::CoreError,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::GroupId,
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
	state::timer::StateStore,
};
#[cfg(reifydb_assertions)]
use reifydb_flow_async::operator::state::reaper::queued;
use reifydb_flow_async::{
	operator::{
		state::{
			reaper::{drain, drain_groups, enqueue},
			seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
		},
		state_access::{get_classified, put},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, KeyspaceFamily, PublishKey, WindowStateKey,
			publish::PublishState,
			rolling::{RollingBuckets, RollingBuffer, RollingEngine, RollingEviction},
			session::{GuestSession, SessionEngine},
			sliding::SlidingEngine,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		kind::session::SessionAssignment,
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
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{
			group_of,
			guest_as_host::GuestAsHost,
			intern_window_groups, observe_batch,
			operator::{Contribution, Emit, KindSet, WindowedOperator},
			publish::{Emitted, Rows, UpdateRow, UpdateRows, load_publish_states, publish_row},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

const SEAL_REAP_BATCH: usize = 256;

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Buckets<A> = TumblingBuckets<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, Contribution<A>>;
type WindowOrder<A> = Vec<(<A as WindowedOperator>::GroupKey, WindowSpan<<A as WindowedOperator>::Coord>)>;
type Trackers<A> = BTreeMap<
	<A as WindowedOperator>::GroupKey,
	(GuestSession<<A as WindowedOperator>::Coord>, GuestSession<<A as WindowedOperator>::Coord>),
>;

struct RollingMode<A: Emit> {
	engine: RollingEngine<A::GroupKey, A::Coord, A::Accumulator>,
	pane: <A::Coord as Coord>::Span,
}

struct SlidingMode<A: Emit> {
	engine: SlidingEngine<A::GroupKey, A::Coord, A::Accumulator>,
}

struct SessionMode<A: Emit> {
	engine: SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
}

#[derive(Clone, Copy)]
struct BatchSession<S> {
	before: Option<(S, S)>,
	start: S,
	last: S,
}

struct SessionBatch<A: Emit> {
	trackers: Trackers<A>,
	sessions: BTreeMap<(A::GroupKey, u64), BatchSession<A::Coord>>,
	buckets: Buckets<A>,
	dropped: u64,
	refused: u64,
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
	sliding: Option<SlidingMode<A>>,
	session: Option<SessionMode<A>>,
	reap_queue_empty: bool,
	seal_span: Option<SealSpan<A>>,
	throttle: Option<<A::Coord as Coord>::Span>,
	settings: WindowSettings<A::Coord>,
}

impl<A> PlainDriver<A>
where
	A: Emit,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn window_span(&self, coord: A::Coord) -> WindowSpan<A::Coord> {
		WindowSpan::for_coord(coord, self.settings.fixed_size())
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

	fn partition_of(group: &A::GroupKey) -> GroupId {
		GroupId::of(&group.into_encoded_key())
	}

	fn session_group(group: &A::GroupKey, id: u64) -> GroupId {
		GroupId::of(&Self::row_key(group, <A::Coord as Coord>::from_order(id)))
	}

	fn session_span(id: u64) -> WindowSpan<A::Coord> {
		WindowSpan::new(<A::Coord as Coord>::from_order(id), <A::Coord as Coord>::from_order(id + 1))
	}

	fn route(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> Result<Buckets<A>> {
		let mut buckets: Buckets<A> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						self.push_all(ctx, &cols, &mut buckets, true)?;
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						self.push_all(ctx, &pre, &mut buckets, false)?;
						self.push_all(ctx, &post, &mut buckets, true)?;
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						self.push_all(ctx, &cols, &mut buckets, false)?;
					}
				}
			}
		}
		Ok(buckets)
	}

	fn push_all<C: ColumnsView>(
		&self,
		ctx: &mut impl GuestContext,
		cols: &C,
		buckets: &mut Buckets<A>,
		is_add: bool,
	) -> Result<()> {
		for i in 0..cols.row_count() {
			let Some(row) = cols.row(i) else {
				continue;
			};
			let Some(coord) = self.aggregator.coord(&row)? else {
				continue;
			};
			let Some((group, contribution)) = self.aggregator.extract(ctx, &row)? else {
				continue;
			};
			match &self.sliding {
				Some(mode) => {
					for span in mode.engine.spans(coord) {
						let event = if is_add {
							AccumulatorEvent::Add(contribution.clone())
						} else {
							AccumulatorEvent::Remove(contribution.clone())
						};
						buckets.entry((group.clone(), span)).or_default().push(event);
					}
				}
				None => {
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
		Ok(())
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
	#[allow(clippy::too_many_arguments)]
	fn expire_through<C: GuestContext>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		settings: &WindowSettings<A::Coord>,
		store: &mut GuestAsHost<'_, C>,
		frontier: A::Coord,
		seal_span: SealSpan<A>,
		emitted: &mut Emitted<A>,
	) -> Result<()> {
		let horizon = <A::Coord as SealDomain>::horizon(frontier, seal_span);
		if horizon <= <A::Coord as Coord>::from_order(0) {
			return Ok(());
		}
		let store_queue_empty = *reap_queue_empty;
		let mut expired = Vec::new();
		for window in engine.expire(store, horizon.to_order().saturating_sub(1))? {
			Self::publish_dirty_close(
				aggregator,
				settings,
				store,
				&window.group,
				window.group_id,
				window.window_start,
				frontier,
				emitted,
			)?;
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

	#[allow(clippy::too_many_arguments)]
	fn publish_dirty_close<C: GuestContext>(
		aggregator: &A,
		settings: &WindowSettings<A::Coord>,
		store: &mut GuestAsHost<'_, C>,
		group: &A::GroupKey,
		group_id: GroupId,
		window_start: A::Coord,
		frontier: A::Coord,
		emitted: &mut Emitted<A>,
	) -> Result<()> {
		let slot = Self::row_key(group, window_start);
		let key = PublishKey::new(group_id, slot.clone());
		let Some(mut state) = get_classified::<_, PublishState>(store, &key)? else {
			return Ok(());
		};
		if !state.dirty {
			return Ok(());
		}
		let accumulator: Option<A::Accumulator> =
			get_classified(store, &WindowStateKey::new(KeyspaceFamily::Guest, group_id, slot))?;
		reifydb_assertions! {
			assert!(
				accumulator.is_some(),
				"a dirty window closed with no accumulator; emptying a window must store a clean publish \
				 state, otherwise the close retracts a row downstream already dropped (group={group_id:?})"
			);
		}
		let out = accumulator.and_then(|accumulator| accumulator.finalize()).and_then(|value| {
			aggregator.build_output(
				group,
				WindowSpan::for_coord(window_start, settings.fixed_size()),
				&value,
			)
		});
		let rows = store.get_or_create_row_numbers_for_groups(&[group_id])?;
		#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
		let (row_number, is_new) = rows[0];
		reifydb_assertions! {
			assert!(
				!is_new,
				"a dirty window already published a row, so it must still own its mapping; minting one \
				 here publishes the closing row under a number no sink has seen (group={group_id:?})"
			);
		}
		publish_row::<A>(row_number, out, &mut state, Some(frontier), emitted)?;
		put(store, &key, state)?;
		Ok(())
	}

	fn expire_rolling<C: GuestContext>(
		aggregator: &A,
		mode: &mut RollingMode<A>,
		settings: &WindowSettings<A::Coord>,
		store: &mut GuestAsHost<'_, C>,
		horizon: A::Coord,
	) -> Result<Rows<A>> {
		if horizon > <A::Coord as Coord>::from_order(0) {
			mode.engine.expire_meta(store, horizon.to_order())?;
		}
		if horizon < <A::Coord as Coord>::from_order(0).add_span(settings.fixed_size()) {
			return Ok(Vec::new());
		}
		let pane = mode.pane;
		Ok(mode.engine
			.expire_dead(store, horizon.saturating_sub_span(settings.fixed_size()), |group, buffer| {
				Self::combine_panes(aggregator, settings, pane, group, buffer)
			})?
			.into_iter()
			.map(|r| (r.row_number, r.value))
			.collect())
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
		let span = WindowSpan::new(end.saturating_sub_span(settings.fixed_size()), end);
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
		let mut before: Option<u64> = None;
		let mut removes: Rows<A> = Vec::new();

		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, coord)| *coord).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, seal_span)?;
			}
			let watermark = seal_frontier::<A::Coord>(&mut store)?;
			let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
			before = mode.engine.earliest_expiry(&mut store)?;
			removes = Self::expire_rolling(aggregator, mode, settings, &mut store, horizon)?;
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
				let after = mode.engine.earliest_expiry(&mut store)?;
				<A::Coord as SealDomain>::rearm_dead(
					&mut store,
					settings.fixed_size(),
					seal_span,
					before,
					after,
				)?;
				return Ok((Vec::new(), Vec::new(), removes));
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
				RollingEviction::Span(settings.fixed_size()),
				|group| (group_of(&groups, group, ()), group.into_encoded_key()),
				|| aggregator.new_accumulator(settings),
				|group, buffer| Self::combine_panes(aggregator, settings, pane, group, buffer),
			)?
		};

		let mut inserts: Rows<A> = Vec::new();
		let mut updates: UpdateRows<A> = Vec::new();
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, None, r.value)),
				EmitKind::Remove => removes.push((r.row_number, r.value)),
			}
		}

		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let after = mode.engine.earliest_expiry(&mut store)?;
			<A::Coord as SealDomain>::rearm_dead(
				&mut store,
				settings.fixed_size(),
				seal_span,
				before,
				after,
			)?;
		}

		Ok((inserts, updates, removes))
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_tumbling<C: GuestContext>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		throttle: Option<<A::Coord as Coord>::Span>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		mut buckets: Buckets<A>,
	) -> Result<Emitted<A>> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		let mut frontier: Option<A::Coord> = None;
		if let Some(seal_span) = seal_span {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, span)| span.start).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, seal_span)?;
			}
			let watermark = seal_frontier::<A::Coord>(&mut store)?;
			frontier = Some(watermark);
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
			Self::expire_through(
				aggregator,
				engine,
				reap_queue_empty,
				settings,
				&mut store,
				watermark,
				seal_span,
				&mut emitted,
			)?;
			if buckets.is_empty() {
				return Ok(emitted);
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

		let mut store = GuestAsHost(ctx);
		let keys: Vec<PublishKey> = results
			.iter()
			.map(|r| {
				PublishKey::new(
					group_of(&groups, &r.group, r.span.start),
					Self::row_key(&r.group, r.span.start),
				)
			})
			.collect();
		let mut states = load_publish_states(&mut store, &keys)?;
		for (r, key) in results.into_iter().zip(keys) {
			let mut state = states.remove(&key.group).unwrap_or_default();
			if r.kind == EmitKind::Remove {
				publish_row::<A>(r.row_number, None, &mut state, frontier, &mut emitted)?;
				put(&mut store, &key, state)?;
				continue;
			}
			let due = match (throttle, state.last_publish, frontier) {
				(Some(throttle), Some(last), Some(frontier)) => {
					<A::Coord as Coord>::from_order(last).add_span(throttle) <= frontier
				}
				_ => true,
			};
			if state.row.is_some() && !due {
				state.dirty = true;
				put(&mut store, &key, state)?;
				continue;
			}
			let out = aggregator.build_output(&r.group, r.span, &r.value);
			publish_row::<A>(r.row_number, out, &mut state, frontier, &mut emitted)?;
			put(&mut store, &key, state)?;
		}
		Ok(emitted)
	}

	fn batch_tracker<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: &A::GroupKey,
	) -> Result<GuestSession<A::Coord>> {
		if let Some((_, now)) = batch.trackers.get(group) {
			return Ok(*now);
		}
		let loaded = engine.load_tracker(&mut GuestAsHost(ctx), Self::partition_of(group))?;
		batch.trackers.insert(group.clone(), (loaded, loaded));
		Ok(loaded)
	}

	fn batch_session<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: &A::GroupKey,
		id: u64,
	) -> Result<Option<BatchSession<A::Coord>>> {
		let key = (group.clone(), id);
		if let Some(session) = batch.sessions.get(&key) {
			return Ok(Some(*session));
		}
		let Some((start, last)) = engine.load_record(&mut GuestAsHost(ctx), Self::session_group(group, id))?
		else {
			return Ok(None);
		};
		let session = BatchSession {
			before: Some((start, last)),
			start,
			last,
		};
		batch.sessions.insert(key, session);
		Ok(Some(session))
	}

	#[allow(clippy::too_many_arguments)]
	fn admit_session_row<C: GuestContext>(
		engine: &mut SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: A::GroupKey,
		row: RowNumber,
		coord: A::Coord,
		contribution: Contribution<A>,
		horizon: A::Coord,
	) -> Result<()> {
		let mut tracker = Self::batch_tracker(engine, batch, ctx, &group)?;
		let id = match engine.assign(&mut tracker, coord) {
			SessionAssignment::Refused => return Ok(()),
			SessionAssignment::Opened(id)
			| SessionAssignment::Rotated {
				opened: id,
				..
			} => {
				if is_sealed(coord, horizon) {
					batch.dropped += 1;
					return Ok(());
				}
				id
			}
			SessionAssignment::Extended(id) => {
				let fresh = batch
					.sessions
					.get(&(group.clone(), id))
					.is_some_and(|session| session.before.is_none());
				let (before, _) = batch.trackers[&group];
				if !fresh && is_sealed(before.last, horizon) {
					batch.dropped += 1;
					return Ok(());
				}
				id
			}
		};
		let before = match Self::batch_session(engine, batch, ctx, &group, id)? {
			Some(session) => session.before,
			None => None,
		};
		batch.sessions.insert(
			(group.clone(), id),
			BatchSession {
				before,
				start: tracker.start,
				last: tracker.last,
			},
		);
		if let Some((_, now)) = batch.trackers.get_mut(&group) {
			*now = tracker;
		}
		engine.index_row(&mut GuestAsHost(ctx), id, Self::session_group(&group, id), row)?;
		batch.buckets
			.entry((group, Self::session_span(id)))
			.or_default()
			.push(AccumulatorEvent::Add(contribution));
		Ok(())
	}

	fn holding_session<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: &A::GroupKey,
		row: RowNumber,
		horizon: A::Coord,
	) -> Result<Option<u64>> {
		let mut id = Self::batch_tracker(engine, batch, ctx, group)?.session_id;
		while let Some(session) = Self::batch_session(engine, batch, ctx, group, id)? {
			if engine.holds_row(&mut GuestAsHost(ctx), Self::session_group(group, id), row)? {
				let anchor = session.before.map_or(session.last, |(_, last)| last);
				if is_sealed(anchor, horizon) {
					batch.dropped += 1;
					return Ok(None);
				}
				return Ok(Some(id));
			}
			let Some(lower) = id.checked_sub(1) else {
				break;
			};
			id = lower;
		}
		batch.dropped += 1;
		Ok(None)
	}

	fn retract_session_row<C: GuestContext>(
		engine: &mut SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: A::GroupKey,
		row: RowNumber,
		contribution: Contribution<A>,
		horizon: A::Coord,
	) -> Result<()> {
		let Some(id) = Self::holding_session(engine, batch, ctx, &group, row, horizon)? else {
			return Ok(());
		};
		engine.unindex_row(&mut GuestAsHost(ctx), Self::session_group(&group, id), row)?;
		batch.buckets
			.entry((group, Self::session_span(id)))
			.or_default()
			.push(AccumulatorEvent::Remove(contribution));
		Ok(())
	}

	fn route_session<C: GuestContext>(
		aggregator: &A,
		engine: &mut SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		ctx: &mut C,
		change: &impl ChangeView,
		horizon: A::Coord,
	) -> Result<SessionBatch<A>> {
		let mut batch = SessionBatch {
			trackers: BTreeMap::new(),
			sessions: BTreeMap::new(),
			buckets: BTreeMap::new(),
			dropped: 0,
			refused: 0,
		};
		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					let Some(cols) = diff.post() else {
						continue;
					};
					for i in 0..cols.row_count() {
						let Some(row) = cols.row(i) else {
							continue;
						};
						let Some(number) = row.row_number() else {
							batch.refused += 1;
							continue;
						};
						let Some(coord) = aggregator.coord(&row)? else {
							continue;
						};
						let Some((group, contribution)) = aggregator.extract(ctx, &row)? else {
							continue;
						};
						Self::admit_session_row(
							engine,
							&mut batch,
							ctx,
							group,
							number,
							coord,
							contribution,
							horizon,
						)?;
					}
				}
				DiffType::Remove => {
					let Some(cols) = diff.pre() else {
						continue;
					};
					for i in 0..cols.row_count() {
						let Some(row) = cols.row(i) else {
							continue;
						};
						let Some(number) = row.row_number() else {
							batch.refused += 1;
							continue;
						};
						let Some((group, contribution)) = aggregator.extract(ctx, &row)? else {
							continue;
						};
						Self::retract_session_row(
							engine,
							&mut batch,
							ctx,
							group,
							number,
							contribution,
							horizon,
						)?;
					}
				}
				DiffType::Update => {
					let (Some(pre), Some(post)) = (diff.pre(), diff.post()) else {
						continue;
					};
					for i in 0..pre.row_count() {
						let (Some(pre_row), Some(post_row)) = (pre.row(i), post.row(i)) else {
							continue;
						};
						let Some(number) = pre_row.row_number() else {
							batch.refused += 2;
							continue;
						};
						let pre_coord = aggregator.coord(&pre_row)?;
						let post_coord = aggregator.coord(&post_row)?;
						let before = aggregator.extract(ctx, &pre_row)?;
						let after = aggregator.extract(ctx, &post_row)?;
						if let (
							Some(pre_coord),
							Some(post_coord),
							Some((pre_group, pre_value)),
							Some((post_group, post_value)),
						) = (pre_coord, post_coord, &before, &after)
							&& pre_group == post_group
							&& (pre_coord == post_coord
								|| engine.refuses(
									&Self::batch_tracker(
										engine, &mut batch, ctx, pre_group,
									)?,
									post_coord,
								)) {
							match Self::holding_session(
								engine, &mut batch, ctx, pre_group, number, horizon,
							)? {
								Some(id) => {
									let events = batch
										.buckets
										.entry((
											pre_group.clone(),
											Self::session_span(id),
										))
										.or_default();
									events.push(AccumulatorEvent::Remove(
										pre_value.clone(),
									));
									events.push(AccumulatorEvent::Add(
										post_value.clone(),
									));
								}
								None => Self::admit_session_row(
									engine,
									&mut batch,
									ctx,
									post_group.clone(),
									number,
									post_coord,
									post_value.clone(),
									horizon,
								)?,
							}
							continue;
						}
						if let Some((group, contribution)) = before {
							Self::retract_session_row(
								engine,
								&mut batch,
								ctx,
								group,
								number,
								contribution,
								horizon,
							)?;
						}
						if let (Some(coord), Some((group, contribution))) = (post_coord, after)
						{
							Self::admit_session_row(
								engine,
								&mut batch,
								ctx,
								group,
								number,
								coord,
								contribution,
								horizon,
							)?;
						}
					}
				}
			}
		}
		batch.refused += engine.take_refused();
		Ok(batch)
	}

	fn apply_session<C: GuestContext>(
		aggregator: &A,
		mode: &mut SessionMode<A>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		change: &impl ChangeView,
	) -> Result<Emitted<A>> {
		let Some(seal_span) = seal_span else {
			panic!("{}: a session window reached apply without a seal span", A::NAME);
		};
		let watermark = seal_frontier::<A::Coord>(&mut GuestAsHost(ctx))?;
		let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
		let SessionBatch {
			trackers,
			sessions,
			buckets,
			dropped,
			refused,
		} = Self::route_session(aggregator, &mut mode.engine, ctx, change, horizon)?;
		if dropped > 0 || refused > 0 {
			debug!(operator = A::NAME, dropped, refused, "session mutations were dropped or refused");
		}

		let mut store = GuestAsHost(ctx);
		let session_of = |group: &A::GroupKey, span: &WindowSpan<A::Coord>| {
			*sessions
				.get(&(group.clone(), span.start.to_order()))
				.expect("every emitted session was routed this batch")
		};
		if let Some(newest) = buckets.keys().map(|(group, span)| session_of(group, span).last).max() {
			observe_batch(&mut store, newest, seal_span)?;
		}
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		Self::expire_through(
			aggregator,
			mode.engine.tumbling_mut(),
			reap_queue_empty,
			settings,
			&mut store,
			watermark,
			seal_span,
			&mut emitted,
		)?;
		if buckets.is_empty() {
			return Ok(emitted);
		}

		let groups = intern_window_groups(
			buckets.keys()
				.map(|(group, span)| ((group.clone(), span.start), Self::row_key(group, span.start))),
		);
		let order: WindowOrder<A> = buckets.keys().cloned().collect();
		let results = mode.engine.tumbling_mut().apply(
			&mut store,
			buckets,
			&order,
			|group, window_start| {
				(group_of(&groups, group, window_start), Self::row_key(group, window_start))
			},
			|| aggregator.new_accumulator(settings),
		)?;

		for ((group, id), session) in &sessions {
			if session.before == Some((session.start, session.last)) {
				continue;
			}
			let session_group = Self::session_group(group, *id);
			mode.engine.save_record(&mut store, session_group, session.start, session.last)?;
			mode.engine.reindex_session(
				&mut store,
				group,
				*id,
				session_group,
				&Self::row_key(group, <A::Coord as Coord>::from_order(*id)),
				session.before.map(|(_, last)| last),
				session.last,
			)?;
		}
		for (group, (before, now)) in &trackers {
			if before != now {
				mode.engine.save_tracker(&mut store, Self::partition_of(group), now)?;
			}
		}

		let gap = mode.engine.gap();
		let (mut inserts, mut updates, mut removes) = emitted;
		for r in results {
			let session = session_of(&r.group, &r.span);
			let span = WindowSpan {
				start: session.start,
				end: session.last.add_span(gap),
			};
			let Some(out) = aggregator.build_output(&r.group, span, &r.value) else {
				continue;
			};
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, out)),
				EmitKind::Update => updates.push((r.row_number, None, out)),
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
		updates: &[UpdateRow<A>],
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
			for (rn, pre, post) in updates {
				batch.push(*rn, pre.as_ref().unwrap_or(post), post)?;
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
		needs_pane: false,
		throttles: true,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = None;

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.reject_retention()?;
		with.check_throttle(true)?;
		let rolls = <A::Kinds as KindSet<A>>::ROLLING
			&& with.window.as_ref().is_some_and(|kind| kind.name() == "rolling");
		let slides = with.window.as_ref().is_some_and(|kind| kind.name() == "sliding");
		let sessions = with.window.as_ref().is_some_and(|kind| kind.name() == "session");
		if !rolls && !slides && !sessions {
			with.require_window("tumbling")?;
		}
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let throttle = <A::Coord as SealDomain>::throttle_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		with.check_session_window()?;
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
		let sliding = if slides {
			let Some(slide) = settings.slide else {
				panic!("{}: a sliding window reached create without a slide", A::NAME);
			};
			Some(SlidingMode {
				engine: SlidingEngine::new(window_engine_config(params), settings.fixed_size(), slide),
			})
		} else {
			None
		};
		let session = if sessions {
			let Some(gap) = settings.gap else {
				panic!("{}: a session window reached create without a gap", A::NAME);
			};
			Some(SessionMode {
				engine: SessionEngine::new(window_engine_config(params), gap),
			})
		} else {
			None
		};
		Ok(Self {
			aggregator,
			engine: TumblingEngine::new(window_engine_config(params)),
			rolling,
			sliding,
			session,
			reap_queue_empty: false,
			seal_span,
			throttle,
			settings,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_span) = self.seal_span else {
			return Ok(());
		};
		let Self {
			aggregator,
			engine,
			rolling,
			sliding,
			session,
			reap_queue_empty,
			settings,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		let Some(frontier) = timer_frontier::<A::Coord>(&mut store, timer)? else {
			return Ok(());
		};
		let (inserts, updates, removes) = match rolling {
			Some(mode) => {
				let before = mode.engine.earliest_expiry(&mut store)?;
				let removes = Self::expire_rolling(
					aggregator,
					mode,
					settings,
					&mut store,
					<A::Coord as SealDomain>::horizon(frontier, seal_span),
				)?;
				let after = mode.engine.earliest_expiry(&mut store)?;
				<A::Coord as SealDomain>::rearm_dead(
					&mut store,
					settings.fixed_size(),
					seal_span,
					before,
					after,
				)?;
				(Vec::new(), Vec::new(), removes)
			}
			None => {
				let tumbling = match (sliding, session) {
					(Some(mode), _) => mode.engine.tumbling_mut(),
					(None, Some(mode)) => mode.engine.tumbling_mut(),
					(None, None) => engine,
				};
				let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
				Self::expire_through(
					aggregator,
					tumbling,
					reap_queue_empty,
					settings,
					&mut store,
					frontier,
					seal_span,
					&mut emitted,
				)?;
				emitted
			}
		};
		self.emit_batches(ctx, &inserts, &updates, &removes)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		if let Some(mode) = &mut self.session {
			let (inserts, updates, removes) = Self::apply_session(
				&self.aggregator,
				mode,
				&mut self.reap_queue_empty,
				self.seal_span,
				&self.settings,
				ctx,
				&change,
			)?;
			return self.emit_batches(ctx, &inserts, &updates, &removes);
		}
		let buckets = self.route(ctx, &change)?;
		if buckets.is_empty() {
			return Ok(());
		}

		let (inserts, updates, removes) = {
			let Self {
				aggregator,
				engine,
				rolling,
				sliding,
				reap_queue_empty,
				seal_span,
				throttle,
				settings,
				..
			} = &mut *self;
			match rolling {
				Some(mode) => {
					Self::apply_rolling(aggregator, mode, *seal_span, settings, ctx, buckets)?
				}
				None => {
					let tumbling = match sliding {
						Some(mode) => mode.engine.tumbling_mut(),
						None => engine,
					};
					Self::apply_tumbling(
						aggregator,
						tumbling,
						reap_queue_empty,
						*seal_span,
						*throttle,
						settings,
						ctx,
						buckets,
					)?
				}
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
		common::{WindowKind, WindowSize, WindowSizeDomain},
		operator_with::WithSpan,
	};
	use reifydb_flow_async::window::{
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

				fn coord(
					&self,
					_: &impl crate::flow::operator::view::RowView,
				) -> Result<Option<$coord>> {
					Ok(None)
				}

				fn extract(
					&self,
					_: &mut impl GuestContext<Windowed>,
					_: &impl crate::flow::operator::view::RowView,
				) -> Result<Option<($group, i64)>> {
					Ok(None)
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
			retention: None,
			throttle: None,
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
			retention: None,
			throttle: None,
		};

		let driver = PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).unwrap();

		assert_eq!(driver.settings.size, Some(RowSpan::of(10)));
		assert_eq!(driver.settings.lateness, RowSpan::of(4));
		assert_eq!(driver.settings.immutable, Some(RowSpan::of(2)));
		assert_eq!(
			driver.window_span(OrdinalCoord::from_arrival_counter(23)),
			WindowSpan::new(OrdinalCoord::from_arrival_counter(20), OrdinalCoord::from_arrival_counter(30))
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
			throttle: None,
		};

		let driver = PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).unwrap();
		let sliding = driver.sliding.as_ref().expect("a sliding view builds a sliding engine");

		assert_eq!(driver.settings.slide, Some(RowSpan::of(4)));
		assert_eq!(
			sliding.engine.spans(OrdinalCoord::from_arrival_counter(23)),
			vec![
				WindowSpan::new(
					OrdinalCoord::from_arrival_counter(16),
					OrdinalCoord::from_arrival_counter(26)
				),
				WindowSpan::new(
					OrdinalCoord::from_arrival_counter(20),
					OrdinalCoord::from_arrival_counter(30)
				),
			]
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
			retention: None,
			throttle: None,
		};

		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &rolling).is_err());
		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &ApplyWith::default()).is_err());
	}

	#[test]
	fn create_refuses_throttle_on_sliding() {
		// Throttle is defined for tumbling only; a sliding driver that accepted it would silently ignore it.
		let mut with = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Duration(secs(60)),
				slide: WindowSize::Duration(secs(10)),
			}),
			lateness: None,
			immutable: None,
			retention: None,
			throttle: None,
		};
		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &with).is_ok(), "precondition");
		with.throttle = Some(secs(5));
		assert!(PlainDriver::<TimeProbe>::create(OperatorId(1), &params(), &with).is_err());
	}

	#[test]
	fn create_refuses_throttle_without_a_closing_rule() {
		// A window that never closes would hold its last throttled change unpublished forever.
		let mut with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
			retention: None,
			throttle: None,
		};
		assert!(PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).is_ok(), "precondition");
		with.throttle = Some(secs(5));
		assert!(PlainDriver::<SlotProbe>::create(OperatorId(1), &params(), &with).is_err());
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
				throttles: true,
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
}
