// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

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
	state::timer::StateStore,
};
use reifydb_flow_async::{
	operator::state::{
		reaper::{drain, drain_groups, enqueue, enqueue_retired},
		seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
	},
	window::{
		accumulator::AccumulatorOp,
		driver::{
			route::{Routed, route_tumbling},
			tumbling::finish_tumbling,
		},
		engine::{
			EmitKind, ExpiryAnchor, WindowReindex,
			pane::{PaneEngine, PaneEvent, PaneSpans, RollingEmit},
			publish::RowPublisher,
			session::SessionEngine,
			sliding::SlidingEngine,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		kind::session::open::{Insertion, Member, OpenSessions, Removal},
		meta::NoKey,
		prefetch::Prefetched,
		settings::WindowSettings,
		span::{WindowAnchor, WindowSpan, window_row_key},
	},
};
use reifydb_value::{
	Result as ValueResult,
	config::ExtensionParams,
	error::Error,
	reifydb_assertions,
	value::{datetime::DateTime, diff_type::DiffType, row_number::RowNumber},
};
use tracing::debug;

use crate::{
	error::{Result, SdkError},
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::{operator::OperatorColumn, row::Row},
		context::{GuestContext, Windowed},
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			RollingGroups, Routing, WindowGroups, all_rows, group_of,
			guest_as_host::GuestAsHost,
			hooks::SdkHooks,
			intern_window_groups, observe_batch,
			operator::{
				Contribution, Contributions, Emit, Extracted, KindSet, WindowedOperator, batch_len,
				counted_merge, extract_rows,
			},
			operator_watermark, partition_of,
			publish::{Emitted, UpdateRow},
			rolling_cutoff, rolling_output, route_rolling,
			rows::SdkRows,
			timer_frontier, window_engine_config,
		},
	},
};

const FORGET_BATCH: usize = 256;

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Buckets<A> = Routing<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, Contributions<A>>;
struct RollingMode<A: Emit> {
	engine: PaneEngine<A::GroupKey, A::Coord, A::Accumulator>,
	pane: <A::Coord as Coord>::Span,
	carries: bool,
}

type RollingEmits<A> = Vec<RollingEmit<<A as WindowedOperator>::GroupKey, NoKey, <A as WindowedOperator>::Output>>;

struct SlidingMode<A: Emit> {
	engine: SlidingEngine<A::GroupKey, A::Coord, A::Accumulator>,
}

struct SessionMode<A: Emit> {
	engine: SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
}

enum SessionRow<A: Emit> {
	Admit {
		group: A::GroupKey,
		number: RowNumber,
		coord: A::Coord,
		contribution: Contribution<A>,
	},
	Retract {
		group: A::GroupKey,
		number: RowNumber,
		contribution: Contribution<A>,
	},
	Update {
		number: RowNumber,
		pre_coord: Option<A::Coord>,
		post_coord: Option<A::Coord>,
		before: Option<(A::GroupKey, Contribution<A>)>,
		after: Option<(A::GroupKey, Contribution<A>)>,
	},
}

struct GroupSessions<S: Coord> {
	partition: GroupId,
	open: OpenSessions<S>,
	changed: bool,
}

struct SessionBatch<A: Emit> {
	groups: BTreeMap<A::GroupKey, GroupSessions<A::Coord>>,
	touched: BTreeSet<(A::GroupKey, u64)>,
	removed: BTreeSet<(A::GroupKey, u64)>,
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

	fn session_group(group: &A::GroupKey, id: u64) -> GroupId {
		GroupId::of(&Self::row_key(group, <A::Coord as Coord>::from_order(id)))
	}

	fn session_span(id: u64) -> WindowSpan<A::Coord> {
		WindowSpan::new(<A::Coord as Coord>::from_order(id), <A::Coord as Coord>::from_order(id + 1))
	}

	fn route(
		aggregator: &A,
		mode: &SlidingMode<A>,
		ctx: &mut impl GuestContext,
		change: &impl ChangeView,
	) -> Result<Buckets<A>> {
		let mut buckets: Buckets<A> = Routing::default();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						Self::push_all(aggregator, mode, ctx, &cols, &mut buckets, true)?;
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						let before = extract_rows(aggregator, ctx, &pre, &all_rows(&pre))?;
						let after = extract_rows(aggregator, ctx, &post, &all_rows(&post))?;
						Self::push_extracted(mode, before, &mut buckets, false);
						Self::push_extracted(mode, after, &mut buckets, true);
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						Self::push_all(aggregator, mode, ctx, &cols, &mut buckets, false)?;
					}
				}
			}
		}
		Ok(buckets)
	}

	fn push_all<C: ColumnsView>(
		aggregator: &A,
		mode: &SlidingMode<A>,
		ctx: &mut impl GuestContext,
		cols: &C,
		buckets: &mut Buckets<A>,
		is_add: bool,
	) -> Result<()> {
		let extracted = extract_rows(aggregator, ctx, cols, &all_rows(cols))?;
		Self::push_extracted(mode, extracted, buckets, is_add);
		Ok(())
	}

	fn push_extracted(mode: &SlidingMode<A>, extracted: Vec<Extracted<A>>, buckets: &mut Buckets<A>, is_add: bool) {
		let op = if is_add {
			AccumulatorOp::Add
		} else {
			AccumulatorOp::Remove
		};
		for (_, coord, group, contribution) in extracted {
			for span in mode.engine.spans(coord) {
				buckets.push(group.clone(), span, op, contribution.clone());
			}
		}
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
	#[allow(clippy::too_many_arguments)]
	fn expire_through<C: GuestContext>(
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		store: &mut GuestAsHost<'_, C>,
		frontier: A::Coord,
		seal_span: SealSpan<A>,
		may_retire: bool,
	) -> Result<()> {
		let horizon = <A::Coord as SealDomain>::horizon(frontier, seal_span);
		if horizon <= <A::Coord as Coord>::from_order(0) {
			return Ok(());
		}
		let store_queue_empty = *reap_queue_empty;
		let closing = engine.expire(store, horizon.to_order().saturating_sub(1))?;
		let expired: Vec<GroupId> = closing.iter().map(|window| window.group_id).collect();
		if may_retire && StateStore::state_retires_groups(store) {
			enqueue_retired(store, &expired)?;
			*reap_queue_empty = true;
			return Ok(());
		}
		enqueue(store, &expired)?;
		if !expired.is_empty() {
			*reap_queue_empty = false;
		}
		if *reap_queue_empty {
			reifydb_assertions! {
				let pending = reifydb_flow_async::operator::state::reaper::queued(store, 1)?;
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
			true => drain_groups(store, &expired, engine, FORGET_BATCH)?,
			false => drain(store, engine, FORGET_BATCH)?,
		};
		*reap_queue_empty = drained.queue_is_empty();
		if !*reap_queue_empty {
			observe_batch(store, frontier, seal_span)?;
		}
		Ok(())
	}
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

	fn expire_rolling<C: GuestContext>(
		aggregator: &A,
		mode: &mut RollingMode<A>,
		settings: &WindowSettings<A::Coord>,
		store: &mut GuestAsHost<'_, C>,
		clock: A::Coord,
	) -> Result<Emitted<A>> {
		let pane = mode.pane;
		let finalize = |group: &A::GroupKey, newest: A::Coord, accumulator: &A::Accumulator| {
			rolling_output(aggregator, settings, pane, group, newest, accumulator)
		};
		let carry = |prior: &A::Output| -> ValueResult<Option<A::Output>> { Ok(aggregator.carry_empty(prior)) };
		let zero: Option<&dyn Fn(&A::Output) -> ValueResult<Option<A::Output>>> =
			mode.carries.then_some(&carry);
		let before = mode.engine.earliest_expiry(store)?;
		let emits = match rolling_cutoff::<A>(settings, pane, clock) {
			Some(cutoff) => mode.engine.expire(
				store,
				cutoff,
				&RowPublisher {
					key: None,
					finalize: &finalize,
					zero,
				},
			)?,
			None => Vec::new(),
		};
		let after = mode.engine.earliest_expiry(store)?;
		<A::Coord as SealDomain>::rearm_decay(store, settings, before, after)?;
		Ok(Self::rolling_emitted(emits))
	}

	fn apply_rolling<C: GuestContext>(
		aggregator: &A,
		mode: &mut RollingMode<A>,
		seal_span: Option<SealSpan<A>>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		groups: RollingGroups<A>,
		order: &[A::GroupKey],
	) -> Result<Emitted<A>> {
		let mut store = GuestAsHost(ctx);
		let newest = groups.values().flat_map(|(_, events)| events.iter().flat_map(PaneEvent::panes)).max();
		if let (Some(seal_span), Some(newest)) = (seal_span, newest) {
			observe_batch(&mut store, newest, seal_span)?;
		}
		let frontier = operator_watermark::<A::Coord>(&mut store)?;
		let clock = match (seal_span, newest) {
			(None, Some(newest)) => frontier.max(newest),
			_ => frontier,
		};
		let pane = mode.pane;
		let finalize = |group: &A::GroupKey, newest: A::Coord, accumulator: &A::Accumulator| {
			rolling_output(aggregator, settings, pane, group, newest, accumulator)
		};
		let carry = |prior: &A::Output| -> ValueResult<Option<A::Output>> { Ok(aggregator.carry_empty(prior)) };
		let zero: Option<&dyn Fn(&A::Output) -> ValueResult<Option<A::Output>>> =
			mode.carries.then_some(&carry);
		let cutoff = rolling_cutoff::<A>(settings, pane, clock);
		let before = mode.engine.earliest_expiry(&mut store)?;
		let mut emits = match cutoff {
			Some(cutoff) => mode.engine.expire(
				&mut store,
				cutoff,
				&RowPublisher {
					key: None,
					finalize: &finalize,
					zero,
				},
			)?,
			None => Vec::new(),
		};
		let keys = mode.engine.state_keys(&groups);
		let mut prefetched = Prefetched::read(&mut store, keys)?;
		let applied = mode.engine.apply(
			&mut prefetched,
			groups,
			order,
			cutoff,
			&|| aggregator.new_accumulator(settings),
			&RowPublisher {
				key: Some(&|_: &A::GroupKey| NoKey),
				finalize: &finalize,
				zero,
			},
		)?;
		prefetched.write()?;
		if applied.sealed_drops + applied.stale_retractions > 0 {
			debug!(
				operator = A::NAME,
				sealed_drops = applied.sealed_drops,
				stale_retractions = applied.stale_retractions,
				"rolling input dropped past its group's seal or retract line"
			);
		}
		emits.extend(applied.emits);
		let after = mode.engine.earliest_expiry(&mut store)?;
		<A::Coord as SealDomain>::rearm_decay(&mut store, settings, before, after)?;
		Ok(Self::rolling_emitted(emits))
	}

	#[allow(clippy::too_many_arguments)]
	fn drop_sealed<C: GuestContext>(
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		seal_span: SealSpan<A>,
		ctx: &mut C,
		buckets: &mut TumblingBuckets<A::GroupKey, A::Coord, Contributions<A>>,
		order: &mut Vec<(A::GroupKey, WindowSpan<A::Coord>)>,
	) -> Result<()> {
		let mut store = GuestAsHost(ctx);
		let newest = buckets.keys().map(|(_, span)| span.start).max();
		if let Some(newest) = newest {
			observe_batch(&mut store, newest, seal_span)?;
		}
		let watermark = operator_watermark::<A::Coord>(&mut store)?;
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
		order.retain(|key| buckets.contains_key(key));
		Self::expire_through(engine, reap_queue_empty, &mut store, watermark, seal_span, true)
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_plain_tumbling<C: GuestContext, V: ChangeView>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		change: &V,
	) -> Result<Emitted<A>> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		let mut routed: Routed<A::GroupKey, A::Coord, Contributions<A>> =
			route_tumbling(&mut SdkRows::new(aggregator, ctx, change), settings.fixed_size())?;
		if routed.buckets.is_empty() {
			return Ok(emitted);
		}
		if let Some(seal_span) = seal_span {
			Self::drop_sealed(
				engine,
				reap_queue_empty,
				seal_span,
				ctx,
				&mut routed.buckets,
				&mut routed.arrival,
			)?;
			if routed.buckets.is_empty() {
				return Ok(emitted);
			}
		}
		let emits = finish_tumbling(
			engine,
			&mut GuestAsHost(ctx),
			routed,
			&mut SdkHooks::new(aggregator, settings),
			ExpiryAnchor::WindowStart,
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

	fn apply_tumbling<C: GuestContext>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, A::Coord, A::Accumulator>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		routing: Buckets<A>,
	) -> Result<Emitted<A>> {
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		let (mut buckets, mut order) = routing.finish();
		if let Some(seal_span) = seal_span {
			Self::drop_sealed(engine, reap_queue_empty, seal_span, ctx, &mut buckets, &mut order)?;
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
			engine.apply(
				&mut store,
				buckets,
				&order,
				|group, window_start| group_of(&groups, group, window_start),
				|| aggregator.new_accumulator(settings),
			)?
		};

		if seal_span.is_some() {
			let inserted: Vec<_> = results
				.iter()
				.filter(|r| r.kind == EmitKind::Insert)
				.map(|r| (r, group_of(&groups, &r.group, r.span.start)))
				.collect();
			let entries: Vec<_> = inserted
				.iter()
				.map(|(r, (group, key))| WindowReindex {
					group: &r.group,
					window_start: r.span.start,
					id: *group,
					slot_key: key,
					prior: None,
					new: Some(r.span.start.to_order()),
				})
				.collect();
			engine.reindex_windows(&mut GuestAsHost(ctx), &entries)?;
		}

		if results.is_empty() {
			return Ok(emitted);
		}
		for r in results {
			let Some(time) = r.first_time else {
				return Err(SdkError::Other(format!(
					"a tumbling result for row {:?} carries no first time",
					r.row_number
				)));
			};
			let pre = r.prior.as_ref().and_then(|prior| aggregator.build_output(&r.group, r.span, prior));
			let out = match r.kind {
				EmitKind::Remove => None,
				EmitKind::Insert | EmitKind::Update => {
					aggregator.build_output(&r.group, r.span, &r.value)
				}
			};
			match (out, pre) {
				(Some(out), None) => emitted.0.push((r.row_number, time, out)),
				(Some(out), Some(pre)) => emitted.1.push((r.row_number, time, Some(pre), out)),
				(None, Some(pre)) => emitted.2.push((r.row_number, time, pre)),
				(None, None) => {}
			}
		}
		Ok(emitted)
	}

	#[allow(clippy::too_many_arguments)]
	fn move_members<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		buckets: &mut Buckets<A>,
		ctx: &mut C,
		group: &A::GroupKey,
		from: u64,
		to: u64,
		members: &[Member<A::Coord>],
	) -> Result<()> {
		let (from_group, to_group) = (Self::session_group(group, from), Self::session_group(group, to));
		for member in members {
			let contribution: Contribution<A> =
				engine.load_member(&mut GuestAsHost(ctx), from_group, member.row)?;
			buckets.push(
				group.clone(),
				Self::session_span(from),
				AccumulatorOp::Remove,
				contribution.clone(),
			);
			buckets.push(group.clone(), Self::session_span(to), AccumulatorOp::Add, contribution.clone());
			engine.drop_member(&mut GuestAsHost(ctx), from_group, member.row)?;
			engine.store_member(&mut GuestAsHost(ctx), to_group, member.row, &contribution)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	fn admit_session_row<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: A::GroupKey,
		row: RowNumber,
		coord: A::Coord,
		contribution: Contribution<A>,
		horizon: A::Coord,
	) -> Result<()> {
		let sessions = batch.groups.get_mut(&group).expect("every routed group's sessions are loaded first");
		let insertion = sessions.open.insert(coord, row, horizon);
		if insertion != Insertion::Refused {
			sessions.changed = true;
		}
		let target = match insertion {
			Insertion::Refused => {
				batch.refused += 1;
				return Ok(());
			}
			Insertion::Opened(id) | Insertion::Extended(id) => id,
			Insertion::Merged {
				survivor,
				absorbed,
			} => {
				for session in &absorbed {
					Self::move_members(
						engine,
						&mut batch.buckets,
						ctx,
						&group,
						session.id,
						survivor,
						&session.members,
					)?;
					batch.removed.insert((group.clone(), session.id));
				}
				survivor
			}
		};
		batch.touched.insert((group.clone(), target));
		engine.store_member(&mut GuestAsHost(ctx), Self::session_group(&group, target), row, &contribution)?;
		batch.buckets.push(group, Self::session_span(target), AccumulatorOp::Add, contribution);
		Ok(())
	}

	fn holder(batch: &SessionBatch<A>, group: &A::GroupKey, row: RowNumber) -> Option<u64> {
		batch.groups
			.get(group)?
			.open
			.sessions()
			.iter()
			.find(|session| session.members.iter().any(|m| m.row == row))
			.map(|session| session.id)
	}

	fn retract_session_row<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		group: A::GroupKey,
		row: RowNumber,
		contribution: Contribution<A>,
	) -> Result<()> {
		let Some(id) = Self::holder(batch, &group, row) else {
			batch.dropped += 1;
			return Ok(());
		};
		let sessions = batch.groups.get_mut(&group).expect("a held row's group is loaded");
		let removal = sessions
			.open
			.remove(id, row)
			.map_err(|err| SdkError::OperatorError(err.to_string()))?
			.expect("the row was found in an open session");
		sessions.changed = true;
		batch.buckets.push(group.clone(), Self::session_span(id), AccumulatorOp::Remove, contribution);
		engine.drop_member(&mut GuestAsHost(ctx), Self::session_group(&group, id), row)?;
		match removal {
			Removal::Emptied(id) => {
				batch.removed.insert((group, id));
			}
			Removal::Shrunk(id) => {
				batch.touched.insert((group, id));
			}
			Removal::Split {
				kept,
				moved,
			} => {
				batch.touched.insert((group.clone(), kept));
				batch.touched.insert((group.clone(), moved.id));
				Self::move_members(
					engine,
					&mut batch.buckets,
					ctx,
					&group,
					kept,
					moved.id,
					&moved.members,
				)?;
			}
		}
		Ok(())
	}

	fn extract_session_rows<C: GuestContext>(
		aggregator: &A,
		ctx: &mut C,
		change: &impl ChangeView,
		refused: &mut u64,
	) -> Result<Vec<SessionRow<A>>> {
		let mut rows = Vec::new();
		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					let Some(cols) = diff.post() else {
						continue;
					};
					let numbers = cols.row_numbers();
					let numbered: Vec<usize> = (0..cols.row_count().min(numbers.len())).collect();
					*refused += (cols.row_count() - numbered.len()) as u64;
					for (i, coord, group, contribution) in
						extract_rows(aggregator, ctx, &cols, &numbered)?
					{
						rows.push(SessionRow::Admit {
							group,
							number: numbers[i],
							coord,
							contribution,
						});
					}
				}
				DiffType::Remove => {
					let Some(cols) = diff.pre() else {
						continue;
					};
					let numbers = cols.row_numbers();
					let numbered: Vec<usize> = (0..cols.row_count().min(numbers.len())).collect();
					*refused += (cols.row_count() - numbered.len()) as u64;
					let extracted = batch_len::<A, _>(
						aggregator.extract(ctx, &cols, &numbered)?,
						numbered.len(),
						"extract_batch",
					)?;
					for (i, extracted) in numbered.into_iter().zip(extracted) {
						let Some((group, contribution)) = extracted else {
							continue;
						};
						rows.push(SessionRow::Retract {
							group,
							number: numbers[i],
							contribution,
						});
					}
				}
				DiffType::Update => {
					let (Some(pre), Some(post)) = (diff.pre(), diff.post()) else {
						return Err(SdkError::OperatorError(format!(
							"{} received an update diff without both pre and post columns",
							A::NAME
						)));
					};
					let paired = pre.row_count();
					if post.row_count() != paired {
						return Err(SdkError::OperatorError(format!(
							"{} received an update diff pairing {paired} pre rows with {} post rows",
							A::NAME,
							post.row_count()
						)));
					}
					let numbers = pre.row_numbers();
					let numbered: Vec<usize> = (0..paired.min(numbers.len())).collect();
					*refused += 2 * (paired - numbered.len()) as u64;
					let rows_len = numbered.len();
					let pre_coords = batch_len::<A, _>(
						aggregator.coord(&pre, &numbered)?,
						rows_len,
						"coord_batch",
					)?;
					let post_coords = batch_len::<A, _>(
						aggregator.coord(&post, &numbered)?,
						rows_len,
						"coord_batch",
					)?;
					let before = batch_len::<A, _>(
						aggregator.extract(ctx, &pre, &numbered)?,
						rows_len,
						"extract_batch",
					)?;
					let after = batch_len::<A, _>(
						aggregator.extract(ctx, &post, &numbered)?,
						rows_len,
						"extract_batch",
					)?;
					let paired_rows = numbered
						.into_iter()
						.zip(pre_coords)
						.zip(post_coords)
						.zip(before)
						.zip(after);
					for ((((i, pre_coord), post_coord), before), after) in paired_rows {
						rows.push(SessionRow::Update {
							number: numbers[i],
							pre_coord,
							post_coord,
							before,
							after,
						});
					}
				}
			}
		}
		Ok(rows)
	}

	fn load_session_groups<C: GuestContext>(
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		batch: &mut SessionBatch<A>,
		ctx: &mut C,
		rows: &[SessionRow<A>],
		horizon: A::Coord,
	) -> Result<()> {
		let mut groups: BTreeSet<A::GroupKey> = BTreeSet::new();
		for row in rows {
			match row {
				SessionRow::Admit {
					group,
					..
				}
				| SessionRow::Retract {
					group,
					..
				} => {
					groups.insert(group.clone());
				}
				SessionRow::Update {
					before,
					after,
					..
				} => {
					for (group, _) in before.iter().chain(after.iter()) {
						groups.insert(group.clone());
					}
				}
			}
		}
		if groups.is_empty() {
			return Ok(());
		}
		let partitions: Vec<GroupId> = groups.iter().map(partition_of).collect();
		let loaded = engine.load_open(&mut GuestAsHost(ctx), &partitions)?;
		for ((group, partition), mut open) in groups.into_iter().zip(partitions).zip(loaded) {
			let changed = !open.close(horizon).is_empty();
			batch.groups.insert(
				group,
				GroupSessions {
					partition,
					open,
					changed,
				},
			);
		}
		Ok(())
	}

	fn route_session<C: GuestContext>(
		aggregator: &A,
		engine: &SessionEngine<A::GroupKey, A::Coord, A::Accumulator>,
		ctx: &mut C,
		change: &impl ChangeView,
		horizon: A::Coord,
	) -> Result<SessionBatch<A>> {
		let mut batch = SessionBatch {
			groups: BTreeMap::new(),
			touched: BTreeSet::new(),
			removed: BTreeSet::new(),
			buckets: Routing::default(),
			dropped: 0,
			refused: 0,
		};
		let rows = Self::extract_session_rows(aggregator, ctx, change, &mut batch.refused)?;
		Self::load_session_groups(engine, &mut batch, ctx, &rows, horizon)?;
		for row in rows {
			match row {
				SessionRow::Admit {
					group,
					number,
					coord,
					contribution,
				} => {
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
				SessionRow::Retract {
					group,
					number,
					contribution,
				} => {
					Self::retract_session_row(
						engine,
						&mut batch,
						ctx,
						group,
						number,
						contribution,
					)?;
				}
				SessionRow::Update {
					number,
					pre_coord,
					post_coord,
					before,
					after,
				} => {
					if let (
						Some(pre_coord),
						Some(post_coord),
						Some((pre_group, pre_value)),
						Some((post_group, post_value)),
					) = (pre_coord, post_coord, &before, &after)
						&& pre_group == post_group && pre_coord == post_coord
						&& let Some(id) = Self::holder(&batch, pre_group, number)
					{
						let span = Self::session_span(id);
						batch.buckets.push(
							pre_group.clone(),
							span,
							AccumulatorOp::Remove,
							pre_value.clone(),
						);
						batch.buckets.push(
							pre_group.clone(),
							span,
							AccumulatorOp::Add,
							post_value.clone(),
						);
						engine.store_member(
							&mut GuestAsHost(ctx),
							Self::session_group(pre_group, id),
							number,
							post_value,
						)?;
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
						)?;
					}
					if let (Some(coord), Some((group, contribution))) = (post_coord, after) {
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
		let now = ctx.batch_time();
		let watermark = operator_watermark::<A::Coord>(&mut GuestAsHost(ctx))?;
		let horizon = <A::Coord as SealDomain>::horizon(watermark, seal_span);
		let SessionBatch {
			groups: session_groups,
			touched,
			removed,
			buckets: routing,
			dropped,
			refused,
		} = Self::route_session(aggregator, &mode.engine, ctx, change, horizon)?;
		let (buckets, order) = routing.finish();
		if dropped > 0 || refused > 0 {
			debug!(operator = A::NAME, dropped, refused, "session mutations were dropped or refused");
		}

		let mut store = GuestAsHost(ctx);
		let mut saves = Vec::new();
		let mut drops = Vec::new();
		for sessions in session_groups.values().filter(|sessions| sessions.changed) {
			if sessions.open.sessions().is_empty() && sessions.open.sealed_last().is_none() {
				drops.push(sessions.partition);
			} else {
				saves.push((sessions.partition, &sessions.open));
			}
		}
		mode.engine.save_open(&mut store, &saves, &drops)?;

		let mut keys: BTreeSet<(A::GroupKey, u64)> =
			buckets.keys().map(|(group, span)| (group.clone(), span.start.to_order())).collect();
		keys.extend(touched.iter().cloned());
		keys.extend(removed.iter().cloned());
		let keys: Vec<(A::GroupKey, u64)> = keys.into_iter().collect();
		let session_ids: Vec<GroupId> =
			keys.iter().map(|(group, id)| Self::session_group(group, *id)).collect();
		let records = mode.engine.load_records(&mut store, &session_ids)?;
		let before: BTreeMap<(A::GroupKey, u64), (DateTime, A::Coord, A::Coord)> = keys
			.iter()
			.cloned()
			.zip(records)
			.filter_map(|(key, record)| record.map(|record| (key, record)))
			.collect();
		let current = |group: &A::GroupKey, id: u64| {
			session_groups.get(group).and_then(|sessions| {
				sessions.open
					.sessions()
					.iter()
					.find(|session| session.id == id)
					.map(|s| (s.start, s.last))
			})
		};
		let bounds_of = |group: &A::GroupKey, span: &WindowSpan<A::Coord>| {
			let id = span.start.to_order();
			current(group, id)
				.or_else(|| before.get(&(group.clone(), id)).map(|&(_, start, last)| (start, last)))
				.expect("every emitted session is open or was recorded before this batch")
		};

		if let Some(newest) = buckets.keys().map(|(group, span)| bounds_of(group, span).1).max() {
			observe_batch(&mut store, newest, seal_span)?;
		}
		let emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		Self::expire_through(
			mode.engine.tumbling_mut(),
			reap_queue_empty,
			&mut store,
			watermark,
			seal_span,
			false,
		)?;

		let mut records = Vec::new();
		let mut stale = Vec::new();
		let mut moved = Vec::new();
		for (group, id) in &touched {
			let Some((start, last)) = current(group, *id) else {
				continue;
			};
			let prior = before.get(&(group.clone(), *id)).copied();
			if prior.is_some_and(|(_, prior_start, prior_last)| (prior_start, prior_last) == (start, last))
			{
				continue;
			}
			let session = Self::session_group(group, *id);
			let first = prior.map_or_else(|| start.as_time().unwrap_or(now), |(first, ..)| first);
			records.push((session, first, start, last));
			moved.push((group, *id, session, prior.map(|(.., last)| last), Some(last)));
		}
		for (group, id) in &removed {
			if current(group, *id).is_some() {
				continue;
			}
			let session = Self::session_group(group, *id);
			stale.push(session);
			if let Some((.., last)) = before.get(&(group.clone(), *id)) {
				moved.push((group, *id, session, Some(*last), None));
			}
		}
		let slot_keys: Vec<EncodedKey> = moved
			.iter()
			.map(|(group, id, ..)| Self::row_key(group, <A::Coord as Coord>::from_order(*id)))
			.collect();
		let entries: Vec<_> = moved
			.iter()
			.zip(&slot_keys)
			.map(|((group, id, session, prior, new), key)| WindowReindex {
				group: *group,
				window_start: <A::Coord as Coord>::from_order(*id),
				id: *session,
				slot_key: key,
				prior: prior.map(Coord::to_order),
				new: new.map(Coord::to_order),
			})
			.collect();
		mode.engine.tumbling_mut().reindex_windows(&mut store, &entries)?;
		mode.engine.save_records(&mut store, &records, &stale)?;

		if buckets.is_empty() {
			drain_groups(&mut store, &stale, mode.engine.tumbling_mut(), usize::MAX)?;
			return Ok(emitted);
		}
		let groups: WindowGroups<A::GroupKey, A::Coord> = buckets
			.keys()
			.map(|(group, span)| {
				(
					(group.clone(), span.start),
					(
						Self::session_group(group, span.start.to_order()),
						Self::row_key(group, span.start),
					),
				)
			})
			.collect();
		let results = mode.engine.tumbling_mut().apply(
			&mut store,
			buckets,
			&order,
			|group, window_start| group_of(&groups, group, window_start),
			|| aggregator.new_accumulator(settings),
		)?;
		drain_groups(&mut store, &stale, mode.engine.tumbling_mut(), usize::MAX)?;

		let gap = mode.engine.gap();
		let (mut inserts, mut updates, mut removes) = emitted;
		for r in results {
			let id = r.span.start.to_order();
			let prior = before.get(&(r.group.clone(), id)).copied();
			let (start, last) = match r.kind {
				EmitKind::Remove => prior
					.map(|(_, start, last)| (start, last))
					.unwrap_or_else(|| bounds_of(&r.group, &r.span)),
				EmitKind::Insert | EmitKind::Update => bounds_of(&r.group, &r.span),
			};
			let span = WindowSpan {
				start,
				end: last.add_span(gap),
			};
			let time = prior.map_or_else(|| span.start.as_time().unwrap_or(now), |(first, ..)| first);
			let Some(out) = aggregator.build_output(&r.group, span, &r.value) else {
				continue;
			};
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, time, out)),
				EmitKind::Update => {
					let pre_span = prior.map_or(span, |(_, start, last)| WindowSpan {
						start,
						end: last.add_span(gap),
					});
					let pre = r
						.prior
						.as_ref()
						.and_then(|prior| aggregator.build_output(&r.group, pre_span, prior));
					updates.push((r.row_number, time, pre, out))
				}
				EmitKind::Remove => removes.push((r.row_number, time, out)),
			}
		}
		Ok((inserts, updates, removes))
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
			let merge = counted_merge::<A>();
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
				engine: PaneEngine::queue(window_engine_config(params), spans, merge),
				pane,
				carries,
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
			Some(mode) => Self::expire_rolling(aggregator, mode, settings, &mut store, frontier)?,
			None => {
				let may_retire = session.is_none();
				let tumbling = match (sliding, session) {
					(Some(mode), _) => mode.engine.tumbling_mut(),
					(None, Some(mode)) => mode.engine.tumbling_mut(),
					(None, None) => engine,
				};
				Self::expire_through(
					tumbling,
					reap_queue_empty,
					&mut store,
					frontier,
					seal_span,
					may_retire,
				)?;
				(Vec::new(), Vec::new(), Vec::new())
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
		if let Some(pane) = self.rolling.as_ref().map(|mode| mode.pane) {
			let (groups, order) = route_rolling(&self.aggregator, pane, ctx, &change)?;
			if groups.is_empty() {
				return Ok(());
			}
			let Self {
				aggregator,
				rolling: Some(mode),
				seal_span,
				settings,
				..
			} = &mut *self
			else {
				unreachable!("the rolling mode was present a line above")
			};
			let (inserts, updates, removes) =
				Self::apply_rolling(aggregator, mode, *seal_span, settings, ctx, groups, &order)?;
			return self.emit_batches(ctx, &inserts, &updates, &removes);
		}
		let (inserts, updates, removes) = {
			let Self {
				aggregator,
				engine,
				sliding,
				reap_queue_empty,
				seal_span,
				settings,
				..
			} = &mut *self;
			match sliding {
				Some(mode) => {
					let buckets = Self::route(aggregator, mode, ctx, &change)?;
					if buckets.is_empty() {
						return Ok(());
					}
					Self::apply_tumbling(
						aggregator,
						mode.engine.tumbling_mut(),
						reap_queue_empty,
						*seal_span,
						settings,
						ctx,
						buckets,
					)?
				}
				None => Self::apply_plain_tumbling(
					aggregator,
					engine,
					reap_queue_empty,
					*seal_span,
					settings,
					ctx,
					&change,
				)?,
			}
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
		accumulator::{WindowAccumulator, invertible::last_value::LastValue},
		coord::{DateTimeCoord, RowSpan, U8Coord},
	};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::row;

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
		let sliding = driver.sliding.as_ref().expect("a sliding view builds a sliding engine");

		assert_eq!(driver.settings.slide, Some(RowSpan::of(4)));
		assert_eq!(
			sliding.engine.spans(U8Coord(23)),
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

		assert_eq!(rolling_cutoff::<TimeProbe>(&lagged, secs(1), at(20)), Some(at(13)));
		assert_eq!(rolling_cutoff::<TimeProbe>(&unlagged, secs(1), at(20)), Some(at(15)));
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
}
