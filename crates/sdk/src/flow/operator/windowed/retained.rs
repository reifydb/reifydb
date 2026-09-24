// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::{OperatorState, StateCodec},
};
use reifydb_core::{
	common::WindowRequirements,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::GroupId,
	metrics::heap::{HeapSize, OperatorSample},
	operator_with::ApplyWith,
	state::timer::StateStore,
};
#[cfg(reifydb_assertions)]
use reifydb_flow::operator::state::reaper::queued;
use reifydb_flow::{
	operator::{
		state::{
			reaper::{drain, drain_groups, enqueue},
			seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
		},
		state_access::{get_classified, put},
	},
	window::{
		accumulator::invertible::retained_map::RetainedAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, PublishKey,
			publish::PublishState,
			tumbling::TumblingBuckets,
			tumbling_retained::{RetainedEntry, RetainedTumblingEngine},
		},
		settings::WindowSettings,
		span::{WindowSpan, window_row_key},
	},
};
use reifydb_value::{
	config::ExtensionParams,
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
			operator::{Emit, WindowedOperator},
			publish::{Emitted, UpdateRow, load_publish_states, publish_row},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

const SEAL_REAP_BATCH: usize = 256;

type SealSpan<A> = <<A as WindowedOperator>::Coord as SealDomain>::SealSpan;
type Buckets<A, K, V> = TumblingBuckets<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, (K, V)>;
type WindowOrder<A> = Vec<(<A as WindowedOperator>::GroupKey, WindowSpan<<A as WindowedOperator>::Coord>)>;
type Engine<A, K, V> = RetainedTumblingEngine<<A as WindowedOperator>::GroupKey, <A as WindowedOperator>::Coord, K, V>;

pub struct RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>>,
	A::Output: Row,
	K: Ord,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: Engine<A, K, V>,
	reap_queue_empty: bool,
	seal_span: Option<SealSpan<A>>,
	throttle: Option<<A::Coord as Coord>::Span>,
	settings: WindowSettings<A::Coord>,
}

impl<A, K, V> RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>> + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	K: Ord + Clone + Debug + HeapSize + StateCodec + Send + Sync,
	V: Clone + Debug + PartialEq + HeapSize + Send + Sync,
	RetainedEntry<K, V>: OperatorState,
	RetainedAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn window_span(&self, coord: A::Coord) -> WindowSpan<A::Coord> {
		WindowSpan::for_coord(coord, self.settings.fixed_size())
	}

	fn row_key(group: &A::GroupKey, window_start: A::Coord) -> EncodedKey {
		window_row_key(group.into_encoded_key(), window_start)
	}

	fn route(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> Buckets<A, K, V> {
		let mut buckets: Buckets<A, K, V> = BTreeMap::new();

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
		buckets: &mut Buckets<A, K, V>,
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
			let span = self.window_span(coord);
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, span)).or_default().push(event);
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn expire_through<C: GuestContext>(
		aggregator: &A,
		engine: &mut Engine<A, K, V>,
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
				engine,
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
		engine: &Engine<A, K, V>,
		settings: &WindowSettings<A::Coord>,
		store: &mut GuestAsHost<'_, C>,
		group: &A::GroupKey,
		group_id: GroupId,
		window_start: A::Coord,
		frontier: A::Coord,
		emitted: &mut Emitted<A>,
	) -> Result<()> {
		let key = PublishKey::new(group_id, Self::row_key(group, window_start));
		let Some(mut state) = get_classified::<_, PublishState>(store, &key)? else {
			return Ok(());
		};
		if !state.dirty {
			return Ok(());
		}
		let entries = engine.load_entries(store, group_id)?;
		reifydb_assertions! {
			assert!(
				!entries.is_empty(),
				"a dirty window closed with no entries; emptying a window must store a clean publish state, \
				 otherwise the close retracts a row downstream already dropped (group={group_id:?})"
			);
		}
		let out = match entries.is_empty() {
			true => None,
			false => aggregator.build_output(
				group,
				WindowSpan::for_coord(window_start, settings.fixed_size()),
				&entries,
			),
		};
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

	#[allow(clippy::too_many_arguments)]
	fn apply_tumbling<C: GuestContext>(
		aggregator: &A,
		engine: &mut Engine<A, K, V>,
		reap_queue_empty: &mut bool,
		seal_span: Option<SealSpan<A>>,
		throttle: Option<<A::Coord as Coord>::Span>,
		settings: &WindowSettings<A::Coord>,
		ctx: &mut C,
		mut buckets: Buckets<A, K, V>,
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
			engine.apply(&mut store, buckets, &order, |group, window_start| {
				(group_of(&groups, group, window_start), Self::row_key(group, window_start))
			})?
		};

		if seal_span.is_some() {
			let mut store = GuestAsHost(ctx);
			for r in &results {
				if r.kind == EmitKind::Insert {
					engine.reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						r.group_id,
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
			.map(|r| PublishKey::new(r.group_id, Self::row_key(&r.group, r.span.start)))
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
			let entries = engine.load_entries(&mut store, r.group_id)?;
			let out = aggregator.build_output(&r.group, r.span, &entries);
			publish_row::<A>(r.row_number, out, &mut state, frontier, &mut emitted)?;
			put(&mut store, &key, state)?;
		}
		Ok(emitted)
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

impl<A, K, V> OperatorMetadata for RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>> + 'static,
	A::Output: Row,
	K: Ord,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = A::CAPABILITIES;
}

impl<A, K, V> MountedOperator for RetainedDriver<A, K, V>
where
	A: Emit<Accumulator = RetainedAccumulator<K, V>> + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	K: Ord + Clone + Debug + HeapSize + StateCodec + Send + Sync + 'static,
	V: Clone + Debug + PartialEq + HeapSize + Send + Sync + 'static,
	RetainedEntry<K, V>: OperatorState,
	RetainedAccumulator<K, V>: OperatorState + StateCodec + HeapSize,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	type Class = Windowed;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: &["tumbling"],
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
		with.require_window("tumbling")?;
		with.check_throttle(true)?;
		let seal_span = <A::Coord as SealDomain>::seal_span_of(with)?;
		let throttle = <A::Coord as SealDomain>::throttle_of(with)?;
		let settings = <A::Coord as SealDomain>::window_settings_of(with)?;
		let aggregator = A::create(operator_id, params, with)?;
		Ok(Self {
			aggregator,
			engine: RetainedTumblingEngine::new(window_engine_config(params)),
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
			reap_queue_empty,
			settings,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		let Some(frontier) = timer_frontier::<A::Coord>(&mut store, timer)? else {
			return Ok(());
		};
		let mut emitted: Emitted<A> = (Vec::new(), Vec::new(), Vec::new());
		Self::expire_through(
			aggregator,
			engine,
			reap_queue_empty,
			settings,
			&mut store,
			frontier,
			seal_span,
			&mut emitted,
		)?;
		let (inserts, updates, removes) = emitted;
		self.emit_batches(ctx, &inserts, &updates, &removes)
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
				reap_queue_empty,
				seal_span,
				throttle,
				settings,
			} = &mut *self;
			Self::apply_tumbling(
				aggregator,
				engine,
				reap_queue_empty,
				*seal_span,
				*throttle,
				settings,
				ctx,
				buckets,
			)?
		};
		self.emit_batches(ctx, &inserts, &updates, &removes)
	}
}

#[cfg(test)]
mod tests {
	use std::marker::PhantomData;

	use reifydb_core::common::{WindowKind, WindowSize};
	use reifydb_flow::window::coord::OrdinalCoord;
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::*;
	use crate::row;

	struct Out {
		v: i64,
	}

	row!(Out {
		v: i64
	});

	macro_rules! probe {
		($name:ident, $coord:ty) => {
			struct $name(PhantomData<$coord>);

			impl OperatorMetadata for $name {
				const NAME: &'static str = "probe";
				const VERSION: &'static str = "0";
				const DESCRIPTION: &'static str = "probe";
				const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
				const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
				const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
			}

			impl WindowedOperator for $name {
				type Coord = $coord;
				type GroupKey = u64;
				type Accumulator = RetainedAccumulator<u64, i64>;
				type Output = Out;

				fn create(_: OperatorId, _: &ExtensionParams, _: &ApplyWith) -> Result<Self> {
					Ok(Self(PhantomData))
				}

				fn coord(&self, _: &impl crate::flow::operator::view::RowView) -> Option<$coord> {
					None
				}

				fn extract(
					&self,
					_: &mut impl GuestContext<Windowed>,
					_: &impl crate::flow::operator::view::RowView,
				) -> Option<(u64, (u64, i64))> {
					None
				}

				fn new_accumulator(&self, _: &WindowSettings<$coord>) -> RetainedAccumulator<u64, i64> {
					RetainedAccumulator::default()
				}
			}

			impl Emit for $name {
				type Kinds = crate::flow::operator::windowed::operator::NoRolling;

				fn build_output(
					&self,
					_: &u64,
					_: WindowSpan<$coord>,
					_: &BTreeMap<u64, i64>,
				) -> Option<Out> {
					None
				}
			}
		};
	}

	probe!(TimeProbe, DateTime);
	probe!(SlotProbe, OrdinalCoord);

	fn params() -> ExtensionParams {
		ExtensionParams::new("probe", Default::default())
	}

	#[test]
	fn create_refuses_a_window_that_is_not_tumbling() {
		// Only the tumbling path reads entries; a sliding window accepted here would publish from the wrong
		// spans.
		let tumbling = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
			}),
			..ApplyWith::default()
		};
		let sliding = ApplyWith {
			window: Some(WindowKind::Sliding {
				size: WindowSize::Duration(Duration::from_seconds(60).unwrap()),
				slide: WindowSize::Duration(Duration::from_seconds(10).unwrap()),
			}),
			..ApplyWith::default()
		};
		assert!(
			RetainedDriver::<TimeProbe, u64, i64>::create(OperatorId(1), &params(), &tumbling).is_ok(),
			"precondition"
		);
		assert!(RetainedDriver::<TimeProbe, u64, i64>::create(OperatorId(1), &params(), &sliding).is_err());
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
		assert!(
			RetainedDriver::<SlotProbe, u64, i64>::create(OperatorId(1), &params(), &with).is_ok(),
			"precondition"
		);
		with.throttle = Some(Duration::from_seconds(5).unwrap());
		assert!(RetainedDriver::<SlotProbe, u64, i64>::create(OperatorId(1), &params(), &with).is_err());
	}

	#[test]
	fn a_throttle_on_a_count_window_names_the_throttle() {
		// Without the throttle check the user is told the count window is wrong, never that the throttle is.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: None,
			immutable: None,
			retention: None,
			throttle: Some(Duration::from_seconds(5).unwrap()),
		};
		let err = RetainedDriver::<TimeProbe, u64, i64>::create(OperatorId(1), &params(), &with)
			.err()
			.expect("a throttle on a count window must be refused");
		assert!(err.to_string().contains("FLOW_083"), "expected FLOW_083, got: {err}");
	}
}
