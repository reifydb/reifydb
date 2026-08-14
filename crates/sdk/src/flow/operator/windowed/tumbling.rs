// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::StateCodec,
};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::DiffType, flow::OperatorCapability},
	metrics::heap::{HeapSize, OperatorSample},
};
use reifydb_flow::{
	operator::state::{
		reaper::{drain, enqueue},
		seal::{coord::Coord, ledger::FiredAt, policy::is_sealed},
	},
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		span::WindowSpan,
	},
};
use reifydb_value::{
	config::Config,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::debug;

use crate::{
	error::Result,
	flow::operator::{
		GuestOperator, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::GuestContext,
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{
			advance_seal_frontier, arm_seal_timer, group_of, guest_as_host::GuestAsHost,
			intern_window_groups, seal_frontier, seal_horizon_of, window_engine_config,
		},
	},
};

const SEAL_REAP_BATCH: usize = 256;

type AccumulatorContribution<A> = <<A as TumblingOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingOperator>::Accumulator as WindowAccumulator>::Output;
type Buckets<A> = TumblingBuckets<<A as TumblingOperator>::GroupKey, DateTime, AccumulatorContribution<A>>;
type WindowOrder<A> = Vec<(<A as TumblingOperator>::GroupKey, WindowSpan<DateTime>)>;

pub trait TumblingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + StateCodec;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

	fn extract(
		&self,
		ctx: &mut impl GuestContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, AccumulatorContribution<Self>)>;

	fn window_for(&self, coord: DateTime) -> WindowSpan<DateTime>;

	fn seal_after(&self) -> Option<Duration> {
		None
	}

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<DateTime>,
		value: AccumulatorValue<Self>,
	) -> Option<Self::Output>;

	fn new_accumulator(&self) -> Self::Accumulator {
		Self::Accumulator::default()
	}
}

pub trait TumblingRegistration: TumblingOperator + Sized
where
	Self::Output: Row,
	for<'a> &'a Self::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: &'static [OperatorCapability];

	fn from_config(operator_id: OperatorId, config: &Config) -> Result<Self>;

	fn encode_row_key(&self, group: &Self::GroupKey, window_start: DateTime) -> EncodedKey;
}

pub struct TumblingDriver<A>
where
	A: TumblingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: TumblingEngine<A::GroupKey, DateTime, A::Accumulator>,
}

impl<A> TumblingDriver<A>
where
	A: TumblingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
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
			let Some(coord) = row.row_time() else {
				continue;
			};
			let Some((group, contribution)) = self.aggregator.extract(ctx, &row) else {
				continue;
			};
			let span = self.aggregator.window_for(coord);
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, span)).or_default().push(event);
		}
	}
}

impl<A> TumblingDriver<A>
where
	A: TumblingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: GuestContext>(
		engine: &mut TumblingEngine<A::GroupKey, DateTime, A::Accumulator>,
		store: &mut GuestAsHost<'_, C>,
		frontier: DateTime,
		seal_after: Duration,
	) -> Result<()> {
		let horizon = seal_horizon_of(frontier, seal_after);
		if horizon <= <DateTime as Coord>::from_order(0) {
			return Ok(());
		}
		for window in engine.expire(store, horizon.to_order().saturating_sub(1))? {
			enqueue(store, window.group_id)?;
		}
		engine.expire_meta(store, horizon.to_order())?;
		let drained = drain(store, engine, SEAL_REAP_BATCH)?;
		if !drained.queue_is_empty() {
			arm_seal_timer(store, frontier, seal_after)?;
		}
		Ok(())
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

impl<A> OperatorMetadata for TumblingDriver<A>
where
	A: TumblingRegistration + 'static,
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

impl<A> GuestOperator for TumblingDriver<A>
where
	A: TumblingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		let engine_config = window_engine_config(config);
		Ok(Self {
			aggregator,
			engine: TumblingEngine::new(engine_config),
		})
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_after) = self.aggregator.seal_after() else {
			return Ok(());
		};
		let Self {
			engine,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
		let fired = FiredAt::of(&FlowTimer {
			due: timer.due,
			kind: timer.kind,
			key: EncodedKey::new(timer.key),
		});
		let frontier = advance_seal_frontier(&mut store, fired)?;
		Self::expire_through(engine, &mut store, frontier, seal_after)
	}

	fn seal_after(&self) -> Option<Duration> {
		self.aggregator.seal_after()
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let seal_after = self.aggregator.seal_after();
		if let Some(seal_after) = seal_after {
			let Self {
				engine,
				..
			} = &mut *self;
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, span)| span.start).max();
			if let Some(newest) = newest {
				arm_seal_timer(&mut store, newest, seal_after)?;
			}
			let watermark = seal_frontier(&mut store)?;
			let horizon = seal_horizon_of(watermark, seal_after);
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
			Self::expire_through(engine, &mut store, watermark, seal_after)?;
			if buckets.is_empty() {
				return Ok(());
			}
		}

		let groups = intern_window_groups(
			ctx,
			buckets.keys().map(|(group, span)| {
				((group.clone(), span.start), self.aggregator.encode_row_key(group, span.start))
			}),
		)?;

		let results = {
			let Self {
				aggregator,
				engine,
				..
			} = &mut *self;
			let mut store = GuestAsHost(ctx);
			let order: WindowOrder<A> = buckets.keys().cloned().collect();
			engine.apply(
				&mut store,
				buckets,
				&order,
				|group, window_start| {
					(
						group_of(&groups, group, window_start),
						aggregator.encode_row_key(group, window_start),
					)
				},
				|| aggregator.new_accumulator(),
			)?
		};

		if seal_after.is_some() {
			let mut store = GuestAsHost(ctx);
			for r in &results {
				if r.kind == EmitKind::Insert {
					let group = group_of(&groups, &r.group, r.span.start);
					self.engine.reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						group,
						&self.aggregator.encode_row_key(&r.group, r.span.start),
						None,
						Some(r.span.start.to_order()),
					)?;
				}
			}
		}

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();
		for r in results {
			let Some(out) = self.aggregator.build_output(&r.group, r.span, r.value) else {
				continue;
			};
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, out)),
				EmitKind::Update => updates.push((r.row_number, out)),
				EmitKind::Remove => removes.push((r.row_number, out)),
			}
		}
		self.emit_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}
