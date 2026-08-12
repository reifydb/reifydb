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
	seal::{coord::Coord, ledger::FiredAt, policy::is_sealed},
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, WindowResult, config::TumblingCarryConfig,
			tumbling::TumblingBuckets, tumbling_carry::TumblingCarryEngine,
		},
		span::WindowSpan,
	},
};
use reifydb_value::{
	config::Config,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::{debug, instrument};

use crate::{
	error::Result,
	flow::operator::{
		OperatorLogic, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::OperatorContext,
		timer::Timer,
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{
			advance_seal_frontier, arm_seal_timer, bridge::OperatorContextStore, seal_frontier,
			seal_horizon_of, window_engine_config,
		},
	},
};

type AccumulatorContribution<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Output;
type CarryEngine<A> = TumblingCarryEngine<
	<A as TumblingCarryOperator>::GroupKey,
	DateTime,
	<A as TumblingCarryOperator>::Accumulator,
	<A as TumblingCarryOperator>::Carry,
	<A as TumblingCarryOperator>::Output,
>;
type Buckets<A> = TumblingBuckets<<A as TumblingCarryOperator>::GroupKey, DateTime, AccumulatorContribution<A>>;
type WindowResults<A> =
	Vec<WindowResult<<A as TumblingCarryOperator>::GroupKey, DateTime, <A as TumblingCarryOperator>::Output>>;

pub trait TumblingCarryOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + StateCodec;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq + StateCodec + HeapSize;

	type Carry: Clone + Debug + StateCodec + HeapSize;

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
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
		value: &AccumulatorValue<Self>,
		prev_carry: Option<&Self::Carry>,
	) -> Option<Self::Output>;

	fn carry_forward(
		&self,
		value: &AccumulatorValue<Self>,
		prev_carry: Option<&Self::Carry>,
	) -> Option<Self::Carry>;

	fn new_accumulator(&self) -> Self::Accumulator {
		Self::Accumulator::default()
	}

	fn retention(&self) -> Option<Duration> {
		None
	}
}

pub trait TumblingCarryRegistration: TumblingCarryOperator + Sized
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

pub struct TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: CarryEngine<A>,
}

impl<A> TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[instrument(name = "flow::operator::tumbling::route", level = "trace", skip_all, fields(operator = A::NAME))]
	fn route(&self, ctx: &mut impl OperatorContext, change: &impl ChangeView) -> Buckets<A> {
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
		ctx: &mut impl OperatorContext,
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

	#[inline]
	#[instrument(name = "flow::operator::tumbling::emit", level = "trace", skip_all, fields(operator = A::NAME))]
	fn emit_batches(
		&self,
		ctx: &mut impl OperatorContext,
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

impl<A> TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: OperatorContext>(
		engine: &mut CarryEngine<A>,
		store: &mut OperatorContextStore<'_, C>,
		horizon: DateTime,
	) -> Result<()> {
		if horizon > <DateTime as Coord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}

	#[instrument(name = "flow::operator::tumbling::seal", level = "trace", skip_all, fields(operator = A::NAME))]
	fn seal(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &mut Buckets<A>,
		seal_after: Duration,
	) -> Result<()> {
		let mut store = OperatorContextStore(ctx);
		let newest = buckets.keys().map(|(_, span)| span.start).max();
		if let Some(newest) = newest {
			arm_seal_timer(&mut store, newest, seal_after)?;
		}
		let watermark: DateTime = seal_frontier(&mut store)?;
		let horizon = seal_horizon_of(watermark, seal_after);
		Self::expire_through(&mut self.engine, &mut store, horizon)?;
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
		Ok(())
	}

	#[instrument(name = "flow::operator::tumbling::accumulate", level = "trace", skip_all, fields(operator = A::NAME))]
	fn accumulate(&mut self, ctx: &mut impl OperatorContext, buckets: Buckets<A>) -> Result<WindowResults<A>> {
		let Self {
			aggregator,
			engine,
			..
		} = &mut *self;
		let mut store = OperatorContextStore(ctx);
		Ok(engine.apply(
			&mut store,
			buckets,
			|group, window_start| aggregator.encode_row_key(group, window_start),
			|| aggregator.new_accumulator(),
			|group, span, value, prev_carry| aggregator.build_output(group, span, value, prev_carry),
			|value, prev_carry| aggregator.carry_forward(value, prev_carry),
		)?)
	}
}

impl<A> OperatorMetadata for TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration + 'static,
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

impl<A> OperatorLogic for TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn create(operator_id: OperatorId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		let retention = aggregator.retention();
		let engine_config = window_engine_config(config);
		Ok(Self {
			aggregator,
			engine: TumblingCarryEngine::new(
				TumblingCarryConfig::builder(engine_config).retention(retention).build(),
			),
		})
	}

	fn on_timer(&mut self, ctx: &mut impl OperatorContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_after) = self.aggregator.seal_after() else {
			return Ok(());
		};
		let mut store = OperatorContextStore(ctx);
		let fired = FiredAt::of(&FlowTimer {
			at: timer.at,
			kind: timer.kind,
			key: EncodedKey::new(timer.key),
		});
		let frontier: DateTime = advance_seal_frontier(&mut store, fired)?;
		Self::expire_through(&mut self.engine, &mut store, seal_horizon_of(frontier, seal_after))
	}

	fn seal_after(&self) -> Option<Duration> {
		self.aggregator.seal_after()
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let seal_after = self.aggregator.seal_after();
		if let Some(seal_after) = seal_after {
			self.seal(ctx, &mut buckets, seal_after)?;
			if buckets.is_empty() {
				return Ok(());
			}
		}

		let results = self.accumulate(ctx, buckets)?;

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, r.value)),
				EmitKind::Remove => removes.push((r.row_number, r.value)),
			}
		}
		self.emit_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		let mut store = OperatorContextStore(ctx);
		self.engine.flush(&mut store)?;
		Ok(())
	}
}
