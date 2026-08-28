// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	row::operator::state::StateCodec,
};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::{HeapSize, OperatorSample},
};
use reifydb_flow::{
	operator::state::seal::{coord::Coord, domain::SealDomain, policy::is_sealed},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, WindowResult, config::TumblingCarryConfig,
			tumbling::TumblingBuckets, tumbling_carry::TumblingCarryEngine,
		},
		span::{Slot, SlotCoord, SlotSpan, WindowAnchor, WindowSpan},
	},
};
use reifydb_value::{
	config::Config,
	value::{diff_type::DiffType, duration::Duration, row_number::RowNumber},
};
use tracing::{debug, instrument};

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
			guest_as_host::GuestAsHost, observe_batch, seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

type AccumulatorContribution<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Output;
type Anchor<A> = SlotCoord<<A as TumblingCarryOperator>::WindowSlot>;
type Lateness<A> = <Anchor<A> as SealDomain>::Lateness;
type CarryEngine<A> = TumblingCarryEngine<
	<A as TumblingCarryOperator>::GroupKey,
	Anchor<A>,
	<A as TumblingCarryOperator>::Accumulator,
	<A as TumblingCarryOperator>::Carry,
	<A as TumblingCarryOperator>::Output,
>;
type Buckets<A> = TumblingBuckets<<A as TumblingCarryOperator>::GroupKey, Anchor<A>, AccumulatorContribution<A>>;
type WindowResults<A> =
	Vec<WindowResult<<A as TumblingCarryOperator>::GroupKey, Anchor<A>, <A as TumblingCarryOperator>::Output>>;

pub trait TumblingCarryOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + StateCodec;

	type WindowSlot: Slot<Coord: WindowAnchor + SealDomain + Hash + StateCodec + HeapSize + Send + Sync>
		+ Hash
		+ StateCodec
		+ HeapSize;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq + StateCodec + HeapSize;

	type Carry: Clone + Debug + StateCodec + HeapSize;

	fn coord(&self, row: &impl RowView) -> Option<Self::WindowSlot>;

	fn extract(
		&self,
		ctx: &mut impl GuestContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, AccumulatorContribution<Self>)>;

	fn window_for(&self, coord: SlotCoord<Self::WindowSlot>) -> WindowSpan<SlotCoord<Self::WindowSlot>>;

	fn lateness(&self) -> Option<<SlotCoord<Self::WindowSlot> as SealDomain>::Lateness> {
		None
	}

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<SlotCoord<Self::WindowSlot>>,
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

	fn retention(&self) -> Option<SlotSpan<Self::WindowSlot>> {
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

	fn encode_row_key(&self, group: &Self::GroupKey, window_start: SlotCoord<Self::WindowSlot>) -> EncodedKey;
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
			let Some(slot) = self.aggregator.coord(&row) else {
				continue;
			};
			let Some((group, contribution)) = self.aggregator.extract(ctx, &row) else {
				continue;
			};
			let span = self.aggregator.window_for(slot.order_key());
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

impl<A> TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: GuestContext>(
		engine: &mut CarryEngine<A>,
		store: &mut GuestAsHost<'_, C>,
		horizon: Anchor<A>,
	) -> Result<()> {
		if horizon > <Anchor<A> as Coord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}

	#[instrument(name = "flow::operator::tumbling::seal", level = "trace", skip_all, fields(operator = A::NAME))]
	fn seal(&mut self, ctx: &mut impl GuestContext, buckets: &mut Buckets<A>, lateness: Lateness<A>) -> Result<()> {
		let mut store = GuestAsHost(ctx);
		let newest = buckets.keys().map(|(_, span)| span.start).max();
		if let Some(newest) = newest {
			observe_batch(&mut store, newest, lateness)?;
		}
		let watermark = seal_frontier::<Anchor<A>>(&mut store)?;
		let horizon = <Anchor<A> as SealDomain>::horizon(watermark, lateness);
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
	fn accumulate(&mut self, ctx: &mut impl GuestContext, buckets: Buckets<A>) -> Result<WindowResults<A>> {
		let Self {
			aggregator,
			engine,
			..
		} = &mut *self;
		let mut store = GuestAsHost(ctx);
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

impl<A> GuestOperator for TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
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

	fn on_timer(&mut self, ctx: &mut impl GuestContext, timer: Timer<'_>) -> Result<()> {
		let Some(lateness) = self.aggregator.lateness() else {
			return Ok(());
		};
		let mut store = GuestAsHost(ctx);
		let Some(frontier) = timer_frontier::<Anchor<A>>(&mut store, timer)? else {
			return Ok(());
		};
		let horizon = <Anchor<A> as SealDomain>::horizon(frontier, lateness);
		Self::expire_through(&mut self.engine, &mut store, horizon)
	}

	fn lateness(&self) -> Option<Duration> {
		self.aggregator.lateness().and_then(<Anchor<A> as SealDomain>::lateness_duration)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> Result<()> {
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let lateness = self.aggregator.lateness();
		if let Some(lateness) = lateness {
			self.seal(ctx, &mut buckets, lateness)?;
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
}
