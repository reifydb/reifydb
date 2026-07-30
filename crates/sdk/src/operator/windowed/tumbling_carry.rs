// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	state::ArchiveState,
};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_state::GroupSet,
	metrics::heap::{HeapSize, OperatorSample},
};
use reifydb_flow::{
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, config::TumblingCarryConfig, is_sealed, tumbling::TumblingBuckets,
			tumbling_carry::TumblingCarryEngine,
		},
		ledger::FiredAt,
		span::{Slot, SlotCoord, SlotSpan, WindowAnchor, WindowCoord, WindowSpan},
	},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};
use tracing::warn;

use crate::{
	config::Config,
	error::Result,
	operator::{
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
			WindowedBudget, advance_seal_frontier, arm_seal_timer, bridge::OperatorContextStore,
			seal_frontier, seal_horizon_of, window_engine_config,
		},
	},
};

type AccumulatorContribution<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Output;
type CarryEngine<A> = TumblingCarryEngine<
	<A as TumblingCarryOperator>::GroupKey,
	SlotCoord<<A as TumblingCarryOperator>::WindowSlot>,
	<A as TumblingCarryOperator>::Accumulator,
	<A as TumblingCarryOperator>::Carry,
	<A as TumblingCarryOperator>::Output,
>;
type Buckets<A> = TumblingBuckets<
	<A as TumblingCarryOperator>::GroupKey,
	SlotCoord<<A as TumblingCarryOperator>::WindowSlot>,
	AccumulatorContribution<A>,
>;

pub trait TumblingCarryOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + ArchiveState;

	type WindowSlot: Slot<Coord: WindowAnchor + Hash + ArchiveState + HeapSize + Send + Sync>
		+ Hash
		+ ArchiveState
		+ HeapSize;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq + ArchiveState + HeapSize;

	type Carry: Clone + Debug + ArchiveState + HeapSize;

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, Self::WindowSlot, AccumulatorContribution<Self>)>;

	fn window_for(&self, coord: Self::WindowSlot) -> WindowSpan<SlotCoord<Self::WindowSlot>>;

	fn seal_after(&self) -> Option<Duration> {
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

	fn from_config(operator_id: FlowNodeId, config: &Config) -> Result<Self>;

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
	budget: WindowedBudget,
}

impl<A> TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
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
			let Some((group, coord, contribution)) = self.aggregator.extract(ctx, &row) else {
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
	A::WindowSlot: Send + Sync + HeapSize,
	SlotSpan<A::WindowSlot>: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: OperatorContext>(
		engine: &mut CarryEngine<A>,
		store: &mut OperatorContextStore<'_, C>,
		horizon: SlotCoord<A::WindowSlot>,
	) -> Result<()> {
		if horizon > <SlotCoord<A::WindowSlot> as WindowCoord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const API: u32 = 1;
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
	A::WindowSlot: Send + Sync + HeapSize,
	SlotSpan<A::WindowSlot>: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn sample(&self) -> Option<OperatorSample> {
		Some(OperatorSample::with_memory(self.engine.approximate_memory())
			.with_dirty_memory(self.engine.dirty_memory())
			.with_membership(self.engine.membership_memory())
			.with_completeness(self.engine.completeness())
			.with_pool(self.budget.stat()))
	}

	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		let retention = aggregator.retention();
		let engine_config = window_engine_config(config);
		let budget = WindowedBudget::new(config, &engine_config);
		Ok(Self {
			aggregator,
			engine: TumblingCarryEngine::new(
				TumblingCarryConfig::builder(engine_config).retention(retention).build(),
			),
			budget,
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
		let frontier: SlotCoord<A::WindowSlot> = advance_seal_frontier(&mut store, fired)?;
		Self::expire_through(&mut self.engine, &mut store, seal_horizon_of(frontier, seal_after))
	}

	fn seal_after_ms(&self) -> Option<u64> {
		self.aggregator.seal_after().and_then(<DateTime as WindowCoord>::span_millis)
	}

	fn invalidate_groups(&mut self, groups: &GroupSet) {
		self.engine.invalidate_groups(groups);
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		self.budget.sync_from_lease(ctx.state_lease_bytes());
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let seal_after = self.aggregator.seal_after();
		if let Some(seal_after) = seal_after {
			let mut store = OperatorContextStore(ctx);
			let newest = buckets.keys().map(|(_, span)| span.start.order_key()).max();
			if let Some(newest) = newest {
				arm_seal_timer(&mut store, newest, seal_after)?;
			}
			let watermark = seal_frontier(&mut store)?;
			let horizon = seal_horizon_of(watermark, seal_after);
			Self::expire_through(&mut self.engine, &mut store, horizon)?;
			let mut dropped = 0u64;
			buckets.retain(|(_, span), events| {
				if is_sealed(span.start.order_key(), horizon) {
					dropped += events.len() as u64;
					false
				} else {
					true
				}
			});
			if dropped > 0 {
				warn!(operator = A::NAME, dropped, "mutations targeting sealed windows were dropped");
			}
			if buckets.is_empty() {
				return Ok(());
			}
		}

		let results = {
			let Self {
				aggregator,
				engine,
				..
			} = &mut *self;
			let mut store = OperatorContextStore(ctx);
			engine.apply(
				&mut store,
				buckets,
				|group, window_start| aggregator.encode_row_key(group, window_start),
				|| aggregator.new_accumulator(),
				|group, span, value, prev_carry| {
					aggregator.build_output(group, span, value, prev_carry)
				},
				|value, prev_carry| aggregator.carry_forward(value, prev_carry),
			)?
		};

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
