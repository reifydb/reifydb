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
	state::store::StateStore,
};
use reifydb_flow::{
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, is_sealed,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		ledger::FiredAt,
		span::{Slot, SlotCoord, WindowAnchor, WindowCoord, WindowSpan},
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
			WindowedBudget, advance_seal_frontier, arm_seal_timer, bridge::OperatorContextStore, group_of,
			intern_window_groups, seal_frontier, seal_horizon_of, window_engine_config,
		},
	},
};

type AccumulatorContribution<A> = <<A as TumblingOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingOperator>::Accumulator as WindowAccumulator>::Output;
type Buckets<A> = TumblingBuckets<
	<A as TumblingOperator>::GroupKey,
	SlotCoord<<A as TumblingOperator>::WindowSlot>,
	AccumulatorContribution<A>,
>;
type WindowOrder<A> =
	Vec<(<A as TumblingOperator>::GroupKey, WindowSpan<SlotCoord<<A as TumblingOperator>::WindowSlot>>)>;

pub trait TumblingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + ArchiveState;

	type WindowSlot: Slot<Coord: WindowAnchor + Hash + ArchiveState + HeapSize + Send + Sync>
		+ Hash
		+ ArchiveState
		+ HeapSize;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

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

	fn from_config(operator_id: FlowNodeId, config: &Config) -> Result<Self>;

	fn encode_row_key(&self, group: &Self::GroupKey, window_start: SlotCoord<Self::WindowSlot>) -> EncodedKey;
}

pub struct TumblingDriver<A>
where
	A: TumblingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: TumblingEngine<A::GroupKey, SlotCoord<A::WindowSlot>, A::Accumulator>,
	budget: WindowedBudget,
}

impl<A> TumblingDriver<A>
where
	A: TumblingRegistration,
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
}

impl<A> TumblingDriver<A>
where
	A: TumblingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: OperatorContext>(
		aggregator: &A,
		engine: &mut TumblingEngine<A::GroupKey, SlotCoord<A::WindowSlot>, A::Accumulator>,
		store: &mut OperatorContextStore<'_, C>,
		horizon: SlotCoord<A::WindowSlot>,
	) -> Result<()> {
		if horizon <= <SlotCoord<A::WindowSlot> as WindowCoord>::from_order(0) {
			return Ok(());
		}
		for expired in engine.expire(store, horizon.to_order().saturating_sub(1))? {
			if expired.accumulator_present {
				store.remove_row_number(
					expired.group_id,
					&aggregator.encode_row_key(&expired.group, expired.window_start),
				)?;
			}
		}
		engine.expire_meta(store, horizon.to_order())?;
		Ok(())
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

impl<A> OperatorMetadata for TumblingDriver<A>
where
	A: TumblingRegistration + 'static,
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

impl<A> OperatorLogic for TumblingDriver<A>
where
	A: TumblingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
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
		let engine_config = window_engine_config(config);
		let budget = WindowedBudget::new(config, &engine_config);
		Ok(Self {
			aggregator,
			engine: TumblingEngine::group_scoped(engine_config),
			budget,
		})
	}

	fn on_timer(&mut self, ctx: &mut impl OperatorContext, timer: Timer<'_>) -> Result<()> {
		let Some(seal_after) = self.aggregator.seal_after() else {
			return Ok(());
		};
		let Self {
			aggregator,
			engine,
			..
		} = &mut *self;
		let mut store = OperatorContextStore(ctx);
		let fired = FiredAt::of(&FlowTimer {
			at: timer.at,
			kind: timer.kind,
			key: EncodedKey::new(timer.key),
		});
		let frontier = advance_seal_frontier(&mut store, fired)?;
		Self::expire_through(aggregator, engine, &mut store, seal_horizon_of(frontier, seal_after))
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
			let Self {
				aggregator,
				engine,
				..
			} = &mut *self;
			let mut store = OperatorContextStore(ctx);
			let newest = buckets.keys().map(|(_, span)| span.start.order_key()).max();
			if let Some(newest) = newest {
				arm_seal_timer(&mut store, newest, seal_after)?;
			}
			let watermark = seal_frontier(&mut store)?;
			let horizon = seal_horizon_of(watermark, seal_after);
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
			Self::expire_through(aggregator, engine, &mut store, horizon)?;
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
			let mut store = OperatorContextStore(ctx);
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
			let mut store = OperatorContextStore(ctx);
			for r in &results {
				if r.kind == EmitKind::Insert {
					let group = group_of(&groups, &r.group, r.span.start);
					self.engine.reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						group,
						r.row_number,
						None,
						Some(r.span.start.order_key().to_order()),
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

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		let mut store = OperatorContextStore(ctx);
		self.engine.flush(&mut store)?;
		Ok(())
	}
}
