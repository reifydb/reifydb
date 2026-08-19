// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::key::encoded::IntoEncodedKey;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::DiffType, flow::OperatorCapability},
	metrics::heap::{HeapSize, OperatorSample},
};
use reifydb_flow::{
	operator::state::seal::{coord::Coord, domain::SealDomain, policy::is_sealed},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, rolling::RollingBuckets,
			rolling_incremental::RollingIncrementalEngine,
		},
		span::{Slot, SlotCoord},
	},
};
use reifydb_value::{
	config::Config,
	value::{duration::Duration, row_number::RowNumber},
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
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			bucket_of,
			guest_as_host::GuestAsHost,
			observe_batch,
			rolling::{Anchor, RollingOperator, RollingRegistration},
			seal_frontier, timer_frontier, window_engine_config,
		},
	},
};

type WindowContribution<A> = <<A as RollingOperator>::Accumulator as WindowAccumulator>::Contribution;
type WindowValue<A> = <<A as RollingOperator>::Accumulator as WindowAccumulator>::Output;
type RunningContribution<A> = <<A as RollingIncrementalOperator>::Running as WindowAccumulator>::Contribution;

pub trait RollingIncrementalOperator: RollingOperator {
	type Running: WindowAccumulator;

	fn window_contribution(&self, window_value: &WindowValue<Self>) -> RunningContribution<Self>;

	fn combine_running(
		&self,
		group: &Self::GroupKey,
		running: &Self::Running,
		newest_value: &WindowValue<Self>,
		newest_coord: SlotCoord<Self::WindowSlot>,
	) -> Option<Self::Output>;
}

pub type RollingBuffer<A> = BTreeMap<Anchor<A>, <A as RollingOperator>::Accumulator>;

pub struct RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: RollingIncrementalEngine<A::GroupKey, Anchor<A>, A::Accumulator, A::Running>,
}

impl<A> RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Running: Send + Sync + HeapSize,
	WindowContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: GuestContext>(
		engine: &mut RollingIncrementalEngine<A::GroupKey, Anchor<A>, A::Accumulator, A::Running>,
		store: &mut GuestAsHost<'_, C>,
		horizon: Anchor<A>,
	) -> Result<()> {
		if horizon > <Anchor<A> as Coord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + 'static,
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

impl<A> GuestOperator for RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Running: Send + Sync + HeapSize,
	WindowContribution<A>: Send + Sync,
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
			engine: RollingIncrementalEngine::new(engine_config),
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
		let mut buckets = self.route_diffs_to_buckets(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		if let Some(lateness) = self.aggregator.lateness() {
			let mut store = GuestAsHost(ctx);
			let newest = buckets.keys().map(|(_, coord)| *coord).max();
			if let Some(newest) = newest {
				observe_batch(&mut store, newest, lateness)?;
			}
			let watermark = seal_frontier::<Anchor<A>>(&mut store)?;
			let horizon = <Anchor<A> as SealDomain>::horizon(watermark, lateness);
			Self::expire_through(&mut self.engine, &mut store, horizon)?;
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
				debug!(operator = A::NAME, dropped, "mutations targeting sealed coords were dropped");
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
			let capacity = aggregator.capacity();
			let mut store = GuestAsHost(ctx);
			engine.apply(
				&mut store,
				buckets,
				capacity,
				|group| aggregator.encode_row_key(group),
				|value| aggregator.window_contribution(value),
				|group, running, newest, coord| {
					aggregator.combine_running(group, running, newest, coord)
				},
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
		Self::emit_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}

type EventBuckets<A> = RollingBuckets<<A as RollingOperator>::GroupKey, Anchor<A>, WindowContribution<A>>;

impl<A> RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	A::Running: Send + Sync,
	WindowContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[inline]
	fn route_diffs_to_buckets(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> EventBuckets<A> {
		let mut buckets: EventBuckets<A> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						for i in 0..cols.row_count() {
							let Some(row) = cols.row(i) else {
								continue;
							};
							let Some(slot) = self.aggregator.coord(&row) else {
								continue;
							};
							let Some((group, contribution)) =
								self.aggregator.extract(ctx, &row)
							else {
								continue;
							};
							buckets.entry((
								group,
								bucket_of(
									slot.order_key(),
									self.aggregator.bucket_size(),
								),
							))
							.or_default()
							.push(AccumulatorEvent::Add(contribution));
						}
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						let n = pre.row_count().min(post.row_count());
						for i in 0..n {
							if let Some(pre_row) = pre.row(i)
								&& let Some(slot) = self.aggregator.coord(&pre_row)
								&& let Some((group, contribution)) =
									self.aggregator.extract(ctx, &pre_row)
							{
								buckets.entry((
									group,
									bucket_of(
										slot.order_key(),
										self.aggregator.bucket_size(),
									),
								))
								.or_default()
								.push(AccumulatorEvent::Remove(contribution));
							}
							if let Some(post_row) = post.row(i)
								&& let Some(slot) = self.aggregator.coord(&post_row)
								&& let Some((group, contribution)) =
									self.aggregator.extract(ctx, &post_row)
							{
								buckets.entry((
									group,
									bucket_of(
										slot.order_key(),
										self.aggregator.bucket_size(),
									),
								))
								.or_default()
								.push(AccumulatorEvent::Add(contribution));
							}
						}
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						for i in 0..cols.row_count() {
							let Some(row) = cols.row(i) else {
								continue;
							};
							let Some(slot) = self.aggregator.coord(&row) else {
								continue;
							};
							let Some((group, contribution)) =
								self.aggregator.extract(ctx, &row)
							else {
								continue;
							};
							buckets.entry((
								group,
								bucket_of(
									slot.order_key(),
									self.aggregator.bucket_size(),
								),
							))
							.or_default()
							.push(AccumulatorEvent::Remove(contribution));
						}
					}
				}
			}
		}

		buckets
	}

	#[inline]
	fn emit_batches(
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
