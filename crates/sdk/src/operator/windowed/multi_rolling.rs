// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	state::ArchiveState,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_group_state::GroupSet,
	metrics::heap::{HeapSize, OperatorSample},
};
use reifydb_flow::{
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, is_sealed,
			multi_rolling::{MultiEmit, MultiRollingEngine},
			rolling::RollingBuckets,
		},
		ledger::FiredAt,
		span::{Slot, WindowCoord},
	},
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};
use tracing::debug;

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

type AccumulatorContribution<A> = <<A as MultiRollingOperator>::Accumulator as WindowAccumulator>::Contribution;

type Buckets<A> = RollingBuckets<
	<A as MultiRollingOperator>::GroupKey,
	<A as MultiRollingOperator>::WindowSlot,
	AccumulatorContribution<A>,
>;

pub trait MultiRollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + ArchiveState;

	type WindowSlot: Slot + Hash + ArchiveState + HeapSize;

	type Accumulator: WindowAccumulator;

	type SecondaryKey: Clone + Eq + Ord + Hash + Debug + ArchiveState + HeapSize;

	type Output: Clone + Debug + PartialEq + ArchiveState + HeapSize;

	fn seal_after(&self) -> Option<Duration> {
		None
	}
	fn capacity(&self) -> usize;

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, Self::WindowSlot, AccumulatorContribution<Self>)>;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowSlot, Self::Accumulator>,
	) -> BTreeMap<Self::SecondaryKey, Self::Output>;
}

pub trait MultiRollingRegistration: MultiRollingOperator + Sized
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

	fn encode_state_key(&self, group: &Self::GroupKey) -> EncodedKey;

	fn encode_row_key(&self, group: &Self::GroupKey, secondary: &Self::SecondaryKey) -> EncodedKey;
}

pub type MultiRollingBuffer<A> =
	BTreeMap<<A as MultiRollingOperator>::WindowSlot, <A as MultiRollingOperator>::Accumulator>;

pub type MultiRollingEmit<A> = BTreeMap<<A as MultiRollingOperator>::SecondaryKey, <A as MultiRollingOperator>::Output>;

pub struct MultiRollingDriver<A>
where
	A: MultiRollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	#[allow(clippy::type_complexity)]
	engine: MultiRollingEngine<A::GroupKey, A::WindowSlot, A::Accumulator, A::SecondaryKey, A::Output>,
	budget: WindowedBudget,
}

impl<A> MultiRollingDriver<A>
where
	A: MultiRollingRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync + HeapSize,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	A::SecondaryKey: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[allow(clippy::type_complexity)]
	fn expire_through<C: OperatorContext>(
		engine: &mut MultiRollingEngine<A::GroupKey, A::WindowSlot, A::Accumulator, A::SecondaryKey, A::Output>,
		store: &mut OperatorContextStore<'_, C>,
		horizon: <A::WindowSlot as Slot>::Coord,
	) -> Result<()> {
		if horizon > <<A::WindowSlot as Slot>::Coord as WindowCoord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for MultiRollingDriver<A>
where
	A: MultiRollingRegistration + 'static,
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

impl<A> OperatorLogic for MultiRollingDriver<A>
where
	A: MultiRollingRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync + HeapSize,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	A::SecondaryKey: Send + Sync + HeapSize,
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

	fn create(operator_id: OperatorId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		let engine_config = window_engine_config(config);
		let budget = WindowedBudget::new(config, &engine_config);
		Ok(Self {
			aggregator,
			engine: MultiRollingEngine::new(engine_config),
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
		let frontier: <A::WindowSlot as Slot>::Coord = advance_seal_frontier(&mut store, fired)?;
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
		let mut buckets = self.route_diffs_to_buckets(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let seal_after = self.aggregator.seal_after();
		if let Some(seal_after) = seal_after {
			let mut store = OperatorContextStore(ctx);
			let newest = buckets.keys().map(|(_, coord)| coord.order_key()).max();
			if let Some(newest) = newest {
				arm_seal_timer(&mut store, newest, seal_after)?;
			}
			let watermark: <<A as MultiRollingOperator>::WindowSlot as Slot>::Coord =
				seal_frontier(&mut store)?;
			let horizon = seal_horizon_of(watermark, seal_after);
			Self::expire_through(&mut self.engine, &mut store, horizon)?;
			let mut dropped = 0u64;
			buckets.retain(|(_, coord), events| {
				if is_sealed(coord.order_key(), horizon) {
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

		let emits = {
			let Self {
				aggregator,
				engine,
				..
			} = &mut *self;
			let capacity = aggregator.capacity();
			let mut store = OperatorContextStore(ctx);
			engine.apply(
				&mut store,
				buckets,
				capacity,
				|group| aggregator.encode_state_key(group),
				|group, secondary| aggregator.encode_row_key(group, secondary),
				|group, buffer| aggregator.combine(group, buffer).into(),
			)?
		};

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();
		for emit in emits {
			match emit {
				MultiEmit::Insert {
					row_number,
					value,
				} => inserts.push((row_number, value)),
				MultiEmit::Update {
					row_number,
					prior,
					value,
				} => updates.push((row_number, prior, value)),
				MultiEmit::Remove {
					row_number,
					value,
				} => removes.push((row_number, value)),
			}
		}
		Self::emit_three_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		let mut store = OperatorContextStore(ctx);
		self.engine.flush(&mut store)?;
		Ok(())
	}
}

impl<A> MultiRollingDriver<A>
where
	A: MultiRollingRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	A::SecondaryKey: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[inline]
	#[allow(clippy::type_complexity)]
	fn route_diffs_to_buckets(&self, ctx: &mut impl OperatorContext, change: &impl ChangeView) -> Buckets<A> {
		let mut buckets: Buckets<A> = BTreeMap::new();

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
							if let Some((group, coord, contribution)) =
								self.aggregator.extract(ctx, &row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccumulatorEvent::Add(contribution));
							}
						}
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						let n = pre.row_count().min(post.row_count());
						for i in 0..n {
							if let Some(pre_row) = pre.row(i)
								&& let Some((group, coord, contribution)) =
									self.aggregator.extract(ctx, &pre_row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccumulatorEvent::Remove(contribution));
							}
							if let Some(post_row) = post.row(i)
								&& let Some((group, coord, contribution)) =
									self.aggregator.extract(ctx, &post_row)
							{
								buckets.entry((group, coord))
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
							if let Some((group, coord, contribution)) =
								self.aggregator.extract(ctx, &row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccumulatorEvent::Remove(contribution));
							}
						}
					}
				}
			}
		}

		buckets
	}

	#[inline]
	fn emit_three_batches(
		ctx: &mut impl OperatorContext,
		inserts: &[(RowNumber, A::Output)],
		updates: &[(RowNumber, A::Output, A::Output)],
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
			for (rn, prior, new) in updates {
				batch.push(*rn, prior, new)?;
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
