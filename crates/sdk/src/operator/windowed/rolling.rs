// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt::Debug,
	hash::Hash,
};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	state::ArchiveState,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	metrics::heap::{HeapSize, OperatorSample},
	state::store::StateStore,
};
use reifydb_flow::{
	timer::Timer as FlowTimer,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, is_sealed,
			rolling::{RollingBuckets, RollingEngine},
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
			WindowedBudget, advance_seal_frontier, arm_seal_timer, bridge::OperatorContextStore, group_of,
			intern_window_groups, seal_frontier, seal_horizon_of, window_engine_config,
		},
	},
};

type AccumulatorContribution<A> = <<A as RollingOperator>::Accumulator as WindowAccumulator>::Contribution;

pub trait RollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + ArchiveState;

	type WindowSlot: Slot + Hash + ArchiveState + HeapSize;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

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
	) -> Option<Self::Output>;
}

pub trait RollingRegistration: RollingOperator + Sized
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

	fn encode_row_key(&self, group: &Self::GroupKey) -> EncodedKey;

	fn seal_after(&self) -> Option<Duration> {
		None
	}
}

pub type RollingBuffer<A> = BTreeMap<<A as RollingOperator>::WindowSlot, <A as RollingOperator>::Accumulator>;

type Buckets<A> = RollingBuckets<
	<A as RollingOperator>::GroupKey,
	<A as RollingOperator>::WindowSlot,
	AccumulatorContribution<A>,
>;

pub struct RollingDriver<A>
where
	A: RollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: RollingEngine<A::GroupKey, A::WindowSlot, A::Accumulator>,
	budget: WindowedBudget,
}

impl<A> RollingDriver<A>
where
	A: RollingRegistration,
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
						self.push_updates(ctx, &pre, &post, &mut buckets);
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
			let event = if is_add {
				AccumulatorEvent::Add(contribution)
			} else {
				AccumulatorEvent::Remove(contribution)
			};
			buckets.entry((group, coord)).or_default().push(event);
		}
	}

	fn push_updates<P: ColumnsView, Q: ColumnsView>(
		&self,
		ctx: &mut impl OperatorContext,
		pre: &P,
		post: &Q,
		buckets: &mut Buckets<A>,
	) {
		let n = pre.row_count().min(post.row_count());
		for i in 0..n {
			if let Some(pre_row) = pre.row(i)
				&& let Some((group, coord, contribution)) = self.aggregator.extract(ctx, &pre_row)
			{
				buckets.entry((group, coord)).or_default().push(AccumulatorEvent::Remove(contribution));
			}
			if let Some(post_row) = post.row(i)
				&& let Some((group, coord, contribution)) = self.aggregator.extract(ctx, &post_row)
			{
				buckets.entry((group, coord)).or_default().push(AccumulatorEvent::Add(contribution));
			}
		}
	}

	#[inline]
	fn emit_batches(
		ctx: &mut impl OperatorContext,
		inserts: Vec<(RowNumber, A::Output)>,
		updates: Vec<(RowNumber, A::Output)>,
		removes: Vec<(RowNumber, A::Output)>,
	) -> Result<()> {
		if !inserts.is_empty() {
			let mut batch = InsertBatch::<A::Output, _>::new(ctx, inserts.len())?;
			for (rn, data) in &inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<A::Output, _>::new(ctx, updates.len())?;
			for (rn, data) in &updates {
				batch.push(*rn, data, data)?;
			}
			batch.finish()?;
		}
		if !removes.is_empty() {
			let mut batch = RemoveBatch::<A::Output, _>::new(ctx, removes.len())?;
			for (rn, data) in &removes {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		Ok(())
	}
}

impl<A> RollingDriver<A>
where
	A: RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn expire_through<C: OperatorContext>(
		engine: &mut RollingEngine<A::GroupKey, A::WindowSlot, A::Accumulator>,
		store: &mut OperatorContextStore<'_, C>,
		horizon: <A::WindowSlot as Slot>::Coord,
	) -> Result<()> {
		if horizon > <<A::WindowSlot as Slot>::Coord as WindowCoord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for RollingDriver<A>
where
	A: RollingRegistration + 'static,
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

impl<A> OperatorLogic for RollingDriver<A>
where
	A: RollingRegistration + Send + Sync + 'static,
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

	fn create(operator_id: OperatorId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		let engine_config = window_engine_config(config);
		let budget = WindowedBudget::new(config, &engine_config);
		Ok(Self {
			aggregator,
			engine: RollingEngine::group_scoped(engine_config),
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

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		self.budget.sync_from_lease(ctx.state_lease_bytes());
		let mut buckets = self.route(ctx, &change);
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
			let watermark: <<A as RollingOperator>::WindowSlot as Slot>::Coord = seal_frontier(&mut store)?;
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

		let groups = intern_window_groups(
			ctx,
			buckets.keys().map(|(group, _)| group.clone()).collect::<BTreeSet<_>>().into_iter().map(
				|group| {
					let key = self.aggregator.encode_row_key(&group);
					((group, ()), key)
				},
			),
		)?;

		let results = {
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
				|group| (group_of(&groups, group, ()), aggregator.encode_row_key(group)),
				|group, buffer| aggregator.combine(group, buffer),
			)?
		};

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut removed_groups: Vec<A::GroupKey> = Vec::new();
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, r.value)),
				EmitKind::Remove => {
					removed_groups.push(r.group);
					removes.push((r.row_number, r.value));
				}
			}
		}
		Self::emit_batches(ctx, inserts, updates, removes)?;

		if !removed_groups.is_empty() {
			let mut store = OperatorContextStore(ctx);
			for group in &removed_groups {
				store.remove_row_number(
					group_of(&groups, group, ()),
					&self.aggregator.encode_row_key(group),
				)?;
			}
		}

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		let mut store = OperatorContextStore(ctx);
		self.engine.flush(&mut store)?;
		Ok(())
	}
}
