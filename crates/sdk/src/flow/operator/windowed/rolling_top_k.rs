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
	operator::state::seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent,
			rolling::RollingBuckets,
			rolling_top_k::{RollingTopKEngine, TopKEmit},
		},
		span::{Slot, SlotCoord, SlotSpan, WindowAnchor},
	},
};
use reifydb_value::{
	config::Config,
	value::{diff_type::DiffType, duration::Duration, row_number::RowNumber},
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
			bucket_of, guest_as_host::GuestAsHost, observe_batch, seal_frontier, timer_frontier,
			window_engine_config,
		},
	},
};

type AccumulatorContribution<A> = <<A as RollingTopKOperator>::Accumulator as WindowAccumulator>::Contribution;

type Anchor<A> = SlotCoord<<A as RollingTopKOperator>::WindowSlot>;
type Buckets<A> = RollingBuckets<<A as RollingTopKOperator>::GroupKey, Anchor<A>, AccumulatorContribution<A>>;

pub trait RollingTopKOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + StateCodec;

	type WindowSlot: Slot<Coord: WindowAnchor + SealDomain + Hash + StateCodec + HeapSize + Send + Sync>
		+ Hash
		+ StateCodec
		+ HeapSize;

	type Accumulator: WindowAccumulator;

	type SecondaryKey: Clone + Eq + Ord + Hash + Debug + StateCodec + HeapSize;

	type Output: Clone + Debug + PartialEq + StateCodec + HeapSize;

	fn lateness(&self) -> Option<<SlotCoord<Self::WindowSlot> as SealDomain>::Lateness> {
		None
	}
	fn capacity(&self) -> usize;

	fn bucket_size(&self) -> SlotSpan<Self::WindowSlot>;

	fn coord(&self, row: &impl RowView) -> Option<Self::WindowSlot>;

	fn extract(
		&self,
		ctx: &mut impl GuestContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, AccumulatorContribution<Self>)>;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<SlotCoord<Self::WindowSlot>, Self::Accumulator>,
	) -> BTreeMap<Self::SecondaryKey, Self::Output>;
}

pub trait RollingTopKRegistration: RollingTopKOperator + Sized
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

pub type RollingTopKBuffer<A> = BTreeMap<Anchor<A>, <A as RollingTopKOperator>::Accumulator>;

pub type RollingTopKEmit<A> = BTreeMap<<A as RollingTopKOperator>::SecondaryKey, <A as RollingTopKOperator>::Output>;

pub struct RollingTopKDriver<A>
where
	A: RollingTopKRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	#[allow(clippy::type_complexity)]
	engine: RollingTopKEngine<A::GroupKey, Anchor<A>, A::Accumulator, A::SecondaryKey, A::Output>,
}

impl<A> RollingTopKDriver<A>
where
	A: RollingTopKRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync + HeapSize,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	A::SecondaryKey: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[allow(clippy::type_complexity)]
	fn expire_through<C: GuestContext>(
		engine: &mut RollingTopKEngine<A::GroupKey, Anchor<A>, A::Accumulator, A::SecondaryKey, A::Output>,
		store: &mut GuestAsHost<'_, C>,
		horizon: Anchor<A>,
	) -> Result<()> {
		if horizon > <Anchor<A> as Coord>::from_order(0) {
			engine.expire_meta(store, horizon.to_order())?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for RollingTopKDriver<A>
where
	A: RollingTopKRegistration + 'static,
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

impl<A> GuestOperator for RollingTopKDriver<A>
where
	A: RollingTopKRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync + HeapSize,
	A::GroupKey: Send + Sync,
	A::WindowSlot: Send + Sync,
	A::Accumulator: Send + Sync,
	A::SecondaryKey: Send + Sync + HeapSize,
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
			engine: RollingTopKEngine::new(engine_config),
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

		let lateness = self.aggregator.lateness();
		if let Some(lateness) = lateness {
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

		let emits = {
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
				|group| aggregator.encode_state_key(group),
				|group, secondary| aggregator.encode_row_key(group, secondary),
				|group, buffer| aggregator.combine(group, buffer),
			)?
		};

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();
		for emit in emits {
			match emit {
				TopKEmit::Insert {
					row_number,
					value,
				} => inserts.push((row_number, value)),
				TopKEmit::Update {
					row_number,
					prior,
					value,
				} => updates.push((row_number, prior, value)),
				TopKEmit::Remove {
					row_number,
					value,
				} => removes.push((row_number, value)),
			}
		}
		Self::emit_three_batches(ctx, &inserts, &updates, &removes)?;

		Ok(())
	}
}

impl<A> RollingTopKDriver<A>
where
	A: RollingTopKRegistration + Send + Sync + 'static,
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
	fn route_diffs_to_buckets(&self, ctx: &mut impl GuestContext, change: &impl ChangeView) -> Buckets<A> {
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
							let Some(slot) = self.aggregator.coord(&row) else {
								continue;
							};
							if let Some((group, contribution)) =
								self.aggregator.extract(ctx, &row)
							{
								let bucket = bucket_of(
									slot.order_key(),
									self.aggregator.bucket_size(),
								);
								buckets.entry((group, bucket))
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
								&& let Some(slot) = self.aggregator.coord(&pre_row)
								&& let Some((group, contribution)) =
									self.aggregator.extract(ctx, &pre_row)
							{
								let bucket = bucket_of(
									slot.order_key(),
									self.aggregator.bucket_size(),
								);
								buckets.entry((group, bucket))
									.or_default()
									.push(AccumulatorEvent::Remove(contribution));
							}
							if let Some(post_row) = post.row(i)
								&& let Some(slot) = self.aggregator.coord(&post_row)
								&& let Some((group, contribution)) =
									self.aggregator.extract(ctx, &post_row)
							{
								let bucket = bucket_of(
									slot.order_key(),
									self.aggregator.bucket_size(),
								);
								buckets.entry((group, bucket))
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
							if let Some((group, contribution)) =
								self.aggregator.extract(ctx, &row)
							{
								let bucket = bucket_of(
									slot.order_key(),
									self.aggregator.bucket_size(),
								);
								buckets.entry((group, bucket))
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
		ctx: &mut impl GuestContext,
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
