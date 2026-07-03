// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent,
			multi_rolling::{MultiEmit, MultiRollingEngine},
			rolling::RollingBuckets,
		},
		span::Slot,
	},
};
use reifydb_value::value::row_number::RowNumber;
use serde::{Serialize, de::DeserializeOwned};

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
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{bridge::OperatorContextStore, window_engine_config},
	},
};

type AccumulatorContribution<A> = <<A as MultiRollingOperator>::Accumulator as WindowAccumulator>::Contribution;

type Buckets<A> = RollingBuckets<
	<A as MultiRollingOperator>::GroupKey,
	<A as MultiRollingOperator>::WindowCoord,
	AccumulatorContribution<A>,
>;

pub trait MultiRollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type Accumulator: WindowAccumulator;

	type SecondaryKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned;

	fn capacity(&self) -> usize;

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, Self::WindowCoord, AccumulatorContribution<Self>)>;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowCoord, Self::Accumulator>,
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

	fn from_config(operator_id: FlowNodeId, config: &Config) -> Result<Self>;

	fn encode_state_key(&self, group: &Self::GroupKey) -> EncodedKey;

	fn encode_row_key(&self, group: &Self::GroupKey, secondary: &Self::SecondaryKey) -> EncodedKey;
}

pub type MultiRollingBuffer<A> =
	BTreeMap<<A as MultiRollingOperator>::WindowCoord, <A as MultiRollingOperator>::Accumulator>;

pub type MultiRollingEmit<A> = BTreeMap<<A as MultiRollingOperator>::SecondaryKey, <A as MultiRollingOperator>::Output>;

pub struct MultiRollingDriver<A>
where
	A: MultiRollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	#[allow(clippy::type_complexity)]
	engine: MultiRollingEngine<A::GroupKey, A::WindowCoord, A::Accumulator, A::SecondaryKey, A::Output>,
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
	A::Output: Row + Send + Sync,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::Accumulator: Send + Sync,
	A::SecondaryKey: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			engine: MultiRollingEngine::new(window_engine_config(config)),
		})
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let buckets = self.route_diffs_to_buckets(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		let emits = {
			let Self {
				aggregator,
				engine,
			} = &mut *self;
			let capacity = aggregator.capacity();
			let mut store = OperatorContextStore(ctx);
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
	A::WindowCoord: Send + Sync,
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

#[cfg(test)]
mod tests {
	use std::{cmp::Ordering, collections::BTreeMap};

	use reifydb_codec::{
		encoded::shape::{RowShape, RowShapeField},
		key::encoded::EncodedKey,
	};
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		row::Row as CoreRow,
		window::accumulator::invertible::{KeyedInvertibleAccumulator, Moments},
	};
	use reifydb_value::value::{Value, value_type::ValueType};
	use serde::{Deserialize, Serialize};

	use super::*;
	use crate::{
		operator::{FFIOperatorAdapter, view::RowView},
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// Rolling top-2 traders by summed volume over the last 3 windows. Each
	// window cell is a KeyedInvertibleAccumulator<trader, Moments> so a trade's
	// volume accumulates per trader and an Update/Remove subtracts it
	// (invertible). combine merges all buffered windows' per-trader sums,
	// ranks by total volume, and emits the top 2 keyed by rank.

	#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
	struct TopOut {
		group: String,
		rank: u32,
		trader: u64,
		volume: f64,
	}

	row!(TopOut {
		group: String,
		rank: u32,
		trader: u64,
		volume: f64
	});

	struct TestTopVolume;

	impl MultiRollingOperator for TestTopVolume {
		type GroupKey = String;
		type WindowCoord = u64;
		type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
		type SecondaryKey = u32;
		type Output = TopOut;

		fn capacity(&self) -> usize {
			3
		}

		fn extract(
			&self,
			_ctx: &mut impl OperatorContext,
			row: &impl RowView,
		) -> Option<(String, u64, (u64, f64))> {
			let group = row.utf8("group")?.to_string();
			let window_start = row.u64("window_start")?;
			let trader = row.u64("trader")?;
			let volume = row.f64("volume")?;
			Some((group, window_start, (trader, volume)))
		}

		fn combine(
			&self,
			group: &String,
			buffer: &BTreeMap<u64, KeyedInvertibleAccumulator<u64, Moments>>,
		) -> BTreeMap<u32, TopOut> {
			let mut totals: BTreeMap<u64, f64> = BTreeMap::new();
			for window in buffer.values() {
				if let Some(per_trader) = window.finalize() {
					for (trader, moments) in per_trader {
						*totals.entry(trader).or_insert(0.0) += moments.sum();
					}
				}
			}
			let mut ranked: Vec<(u64, f64)> = totals.into_iter().collect();
			ranked.sort_by(|a, b| {
				b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then_with(|| a.0.cmp(&b.0))
			});
			let mut out = BTreeMap::new();
			for (i, (trader, volume)) in ranked.into_iter().take(2).enumerate() {
				let rank = (i as u32) + 1;
				out.insert(
					rank,
					TopOut {
						group: group.clone(),
						rank,
						trader,
						volume,
					},
				);
			}
			out
		}
	}

	impl MultiRollingRegistration for TestTopVolume {
		const NAME: &'static str = "test_top_volume";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn encode_state_key(&self, group: &String) -> EncodedKey {
			EncodedKey::builder().str("state").str(group).build()
		}

		fn encode_row_key(&self, group: &String, secondary: &u32) -> EncodedKey {
			EncodedKey::builder().str("row").str(group).u32(*secondary).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("window_start", ValueType::Uint8),
			RowShapeField::unconstrained("trader", ValueType::Uint8),
			RowShapeField::unconstrained("volume", ValueType::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, window_start: u64, trader: u64, volume: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![
				Value::Utf8(group.into()),
				Value::Uint8(window_start),
				Value::Uint8(trader),
				Value::float8(volume),
			])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn same_window_volume_accumulates_per_trader() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		// Two trades for trader 100 in one window must SUM (5+3=8), unlike
		// the old last-write-wins fold. Trader 200 has 9.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 0, 200, 9.0))
				.insert(input_row(3, "BTC", 0, 100, 3.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0)), "trader 200 leads at 9.0");
		assert_eq!(by_rank.get(&2).copied(), Some((100u64, 8.0)), "trader 100 volume summed 5+3 = 8.0");
	}

	#[test]
	fn update_subtracts_old_volume_no_double_count() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 0, 200, 9.0))
				.build())
			.expect("apply");
		// Update trader 100's trade 5 -> 20. The driver routes remove(5)+add(20)
		// so trader 100's total is 20, NOT 5+20=25. It overtakes trader 200.
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 100, 5.0), input_row(1, "BTC", 0, 100, 20.0))
				.build())
			.expect("apply");
		let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
		assert!(kinds.contains(&DiffType::Update), "ranks changed, expect Update");
		let post = out.diffs.iter().find(|d| d.kind() == DiffType::Update).unwrap().post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((100u64, 20.0)), "trader 100 now leads at 20, not 25");
	}

	#[test]
	fn top_2_across_three_windows() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.insert(input_row(3, "BTC", 120, 300, 7.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0)));
		assert_eq!(by_rank.get(&2).copied(), Some((300u64, 7.0)));
	}

	#[test]
	fn vanishing_rank_emits_remove_at_high_water() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.build())
			.expect("apply");
		// Remove the newest window (wk=60 == high_water, not late). Trader
		// 200's only trade leaves -> its window empties and is dropped from
		// the buffer; rank-1 changes to trader 100, rank-2 vanishes -> Remove.
		let out = h
			.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 60, 200, 9.0)).build())
			.expect("apply");
		let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
		assert!(kinds.contains(&DiffType::Update), "rank-1 changed identity, expect Update");
		assert!(kinds.contains(&DiffType::Remove), "rank-2 vanished, expect Remove");
	}

	#[test]
	fn capacity_eviction_drops_oldest_window() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		// Capacity 3; 4 windows. Window 0 (trader 100, vol 1) is evicted.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 1.0))
				.insert(input_row(2, "BTC", 60, 200, 8.0))
				.insert(input_row(3, "BTC", 120, 300, 2.0))
				.insert(input_row(4, "BTC", 180, 400, 5.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 8.0)));
		assert_eq!(by_rank.get(&2).copied(), Some((400u64, 5.0)), "window 0 evicted; trader 100 gone");
	}

	#[test]
	fn buried_window_insert_dropped_silently() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 100, 5.0)).build())
			.expect("apply");
		let out = h
			.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 999, 999.0)).build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 0, "insert below high-water dropped");
	}
}
