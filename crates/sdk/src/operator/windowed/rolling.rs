// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::catalog::flow::FlowNodeId,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind,
			rolling::{RollingBuckets, RollingEngine},
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
			batch::{InsertBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::OperatorContext,
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{bridge::OperatorContextStore, late_policy_from_config},
	},
};

type AccumulatorContribution<A> = <<A as RollingOperator>::Accumulator as WindowAccumulator>::Contribution;

pub trait RollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

	fn capacity(&self) -> usize;

	fn extract(
		&self,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, Self::WindowCoord, AccumulatorContribution<Self>)>;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowCoord, Self::Accumulator>,
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

	fn from_config(operator_id: FlowNodeId, config: &Config) -> Result<Self>;

	fn encode_row_key(&self, group: &Self::GroupKey) -> EncodedKey;
}

pub type RollingBuffer<A> = BTreeMap<<A as RollingOperator>::WindowCoord, <A as RollingOperator>::Accumulator>;

type Buckets<A> = RollingBuckets<
	<A as RollingOperator>::GroupKey,
	<A as RollingOperator>::WindowCoord,
	AccumulatorContribution<A>,
>;

pub struct RollingDriver<A>
where
	A: RollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: RollingEngine<A::GroupKey, A::WindowCoord, A::Accumulator>,
}

impl<A> RollingDriver<A>
where
	A: RollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn route(&self, change: &impl ChangeView) -> Buckets<A> {
		let mut buckets: Buckets<A> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						self.push_all(&cols, &mut buckets, true);
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						self.push_updates(&pre, &post, &mut buckets);
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						self.push_all(&cols, &mut buckets, false);
					}
				}
			}
		}
		buckets
	}

	fn push_all<C: ColumnsView>(&self, cols: &C, buckets: &mut Buckets<A>, is_add: bool) {
		for i in 0..cols.row_count() {
			let Some(row) = cols.row(i) else {
				continue;
			};
			let Some((group, coord, contribution)) = self.aggregator.extract(&row) else {
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

	fn push_updates<P: ColumnsView, Q: ColumnsView>(&self, pre: &P, post: &Q, buckets: &mut Buckets<A>) {
		let n = pre.row_count().min(post.row_count());
		for i in 0..n {
			if let Some(pre_row) = pre.row(i)
				&& let Some((group, coord, contribution)) = self.aggregator.extract(&pre_row)
			{
				buckets.entry((group, coord)).or_default().push(AccumulatorEvent::Remove(contribution));
			}
			if let Some(post_row) = post.row(i)
				&& let Some((group, coord, contribution)) = self.aggregator.extract(&post_row)
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
	A::WindowCoord: Send + Sync,
	A::Accumulator: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			engine: RollingEngine::with_late_policy(late_policy_from_config(config)),
		})
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let buckets = self.route(&change);
		if buckets.is_empty() {
			return Ok(());
		}

		let results = {
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
				|group| aggregator.encode_row_key(group),
				|group, buffer| aggregator.combine(group, buffer),
			)?
		};

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, r.value)),
				EmitKind::Remove => {}
			}
		}
		Self::emit_batches(ctx, inserts, updates)?;

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		let mut store = OperatorContextStore(ctx);
		self.engine.flush(&mut store)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		encoded::{
			key::EncodedKey,
			shape::{RowShape, RowShapeField},
		},
		interface::catalog::flow::FlowNodeId,
		row::Row as CoreRow,
		window::accumulator::invertible::Moments,
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

	// Rolling sum over the last 3 windows, where EACH window is itself an
	// invertible sum accumulator. This exercises the rolling improvement:
	// multiple input rows can share a window coordinate and accumulate, and a
	// single event can be removed from inside a window (remove(pre)) without
	// dropping the whole window - which the old last-write-wins buffer could
	// not do.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct WindowSum {
		moments: Moments,
	}

	impl WindowAccumulator for WindowSum {
		type Contribution = f64;
		type Output = f64;

		fn add(&mut self, contribution: &f64) {
			self.moments.add(*contribution);
		}

		fn remove(&mut self, contribution: &f64) {
			self.moments.remove(*contribution);
		}

		fn finalize(&self) -> Option<f64> {
			(!self.moments.is_empty()).then(|| self.moments.sum())
		}

		fn is_empty(&self) -> bool {
			self.moments.is_empty()
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	struct TestOut {
		group: String,
		rolling_sum: f64,
		windows: u32,
	}

	row!(TestOut {
		group: String,
		rolling_sum: f64,
		windows: u32
	});

	struct TestRollingSum {
		capacity: usize,
	}

	impl RollingOperator for TestRollingSum {
		type GroupKey = String;
		type WindowCoord = u64;
		type Accumulator = WindowSum;
		type Output = TestOut;

		fn capacity(&self) -> usize {
			self.capacity
		}

		fn extract(&self, row: &impl RowView) -> Option<(String, u64, f64)> {
			let group = row.utf8("group")?.to_string();
			let window_start = row.u64("window_start")?;
			let value = row.f64("value")?;
			Some((group, window_start, value))
		}

		fn combine(&self, group: &String, buffer: &BTreeMap<u64, WindowSum>) -> Option<TestOut> {
			if buffer.is_empty() {
				return None;
			}
			let rolling_sum = buffer.values().filter_map(|w| w.finalize()).sum();
			Some(TestOut {
				group: group.clone(),
				rolling_sum,
				windows: buffer.len() as u32,
			})
		}
	}

	impl RollingRegistration for TestRollingSum {
		const NAME: &'static str = "test_rolling_sum";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self {
				capacity: 3,
			})
		}

		fn encode_row_key(&self, group: &String) -> EncodedKey {
			EncodedKey::builder().str(group).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("window_start", ValueType::Uint8),
			RowShapeField::unconstrained("value", ValueType::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, window_start: u64, value: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![Value::Utf8(group.into()), Value::Uint8(window_start), Value::float8(value)])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn single_insert_emits_insert() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(10.0));
		assert_eq!(r.u32("windows"), Some(1));
	}

	#[test]
	fn multiple_events_accumulate_within_one_window() {
		// Two rows share window coordinate 0; the window accumulates to 7.
		// This is the capability the old last-write-wins buffer lacked.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 3.0))
				.insert(input_row(2, "BTC", 0, 4.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(7.0));
		assert_eq!(r.u32("windows"), Some(1), "both rows landed in the same window");
	}

	#[test]
	fn partial_remove_within_window_keeps_window_alive() {
		// Window 0 holds two events (sum 7). Removing one event leaves the
		// window with sum 4 - the window is NOT dropped wholesale.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 3.0))
				.insert(input_row(2, "BTC", 0, 4.0))
				.build())
			.expect("apply");
		let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 3.0)).build()).expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(4.0));
		assert_eq!(r.u32("windows"), Some(1), "window survives partial removal");
	}

	#[test]
	fn update_within_window_applies_post_minus_pre() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(25.0), "25, not 10 + 25");
	}

	#[test]
	fn buffer_fills_then_evicts_oldest_window() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 1.0))
				.insert(input_row(2, "BTC", 60, 2.0))
				.insert(input_row(3, "BTC", 120, 3.0))
				.insert(input_row(4, "BTC", 180, 4.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(9.0), "window 0 evicted: 2+3+4");
		assert_eq!(r.u32("windows"), Some(3));
	}

	#[test]
	fn late_window_event_dropped() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 5.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0, "event for a buried window is dropped");
	}

	#[test]
	fn remove_clears_buffer_emits_nothing() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn multiple_groups_isolate_buffers() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "ETH", 0, 50.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		assert_eq!(post.row_ref(0).expect("r0").utf8("group").as_deref(), Some("BTC"));
		assert_eq!(post.row_ref(0).expect("r0").f64("rolling_sum"), Some(10.0));
		assert_eq!(post.row_ref(1).expect("r1").utf8("group").as_deref(), Some("ETH"));
		assert_eq!(post.row_ref(1).expect("r1").f64("rolling_sum"), Some(50.0));
	}
}
