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
			AccumulatorEvent, EmitKind, is_sealed, seal_horizon,
			tumbling::{TumblingBuckets, TumblingEngine, reindex_window},
		},
		span::{Slot, WindowSpan},
	},
};
use reifydb_value::value::row_number::RowNumber;
use serde::{Serialize, de::DeserializeOwned};
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
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{advance_seal_watermark, bridge::OperatorContextStore, window_engine_config},
	},
};

type AccumulatorContribution<A> = <<A as TumblingOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingOperator>::Accumulator as WindowAccumulator>::Output;
type Buckets<A> = TumblingBuckets<
	<A as TumblingOperator>::GroupKey,
	<A as TumblingOperator>::WindowCoord,
	AccumulatorContribution<A>,
>;

pub trait TumblingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

	fn extract(
		&self,
		ctx: &mut impl OperatorContext,
		row: &impl RowView,
	) -> Option<(Self::GroupKey, Self::WindowCoord, AccumulatorContribution<Self>)>;

	fn window_for(&self, coord: Self::WindowCoord) -> WindowSpan<Self::WindowCoord>;

	fn seal_after(&self) -> Option<u64> {
		None
	}

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<Self::WindowCoord>,
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

	fn encode_row_key(&self, group: &Self::GroupKey, window_start: Self::WindowCoord) -> EncodedKey;
}

pub struct TumblingDriver<A>
where
	A: TumblingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	engine: TumblingEngine<A::GroupKey, A::WindowCoord, A::Accumulator>,
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
	A::WindowCoord: Send + Sync,
	A::Accumulator: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
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
	A::WindowCoord: Send + Sync,
	A::Accumulator: Send + Sync,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			engine: TumblingEngine::new(window_engine_config(config)),
		})
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		if let Some(seal_after) = self.aggregator.seal_after() {
			let Self {
				aggregator: _,
				engine,
			} = &mut *self;
			let mut store = OperatorContextStore(ctx);
			let batch_max = buckets.keys().map(|(_, span)| span.start.order_key()).max().unwrap_or(0);
			let watermark = advance_seal_watermark(&mut store, batch_max)?;
			let horizon = seal_horizon(watermark, seal_after);
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
			if horizon > 0 {
				engine.expire(&mut store, horizon - 1)?;
			}
			if buckets.is_empty() {
				return Ok(());
			}
		}

		let results = {
			let Self {
				aggregator,
				engine,
			} = &mut *self;
			let mut store = OperatorContextStore(ctx);
			engine.apply(
				&mut store,
				buckets,
				|group, window_start| aggregator.encode_row_key(group, window_start),
				|| aggregator.new_accumulator(),
			)?
		};

		if self.aggregator.seal_after().is_some() {
			let mut store = OperatorContextStore(ctx);
			for r in &results {
				if r.kind == EmitKind::Insert {
					reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						r.row_number,
						None,
						Some(r.span.start.order_key()),
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

#[cfg(test)]
mod tests {
	use reifydb_codec::{
		encoded::shape::{RowShape, RowShapeField},
		key::encoded::EncodedKey,
	};
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		row::Row as CoreRow,
		window::accumulator::invertible::{Moments, Multiset, OrdF64},
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

	// An invertible volume aggregator. Its accumulator keeps only running
	// Moments (no per-slot map): Insert adds, Update is routed by the driver
	// as remove(pre)+add(post), Remove subtracts. This is the case the old
	// per-slot map existed to handle and that the pre/post diff now subsumes.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct VolumeAccumulator {
		moments: Moments,
	}

	impl WindowAccumulator for VolumeAccumulator {
		type Contribution = f64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &f64) {
			self.moments.add(*contribution);
		}

		fn remove(&mut self, contribution: &f64) {
			self.moments.remove(*contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			(!self.moments.is_empty()).then(|| OrdF64::new(self.moments.sum()).expect("finite"))
		}

		fn is_empty(&self) -> bool {
			self.moments.is_empty()
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	struct VolumeOut {
		group: String,
		window_start: u64,
		volume: f64,
	}

	row!(VolumeOut {
		group: String,
		window_start: u64,
		volume: f64
	});

	struct TestVolume;

	impl TumblingOperator for TestVolume {
		type GroupKey = String;
		type WindowCoord = u64;
		type Accumulator = VolumeAccumulator;
		type Output = VolumeOut;

		fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, f64)> {
			let group = row.utf8("group")?.to_string();
			let slot = row.u64("slot")?;
			let size = row.f64("size")?;
			Some((group, slot, size))
		}

		fn window_for(&self, coord: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(coord, 60)
		}

		fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<VolumeOut> {
			Some(VolumeOut {
				group: group.clone(),
				window_start: span.start,
				volume: value.get(),
			})
		}
	}

	impl TumblingRegistration for TestVolume {
		const NAME: &'static str = "test_volume";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
			EncodedKey::builder().str(group).u64(window_start).build()
		}
	}

	// TestVolume with sealing enabled: 60ms windows + 60ms grace, so windows
	// seal once the watermark (tracked from routed window starts) moves more
	// than 120 past their start.
	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct SealedVolume;

	impl TumblingOperator for SealedVolume {
		type GroupKey = String;
		type WindowCoord = u64;
		type Accumulator = VolumeAccumulator;
		type Output = VolumeOut;

		fn extract(&self, ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, f64)> {
			TestVolume.extract(ctx, row)
		}

		fn window_for(&self, coord: u64) -> WindowSpan<u64> {
			TestVolume.window_for(coord)
		}

		fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<VolumeOut> {
			TestVolume.build_output(group, span, value)
		}

		fn seal_after(&self) -> Option<u64> {
			Some(120)
		}
	}

	impl TumblingRegistration for SealedVolume {
		const NAME: &'static str = "sealed_volume";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
			EncodedKey::builder().str(group).u64(window_start).build()
		}
	}

	// A removal-safe minimum aggregator over an ordered multiset. Demonstrates
	// the non-invertible family: an Update that replaces the current minimum
	// with a larger value must raise the window minimum, which a scalar
	// running-min could not do.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct MinAccumulator {
		values: Multiset<OrdF64>,
	}

	impl WindowAccumulator for MinAccumulator {
		type Contribution = OrdF64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &OrdF64) {
			self.values.add(*contribution);
		}

		fn remove(&mut self, contribution: &OrdF64) {
			self.values.remove(contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			self.values.min().copied()
		}

		fn is_empty(&self) -> bool {
			self.values.is_empty()
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	struct MinOut {
		group: String,
		window_start: u64,
		min: f64,
	}

	row!(MinOut {
		group: String,
		window_start: u64,
		min: f64
	});

	struct TestMin;

	impl TumblingOperator for TestMin {
		type GroupKey = String;
		type WindowCoord = u64;
		type Accumulator = MinAccumulator;
		type Output = MinOut;

		fn extract(
			&self,
			_ctx: &mut impl OperatorContext,
			row: &impl RowView,
		) -> Option<(String, u64, OrdF64)> {
			let group = row.utf8("group")?.to_string();
			let slot = row.u64("slot")?;
			let size = row.f64("size")?;
			Some((group, slot, OrdF64::new(size)?))
		}

		fn window_for(&self, coord: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(coord, 60)
		}

		fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<MinOut> {
			Some(MinOut {
				group: group.clone(),
				window_start: span.start,
				min: value.get(),
			})
		}
	}

	impl TumblingRegistration for TestMin {
		const NAME: &'static str = "test_min";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
			EncodedKey::builder().str(group).u64(window_start).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("slot", ValueType::Uint8),
			RowShapeField::unconstrained("size", ValueType::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, slot: u64, size: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![Value::Utf8(group.into()), Value::Uint8(slot), Value::float8(size)])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn single_insert_emits_insert() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.utf8("group").as_deref(), Some("BTC"));
		assert_eq!(r.u64("window_start"), Some(0));
		assert_eq!(r.f64("volume"), Some(10.0));
	}

	#[test]
	fn update_applies_post_minus_pre_no_double_count() {
		// The crux of the redesign: an Update carries pre=10, post=25.
		// The driver routes it as remove(10)+add(25) on a running sum,
		// yielding 25 - not 10 + 25 = 35 - with NO per-slot map.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("volume"), Some(25.0));
	}

	#[test]
	fn two_contributions_then_remove_subtracts_pre() {
		// Two distinct slots in one window sum to 15; a Remove carrying
		// pre=5 subtracts that contribution, leaving 10. No slot key is
		// needed - the diff's pre value is what gets subtracted.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 30, 5.0))
				.build())
			.expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("volume"), Some(10.0));
	}

	#[test]
	fn remove_clears_window_emits_remove() {
		// An emptied window emits a Remove of its previously emitted aggregate
		// row, so a downstream consumer withdraws the stale row instead of
		// leaking it. The accumulator is empty (finalize returns None); the
		// engine carries the prior value so the driver can emit the Remove.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		assert_eq!(out.diffs[0].kind(), DiffType::Remove);
		let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
		assert_eq!(r.f64("volume"), Some(10.0));
	}

	#[test]
	fn boundary_slot_belongs_to_next_window() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 59, 1.0))
				.insert(input_row(2, "BTC", 60, 1.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		assert_eq!(post.row_ref(0).expect("r0").u64("window_start"), Some(0));
		assert_eq!(post.row_ref(1).expect("r1").u64("window_start"), Some(60));
	}

	#[test]
	fn late_event_for_sealed_window_dropped() {
		// Grace semantics: SealedVolume seals windows whose start falls more
		// than seal_after (window 60 + grace 60 = 120) behind the routed
		// watermark. Advancing to window 180 seals window 0; a late insert for
		// it must be dropped. An ungated driver (TestVolume) accepts the same
		// late insert - covered by late_event_without_sealing_is_accepted.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0, "insert into a sealed window must be dropped");
	}

	#[test]
	fn late_event_within_grace_is_accepted() {
		// Window 0 stays mutable while the watermark has not passed
		// start + seal_after: with the watermark at 120 (== 0 + 120), the
		// boundary is inclusive on the mutable side.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 120, 5.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1, "window 0 is still within grace at watermark 120");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_ref(0).expect("r0").f64("volume"), Some(99.0));
	}

	#[test]
	fn late_event_without_sealing_is_accepted() {
		// With seal_after = None (the default) there is no gate: drivers accept
		// arbitrarily late mutations and state lives until the operator TTL.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 180, 5.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1, "ungated drivers accept late inserts");
	}

	#[test]
	fn remove_within_grace_is_applied_and_sealed_remove_is_dropped() {
		// Grace is the single mutability horizon for every mutation kind: a
		// retraction (reorg correction) is honored while the window is open or
		// within grace, and dropped once the window seals - the sealed value is
		// final by contract. Window 0 holds 15; a remove at watermark 60 (well
		// inside start + seal_after = 120) subtracts, leaving 10. Advancing the
		// watermark to 240 seals window 0 and reclaims its state; a further
		// remove is dropped and emits nothing, leaving the last published value
		// untouched.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 30, 5.0))
				.build())
			.expect("apply");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 60, 1.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1, "retraction within grace must be honored");
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("volume"), Some(10.0));

		let _ = h.apply(TestChangeBuilder::new().insert(input_row(4, "BTC", 240, 2.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0, "retraction of a sealed window must be dropped");
	}

	#[test]
	fn multiple_groups_isolate_state() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
		assert_eq!(post.row_ref(0).expect("r0").f64("volume"), Some(10.0));
		assert_eq!(post.row_ref(1).expect("r1").utf8("group").as_deref(), Some("ETH"));
		assert_eq!(post.row_ref(1).expect("r1").f64("volume"), Some(50.0));
	}

	#[test]
	fn min_update_replacing_minimum_raises_window_min() {
		// The removal-safe multiset case: window holds {5, 8, 6}, min = 5.
		// An Update replacing the 5 with 10 must raise the min to 6. A
		// running scalar min cannot do this; the multiset remove(5)+add(10)
		// leaves {6, 8, 10}.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestMin>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 5.0))
				.insert(input_row(2, "BTC", 10, 8.0))
				.insert(input_row(3, "BTC", 20, 6.0))
				.build())
			.expect("apply");
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 5.0), input_row(1, "BTC", 0, 10.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("min"), Some(6.0));
	}

	#[test]
	fn sealing_frees_window_state_from_the_store() {
		// Sealing must reclaim the sealed window's accumulator state, not
		// just gate its mutations: state left behind is only reaped by the
		// wall-clock operator-state TTL backstop, which the paced jupiter
		// replay showed retaining hours of sealed windows. Window 0 is
		// created, then an insert at 240 moves the watermark so the seal
		// horizon (240 - 120) passes window 0: at least one of its store
		// keys must be gone afterwards.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<SealedVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let before = h.snapshot_state();
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 240, 2.0)).build()).expect("apply");
		let after = h.snapshot_state();
		let freed = before.keys().filter(|k| !after.contains_key(*k)).count();
		assert!(freed > 0, "sealing window 0 must remove its accumulator state from the store");

		// Control: without seal_after, ordinary apply churn must never
		// remove a key - reclamation may only come from the seal sweep.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let before = h.snapshot_state();
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 240, 2.0)).build()).expect("apply");
		let after = h.snapshot_state();
		assert!(before.keys().all(|k| after.contains_key(k)), "an ungated driver must not reclaim any state");
	}

	#[test]
	fn min_remove_duplicate_keeps_value_until_last_removed() {
		// Two events share value 5. Removing one occurrence must keep the
		// min at 5 (the multiset still holds one 5).
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestMin>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 5.0))
				.insert(input_row(2, "BTC", 10, 5.0))
				.insert(input_row(3, "BTC", 20, 9.0))
				.build())
			.expect("apply");
		let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 5.0)).build()).expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("min"), Some(5.0), "one occurrence of 5 remains, min stays 5");
	}
}
