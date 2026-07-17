// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	util::memory::{HeapSize, OperatorSample},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, config::TumblingCarryConfig, is_sealed, seal_horizon,
			tumbling::TumblingBuckets, tumbling_carry::TumblingCarryEngine,
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

type AccumulatorContribution<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Contribution;
type AccumulatorValue<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Output;
type CarryEngine<A> = TumblingCarryEngine<
	<A as TumblingCarryOperator>::GroupKey,
	<A as TumblingCarryOperator>::WindowCoord,
	<A as TumblingCarryOperator>::Accumulator,
	<A as TumblingCarryOperator>::Carry,
	<A as TumblingCarryOperator>::Output,
>;
type Buckets<A> = TumblingBuckets<
	<A as TumblingCarryOperator>::GroupKey,
	<A as TumblingCarryOperator>::WindowCoord,
	AccumulatorContribution<A>,
>;

pub trait TumblingCarryOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type Accumulator: WindowAccumulator;

	type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned;

	type Carry: Clone + Debug + Serialize + DeserializeOwned;

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

	fn retention(&self) -> Option<<Self::WindowCoord as Slot>::Duration> {
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

	fn encode_row_key(&self, group: &Self::GroupKey, window_start: Self::WindowCoord) -> EncodedKey;
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
	A::WindowCoord: Send + Sync + HeapSize,
	<A::WindowCoord as Slot>::Duration: Send + Sync,
	A::Accumulator: Send + Sync + HeapSize,
	A::Carry: Send + Sync + HeapSize,
	A::Output: Send + Sync + HeapSize,
	AccumulatorContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn sample(&self) -> Option<OperatorSample> {
		Some(OperatorSample::with_memory(self.engine.approximate_memory()))
	}

	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		let retention = aggregator.retention();
		Ok(Self {
			aggregator,
			engine: TumblingCarryEngine::new(
				TumblingCarryConfig::builder()
					.base(window_engine_config(config))
					.retention(retention)
					.build(),
			),
		})
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let mut buckets = self.route(ctx, &change);
		if buckets.is_empty() {
			return Ok(());
		}

		if let Some(seal_after) = self.aggregator.seal_after() {
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::{
		encoded::shape::{RowShape, RowShapeField},
		key::encoded::EncodedKey,
	};
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId, row::Row as CoreRow,
		window::accumulator::invertible::RetainedAccumulator,
	};
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;
	use crate::{
		operator::FFIOperatorAdapter,
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// A TWAP-shaped fixture exercising the carry-forward rotation in
	// isolation. Each window retains its observations (RetainedAccumulator keyed by
	// timestamp); the value summed is incidental. `carry_in` echoes the
	// prior window's closing observation so assertions can prove the carry
	// rotated across the window boundary, not whether the integral math is
	// right (that lives in the operator's own tests).

	#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
	struct CarryOut {
		group: String,
		window_start: u64,
		sum: f64,
		carry_in: f64,
		has_carry: bool,
	}

	row!(CarryOut {
		group: String,
		window_start: u64,
		sum: f64,
		carry_in: f64,
		has_carry: bool
	});

	struct TestCarry;

	impl TumblingCarryOperator for TestCarry {
		type GroupKey = String;
		type WindowCoord = u64;
		type Accumulator = RetainedAccumulator<u64, f64>;
		type Output = CarryOut;
		type Carry = f64;

		fn extract(
			&self,
			_ctx: &mut impl OperatorContext,
			row: &impl RowView,
		) -> Option<(String, u64, (u64, f64))> {
			let group = row.utf8("group")?.to_string();
			let ts = row.u64("ts")?;
			let price = row.f64("price")?;
			Some((group, ts, (ts, price)))
		}

		fn window_for(&self, coord: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(coord, 60)
		}

		fn build_output(
			&self,
			group: &String,
			span: WindowSpan<u64>,
			value: &BTreeMap<u64, f64>,
			prev_carry: Option<&f64>,
		) -> Option<CarryOut> {
			(!value.is_empty()).then(|| CarryOut {
				group: group.clone(),
				window_start: span.start,
				sum: value.values().sum(),
				carry_in: prev_carry.copied().unwrap_or(0.0),
				has_carry: prev_carry.is_some(),
			})
		}

		fn carry_forward(&self, value: &BTreeMap<u64, f64>, _prev_carry: Option<&f64>) -> Option<f64> {
			value.last_key_value().map(|(_, v)| *v)
		}
	}

	impl TumblingCarryRegistration for TestCarry {
		const NAME: &'static str = "test_carry";
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
			RowShapeField::unconstrained("ts", ValueType::Uint8),
			RowShapeField::unconstrained("price", ValueType::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, ts: u64, price: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![Value::Utf8(group.into()), Value::Uint8(ts), Value::float8(price)])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn first_window_has_no_carry() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 30, 20.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.u64("window_start"), Some(0));
		assert_eq!(r.f64("sum"), Some(30.0));
		assert_eq!(r.bool("has_carry"), Some(false), "first window has no prior close to carry in");
		assert_eq!(r.f64("carry_in"), Some(0.0));
	}

	#[test]
	fn remove_empties_window_emits_remove() {
		// Removing the only observation empties the window's accumulator, so the
		// carry driver must withdraw the previously emitted row (terminal Remove
		// carrying the prior output) rather than leak a ghost row - required for
		// reorg-retraction correctness of the carry/EMA-family views.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		assert_eq!(out.diffs[0].kind(), DiffType::Remove);
		let r = out.diffs[0].pre().expect("remove pre").row_ref(0).expect("r0");
		assert_eq!(r.u64("window_start"), Some(0));
		assert_eq!(r.f64("sum"), Some(10.0));
	}

	#[test]
	fn second_window_carries_in_prior_window_close() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		// Window [0,60): closing observation (largest ts) is price 20.
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 30, 20.0))
				.build())
			.expect("apply");
		// Window [60,120) opens: its carry_in must be the prior close, 20.
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(3, "BTC", 70, 5.0)).build()).expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.u64("window_start"), Some(60));
		assert_eq!(r.f64("sum"), Some(5.0));
		assert_eq!(r.bool("has_carry"), Some(true));
		assert_eq!(r.f64("carry_in"), Some(20.0), "carry rotated from the closed window's last observation");
	}

	#[test]
	fn carry_rotates_across_three_windows_in_one_batch() {
		// Windows in a single batch must rotate the carry in window order:
		// w0 closes at 10 -> w60 carries 10 (closes at 20) -> w120 carries 20.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 60, 20.0))
				.insert(input_row(3, "BTC", 120, 30.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 3);
		let w0 = post.row_ref(0).expect("r0");
		assert_eq!(w0.u64("window_start"), Some(0));
		assert_eq!(w0.bool("has_carry"), Some(false));
		let w60 = post.row_ref(1).expect("r1");
		assert_eq!(w60.u64("window_start"), Some(60));
		assert_eq!(w60.f64("carry_in"), Some(10.0));
		let w120 = post.row_ref(2).expect("r2");
		assert_eq!(w120.u64("window_start"), Some(120));
		assert_eq!(w120.f64("carry_in"), Some(20.0));
	}

	#[test]
	fn update_in_current_window_recomputes_carry() {
		// The carry is derived from the (delta-correct) window value, so an
		// Update that changes the closing observation must change what the
		// next window carries in.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		// Update the still-open window's observation 10 -> 50.
		let _ = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 50.0))
				.build())
			.expect("apply");
		// New window carries in the updated close, 50 - not the original 10.
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 60, 1.0)).build()).expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.u64("window_start"), Some(60));
		assert_eq!(r.f64("carry_in"), Some(50.0), "carry reflects the post-update close");
	}

	#[test]
	fn late_event_accepted_without_sealing() {
		// The carry driver carries no seal envelope in this fixture, so under
		// grace semantics there is no gate: a late event for the earlier window
		// [0,60) is accepted and (re)opens that window. Bounding mutability is
		// the seal gate's job (opt-in via seal_after / sealed_up_to), not an
		// implicit high-water drop.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 20.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert!(!out.diffs.is_empty(), "ungated carry driver accepts late events");
	}
}
