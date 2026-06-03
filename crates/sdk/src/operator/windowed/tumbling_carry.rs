// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::catalog::flow::FlowNodeId,
	window::{
		accumulator::WindowAccumulator,
		engine::{AccEvent, EmitKind, tumbling::TumblingBuckets, tumbling_carry::TumblingCarryEngine},
		span::{Slot, WindowSpan},
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
		windowed::bridge::OperatorContextStore,
	},
};

type AccContribution<A> = <<A as TumblingCarryOperator>::Acc as WindowAccumulator>::Contribution;
type AccValue<A> = <<A as TumblingCarryOperator>::Acc as WindowAccumulator>::Output;
type Buckets<A> = TumblingBuckets<
	<A as TumblingCarryOperator>::GroupKey,
	<A as TumblingCarryOperator>::WindowCoord,
	AccContribution<A>,
>;

pub trait TumblingCarryOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type Acc: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

	type Carry: Clone + Debug + Serialize + DeserializeOwned;

	fn extract(&self, row: &impl RowView) -> Option<(Self::GroupKey, Self::WindowCoord, AccContribution<Self>)>;

	fn window_for(&self, coord: Self::WindowCoord) -> WindowSpan<Self::WindowCoord>;

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<Self::WindowCoord>,
		value: &AccValue<Self>,
		prev_carry: Option<&Self::Carry>,
	) -> Option<Self::Output>;

	fn carry_forward(&self, value: &AccValue<Self>, prev_carry: Option<&Self::Carry>) -> Option<Self::Carry>;

	fn new_accumulator(&self) -> Self::Acc {
		Self::Acc::default()
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
	engine: TumblingCarryEngine<A::GroupKey, A::WindowCoord, A::Acc, A::Carry>,
}

impl<A> TumblingCarryDriver<A>
where
	A: TumblingCarryRegistration,
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
						self.push_all(&pre, &mut buckets, false);
						self.push_all(&post, &mut buckets, true);
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
			let span = self.aggregator.window_for(coord);
			let event = if is_add {
				AccEvent::Add(contribution)
			} else {
				AccEvent::Remove(contribution)
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
	A::WindowCoord: Send + Sync,
	A::Acc: Send + Sync,
	A::Carry: Send + Sync,
	AccContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			engine: TumblingCarryEngine::new(),
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
		for r in results {
			match r.kind {
				EmitKind::Insert => inserts.push((r.row_number, r.value)),
				EmitKind::Update => updates.push((r.row_number, r.value)),
				EmitKind::Remove => {}
			}
		}
		self.emit_batches(ctx, &inserts, &updates)?;

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

	use reifydb_core::{
		encoded::{
			key::EncodedKey,
			shape::{RowShape, RowShapeField},
		},
		interface::catalog::flow::FlowNodeId,
		row::Row as CoreRow,
	};
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;
	use crate::{
		operator::{FFIOperatorAdapter, windowed::accumulator::RetainedAcc},
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// A TWAP-shaped fixture exercising the carry-forward rotation in
	// isolation. Each window retains its observations (RetainedAcc keyed by
	// timestamp); the value summed is incidental. `carry_in` echoes the
	// prior window's closing observation so assertions can prove the carry
	// rotated across the window boundary, not whether the integral math is
	// right (that lives in the operator's own tests).

	#[derive(Clone, Debug, PartialEq)]
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
		type Acc = RetainedAcc<u64, f64>;
		type Output = CarryOut;
		type Carry = f64;

		fn extract(&self, row: &impl RowView) -> Option<(String, u64, (u64, f64))> {
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
	fn late_event_dropped_and_carry_untouched() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingCarryDriver<TestCarry>>>::new()
			.build()
			.expect("harness");
		// Open window [60,120); high-water advances to 60.
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 20.0)).build()).expect("apply");
		// A late event for the already-closed window [0,60) is dropped.
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}
}
