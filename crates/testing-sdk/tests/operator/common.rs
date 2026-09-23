// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared fixtures, shapes, samplers and config matrices for the windowed operator
//! differential-chaos suite.
//!
//! Each fixture implements the windowed operator traits, so its accumulator oracle can
//! simulate it and the driver can run it through the `ChaosHarness`, and mirrors the in-crate
//! unit-test fixtures so both suites exercise the same code paths.

#![allow(dead_code)]

use std::collections::BTreeMap;

use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::ApplyWith,
};
use reifydb_flow::{
	operator::state::{
		seal::{coord::Coord, domain::SealDomain},
		sealing::{endpoint::SealingEndpoint, max::SealingMax, min::SealingMin},
	},
	window::{
		accumulator::{
			MergeAccumulator, WindowAccumulator,
			invertible::{
				keyed::KeyedInvertibleAccumulator, moments::Moments, multiset::Multiset,
				ordf64::OrdF64, retained_map::RetainedAccumulator,
			},
		},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		view::RowView,
		windowed::operator::{AllKinds, CarryEmit, Emit, NoRolling, WindowedOperator},
	},
	row,
};
use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario, SupportedOps};
use reifydb_testing_sdk::chaos::strategy::{ColumnSampler, samplers};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::{at_millis, millis},
	value::{Value, datetime::DateTime, value_type::ValueType},
};

pub fn settings(with: &ApplyWith) -> WindowSettings<DateTime> {
	<DateTime as SealDomain>::window_settings_of(with).expect("valid window settings")
}

pub const WINDOW: u64 = 60;
/// Held below WINDOW so aging is reachable inside a single window.
pub const OHLCV_IMMUTABLE: u64 = 20;
pub const ROLLING_CAPACITY: usize = 3;

/// Event times are sampled far finer than this, so flooring genuinely collapses rows into buckets.
pub const ROLLING_BUCKET: u64 = 10;

/// Replayed for every config so a failure names a reproducible run.
pub const SEEDS: [u64; 6] = [1, 7, 42, 99, 12_345, 2_024];

/// One event per Change, so the operator snapshots per single diff.
pub fn baseline(steps: u32, ops: SupportedOps) -> Scenario {
	Scenario::mixed(steps)
		.with_ops(ops)
		.with_max_live(40)
		.with_batch(BatchSize::Constant(1))
		.with_duplicate_update_burst(0.0)
		.with_update_as_remove_insert(0.0)
}

/// Multi-event batches plus both adversarial primitives; this is the configuration that
/// reaches the double-count-on-Update bug class.
pub fn full_chaos(steps: u32) -> Scenario {
	Scenario::mixed(steps)
		.with_ops(SupportedOps::all())
		.with_max_live(30)
		.with_batch(BatchSize::Geometric {
			p: 0.4,
			max: 8,
		})
		.with_duplicate_update_burst(0.6)
		.with_update_as_remove_insert(0.4)
}

/// Draws `none` roughly a quarter of the time, so operator and oracle have to agree on
/// skipping rows whose measured column is missing.
pub fn maybe_none_f64(lo: f64, hi: f64) -> ColumnSampler {
	samplers::select(&[
		Value::float8(lo),
		Value::float8((lo + hi) / 2.0),
		Value::float8(hi),
		Value::none_of(ValueType::Float8),
	])
}

/// The probe's identity must be absent from `initial`, or the round-trip proves nothing.
pub fn assert_add_remove_is_inverse<A: WindowAccumulator>(initial: &[A::Contribution], probe: A::Contribution) {
	let mut accumulator = A::default();
	for c in initial {
		accumulator.add(c);
	}
	let before = accumulator.finalize();
	accumulator.add(&probe);
	accumulator.remove(&probe);
	assert_eq!(accumulator.finalize(), before, "add then remove must restore finalize()");
}

/// Only valid for commutative families: the multiset, not the order, decides `finalize()`.
pub fn assert_order_independent<A: WindowAccumulator>(contributions: &[A::Contribution]) {
	let mut forward = A::default();
	for c in contributions {
		forward.add(c);
	}
	let mut backward = A::default();
	for c in contributions.iter().rev() {
		backward.add(c);
	}
	assert_eq!(forward.finalize(), backward.finalize(), "finalize() must be order-independent");
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
pub struct VolumeAccumulator {
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
pub struct VolumeOut {
	pub group: String,
	pub window_start: u64,
	pub volume: f64,
}

row!(VolumeOut {
	group: String,
	window_start: u64,
	volume: f64
});

pub struct VolumeTumbling;

impl OperatorMetadata for VolumeTumbling {
	const NAME: &'static str = "operator_test_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: invertible volume sum";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for VolumeTumbling {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = VolumeAccumulator;
	type Output = VolumeOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, f64)> {
		let group = row.utf8("group")?.to_string();
		let size = row.f64("size")?;
		Some((group, size))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> VolumeAccumulator {
		VolumeAccumulator::default()
	}
}

impl Emit for VolumeTumbling {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: &OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			volume: value.get(),
		})
	}
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
pub struct MinAccumulator {
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
pub struct MinOut {
	pub group: String,
	pub window_start: u64,
	pub min: f64,
}

row!(MinOut {
	group: String,
	window_start: u64,
	min: f64
});

pub struct MinTumbling;

impl OperatorMetadata for MinTumbling {
	const NAME: &'static str = "operator_test_min";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: removal-safe min over a multiset";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for MinTumbling {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = MinAccumulator;
	type Output = MinOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, OrdF64)> {
		let group = row.utf8("group")?.to_string();
		let size = row.f64("size")?;
		Some((group, OrdF64::new(size)?))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> MinAccumulator {
		MinAccumulator::default()
	}
}

impl Emit for MinTumbling {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: &OrdF64) -> Option<MinOut> {
		Some(MinOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			min: value.get(),
		})
	}
}

/// Open/high/low/close over a bounded-lateness window. The slot drives aging, so events more
/// than `OHLCV_IMMUTABLE` behind the window high-water mark into the O(1) scalar.
#[reifydb_macro::operator_state]
#[derive(Clone, Debug, HeapSize)]
pub struct OhlcvAcc {
	high: SealingMax<DateTime, OrdF64>,
	low: SealingMin<DateTime, OrdF64>,
	ends: SealingEndpoint<DateTime, OrdF64>,
}

impl Default for OhlcvAcc {
	fn default() -> Self {
		Self {
			high: SealingMax::immutable(millis(OHLCV_IMMUTABLE)),
			low: SealingMin::immutable(millis(OHLCV_IMMUTABLE)),
			ends: SealingEndpoint::immutable(millis(OHLCV_IMMUTABLE)),
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct OhlcvValue {
	pub open: OrdF64,
	pub high: OrdF64,
	pub low: OrdF64,
	pub close: OrdF64,
}

impl WindowAccumulator for OhlcvAcc {
	type Contribution = (DateTime, OrdF64);
	type Output = OhlcvValue;

	fn add(&mut self, contribution: &(DateTime, OrdF64)) {
		self.high.add(contribution);
		self.low.add(contribution);
		self.ends.add(contribution);
	}

	fn remove(&mut self, contribution: &(DateTime, OrdF64)) {
		self.high.remove(contribution);
		self.low.remove(contribution);
		self.ends.remove(contribution);
	}

	fn finalize(&self) -> Option<OhlcvValue> {
		let high = self.high.finalize()?;
		let low = self.low.finalize()?;
		let (open, close) = self.ends.finalize()?;
		Some(OhlcvValue {
			open,
			high,
			low,
			close,
		})
	}

	fn is_empty(&self) -> bool {
		self.ends.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct OhlcvOut {
	pub group: String,
	pub window_start: u64,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
}

row!(OhlcvOut {
	group: String,
	window_start: u64,
	open: f64,
	high: f64,
	low: f64,
	close: f64
});

pub struct OhlcvSealingTumbling;

impl OperatorMetadata for OhlcvSealingTumbling {
	const NAME: &'static str = "operator_test_ohlcv_sealing";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: sealing OHLCV with bounded lateness";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for OhlcvSealingTumbling {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = OhlcvAcc;
	type Output = OhlcvOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		row: &impl RowView,
	) -> Option<(String, (DateTime, OrdF64))> {
		let group = row.utf8("group")?.to_string();
		let slot = row.u64("slot")?;
		let price = OrdF64::new(row.f64("price")?)?;
		Some((group, (at_millis(slot), price)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> OhlcvAcc {
		OhlcvAcc::default()
	}
}

impl Emit for OhlcvSealingTumbling {
	type Kinds = NoRolling;

	fn build_output(&self, group: &String, span: WindowSpan<DateTime>, value: &OhlcvValue) -> Option<OhlcvOut> {
		Some(OhlcvOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			open: value.open.get(),
			high: value.high.get(),
			low: value.low.get(),
			close: value.close.get(),
		})
	}
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, Default, HeapSize)]
pub struct WindowSum {
	moments: Moments,
	folded: u32,
}

impl WindowSum {
	fn panes(&self) -> u32 {
		// Without the folded count a merged accumulator would report one window whatever it holds.
		if self.folded > 0 {
			self.folded
		} else {
			u32::from(!self.moments.is_empty())
		}
	}
}

impl WindowAccumulator for WindowSum {
	type Contribution = f64;
	type Output = (f64, u32);

	fn add(&mut self, contribution: &f64) {
		self.moments.add(*contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		self.moments.remove(*contribution);
	}

	fn finalize(&self) -> Option<(f64, u32)> {
		(!self.moments.is_empty()).then(|| (self.moments.sum(), self.panes()))
	}

	fn is_empty(&self) -> bool {
		self.moments.is_empty()
	}
}

impl MergeAccumulator for WindowSum {
	fn merge(&mut self, other: &Self) {
		self.folded = self.panes() + other.panes();
		self.moments.merge(&other.moments);
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct RollingOut {
	pub group: String,
	pub rolling_sum: f64,
	pub windows: u32,
}

row!(RollingOut {
	group: String,
	rolling_sum: f64,
	windows: u32
});

pub struct RollingSum;

impl OperatorMetadata for RollingSum {
	const NAME: &'static str = "operator_test_rolling_sum";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: rolling sum over last N windows";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for RollingSum {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = WindowSum;
	type Output = RollingOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, f64)> {
		let group = row.utf8("group")?.to_string();
		let value = row.f64("value")?;
		Some((group, value))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> WindowSum {
		WindowSum::default()
	}
}

impl Emit for RollingSum {
	type Kinds = AllKinds;

	fn build_output(&self, group: &String, _span: WindowSpan<DateTime>, value: &(f64, u32)) -> Option<RollingOut> {
		Some(RollingOut {
			group: group.clone(),
			rolling_sum: value.0,
			windows: value.1,
		})
	}
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
pub struct TopOut {
	pub group: String,
	pub rank: u32,
	pub trader: u64,
	pub volume: f64,
}

row!(TopOut {
	group: String,
	rank: u32,
	trader: u64,
	volume: f64
});

pub struct TopVolumeRollingTopK;

impl OperatorMetadata for TopVolumeRollingTopK {
	const NAME: &'static str = "operator_test_top_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: rolling top-2 volume by trader";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TopVolumeRollingTopK {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
	type Output = BTreeMap<u32, TopOut>;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let trader = row.u64("trader")?;
		let volume = row.f64("volume")?;
		Some((group, (trader, volume)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> KeyedInvertibleAccumulator<u64, Moments> {
		KeyedInvertibleAccumulator::default()
	}
}

impl Emit for TopVolumeRollingTopK {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, Moments>,
	) -> Option<BTreeMap<u32, TopOut>> {
		let mut ranked: Vec<(u64, f64)> =
			value.iter().map(|(trader, moments)| (*trader, moments.sum())).collect();
		ranked.sort_by(|a, b| {
			b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.0.cmp(&b.0))
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
		Some(out)
	}
}

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
pub struct CarryOut {
	pub group: String,
	pub window_start: u64,
	pub sum: f64,
	pub carry_in: f64,
	pub has_carry: bool,
}

row!(CarryOut {
	group: String,
	window_start: u64,
	sum: f64,
	carry_in: f64,
	has_carry: bool
});

pub struct TwapCarry;

impl OperatorMetadata for TwapCarry {
	const NAME: &'static str = "operator_test_carry";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: tumbling carry-forward";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TwapCarry {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(TwapCarry)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let ts = row.u64("ts")?;
		let price = row.f64("price")?;
		Some((group, (ts, price)))
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl CarryEmit for TwapCarry {
	type Carry = f64;

	fn build_output(
		&self,
		group: &String,
		span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
		prev_carry: Option<&f64>,
	) -> Option<CarryOut> {
		(!value.is_empty()).then(|| CarryOut {
			group: group.clone(),
			window_start: span.start.to_order(),
			sum: value.values().sum(),
			carry_in: prev_carry.copied().unwrap_or(0.0),
			has_carry: prev_carry.is_some(),
		})
	}

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, _prev_carry: Option<&f64>) -> Option<f64> {
		value.last_key_value().map(|(_, v)| *v)
	}
}

pub fn tumbling_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("slot", ValueType::Uint8),
			RowShapeField::unconstrained("size", ValueType::Float8),
		],
	)
}

pub fn ohlcv_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("slot", ValueType::Uint8),
			RowShapeField::unconstrained("price", ValueType::Float8),
		],
	)
}

pub fn rolling_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("ts", ValueType::Uint8),
			RowShapeField::unconstrained("value", ValueType::Float8),
		],
	)
}

pub fn rolling_top_k_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("ts", ValueType::Uint8),
			RowShapeField::unconstrained("trader", ValueType::Uint8),
			RowShapeField::unconstrained("volume", ValueType::Float8),
		],
	)
}

pub fn carry_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("ts", ValueType::Uint8),
			RowShapeField::unconstrained("price", ValueType::Float8),
		],
	)
}

fn field(name: &str, ty: ValueType) -> RowShapeField {
	RowShapeField::unconstrained(name, ty)
}

/// Output shapes only need to carry the `output_key` columns; the harness materializes the
/// operator's real emitted columns. The rest is spelled out here for the reader.
pub fn volume_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("window_start", ValueType::Uint8),
			field("volume", ValueType::Float8),
		],
	)
}

pub fn min_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("window_start", ValueType::Uint8),
			field("min", ValueType::Float8),
		],
	)
}

pub fn ohlcv_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("window_start", ValueType::Uint8),
			field("open", ValueType::Float8),
			field("high", ValueType::Float8),
			field("low", ValueType::Float8),
			field("close", ValueType::Float8),
		],
	)
}

pub fn rolling_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("rolling_sum", ValueType::Float8),
			field("windows", ValueType::Uint4),
		],
	)
}

pub fn top_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("rank", ValueType::Uint4),
			field("trader", ValueType::Uint8),
			field("volume", ValueType::Float8),
		],
	)
}

pub fn carry_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("window_start", ValueType::Uint8),
			field("sum", ValueType::Float8),
			field("carry_in", ValueType::Float8),
			field("has_carry", ValueType::Boolean),
		],
	)
}

pub fn velocity_out_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			field("group", ValueType::Utf8),
			field("recent", ValueType::Float8),
			field("baseline", ValueType::Float8),
			field("windows", ValueType::Uint4),
		],
	)
}
