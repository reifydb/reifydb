// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared fixtures, shapes, samplers and config matrices for the windowed
//! V2 operator differential-chaos suite.
//!
//! Each fixture implements both the public operator trait (so the matching
//! accumulator oracle can simulate it) and the registration trait (so the
//! generic driver can be wrapped in `FFIOperatorAdapter` and run through the
//! `ChaosHarness`). The fixtures intentionally mirror the in-crate unit-test
//! fixtures so the chaos suite exercises the same code paths the unit tests
//! pin down, but across thousands of randomized Insert/Update/Remove
//! sequences instead of a handful of hand-built scenarios.

#![allow(dead_code)]

use std::collections::BTreeMap;

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_codec::{
	encoded::shape::{RowShape, RowShapeField},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	metrics::heap::HeapSize,
	window::{
		accumulator::{
			WindowAccumulator,
			invertible::{
				KeyedInvertibleAccumulator, LastValue, Moments, Multiset, OrdF64, RetainedAccumulator,
			},
			sealing::{SealingEndpoint, SealingMax, SealingMin},
		},
		span::WindowSpan,
	},
};
use reifydb_sdk::{
	config::Config,
	error::Result,
	operator::{
		column::operator::OperatorColumn,
		context::OperatorContext,
		view::RowView,
		windowed::{
			multi_rolling::{MultiRollingOperator, MultiRollingRegistration},
			rolling::{RollingOperator, RollingRegistration},
			rolling_incremental::RollingIncrementalOperator,
			tumbling::{TumblingOperator, TumblingRegistration},
			tumbling_carry::{TumblingCarryOperator, TumblingCarryRegistration},
		},
	},
	row,
	testing::chaos::{
		config::{BatchSizeDist, ChaosConfig, SupportedOps},
		strategy::{ColumnSampler, samplers},
	},
};
use reifydb_value::value::{Value, value_type::ValueType};
use serde::{Deserialize, Serialize};

/// Window duration shared by every tumbling-grid fixture.
pub const WINDOW: u64 = 60;
/// Lateness bound for the sealing OHLCV fixture (< WINDOW so aging is reachable
/// within a single window).
pub const OHLCV_GRACE: u64 = 20;
/// Rolling-buffer capacity shared by the rolling/incremental fixtures.
pub const ROLLING_CAPACITY: usize = 3;

/// Seeds replayed for every config so a failure names a reproducible run.
pub const SEEDS: [u64; 6] = [1, 7, 42, 99, 12_345, 2_024];

/// One event per Change. Forces the operator to snapshot per single diff;
/// the cleanest setting for boundary/high-water reasoning.
pub fn baseline(num_ops: usize, ops: SupportedOps) -> ChaosConfig {
	ChaosConfig {
		num_ops,
		max_live_rows: 40,
		duplicate_update_burst: 0.0,
		update_as_remove_insert: 0.0,
		batch_size: BatchSizeDist::Constant(1),
		supported_ops: ops,
	}
}

/// Multi-event batches plus the two adversarial primitives: 60% of Updates
/// spawn a no-op duplicate Update, 40% are rewritten as Remove+Insert. This
/// is the configuration that reproduces the double-count-on-Update bug class.
pub fn full_chaos(num_ops: usize) -> ChaosConfig {
	ChaosConfig {
		num_ops,
		max_live_rows: 30,
		duplicate_update_burst: 0.6,
		update_as_remove_insert: 0.4,
		batch_size: BatchSizeDist::Geometric(0.4),
		supported_ops: SupportedOps::all(),
	}
}

/// A Float8 sampler that returns `none` ~1/4 of the time. Rows whose measured
/// column is `none` must be skipped identically by operator and oracle (their
/// shared `extract` returns `None`).
pub fn maybe_none_f64(lo: f64, hi: f64) -> ColumnSampler {
	samplers::select(&[
		Value::float8(lo),
		Value::float8((lo + hi) / 2.0),
		Value::float8(hi),
		Value::none_of(ValueType::Float8),
	])
}

/// Applying a contribution then removing it must leave `finalize()` exactly
/// where it started. The probe's identity must be absent from `initial`.
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

/// For commutative families the multiset of contributions, not their order,
/// determines `finalize()`.
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, HeapSize)]
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

impl TumblingOperator for VolumeTumbling {
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
		WindowSpan::for_slot(coord, WINDOW)
	}

	fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<VolumeOut> {
		Some(VolumeOut {
			group: group.clone(),
			window_start: span.start,
			volume: value.get(),
		})
	}
}

impl TumblingRegistration for VolumeTumbling {
	const NAME: &'static str = "operator_test_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: invertible volume sum";
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, HeapSize)]
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

	fn remove_if_present(&mut self, contribution: &OrdF64) {
		self.values.remove_if_present(contribution);
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

impl TumblingOperator for MinTumbling {
	type GroupKey = String;
	type WindowCoord = u64;
	type Accumulator = MinAccumulator;
	type Output = MinOut;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, OrdF64)> {
		let group = row.utf8("group")?.to_string();
		let slot = row.u64("slot")?;
		let size = row.f64("size")?;
		Some((group, slot, OrdF64::new(size)?))
	}

	fn window_for(&self, coord: u64) -> WindowSpan<u64> {
		WindowSpan::for_slot(coord, WINDOW)
	}

	fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<MinOut> {
		Some(MinOut {
			group: group.clone(),
			window_start: span.start,
			min: value.get(),
		})
	}
}

impl TumblingRegistration for MinTumbling {
	const NAME: &'static str = "operator_test_min";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: removal-safe min over a multiset";
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

/// Open/high/low/close over a bounded-lateness window. `high`/`low` use the
/// sealing extrema; `open`/`close` use the sealing endpoint. The within-window
/// coordinate (the slot) drives aging, so events more than `OHLCV_GRACE`
/// behind the window high-water seal into the O(1) scalar.
#[derive(Clone, Debug, Serialize, Deserialize, HeapSize)]
pub struct OhlcvAcc {
	high: SealingMax<u64, OrdF64>,
	low: SealingMin<u64, OrdF64>,
	ends: SealingEndpoint<u64, OrdF64>,
}

impl Default for OhlcvAcc {
	fn default() -> Self {
		Self {
			high: SealingMax::with_grace(OHLCV_GRACE),
			low: SealingMin::with_grace(OHLCV_GRACE),
			ends: SealingEndpoint::with_grace(OHLCV_GRACE),
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
	type Contribution = (u64, OrdF64);
	type Output = OhlcvValue;

	fn add(&mut self, contribution: &(u64, OrdF64)) {
		self.high.add(contribution);
		self.low.add(contribution);
		self.ends.add(contribution);
	}

	fn remove(&mut self, contribution: &(u64, OrdF64)) {
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

impl TumblingOperator for OhlcvSealingTumbling {
	type GroupKey = String;
	type WindowCoord = u64;
	type Accumulator = OhlcvAcc;
	type Output = OhlcvOut;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, (u64, OrdF64))> {
		let group = row.utf8("group")?.to_string();
		let slot = row.u64("slot")?;
		let price = OrdF64::new(row.f64("price")?)?;
		Some((group, slot, (slot, price)))
	}

	fn window_for(&self, coord: u64) -> WindowSpan<u64> {
		WindowSpan::for_slot(coord, WINDOW)
	}

	fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OhlcvValue) -> Option<OhlcvOut> {
		Some(OhlcvOut {
			group: group.clone(),
			window_start: span.start,
			open: value.open.get(),
			high: value.high.get(),
			low: value.low.get(),
			close: value.close.get(),
		})
	}
}

impl TumblingRegistration for OhlcvSealingTumbling {
	const NAME: &'static str = "operator_test_ohlcv_sealing";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: sealing OHLCV with bounded lateness";
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, HeapSize)]
pub struct WindowSum {
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

pub struct RollingSum {
	capacity: usize,
}

pub fn rolling_sum() -> RollingSum {
	RollingSum {
		capacity: ROLLING_CAPACITY,
	}
}

impl RollingOperator for RollingSum {
	type GroupKey = String;
	type WindowCoord = u64;
	type Accumulator = WindowSum;
	type Output = RollingOut;

	fn capacity(&self) -> usize {
		self.capacity
	}

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, f64)> {
		let group = row.utf8("group")?.to_string();
		let window_start = row.u64("window_start")?;
		let value = row.f64("value")?;
		Some((group, window_start, value))
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<u64, WindowSum>) -> Option<RollingOut> {
		if buffer.is_empty() {
			return None;
		}
		let rolling_sum = buffer.values().filter_map(|w| w.finalize()).sum();
		Some(RollingOut {
			group: group.clone(),
			rolling_sum,
			windows: buffer.len() as u32,
		})
	}
}

impl RollingRegistration for RollingSum {
	const NAME: &'static str = "operator_test_rolling_sum";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: rolling sum over last N windows";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(rolling_sum())
	}

	fn encode_row_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str(group).build()
	}
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, HeapSize)]
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

pub struct TopVolumeMultiRolling;

impl MultiRollingOperator for TopVolumeMultiRolling {
	type GroupKey = String;
	type WindowCoord = u64;
	type Accumulator = KeyedInvertibleAccumulator<u64, Moments>;
	type SecondaryKey = u32;
	type Output = TopOut;

	fn capacity(&self) -> usize {
		ROLLING_CAPACITY
	}

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, (u64, f64))> {
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
		out
	}
}

impl MultiRollingRegistration for TopVolumeMultiRolling {
	const NAME: &'static str = "operator_test_top_volume";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: rolling top-2 volume by trader";
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, HeapSize)]
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

pub struct TwapCarry {
	retention: Option<u64>,
}

pub fn twap_carry(retention: Option<u64>) -> TwapCarry {
	TwapCarry {
		retention,
	}
}

impl TumblingCarryOperator for TwapCarry {
	type GroupKey = String;
	type WindowCoord = u64;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;
	type Carry = f64;

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, (u64, f64))> {
		let group = row.utf8("group")?.to_string();
		let ts = row.u64("ts")?;
		let price = row.f64("price")?;
		Some((group, ts, (ts, price)))
	}

	fn window_for(&self, coord: u64) -> WindowSpan<u64> {
		WindowSpan::for_slot(coord, WINDOW)
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

	fn retention(&self) -> Option<u64> {
		self.retention
	}
}

impl TumblingCarryRegistration for TwapCarry {
	const NAME: &'static str = "operator_test_carry";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: tumbling carry-forward";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		Ok(TwapCarry {
			retention: config.u64("__retention"),
		})
	}

	fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window_start).build()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct VelocityOut {
	pub group: String,
	pub recent: f64,
	pub baseline: f64,
	pub windows: u32,
}

row!(VelocityOut {
	group: String,
	recent: f64,
	baseline: f64,
	windows: u32
});

pub struct VelocityIncremental {
	capacity: usize,
}

pub fn velocity_incremental() -> VelocityIncremental {
	VelocityIncremental {
		capacity: ROLLING_CAPACITY,
	}
}

impl RollingOperator for VelocityIncremental {
	type GroupKey = String;
	type WindowCoord = u64;
	type Accumulator = LastValue<f64>;
	type Output = VelocityOut;

	fn capacity(&self) -> usize {
		self.capacity
	}

	fn extract(&self, _ctx: &mut impl OperatorContext, row: &impl RowView) -> Option<(String, u64, f64)> {
		let group = row.utf8("group")?.to_string();
		let window_start = row.u64("window_start")?;
		let value = row.f64("value")?;
		Some((group, window_start, value))
	}

	fn combine(&self, group: &String, buffer: &BTreeMap<u64, LastValue<f64>>) -> Option<VelocityOut> {
		let (_, newest) = buffer.iter().next_back()?;
		let newest = *newest.get()?;
		let total = buffer.len();
		let mut sum = 0.0_f64;
		let mut count = 0u32;
		for (i, accumulator) in buffer.values().enumerate() {
			if i + 1 == total {
				continue;
			}
			if let Some(v) = accumulator.get() {
				sum += *v;
				count += 1;
			}
		}
		let baseline = if count > 0 {
			sum / count as f64
		} else {
			0.0
		};
		Some(VelocityOut {
			group: group.clone(),
			recent: newest,
			baseline,
			windows: total as u32,
		})
	}
}

impl RollingIncrementalOperator for VelocityIncremental {
	type Running = Moments;

	fn window_contribution(&self, window_value: &f64) -> f64 {
		*window_value
	}

	fn combine_running(
		&self,
		group: &String,
		running: &Moments,
		newest_value: &f64,
		_newest_coord: u64,
	) -> Option<VelocityOut> {
		let total_count = running.count();
		let baseline_count = total_count.saturating_sub(1);
		let baseline = if baseline_count > 0 {
			(running.sum() - *newest_value) / baseline_count as f64
		} else {
			0.0
		};
		Some(VelocityOut {
			group: group.clone(),
			recent: *newest_value,
			baseline,
			windows: total_count as u32,
		})
	}
}

impl RollingRegistration for VelocityIncremental {
	const NAME: &'static str = "operator_test_velocity";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "chaos fixture: rolling velocity via running moments";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

	fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(velocity_incremental())
	}

	fn encode_row_key(&self, group: &String) -> EncodedKey {
		EncodedKey::builder().str(group).build()
	}
}

/// `(group: Utf8, slot: Uint8, size: Float8)` - the tumbling volume/min input.
pub fn tumbling_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("slot", ValueType::Uint8),
		RowShapeField::unconstrained("size", ValueType::Float8),
	])
}

/// `(group: Utf8, slot: Uint8, price: Float8)` - the sealing OHLCV input.
pub fn ohlcv_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("slot", ValueType::Uint8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	])
}

/// `(group: Utf8, window_start: Uint8, value: Float8)` - rolling/incremental input.
pub fn rolling_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("window_start", ValueType::Uint8),
		RowShapeField::unconstrained("value", ValueType::Float8),
	])
}

/// `(group: Utf8, window_start: Uint8, trader: Uint8, volume: Float8)` - top-K input.
pub fn multi_rolling_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("window_start", ValueType::Uint8),
		RowShapeField::unconstrained("trader", ValueType::Uint8),
		RowShapeField::unconstrained("volume", ValueType::Float8),
	])
}

/// `(group: Utf8, ts: Uint8, price: Float8)` - carry-forward input.
pub fn carry_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("ts", ValueType::Uint8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	])
}

fn field(name: &str, ty: ValueType) -> RowShapeField {
	RowShapeField::unconstrained(name, ty)
}

/// Output shapes only need to name-carry the `output_key` columns; the harness
/// materializes the operator's real emitted columns. They are spelled out in
/// full here for documentation.
pub fn volume_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("window_start", ValueType::Uint8),
		field("volume", ValueType::Float8),
	])
}

pub fn min_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("window_start", ValueType::Uint8),
		field("min", ValueType::Float8),
	])
}

pub fn ohlcv_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("window_start", ValueType::Uint8),
		field("open", ValueType::Float8),
		field("high", ValueType::Float8),
		field("low", ValueType::Float8),
		field("close", ValueType::Float8),
	])
}

pub fn rolling_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("rolling_sum", ValueType::Float8),
		field("windows", ValueType::Uint4),
	])
}

pub fn top_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("rank", ValueType::Uint4),
		field("trader", ValueType::Uint8),
		field("volume", ValueType::Float8),
	])
}

pub fn carry_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("window_start", ValueType::Uint8),
		field("sum", ValueType::Float8),
		field("carry_in", ValueType::Float8),
		field("has_carry", ValueType::Boolean),
	])
}

pub fn velocity_out_shape() -> RowShape {
	RowShape::new(vec![
		field("group", ValueType::Utf8),
		field("recent", ValueType::Float8),
		field("baseline", ValueType::Float8),
		field("windows", ValueType::Uint4),
	])
}
