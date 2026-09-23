// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowRequirements, WindowSize, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
	row::Row as CoreRow,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::invertible::retained_map::RetainedAccumulator, settings::WindowSettings, span::WindowSpan,
	},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		extern_c::binding::operator::ExternCOperatorAdapter,
		view::RowView,
		windowed::{
			carry::CarryDriver,
			operator::{CarryEmit, WindowedOperator},
		},
	},
	row,
};
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	harness::ExternCOperatorHarnessBuilder,
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{Value, datetime::DateTime, value_type::ValueType},
};

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
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

struct Probe;

impl Probe {
	fn output(
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

	fn contribution(row: &impl RowView) -> Option<(String, (u64, f64))> {
		Some((row.utf8("group")?.to_string(), (row.u64("ts")?, row.f64("price")?)))
	}
}

impl OperatorMetadata for Probe {
	const NAME: &'static str = "probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for Probe {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = CarryOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, row: &impl RowView) -> Option<DateTime> {
		row.row_time()
	}

	fn extract(&self, _ctx: &mut impl GuestContext<Windowed>, row: &impl RowView) -> Option<(String, (u64, f64))> {
		Probe::contribution(row)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl CarryEmit for Probe {
	type Carry = f64;

	fn build_output(
		&self,
		group: &String,
		span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
		prev: Option<&f64>,
	) -> Option<CarryOut> {
		Probe::output(group, span, value, prev)
	}

	fn carry_forward(&self, value: &BTreeMap<u64, f64>, _prev: Option<&f64>) -> Option<f64> {
		value.last_key_value().map(|(_, v)| *v)
	}
}

fn input_fields() -> Vec<RowShapeField> {
	vec![
		RowShapeField::unconstrained("group", ValueType::Utf8),
		RowShapeField::unconstrained("ts", ValueType::Uint8),
		RowShapeField::unconstrained("price", ValueType::Float8),
	]
}

fn input_row(rn: u64, group: &str, ts: u64, price: f64) -> CoreRow {
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(ts), Value::float8(price)])
		.with_fields(input_fields())
		.with_time(DateTime::from_millis(ts))
		.build()
}

fn window_with(immutable: Option<i64>) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60)),
		}),
		lateness: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: immutable.map(|n| WithSpan::Duration(millis(n as u64))),
		retention: None,
		throttle: None,
	}
}

macro_rules! feed_windows {
	($driver:ty, $with:expr, $windows:expr) => {{
		let mut h = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<$driver>>::new()
			.with($with)
			.build()
			.expect("harness");
		for i in 0..$windows as u64 {
			h.apply(TestChangeBuilder::new().insert(input_row(i + 1, "BTC", i * 60, i as f64)).build())
				.expect("apply");
		}
		h
	}};
}

#[test]
fn a_carry_window_folds_past_immutable_and_drops_its_accumulator_rows() {
	// With immutable set, windows more than immutable behind a group's newest fold to the carry scalar and
	// free their accumulator rows, so the store stays bounded however many windows the group crosses.
	let short = feed_windows!(CarryDriver<Probe>, window_with(Some(120)), 20).snapshot_state().len();
	let long = feed_windows!(CarryDriver<Probe>, window_with(Some(120)), 40).snapshot_state().len();

	assert!(
		long <= short + 2,
		"twice the windows must not mean more state under immutable, but 20 windows left {short} rows and 40 left {long}"
	);
	assert!(long < 20, "40 windows folded past a 2-window immutable must leave few rows, but left {long}");
}

#[test]
fn a_carry_window_with_no_immutable_keeps_every_accumulator() {
	// Without immutable the fold must not run: every window's accumulator row stays, so state grows with the
	// windows crossed. A default retention would fold here and hide behind a bounded store.
	let short = feed_windows!(CarryDriver<Probe>, window_with(None), 20).snapshot_state().len();
	let long = feed_windows!(CarryDriver<Probe>, window_with(None), 40).snapshot_state().len();

	assert!(
		long >= short + 20,
		"20 more windows must add at least 20 rows without immutable, but 20 windows left {short} rows and 40 left {long}"
	);
}

#[test]
fn the_carry_driver_refuses_a_window_that_is_not_tumbling() {
	// This driver has no rolling engine; accepting a rolling window would run tumbling buckets under a rolling
	// declaration.
	let with = ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(3)),
			lag: None,
			pane: None,
		}),
		lateness: None,
		immutable: None,
		retention: None,
		throttle: None,
	};

	let Err(err) =
		ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<CarryDriver<Probe>>>::new().with(with).build()
	else {
		panic!("create must refuse an unsupported window kind");
	};

	assert!(err.to_string().contains("FLOW_066"), "expected FLOW_066, got: {err}");
}

#[test]
fn a_seal_frees_every_window_of_a_stopped_carry_group_that_declared_no_immutable() {
	// Without immutable nothing folds while the feed runs, so the seal is the only thing that can free the
	// windows; state left behind must not depend on how many windows the group crossed.
	let mut short = feed_windows!(CarryDriver<Probe>, window_with(None), 5);
	let mut long = feed_windows!(CarryDriver<Probe>, window_with(None), 40);
	let before = long.snapshot_state().len();

	short.advance_watermark(DateTime::from_millis(10_000_000)).expect("advance watermark");
	long.advance_watermark(DateTime::from_millis(10_000_000)).expect("advance watermark");

	let (short_after, long_after) = (short.snapshot_state().len(), long.snapshot_state().len());
	assert_eq!(long_after, short_after, "a sealed group must leave the same state however many windows it crossed");
	assert!(long_after * 10 < before, "the seal must free the windows: {before} rows before, {long_after} after");
}

#[test]
fn a_carry_operator_publishes_tumbling_only() {
	// a carry operator that published rolling would admit views it cannot run
	assert_eq!(
		<CarryDriver<Probe> as MountedOperator>::WINDOW,
		WindowRequirements {
			takes_window: true,
			kinds: &["tumbling"],
			domain: WindowSizeDomain::Time,
			needs_pane: false,
			throttles: false,
		}
	);
}
