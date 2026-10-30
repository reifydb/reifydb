// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
};
use reifydb_flow_async::window::{
	accumulator::invertible::retained_map::RetainedAccumulator, settings::WindowSettings, span::WindowSpan,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Windowed},
		view::{ColumnsView, RowView},
		windowed::{
			operator::{AllKinds, Emit, WindowedOperator},
			plain::PlainDriver,
			top_k::TopKDriver,
		},
	},
	row,
};
use reifydb_testing_sdk::in_process::harness::InProcessOperatorHarnessBuilder;
use reifydb_value::{config::ExtensionParams, factory::time::millis, value::datetime::DateTime};

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct Latest {
	group: String,
	key: u64,
	price: f64,
}

row!(Latest {
	group: String,
	key: u64,
	price: f64
});

fn coords(cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTime>>> {
	rows.iter()
		.map(|&i| {
			cols.row(i)
				.as_ref()
				.map(|row| -> Result<Option<DateTime>> { Ok(row.row_time()) })
				.transpose()
				.map(Option::flatten)
		})
		.collect()
}

fn extracts(cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<(String, (u64, f64))>>> {
	rows.iter()
		.map(|&i| {
			cols.row(i)
				.as_ref()
				.map(|row| -> Result<Option<(String, (u64, f64))>> {
					let (Some(group), Some(key), Some(price)) =
						(row.utf8("group")?, row.u64("key")?, row.f64("price")?)
					else {
						return Ok(None);
					};
					Ok(Some((group.to_string(), (key, price))))
				})
				.transpose()
				.map(Option::flatten)
		})
		.collect()
}

struct RetainedPlain;

impl OperatorMetadata for RetainedPlain {
	const NAME: &'static str = "retained_plain";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for RetainedPlain {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = Latest;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTime>>> {
		coords(cols, rows)
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, f64))>>> {
		extracts(cols, rows)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl Emit for RetainedPlain {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
	) -> Option<Latest> {
		value.last_key_value().map(|(&key, &price)| Latest {
			group: group.clone(),
			key,
			price,
		})
	}
}

struct RetainedTopK;

impl OperatorMetadata for RetainedTopK {
	const NAME: &'static str = "retained_top_k";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for RetainedTopK {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, f64>;
	type Output = BTreeMap<u64, Latest>;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTime>>> {
		coords(cols, rows)
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, f64))>>> {
		extracts(cols, rows)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, f64> {
		RetainedAccumulator::default()
	}
}

impl Emit for RetainedTopK {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, f64>,
	) -> Option<BTreeMap<u64, Latest>> {
		Some(value
			.iter()
			.map(|(&key, &price)| {
				(
					key,
					Latest {
						group: group.clone(),
						key,
						price,
					},
				)
			})
			.collect())
	}
}

fn rolling_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Rolling {
			size: WindowSize::Duration(millis(3)),
			lag: None,
			pane: Some(millis(1)),
		}),
		lateness: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: None,
		retention: None,
	}
}

fn tumbling_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(3)),
		}),
		lateness: None,
		immutable: None,
		retention: None,
	}
}

#[test]
fn a_plain_rolling_window_over_retained_partials_reports_flow_092() {
	// retained partials keep every sample, so create must refuse them before any group leaks
	let Err(err) =
		InProcessOperatorHarnessBuilder::<PlainDriver<RetainedPlain>>::new().with(rolling_with()).build()
	else {
		panic!("create must refuse a rolling window whose partials never compact");
	};
	assert!(err.to_string().contains("FLOW_092"), "expected FLOW_092, got: {err}");
}

#[test]
fn a_top_k_rolling_window_over_retained_partials_reports_flow_092() {
	// retained partials keep every sample, so create must refuse them before any group leaks
	let Err(err) = InProcessOperatorHarnessBuilder::<TopKDriver<RetainedTopK>>::new().with(rolling_with()).build()
	else {
		panic!("create must refuse a rolling window whose partials never compact");
	};
	assert!(err.to_string().contains("FLOW_092"), "expected FLOW_092, got: {err}");
}

#[test]
fn a_tumbling_window_over_retained_partials_is_accepted() {
	// a tumbling window never merges partials, so the rolling refusal must never reach it
	InProcessOperatorHarnessBuilder::<PlainDriver<RetainedPlain>>::new()
		.with(tumbling_with())
		.build()
		.expect("a tumbling window over retained partials must be accepted");
}
