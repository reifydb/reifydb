// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	common::{OnEmpty, WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::{ApplyWith, WithSpan},
};
use reifydb_flow_async::window::{
	accumulator::invertible::entry_map::EntryMapAccumulator, coord::ByDateTime, settings::WindowSettings,
	span::WindowSpan,
};
use reifydb_sdk::{
	error::SdkError,
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
use reifydb_value::{config::ExtensionParams, factory::time::millis};

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

fn coords(cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<ByDateTime>>, SdkError> {
	rows.iter()
		.map(|&i| {
			cols.row(i)
				.as_ref()
				.map(|row| -> Result<Option<ByDateTime>, SdkError> {
					Ok(row.row_time().map(ByDateTime))
				})
				.transpose()
				.map(Option::flatten)
		})
		.collect()
}

fn extracts(cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<(String, (u64, f64))>>, SdkError> {
	rows.iter()
		.map(|&i| {
			cols.row(i)
				.as_ref()
				.map(|row| -> Result<Option<(String, (u64, f64))>, SdkError> {
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

struct EntryMapPlain;

impl OperatorMetadata for EntryMapPlain {
	const NAME: &'static str = "entry_map_plain";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for EntryMapPlain {
	type Coord = ByDateTime;
	type GroupKey = String;
	type Accumulator = EntryMapAccumulator<u64, f64>;
	type Output = Latest;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<ByDateTime>>, SdkError> {
		coords(cols, rows)
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, f64))>>, SdkError> {
		extracts(cols, rows)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<ByDateTime>) -> EntryMapAccumulator<u64, f64> {
		EntryMapAccumulator::default()
	}
}

impl Emit for EntryMapPlain {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<ByDateTime>,
		value: &BTreeMap<u64, f64>,
	) -> Option<Latest> {
		value.last_key_value().map(|(&key, &price)| Latest {
			group: group.clone(),
			key,
			price,
		})
	}
}

struct EntryMapTopK;

impl OperatorMetadata for EntryMapTopK {
	const NAME: &'static str = "entry_map_top_k";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for EntryMapTopK {
	type Coord = ByDateTime;
	type GroupKey = String;
	type Accumulator = EntryMapAccumulator<u64, f64>;
	type Output = BTreeMap<u64, Latest>;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<ByDateTime>>, SdkError> {
		coords(cols, rows)
	}

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, f64))>>, SdkError> {
		extracts(cols, rows)
	}

	fn new_accumulator(&self, _settings: &WindowSettings<ByDateTime>) -> EntryMapAccumulator<u64, f64> {
		EntryMapAccumulator::default()
	}
}

impl Emit for EntryMapTopK {
	type Kinds = AllKinds;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<ByDateTime>,
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
			on_empty: OnEmpty::Remove,
		}),
		drop: Some(WithSpan::Duration(millis(3_600_000))),
		immutable: None,
		forget: None,
	}
}

fn tumbling_with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(3)),
		}),
		drop: None,
		immutable: None,
		forget: None,
	}
}

#[test]
fn a_plain_rolling_window_over_entry_map_partials_reports_flow_092() {
	// entry map partials keep every sample, so create must refuse them before any group leaks
	let Err(err) =
		InProcessOperatorHarnessBuilder::<PlainDriver<EntryMapPlain>>::new().with(rolling_with()).build()
	else {
		panic!("create must refuse a rolling window whose partials never compact");
	};
	assert!(err.to_string().contains("FLOW_092"), "expected FLOW_092, got: {err}");
}

#[test]
fn a_top_k_rolling_window_over_entry_map_partials_reports_flow_092() {
	// entry map partials keep every sample, so create must refuse them before any group leaks
	let Err(err) = InProcessOperatorHarnessBuilder::<TopKDriver<EntryMapTopK>>::new().with(rolling_with()).build()
	else {
		panic!("create must refuse a rolling window whose partials never compact");
	};
	assert!(err.to_string().contains("FLOW_092"), "expected FLOW_092, got: {err}");
}

#[test]
fn a_tumbling_window_over_entry_map_partials_is_accepted() {
	// a tumbling window never merges partials, so the rolling refusal must never reach it
	InProcessOperatorHarnessBuilder::<PlainDriver<EntryMapPlain>>::new()
		.with(tumbling_with())
		.build()
		.expect("a tumbling window over entry map partials must be accepted");
}
