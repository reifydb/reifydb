// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::HeapSize,
	operator_with::ApplyWith,
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
			operator::{Emit, NoRolling, WindowedOperator},
			plain::PlainDriver,
			retained::RetainedDriver,
		},
	},
	row,
};
use reifydb_testing_chaos::operator::event::Row as CoreRow;
use reifydb_testing_sdk::{
	builders::{TestChangeBuilder, TestOperatorRowBuilder},
	in_process::harness::InProcessOperatorHarnessBuilder,
};
use reifydb_value::{
	config::ExtensionParams,
	factory::time::millis,
	value::{
		Value,
		column_view::ColumnView,
		datetime::DateTime,
		diff_type::DiffType,
		row_number::RowNumber,
		system_columns::{row_numbers, user_columns},
		value_type::ValueType,
	},
};

#[reifydb_macro::operator_state]
#[derive(Clone, Debug, PartialEq, HeapSize)]
struct RetainedOut {
	group: String,
	count: u64,
	volume: f64,
}

row!(RetainedOut {
	group: String,
	count: u64,
	volume: f64
});

struct TestRetained;

impl OperatorMetadata for TestRetained {
	const NAME: &'static str = "test_retained";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestRetained {
	type Coord = DateTime;
	type GroupKey = String;
	type Accumulator = RetainedAccumulator<u64, i64>;
	type Output = RetainedOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<DateTime>>> {
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

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, i64))>>> {
		rows.iter()
			.map(|&i| {
				cols.row(i)
					.as_ref()
					.map(|row| -> Result<Option<(String, (u64, i64))>> {
						let (Some(group), Some(key), Some(value)) =
							(row.utf8("group")?, row.u64("key")?, row.i64("value")?)
						else {
							return Ok(None);
						};
						Ok(Some((group.to_string(), (key, value))))
					})
					.transpose()
					.map(Option::flatten)
			})
			.collect()
	}

	fn new_accumulator(&self, _settings: &WindowSettings<DateTime>) -> RetainedAccumulator<u64, i64> {
		RetainedAccumulator::default()
	}
}

impl Emit for TestRetained {
	type Kinds = NoRolling;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<DateTime>,
		value: &BTreeMap<u64, i64>,
	) -> Option<RetainedOut> {
		Some(RetainedOut {
			group: group.clone(),
			count: value.len() as u64,
			volume: value.values().sum::<i64>() as f64,
		})
	}
}

type Retained = RetainedDriver<TestRetained, u64, i64>;
type Plain = PlainDriver<TestRetained>;

fn input_row(rn: u64, group: &str, key: u64, at: u64, value: i64) -> CoreRow {
	TestOperatorRowBuilder::new(rn)
		.with_values(vec![Value::Utf8(group.into()), Value::Uint8(key), Value::Int8(value)])
		.with_fields(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("key", ValueType::Uint8),
			RowShapeField::unconstrained("value", ValueType::Int8),
		])
		.with_time(DateTime::from_millis(at as i64))
		.build()
}

fn with() -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(millis(60_000)),
		}),
		lateness: None,
		immutable: None,
		retention: None,
	}
}
type Seen = Vec<(DiffType, Vec<(RowNumber, Vec<Value>)>, Vec<(RowNumber, Vec<Value>)>)>;

fn seen(out: &Change) -> Seen {
	let rows = |columns: Option<&RecordBatch>| {
		columns.map_or_else(Vec::new, |columns| {
			let numbers = row_numbers(columns).unwrap();
			(0..columns.num_rows())
				.map(|i| {
					let values = user_columns(columns)
						.map(|(field, array)| {
							ColumnView::try_from((array, field.as_ref()))
								.unwrap()
								.get_value(i)
						})
						.collect();
					(numbers[i], values)
				})
				.collect()
		})
	};
	out.diffs.iter().map(|d| (d.kind(), rows(d.pre()), rows(d.post()))).collect()
}

#[test]
fn parity_with_plain_driver() {
	// The retained driver replaces the plain one for retained operators, so any drift is a wrong row downstream.
	let mut plain = InProcessOperatorHarnessBuilder::<Plain>::new().with(with()).build().expect("harness");
	let mut retained = InProcessOperatorHarnessBuilder::<Retained>::new().with(with()).build().expect("harness");
	let batches: Vec<Change> = vec![
		TestChangeBuilder::new()
			.insert(input_row(1, "BTC", 1, 0, 10))
			.insert(input_row(2, "ETH", 1, 1_000, 7))
			.build(),
		TestChangeBuilder::new().insert(input_row(3, "BTC", 2, 2_000, 5)).build(),
		TestChangeBuilder::new().update(input_row(1, "BTC", 1, 0, 10), input_row(1, "BTC", 1, 0, 12)).build(),
		TestChangeBuilder::new().insert(input_row(4, "BTC", 1, 3_000, 99)).build(),
		TestChangeBuilder::new().remove(input_row(1, "BTC", 1, 0, 12)).build(),
		TestChangeBuilder::new()
			.remove(input_row(4, "BTC", 1, 3_000, 99))
			.remove(input_row(3, "BTC", 2, 2_000, 5))
			.build(),
		TestChangeBuilder::new().insert(input_row(5, "BTC", 3, 4_000, 1)).build(),
	];
	for (step, batch) in batches.into_iter().enumerate() {
		let theirs = plain.apply(batch.clone()).expect("plain apply");
		let ours = retained.apply(batch).expect("retained apply");
		assert_eq!(seen(&ours), seen(&theirs), "step {step}");
	}
	plain.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	retained.advance_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let close = TestChangeBuilder::new().insert(input_row(6, "ETH", 1, 120_000, 1)).build();
	let theirs = plain.apply(close.clone()).expect("plain apply");
	let ours = retained.apply(close).expect("retained apply");
	assert_eq!(seen(&ours), seen(&theirs), "close");
}
