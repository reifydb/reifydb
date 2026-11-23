// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
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
			entry_map::EntryMapDriver,
			operator::{Emit, NoRolling, WindowedOperator},
			plain::PlainDriver,
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
struct EntryMapOut {
	group: String,
	count: u64,
	volume: f64,
}

row!(EntryMapOut {
	group: String,
	count: u64,
	volume: f64
});

struct TestEntryMap;

impl OperatorMetadata for TestEntryMap {
	const NAME: &'static str = "test_entry_map";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test fixture";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl WindowedOperator for TestEntryMap {
	type Coord = ByDateTime;
	type GroupKey = String;
	type Accumulator = EntryMapAccumulator<u64, i64>;
	type Output = EntryMapOut;

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(Self)
	}

	fn coord(&self, cols: &impl ColumnsView, rows: &[usize]) -> Result<Vec<Option<ByDateTime>>, SdkError> {
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

	fn extract(
		&self,
		_ctx: &mut impl GuestContext<Windowed>,
		cols: &impl ColumnsView,
		rows: &[usize],
	) -> Result<Vec<Option<(String, (u64, i64))>>, SdkError> {
		rows.iter()
			.map(|&i| {
				cols.row(i)
					.as_ref()
					.map(|row| -> Result<Option<(String, (u64, i64))>, SdkError> {
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

	fn new_accumulator(&self, _settings: &WindowSettings<ByDateTime>) -> EntryMapAccumulator<u64, i64> {
		EntryMapAccumulator::default()
	}
}

impl Emit for TestEntryMap {
	type Kinds = NoRolling;

	fn build_output(
		&self,
		group: &String,
		_span: WindowSpan<ByDateTime>,
		value: &BTreeMap<u64, i64>,
	) -> Option<EntryMapOut> {
		Some(EntryMapOut {
			group: group.clone(),
			count: value.len() as u64,
			volume: value.values().sum::<i64>() as f64,
		})
	}
}

type EntryMap = EntryMapDriver<TestEntryMap, u64, i64>;
type Plain = PlainDriver<TestEntryMap>;

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
		drop: None,
		immutable: None,
		forget: None,
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
	// The entry map driver replaces the plain one for entry map operators, so any drift is a wrong row downstream.
	let mut plain = InProcessOperatorHarnessBuilder::<Plain>::new().with(with()).build().expect("harness");
	let mut entry_map = InProcessOperatorHarnessBuilder::<EntryMap>::new().with(with()).build().expect("harness");
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
		let ours = entry_map.apply(batch).expect("entry map apply");
		assert_eq!(seen(&ours), seen(&theirs), "step {step}");
	}
	plain.advance_flow_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	entry_map.advance_flow_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	let close = TestChangeBuilder::new().insert(input_row(6, "ETH", 1, 120_000, 1)).build();
	let theirs = plain.apply(close.clone()).expect("plain apply");
	let ours = entry_map.apply(close).expect("entry map apply");
	assert_eq!(seen(&ours), seen(&theirs), "close");
}

#[test]
fn a_late_insert_into_a_sealed_entry_map_window_is_counted_in_the_sample() {
	// A dropped row the sample does not count never reaches system metrics, so late input stays invisible.
	let sealed = ApplyWith {
		drop: Some(WithSpan::Duration(millis(1_000))),
		..with()
	};
	let mut h = InProcessOperatorHarnessBuilder::<EntryMap>::new().with(sealed).build().expect("harness");
	h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 1, 0, 10)).build()).expect("apply");
	h.advance_flow_watermark(DateTime::from_millis(120_000)).expect("advance watermark");
	assert_eq!(
		h.sample().expect("a windowed operator reports a sample").drops.rows_in_sealed_windows,
		Some(0),
		"a windowed operator must report the reason at zero before anything is dropped"
	);

	let late = h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 2, 1_000, 5)).build()).expect("apply");
	assert!(late.diffs.is_empty(), "precondition: the row for the sealed window is dropped");

	assert_eq!(
		h.sample().expect("a windowed operator reports a sample").drops.rows_in_sealed_windows,
		Some(1),
		"the one row dropped into the sealed window must be counted exactly once"
	);
}
