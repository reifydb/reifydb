// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Backend-agnostic support shared by the ffi/ and native/ operator-test twins:
// operator fixtures, input/output row builders, and emitted-output extractors.
// No assertions live here - each twin file holds the absolute expectations and
// differs from its counterpart only by the `Harness` backend type.

#![allow(dead_code)]

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::{
		key::EncodedKey,
		row::{EncodedRow, SHAPE_HEADER_SIZE},
		shape::{RowShape, RowShapeField},
	},
	interface::{
		catalog::{flow::FlowNodeId, shape::ShapeId},
		change::Change,
	},
	key::{EncodableKey, row::RowKey},
	row::Row,
};
use reifydb_sdk::{
	config::Config,
	error::{Result as SdkResult, SdkError},
	operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::{OperatorContext, StateApi},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
	state::{RawStatefulOperator, window::WindowStateful},
	testing::builders::TestChangeBuilder,
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, constraint::TypeConstraint, row_number::RowNumber, value_type::ValueType},
};

pub const WINDOW_SIZE: i64 = 100;

struct WindowRow {
	window_start: i64,
	count: i64,
}

row!(WindowRow {
	window_start: i64,
	count: i64
});

const WINDOW_INPUT_COLUMNS: &[OperatorColumn] = &[OperatorColumn {
	name: "timestamp",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
	description: "Event timestamp",
}];

const WINDOW_OUTPUT_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "window_start",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "Window start time",
	},
	OperatorColumn {
		name: "count",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "Event count in window",
	},
];

/// Buckets `timestamp` into fixed windows and keeps a per-window count, always
/// emitting an Insert with `(window_start, count)`. Exercises keyed window state
/// accumulation across applies.
pub struct ParityWindow;

impl RawStatefulOperator for ParityWindow {}

impl WindowStateful for ParityWindow {
	type State = i64;
}

impl OperatorMetadata for ParityWindow {
	const NAME: &'static str = "parity_window";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Buckets events into fixed windows and counts per window";
	const INPUT_COLUMNS: &'static [OperatorColumn] = WINDOW_INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = WINDOW_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for ParityWindow {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(ParityWindow)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		let mut emissions: Vec<(i64, i64)> = Vec::new();
		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			let cols = match diff.kind() {
				DiffType::Insert | DiffType::Update => diff.post(),
				DiffType::Remove => continue,
			};
			let Some(cols) = cols else {
				continue;
			};
			for i in 0..cols.row_count() {
				let Some(row) = cols.row(i) else {
					continue;
				};
				let Some(timestamp) = row.i64("timestamp") else {
					continue;
				};
				let window_bucket = (timestamp / WINDOW_SIZE) * WINDOW_SIZE;
				let key = EncodedKey::new(window_bucket.to_be_bytes().to_vec());
				let new_count = self.load_state(ctx, &key)?.unwrap_or(0i64) + 1;
				self.save_state(ctx, &key, &new_count)?;
				emissions.push((window_bucket, new_count));
			}
		}
		if emissions.is_empty() {
			return Ok(());
		}
		let rows: Vec<WindowRow> = emissions
			.iter()
			.map(|(window_start, count)| WindowRow {
				window_start: *window_start,
				count: *count,
			})
			.collect();
		let row_numbers: Vec<RowNumber> = emissions.iter().map(|(s, _)| RowNumber((*s as u64) + 1)).collect();
		ctx.emit_insert(&rows, &row_numbers)
	}
}

struct ProbeRow {
	row_number: i64,
	is_new: i64,
}

row!(ProbeRow {
	row_number: i64,
	is_new: i64
});

const PROBE_OUTPUT_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "row_number",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "Allocated row number for the fixed key",
	},
	OperatorColumn {
		name: "is_new",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "1 if the key was freshly allocated on this apply",
	},
];

/// Allocates a row number for one fixed key per apply and emits
/// `(row_number, is_new)`. Exercises the operator row-number registry's
/// persistence across applies.
pub struct RowNumberProbe;

impl OperatorMetadata for RowNumberProbe {
	const NAME: &'static str = "row_number_probe";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Allocates a row number for a fixed key and reports (row_number, is_new)";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = PROBE_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for RowNumberProbe {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(RowNumberProbe)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		let key = EncodedKey::new(b"fixed-window-key".to_vec());
		let (rn, is_new) = ctx.get_or_create_row_number(&key)?;
		ctx.emit_insert(
			&[ProbeRow {
				row_number: rn.0 as i64,
				is_new: i64::from(is_new),
			}],
			&[RowNumber(1)],
		)
	}
}

/// Writes its state ONLY in `flush_state`, never in `apply`. Lets a test observe
/// the flush cadence: the value must be invisible after apply and visible only
/// after the explicit flush.
pub struct FlushProbe;

impl OperatorMetadata for FlushProbe {
	const NAME: &'static str = "flush_probe";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Writes state only in flush_state to observe flush cadence";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for FlushProbe {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(FlushProbe)
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> SdkResult<()> {
		ctx.state().set::<i64>(&flush_probe_key(), &FLUSH_PROBE_VALUE)
	}
}

pub const FLUSH_PROBE_VALUE: i64 = 42;

pub fn flush_probe_key() -> EncodedKey {
	EncodedKey::new(b"flush-probe".to_vec())
}

/// Never touches state; only exists so a harness can be built to exercise the
/// store-facing range API.
pub struct NoopOperator;

impl OperatorMetadata for NoopOperator {
	const NAME: &'static str = "noop";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Does nothing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for NoopOperator {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(NoopOperator)
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

/// Always fails in apply, so the backend's failure handling is the only thing
/// under test.
pub struct ErroringOperator;

impl OperatorMetadata for ErroringOperator {
	const NAME: &'static str = "erroring";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Always returns Err from apply";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for ErroringOperator {
	fn create(_operator_id: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(ErroringOperator)
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		Err(SdkError::Other("operator apply must abort, not return Err".to_string()))
	}
}

pub fn ts_row(row_number: u64, timestamp: i64) -> Row {
	let shape = RowShape::new(vec![RowShapeField::unconstrained("timestamp", ValueType::Int8)]);
	let mut encoded = shape.allocate();
	shape.set_values(&mut encoded, &[Value::Int8(timestamp)]);
	Row {
		number: RowNumber(row_number),
		encoded,
		shape,
	}
}

pub fn window_change(row_number: u64, timestamp: i64) -> Change {
	TestChangeBuilder::new().insert(ts_row(row_number, timestamp)).build()
}

pub fn trigger() -> Change {
	TestChangeBuilder::new().insert_row(1u64, vec![Value::Int8(0)]).build()
}

pub const STORE_TABLE: u64 = 4096;

pub const STORE_ROW_COUNT: u64 = 1500;

fn store_value(payload: &str) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload.as_bytes());
	EncodedRow(CowVec::new(buf))
}

pub fn store_seed() -> Vec<(EncodedKey, EncodedRow)> {
	(1..=STORE_ROW_COUNT)
		.map(|n| {
			let key = RowKey {
				shape: ShapeId::table(STORE_TABLE),
				row: RowNumber(n),
			}
			.encode();
			(key, store_value(&format!("row-{n}")))
		})
		.collect()
}

/// Reads the Int8 values of the first emitted row in a Change's first diff.
pub fn row_ints(change: &Change) -> Vec<i64> {
	let cols = change.diffs[0].post().expect("emitted diff has post columns");
	assert_eq!(cols.row_count(), 1, "expected exactly one emitted row");
	cols.row(0)
		.into_iter()
		.map(|v| match v {
			Value::Int8(n) => n,
			other => panic!("expected Int8 emitted value, got {other:?}"),
		})
		.collect()
}

pub fn diff_kind(change: &Change) -> DiffType {
	change.diffs[0].kind()
}
