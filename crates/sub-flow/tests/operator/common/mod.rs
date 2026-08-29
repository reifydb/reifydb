// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::{GroupId, KeyspaceId, OperatorStateKey},
};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	flow::operator::{
		GuestOperator, OperatorMetadata,
		column::operator::OperatorColumn,
		context::GuestContext,
		state::GuestRawOperator,
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_value::{
	config::Config,
	value::{constraint::TypeConstraint, diff_type::DiffType, row_number::RowNumber, value_type::ValueType},
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

/// Buckets `timestamp` into fixed windows and keeps a per-window count, always emitting an Insert.
/// Exercises keyed window state accumulating across applies.
pub struct ParityWindow;

impl GuestRawOperator for ParityWindow {}

impl OperatorMetadata for ParityWindow {
	const NAME: &'static str = "parity_window";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Buckets events into fixed windows and counts per window";
	const INPUT_COLUMNS: &'static [OperatorColumn] = WINDOW_INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = WINDOW_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl GuestOperator for ParityWindow {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(ParityWindow)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, change: impl ChangeView) -> SdkResult<()> {
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
				let key = OperatorStateKey::inner_encoded(
					GroupId::ROOT,
					KeyspaceId::CUSTOM_NOT_CACHED,
					window_bucket.to_be_bytes(),
				);
				let new_count = self.state_get::<i64>(ctx, &key)?.unwrap_or(0) + 1;
				self.state_set(ctx, &key, &new_count)?;
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

/// Allocates a row number for one fixed key per apply and emits `(row_number, is_new)`. Exercises
/// the operator row-number registry persisting across applies.
pub struct RowNumberProbe;

impl OperatorMetadata for RowNumberProbe {
	const NAME: &'static str = "row_number_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Allocates a row number for a fixed key and reports (row_number, is_new)";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = PROBE_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl GuestOperator for RowNumberProbe {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(RowNumberProbe)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext, _change: impl ChangeView) -> SdkResult<()> {
		// A row-number key is a SUFFIX - the host frames it under ROW_NUMBER_MAPPING itself.
		let key = EncodedKey::new(b"fixed-window-key");
		let (rn, is_new) = ctx.get_or_create_row_numbers(GroupId::ROOT, &[key])?.remove(0);
		ctx.emit_insert(
			&[ProbeRow {
				row_number: rn.0 as i64,
				is_new: i64::from(is_new),
			}],
			&[RowNumber(1)],
		)
	}
}

/// Never touches state; exists only so a harness can be built to exercise the store-facing range API.
pub struct NoopOperator;

impl OperatorMetadata for NoopOperator {
	const NAME: &'static str = "noop";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Does nothing";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl GuestOperator for NoopOperator {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(NoopOperator)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

/// Always fails in apply, so the backend's failure handling is the only thing under test.
pub struct ErroringOperator;

impl OperatorMetadata for ErroringOperator {
	const NAME: &'static str = "erroring";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Always returns Err from apply";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl GuestOperator for ErroringOperator {
	fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(ErroringOperator)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext, _change: impl ChangeView) -> SdkResult<()> {
		Err(SdkError::Other("operator apply must abort, not return Err".to_string()))
	}
}
