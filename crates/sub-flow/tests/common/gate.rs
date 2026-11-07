// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::LazyLock, thread};

use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_value::{
	config::ExtensionParams,
	value::{
		constraint::TypeConstraint, datetime::DateTime, diff_type::DiffType, duration::Duration,
		row_number::RowNumber, value_type::ValueType,
	},
};

#[derive(Default, Clone, Copy)]
pub struct Gate {
	pub open: bool,
	pub entered: bool,
}

static GATES: LazyLock<Mutex<HashMap<String, Gate>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn set_gate(name: &str, open: bool) {
	GATES.lock().entry(name.to_string()).or_default().open = open;
}

pub fn gate(name: &str) -> Gate {
	GATES.lock().get(name).copied().unwrap_or_default()
}

pub struct GatedPassThrough {
	gate: String,
}

struct KeyValue {
	k: String,
	v: i64,
}

row!(KeyValue {
	k: String,
	v: i64
});

const KEY_VALUE_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "k",
		type_constraint: TypeConstraint::unconstrained(ValueType::Utf8),
		description: "key, copied from the input",
	},
	OperatorColumn {
		name: "v",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "value, copied from the input",
	},
];

impl OperatorMetadata for GatedPassThrough {
	const NAME: &'static str = "gated_pass_through";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds every step until its named gate opens, then passes k and v through";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = KEY_VALUE_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn key_values(columns: &impl ColumnsView) -> SdkResult<(Vec<KeyValue>, Vec<RowNumber>)> {
	let mut rows = Vec::with_capacity(columns.row_count());
	let mut numbers = Vec::with_capacity(columns.row_count());
	for index in 0..columns.row_count() {
		let row = columns.row(index).expect("a row inside row_count must exist");
		rows.push(KeyValue {
			k: row.utf8("k")?.expect("k is set").to_string(),
			v: row.i64("v")?.expect("v is set"),
		});
		numbers.push(row.row_number().expect("a flow row carries its row number"));
	}
	Ok((rows, numbers))
}

impl UnmanagedOperator for GatedPassThrough {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(GatedPassThrough {
			gate: params.require_str("gate"),
		})
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> SdkResult<()> {
		GATES.lock().entry(self.gate.clone()).or_default().entered = true;
		while !gate(&self.gate).open {
			thread::sleep(Duration::from_milliseconds_const(10).to_std());
		}
		for index in 0..change.diff_count() {
			let Some(diff) = change.diff(index) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					let (rows, numbers) = key_values(&diff.post().expect("an insert has a post"))?;
					ctx.emit_insert(&rows, &numbers, &vec![ctx.batch_time(); (&numbers).len()])?;
				}
				DiffType::Update => {
					let (pre, numbers) = key_values(&diff.pre().expect("an update has a pre"))?;
					let (post, _) = key_values(&diff.post().expect("an update has a post"))?;
					ctx.emit_update(
						&pre,
						&post,
						&numbers,
						&vec![ctx.batch_time(); (&numbers).len()],
					)?;
				}
				DiffType::Remove => {
					let (rows, numbers) = key_values(&diff.pre().expect("a remove has a pre"))?;
					ctx.emit_remove(&rows, &numbers, &vec![ctx.batch_time(); (&numbers).len()])?;
				}
			}
		}
		Ok(())
	}
}

pub struct GatedPassThroughTimed {
	gate: String,
}

struct KeyValueTime {
	k: String,
	v: i64,
	ts: DateTime,
}

row!(KeyValueTime {
	k: String,
	v: i64,
	ts: DateTime
});

const KEY_VALUE_TIME_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "k",
		type_constraint: TypeConstraint::unconstrained(ValueType::Utf8),
		description: "key, copied from the input",
	},
	OperatorColumn {
		name: "v",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "value, copied from the input",
	},
	OperatorColumn {
		name: "ts",
		type_constraint: TypeConstraint::unconstrained(ValueType::DateTime),
		description: "event time, copied from the input",
	},
];

impl OperatorMetadata for GatedPassThroughTimed {
	const NAME: &'static str = "gated_pass_through_timed";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str =
		"Holds every step until its named gate opens, then passes k, v and ts through with the input row time";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = KEY_VALUE_TIME_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

fn key_value_times(columns: &impl ColumnsView) -> SdkResult<(Vec<KeyValueTime>, Vec<RowNumber>, Vec<DateTime>)> {
	let mut rows = Vec::with_capacity(columns.row_count());
	let mut numbers = Vec::with_capacity(columns.row_count());
	let mut times = Vec::with_capacity(columns.row_count());
	for index in 0..columns.row_count() {
		let row = columns.row(index).expect("a row inside row_count must exist");
		rows.push(KeyValueTime {
			k: row.utf8("k")?.expect("k is set").to_string(),
			v: row.i64("v")?.expect("v is set"),
			ts: row.datetime("ts")?.expect("ts is set"),
		});
		numbers.push(row.row_number().expect("a flow row carries its row number"));
		times.push(row.row_time().expect("a timed row carries its row time"));
	}
	Ok((rows, numbers, times))
}

impl UnmanagedOperator for GatedPassThroughTimed {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(GatedPassThroughTimed {
			gate: params.require_str("gate"),
		})
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> SdkResult<()> {
		GATES.lock().entry(self.gate.clone()).or_default().entered = true;
		while !gate(&self.gate).open {
			thread::sleep(Duration::from_milliseconds_const(10).to_std());
		}
		for index in 0..change.diff_count() {
			let Some(diff) = change.diff(index) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					let (rows, numbers, times) =
						key_value_times(&diff.post().expect("an insert has a post"))?;
					ctx.emit_insert(&rows, &numbers, &times)?;
				}
				DiffType::Update => {
					let (pre, numbers, _) =
						key_value_times(&diff.pre().expect("an update has a pre"))?;
					let (post, _, times) =
						key_value_times(&diff.post().expect("an update has a post"))?;
					ctx.emit_update(&pre, &post, &numbers, &times)?;
				}
				DiffType::Remove => {
					let (rows, numbers, times) =
						key_value_times(&diff.pre().expect("a remove has a pre"))?;
					ctx.emit_remove(&rows, &numbers, &times)?;
				}
			}
		}
		Ok(())
	}
}
