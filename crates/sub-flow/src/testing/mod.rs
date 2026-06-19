// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod harness;

use harness::NativeOperatorHarness;
use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{interface::change::Change, value::column::columns::Columns};
use reifydb_sdk::{
	operator::{FFIOperatorAdapter, OperatorLogic, OperatorMetadata},
	testing::harness::FFIOperatorHarness,
};
use reifydb_value::value::{Value, row_number::RowNumber};

#[derive(Debug, PartialEq)]
struct ColumnsRender {
	names: Vec<String>,
	row_numbers: Vec<RowNumber>,
	rows: Vec<Vec<Value>>,
}

#[derive(Debug, PartialEq)]
struct DiffRender {
	kind: DiffType,
	pre: Option<ColumnsRender>,
	post: Option<ColumnsRender>,
}

fn render_columns(cols: &Columns) -> ColumnsRender {
	ColumnsRender {
		names: (0..cols.len()).map(|i| cols.name_at(i).text().to_string()).collect(),
		row_numbers: cols.row_numbers.iter().copied().collect(),
		rows: (0..cols.row_count()).map(|r| cols.row(r)).collect(),
	}
}

fn render_change(change: &Change) -> Vec<DiffRender> {
	change.diffs
		.iter()
		.map(|d| DiffRender {
			kind: d.kind(),
			pre: d.pre().map(render_columns),
			post: d.post().map(render_columns),
		})
		.collect()
}

fn run_ffi<C>(config: &[(&str, Value)], inputs: &[Change]) -> Vec<Change>
where
	C: OperatorLogic + OperatorMetadata + 'static,
{
	let mut harness = FFIOperatorHarness::<FFIOperatorAdapter<C>>::builder()
		.with_config(config.iter().cloned())
		.build()
		.expect("ffi harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("ffi apply")).collect()
}

fn run_native<C>(config: &[(&str, Value)], inputs: &[Change]) -> Vec<Change>
where
	C: OperatorLogic + OperatorMetadata + 'static,
{
	let mut harness = NativeOperatorHarness::<C>::builder()
		.with_config(config.iter().cloned())
		.build()
		.expect("native harness build");
	inputs.iter().map(|input| harness.apply(input.clone()).expect("native apply")).collect()
}

pub fn assert_backend_parity<C>(config: Vec<(&str, Value)>, scenarios: &[(&str, Vec<Change>)])
where
	C: OperatorLogic + OperatorMetadata + 'static,
{
	for (name, inputs) in scenarios {
		let ffi = run_ffi::<C>(&config, inputs);
		let native = run_native::<C>(&config, inputs);

		assert_eq!(
			ffi.len(),
			native.len(),
			"scenario '{name}': ffi emitted {} outputs, native emitted {}",
			ffi.len(),
			native.len()
		);

		for (i, (f, n)) in ffi.iter().zip(native.iter()).enumerate() {
			assert_eq!(
				render_change(f),
				render_change(n),
				"scenario '{name}' apply #{i}: ffi vs native emitted-output mismatch"
			);
		}
	}
}
