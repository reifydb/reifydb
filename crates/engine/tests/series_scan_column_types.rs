// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{Value, frame::frame::Frame};

const COLUMNS: &str = "d: decimal, dt: date, u: uuid4, b: blob, i: int16, o: Option(int4), f4: float4";
const ROW: &str = "d: cast('1.25', decimal), dt: cast('2024-01-02', date), u: cast('550e8400-e29b-41d4-a716-446655440000', uuid4), b: blob::hex('dead'), i: 7, o: 3, f4: cast(1.5, float4)";

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

#[test]
fn a_series_scan_reads_every_column_type_back_as_a_table_scan_does() {
	// A column type without its own scan arm must still read back typed, never as debug text.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin(&format!("CREATE TABLE s::t {{ k: int8, {COLUMNS} }}"));
	t.admin(&format!("CREATE SERIES s::x {{ k: int8, {COLUMNS} }} WITH {{ key: k }}"));
	t.command(&format!("INSERT s::t [{{ k: 1, {ROW} }}]"));
	t.command(&format!("INSERT s::x [{{ k: 1, {ROW} }}]"));

	let table = t.query("FROM s::t");
	let series = t.query("FROM s::x");

	for name in ["d", "dt", "u", "b", "i", "o", "f4"] {
		assert_eq!(column_values(&series, name), column_values(&table, name), "column {name}");
	}
}
