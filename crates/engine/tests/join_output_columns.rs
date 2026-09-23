// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Int32Array;
use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{
	Value,
	frame::{data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};

fn column<'a>(frames: &'a [Frame], name: &str) -> &'a FrameColumnData {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	&frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns))
		.data
}

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	column(frames, name).get_type()
}

fn rows(frames: &[Frame], names: &[&str]) -> Vec<Vec<Value>> {
	let mut out = Vec::new();
	for frame in frames {
		let columns: Vec<_> = names
			.iter()
			.map(|name| {
				frame.columns
					.iter()
					.find(|c| c.name == *name)
					.unwrap_or_else(|| panic!("no column {name} in\n{frame}"))
			})
			.collect();
		let row_count = columns.first().map(|c| c.data.len()).unwrap_or(0);
		out.extend((0..row_count).map(|row| columns.iter().map(|c| c.data.get_value(row)).collect()));
	}
	out
}

fn optional_int4() -> ValueType {
	ValueType::Option(Box::new(ValueType::Int4))
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::l { k: int4, v: int4 }");
	t.admin("CREATE TABLE test::r { k: int4, w: int4 }");
	t
}

#[test]
fn a_left_join_that_matches_nothing_keeps_the_declared_type_of_the_right_columns() {
	// Every right cell is none, so a type read off the values reports Option(boolean) instead of int4.
	let t = engine();
	t.command("INSERT test::l [{ k: 1, v: 10 }, { k: 2, v: 20 }]");
	t.command("INSERT test::r [{ k: 8, w: 80 }]");

	let frames = t.query("FROM test::l LEFT JOIN { FROM test::r } AS r USING (k, r.k)");

	assert_eq!(rows(&frames, &["v"]).len(), 2, "a left join keeps every left row:\n{}", frames[0]);
	assert_eq!(column_type(&frames, "r_k"), optional_int4());
	assert_eq!(column_type(&frames, "r_w"), optional_int4());
	assert_eq!(column_type(&frames, "k"), ValueType::Int4);
	assert_eq!(column_type(&frames, "v"), ValueType::Int4);
}

#[test]
fn a_left_join_leaves_the_placeholder_under_a_none_row_zeroed() {
	// Serde and equality read the whole values buffer, so a stale payload under a none row changes bytes.
	let t = engine();
	t.command("INSERT test::l [{ k: 1, v: 10 }, { k: 2, v: 20 }]");
	t.command("INSERT test::r [{ k: 2, w: 80 }]");

	let frames = t.query("FROM test::l LEFT JOIN { FROM test::r } AS r USING (k, r.k)");

	let FrameColumnData::Option {
		inner,
		bitvec,
	} = column(&frames, "r_w")
	else {
		panic!("an unmatched left row makes the right column optional, got {:?}", column(&frames, "r_w"));
	};
	let FrameColumnData::Int4(values) = inner.as_ref() else {
		panic!("the right column is declared int4, got {inner:?}");
	};
	let none_rows: Vec<usize> = (0..bitvec.len()).filter(|&row| !bitvec.value(row)).collect();
	assert_eq!(none_rows.len(), 1, "exactly one left row has no match:\n{}", frames[0]);
	for row in none_rows {
		assert_eq!(placeholder(values, row), 0, "the placeholder under a none row must stay zeroed");
	}
}

fn placeholder(values: &Int32Array, row: usize) -> i32 {
	values.values()[row]
}

#[test]
fn a_join_keeps_an_all_valid_optional_column_optional() {
	// Every bit is valid, so a type read off the values narrows Option(int4) to int4 and drops none.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::l { k: int4, v: Option(int4) }");
	t.admin("CREATE TABLE test::r { k: int4, w: Option(int4) }");
	t.command("INSERT test::l [{ k: 1, v: 10 }]");
	t.command("INSERT test::r [{ k: 1, w: 70 }]");

	let frames = t.query("FROM test::l INNER JOIN { FROM test::r } AS r USING (k, r.k)");

	assert_eq!(rows(&frames, &["v", "r_w"]), vec![vec![Value::Int4(10), Value::Int4(70)]]);
	assert_eq!(column_type(&frames, "v"), optional_int4(), "the left side is carried, not rebuilt");
	assert_eq!(column_type(&frames, "r_w"), optional_int4(), "the right side is carried, not rebuilt");
}

#[test]
fn a_hash_join_output_spanning_several_probe_batches_keeps_every_matched_row() {
	// An output batch fills from several probe batches, so reading only the last one loses earlier rows.
	let t = engine();
	t.set_config(ConfigKey::QueryRowBatchSize, Value::Uint2(2));
	t.command(
		"INSERT test::l [{ k: 1, v: 10 }, { k: 2, v: 20 }, { k: 3, v: 30 }, { k: 4, v: 40 }, { k: 5, v: \
		 50 }, { k: 6, v: 60 }]",
	);
	t.command("INSERT test::r [{ k: 2, w: 200 }, { k: 4, w: 400 }, { k: 6, w: 600 }]");

	let frames = t.query("FROM test::l INNER JOIN { FROM test::r } AS r USING (k, r.k)");

	let mut matched = rows(&frames, &["k", "v", "r_w"]);
	matched.sort_by_key(|row| format!("{:?}", row[0]));
	assert_eq!(
		matched,
		vec![
			vec![Value::Int4(2), Value::Int4(20), Value::Int4(200)],
			vec![Value::Int4(4), Value::Int4(40), Value::Int4(400)],
			vec![Value::Int4(6), Value::Int4(60), Value::Int4(600)],
		]
	);
}

#[test]
fn a_natural_left_join_that_matches_nothing_keeps_the_declared_type_of_the_right_columns() {
	// The natural join builds its output on its own path, so it can lose a type the hash join keeps.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::nl { s: int4, a: int4 }");
	t.admin("CREATE TABLE test::nr { s: int4, b: int4 }");
	t.command("INSERT test::nl [{ s: 1, a: 10 }]");
	t.command("INSERT test::nr [{ s: 8, b: 80 }]");

	let frames = t.query("FROM test::nl NATURAL LEFT JOIN { FROM test::nr } AS r");

	assert_eq!(rows(&frames, &["a"]).len(), 1, "a left join keeps every left row:\n{}", frames[0]);
	assert_eq!(column_type(&frames, "r_b"), optional_int4());
	assert_eq!(column_type(&frames, "a"), ValueType::Int4);
}
