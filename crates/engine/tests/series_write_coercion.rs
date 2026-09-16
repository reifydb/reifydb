// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

const TARGETS: [&str; 4] = ["s::t", "s::r", "s::q", "s::x"];

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE TABLE s::t { ts: datetime, val: float8 }");
	t.admin("CREATE RINGBUFFER s::r { ts: datetime, val: float8 } WITH { capacity: 10 }");
	t.admin("CREATE QUEUE s::q { ts: datetime, val: float8 } WITH { fifo: {} }");
	t.admin("CREATE SERIES s::x { ts: datetime, val: float8 } WITH { key: ts }");
	t
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
	}
}

fn command_err(t: &TestEngine, rql: &str) -> Diagnostic {
	match command(t, rql) {
		Err(err) => err,
		Ok(frames) => panic!("expected an error, got frames {frames:?}\nrql: {rql}"),
	}
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn row_count(t: &TestEngine, rql: &str) -> usize {
	TestEngine::row_count(&t.query(rql))
}

#[test]
fn inserting_an_int4_from_a_query_into_a_float8_column_stores_float8_in_every_target() {
	// The query value is int4, so a target that skips coercion writes int4 bytes into a float8 slot.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: datetime, val: int4 }");
	t.command("INSERT s::src [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 7 }]");

	for target in TARGETS {
		command(&t, &format!("INSERT {target} FROM s::src")).unwrap_or_else(|e| panic!("{target}: {e:?}"));
		assert_eq!(
			column_values(&t.query(&format!("FROM {target}")), "val"),
			vec![Value::float8(7.0)],
			"{target}"
		);
	}
}

#[test]
fn inserting_text_from_a_query_into_a_float8_column_is_the_cast_error_on_the_value_in_every_target() {
	// Every target must refuse text it cannot cast with the same error, never panic in the row encoder.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: datetime, val: utf8 }");
	t.command("INSERT s::src [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 'abc' }]");

	for target in TARGETS {
		let err = command_err(&t, &format!("INSERT {target} FROM s::src"));
		assert_eq!(err.code, "CAST_002", "{target}: {err:?}");
		assert_eq!(err.fragment.text(), "abc", "{target}: {err:?}");
		assert_eq!(row_count(&t, &format!("FROM {target}")), 0, "{target}");
	}
}

#[test]
fn inserting_a_row_without_a_non_optional_column_is_constraint_007_naming_it_in_every_target() {
	// val is not optional, so a missing value must fail loud instead of being stored as 0.
	let t = engine();

	for target in TARGETS {
		let err =
			command_err(&t, &format!("INSERT {target} [{{ ts: cast('2024-01-01T00:00:00Z', datetime) }}]"));
		assert_eq!(err.code, "CONSTRAINT_007", "{target}: {err:?}");
		assert_eq!(err.fragment.text(), "val", "{target}: {err:?}");
		assert_eq!(row_count(&t, &format!("FROM {target}")), 0, "{target}");
	}
}

#[test]
fn inserting_from_a_query_without_a_non_optional_column_is_constraint_007_in_every_target() {
	// A query that lacks val must fail loud in every target instead of panicking or storing 0.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: datetime }");
	t.command("INSERT s::src [{ ts: cast('2024-01-01T00:00:00Z', datetime) }]");

	for target in TARGETS {
		let err = command_err(&t, &format!("INSERT {target} FROM s::src"));
		assert_eq!(err.code, "CONSTRAINT_007", "{target}: {err:?}");
		assert_eq!(row_count(&t, &format!("FROM {target}")), 0, "{target}");
	}
}

#[test]
fn inserting_text_past_a_utf8_limit_into_a_series_is_the_error_a_table_gives() {
	// A series that skips constraint validation stores text longer than its declared utf8 limit.
	let t = engine();
	t.admin("CREATE TABLE s::tc { k: int8, name: utf8(3) }");
	t.admin("CREATE SERIES s::c { k: int8, name: utf8(3) } WITH { key: k }");
	t.admin("CREATE TABLE s::src { k: int8, name: utf8 }");
	t.command("INSERT s::src [{ k: 2, name: 'abcdef' }]");

	for source in ["[{ k: 1, name: 'abcdef' }]", "FROM s::src"] {
		let table = command_err(&t, &format!("INSERT s::tc {source}"));
		let series = command_err(&t, &format!("INSERT s::c {source}"));
		assert_eq!(series.code, table.code, "{source}: {series:?}");
		assert_eq!(series.fragment.text(), table.fragment.text(), "{source}: {series:?}");
	}
	assert_eq!(row_count(&t, "FROM s::c"), 0);
}

#[test]
fn a_text_datetime_from_a_query_is_cast_into_the_series_key_as_into_a_table_column() {
	// An uncast text key is not a datetime, so the series would replace it with a generated key.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: utf8, val: float8 }");
	t.command("INSERT s::src [{ ts: '2024-01-01T00:00:00Z', val: 1.0 }]");

	command(&t, "INSERT s::t FROM s::src").unwrap_or_else(|e| panic!("table: {e:?}"));
	command(&t, "INSERT s::x FROM s::src").unwrap_or_else(|e| panic!("series: {e:?}"));

	let table = column_values(&t.query("FROM s::t"), "ts");
	assert_eq!(column_values(&t.query("FROM s::x"), "ts"), table);
}

#[test]
fn a_partitioned_series_filled_from_a_query_finds_and_updates_its_row_by_the_partition_column() {
	// The partition must be computed from the stored int8 value, or a filter and update on it miss the row.
	let t = engine();
	t.admin("CREATE SERIES s::p { ts: int8, g: int8, v: float8 } WITH { key: ts, partition: { by: { g } } }");
	t.admin("CREATE TABLE s::src { ts: int4, g: int4, v: int4 }");
	t.command("INSERT s::src [{ ts: 2, g: 7, v: 1 }]");

	command(&t, "INSERT s::p FROM s::src").unwrap_or_else(|e| panic!("{e:?}"));

	assert_eq!(row_count(&t, "FROM s::p | filter { g == 7 }"), 1);
	let updated = command(&t, "UPDATE s::p { v: 2.5 } FILTER { g == 7 }").unwrap_or_else(|e| panic!("{e:?}"));
	assert_eq!(column_values(&updated, "updated"), vec![Value::Uint8(1)]);
	assert_eq!(column_values(&t.query("FROM s::p"), "v"), vec![Value::float8(2.5)]);
}

#[test]
fn updating_a_series_value_to_none_in_a_non_optional_column_is_constraint_007_as_for_a_table() {
	// val is not optional, so an update to none must fail loud instead of storing 0.
	let t = engine();
	for target in ["s::t", "s::x"] {
		t.command(&format!("INSERT {target} [{{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0 }}]"));

		let err = command_err(&t, &format!("UPDATE {target} {{ val: none }} FILTER {{ val == 1.0 }}"));

		assert_eq!(err.code, "CONSTRAINT_007", "{target}: {err:?}");
		assert_eq!(
			column_values(&t.query(&format!("FROM {target}")), "val"),
			vec![Value::float8(1.0)],
			"{target}"
		);
	}
}

#[test]
fn updating_series_text_past_its_utf8_limit_is_the_error_a_table_gives() {
	// A series update that skips constraint validation stores text longer than the declared utf8 limit.
	let t = engine();
	t.admin("CREATE TABLE s::tc { k: int8, name: utf8(3) }");
	t.admin("CREATE SERIES s::c { k: int8, name: utf8(3) } WITH { key: k }");
	t.command("INSERT s::tc [{ k: 1, name: 'abc' }]");
	t.command("INSERT s::c [{ k: 1, name: 'abc' }]");

	let table = command_err(&t, "UPDATE s::tc { name: 'abcdef' } FILTER { k == 1 }");
	let series = command_err(&t, "UPDATE s::c { name: 'abcdef' } FILTER { k == 1 }");

	assert_eq!(series.code, table.code, "{series:?}");
	assert_eq!(series.fragment.text(), table.fragment.text(), "{series:?}");
	assert_eq!(column_values(&t.query("FROM s::c"), "name"), vec![Value::Utf8("abc".to_string())]);
}
