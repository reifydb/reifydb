// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, digest::Digest, frame::frame::Frame, value_type::ValueType},
};

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
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

#[test]
fn inserting_an_integer_into_a_utf8_dictionary_stores_the_text_a_utf8_table_column_stores() {
	// An uncast integer is stored under its debug name, so a lookup of its text never finds the entry.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE TABLE s::tu { value: utf8 }");
	t.admin("CREATE DICTIONARY s::d FOR utf8 AS uint4");
	t.command("INSERT s::tu [{ value: 5 }]");

	let r = t.inner().command_as(TestEngine::identity(), "INSERT s::d [{ value: 5 }]", Params::None);

	if r.error.is_none() {
		let table = column_values(&t.query("FROM s::tu"), "value");
		assert_eq!(table, vec![Value::Utf8("5".to_string())]);
		assert_eq!(column_values(&t.query("FROM s::d"), "value"), table);
	}
}

#[test]
fn text_that_is_not_a_number_is_refused_by_an_int4_dictionary_with_the_error_an_int4_table_column_gives() {
	// A value the dictionary type cannot hold must fail loud, never be stored under a foreign type.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE TABLE s::ti { value: int4 }");
	t.admin("CREATE DICTIONARY s::d FOR int4 AS uint4");

	let table = command(&t, "INSERT s::ti [{ value: 'abc' }]").expect_err("an int4 table column must refuse 'abc'");
	let dictionary =
		command(&t, "INSERT s::d [{ value: 'abc' }]").expect_err("an int4 dictionary must refuse 'abc'");

	assert_eq!(dictionary.code, table.code, "{dictionary:?}");
	assert_eq!(dictionary.fragment.text(), table.fragment.text(), "{dictionary:?}");
	assert_eq!(TestEngine::row_count(&t.query("FROM s::d")), 0);
}

#[test]
fn a_small_integer_and_the_same_int8_value_share_one_int8_dictionary_entry() {
	// Without conversion the literal is stored under its own width, so equal values get two entries.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE DICTIONARY s::d FOR int8 AS uint4");

	let first = command(&t, "INSERT s::d [{ value: 5 }]").expect("an int8 dictionary must accept 5");
	let second =
		command(&t, "INSERT s::d [{ value: cast(5, int8) }]").expect("an int8 dictionary must accept int8 5");

	assert_eq!(column_values(&first, "id"), column_values(&second, "id"));
	assert_eq!(column_values(&t.query("FROM s::d"), "value"), vec![Value::Int8(5)]);
}

#[test]
fn the_insert_result_reports_the_value_in_the_dictionary_value_type() {
	// The result must carry the stored value, never a debug rendering of it as text.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE DICTIONARY s::d FOR float8 AS uint4");

	let frames = command(&t, "INSERT s::d [{ value: 1.5 }]").expect("a float8 dictionary must accept 1.5");

	assert_eq!(column_values(&frames, "value"), vec![Value::float8(1.5)]);
	assert_eq!(column_values(&t.query("FROM s::d"), "value"), vec![Value::float8(1.5)]);
}

#[test]
fn a_digest_is_refused_by_a_utf8_dictionary_with_the_error_a_utf8_table_column_gives() {
	// A digest cast to text would store its rendering, which no digest write to a table column allows.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE TABLE s::tu { value: utf8 }");
	t.admin("CREATE DICTIONARY s::d FOR utf8 AS uint4");
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	digest.add_value(&Value::float8(1.5)).unwrap();
	let params = || Params::from(HashMap::from([("d".to_string(), Value::Digest(Box::new(digest.clone())))]));
	let run = |rql: &str| t.inner().command_as(TestEngine::identity(), rql, params()).error.map(|e| e.diagnostic());

	let table = run("INSERT s::tu [{ value: $d }]").expect("a utf8 table column must refuse a digest");
	let dictionary = run("INSERT s::d [{ value: $d }]").expect("a utf8 dictionary must refuse a digest");

	assert_eq!(dictionary.code, table.code, "{dictionary:?}");
	assert_eq!(TestEngine::row_count(&t.query("FROM s::d")), 0);
}

#[test]
fn every_dictionary_value_type_reports_and_stores_the_inserted_value_in_that_type() {
	// A value type without its own result column arm must still come back typed, never as debug text.
	let cases = [
		("float8", "2.5", ValueType::Float8, "2.5"),
		("int16", "5", ValueType::Int16, "5"),
		("uint16", "5", ValueType::Uint16, "5"),
		("bool", "true", ValueType::Boolean, "true"),
		("date", "cast('2024-01-02', date)", ValueType::Date, "2024-01-02"),
		(
			"uuid4",
			"cast('550e8400-e29b-41d4-a716-446655440000', uuid4)",
			ValueType::Uuid4,
			"550e8400-e29b-41d4-a716-446655440000",
		),
		("blob", "blob::hex('deadbeef')", ValueType::Blob, "0xdeadbeef"),
		("decimal", "cast('1.25', decimal)", ValueType::DECIMAL, "1.2500000000"),
	];
	for (ty, literal, value_type, text) in cases {
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE s");
		t.admin(&format!("CREATE DICTIONARY s::d FOR {ty} AS uint4"));
		let rql = format!("INSERT s::d [{{ value: {literal} }}]");

		let frames = command(&t, &rql).unwrap_or_else(|e| panic!("{rql}: {e:?}"));

		let reported = column_values(&frames, "value");
		let typed: Vec<(ValueType, String)> = reported.iter().map(|v| (v.get_type(), v.to_string())).collect();
		assert_eq!(typed, vec![(value_type, text.to_string())], "{rql}");
		assert_eq!(column_values(&t.query("FROM s::d"), "value"), reported, "{rql}");
	}
}
