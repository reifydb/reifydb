// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, frame::frame::Frame, value_type::ValueType},
};

fn query(t: &TestEngine, rql: &str) -> Vec<Frame> {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	if let Some(err) = result.error {
		panic!("{rql}: the query must succeed, got {:?}", err.diagnostic());
	}
	result.frames
}

fn column(frames: &[Frame], name: &str) -> (ValueType, Vec<Value>) {
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(column.data.get_type(), (0..column.data.len()).map(|i| column.data.get_value(i)).collect())
}

#[test]
fn a_utf8_cast_to_int_uint_or_decimal_gives_that_type_with_the_parsed_value() {
	// Text must parse into the arbitrary-precision type, otherwise the text cast only works in one direction.
	let t = TestEngine::new();

	for (text, target, expected_type, expected_text) in [
		("42", "int", ValueType::INT, "42"),
		("42", "uint", ValueType::UINT, "42"),
		("1.5", "decimal", ValueType::DECIMAL, "1.5000000000"),
	] {
		let frames = query(&t, &format!("map {{ v: cast('{text}', {target}) }}"));

		let (ty, values) = column(&frames, "v");
		assert_eq!(
			(ty, values.iter().map(|v| v.to_string()).collect::<Vec<_>>()),
			(expected_type, vec![expected_text.to_string()]),
			"{target}"
		);
	}
}

#[test]
fn an_int_or_uint_wider_than_128_bits_cast_to_utf8_keeps_every_digit() {
	// A text conversion routed through a fixed-width integer would overflow or truncate these values.
	let t = TestEngine::new();

	for (text, target) in [
		("-170141183460469231731687303715884105729", "int"),
		("340282366920938463463374607431768211456", "uint"),
	] {
		let frames = query(&t, &format!("map {{ v: cast(cast('{text}', {target}), utf8) }}"));

		assert_eq!(column(&frames, "v"), (ValueType::Utf8, vec![Value::Utf8(text.to_string())]), "{target}");
	}
}

#[test]
fn an_arbitrary_precision_number_cast_to_utf8_reads_exactly_as_the_number_displays() {
	// Output renders these values with their display form, so a cast must never produce a different spelling.
	let t = TestEngine::new();

	for (text, target) in
		[("-7", "int"), ("0", "uint"), ("1.50", "decimal"), ("-0.000000000000000000001", "decimal")]
	{
		let source = format!("cast('{text}', {target})");
		let frames = query(&t, &format!("map {{ n: {source}, v: cast({source}, utf8) }}"));

		let (_, numbers) = column(&frames, "n");
		assert_eq!(
			column(&frames, "v"),
			(ValueType::Utf8, vec![Value::Utf8(numbers[0].to_string())]),
			"{target} {text}"
		);
	}
}

#[test]
fn a_none_int_cast_to_utf8_stays_none_beside_its_defined_rows() {
	// A none row must not become text or shift the rows after it when the column is cast.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::n { id: int4, v: Option(int) }");
	t.command("INSERT test::n [{ id: 1, v: 42 }, { id: 2, v: none }, { id: 3, v: 7 }]");

	let frames = query(&t, "FROM test::n | sort { id: asc } | map { v: cast(v, utf8) }");

	let (_, values) = column(&frames, "v");
	assert_eq!(values.iter().map(|v| v.to_string()).collect::<Vec<_>>(), vec!["42", "none", "7"]);
	assert!(matches!(values[1], Value::None { .. }), "the none row must stay none, got {:?}", values[1]);
}
