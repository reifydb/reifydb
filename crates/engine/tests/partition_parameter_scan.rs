// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::r { id: int4, region: utf8 } WITH { partition: { by: { region } } }");
	t.admin("CREATE TABLE test::k { id: int4, k: int4 } WITH { partition: { by: { k } } }");
	t.admin(
		"CREATE TABLE test::m { id: int4, region: utf8, tier: utf8 } WITH { partition: { by: { region, tier } } }",
	);
	t.admin(
		"CREATE TRANSACTIONAL VIEW test::v { id: int4, region: utf8 } WITH { partition: { by: { region } } } AS { FROM test::r }",
	);
	t.command("INSERT test::r [{ id: 1, region: 'eu' }, { id: 2, region: 'us' }, { id: 3, region: 'eu' }]");
	t.command("INSERT test::k [{ id: 1, k: 2 }, { id: 2, k: 3 }, { id: 3, k: 2 }, { id: 4, k: -1 }]");
	t.command(
		"INSERT test::m [{ id: 1, region: 'eu', tier: 'gold' }, { id: 2, region: 'eu', tier: 'free' }, { id: 3, region: 'us', tier: 'gold' }]",
	);
	t
}

#[test]
fn a_partition_parameter_answers_exactly_what_the_plain_filter_answers() {
	// The wrapped filter never prunes, so any difference means a pruned scan dropped or added a row.
	let t = engine();
	let cases: [(&str, &str, Vec<Value>); 4] = [
		(
			"test::r",
			"region == $1",
			vec![
				Value::Utf8("eu".to_string()),
				Value::Utf8("us".to_string()),
				Value::Utf8("zz".to_string()),
				Value::Int4(3),
				Value::none(),
			],
		),
		(
			"test::v",
			"region == $1",
			vec![
				Value::Utf8("eu".to_string()),
				Value::Utf8("us".to_string()),
				Value::Int4(3),
				Value::none(),
			],
		),
		(
			"test::k",
			"k == $1",
			vec![
				Value::Int4(2),
				Value::Int8(2),
				Value::Uint1(2),
				Value::Int1(-1),
				Value::Int8(5_000_000_000),
				Value::Utf8("2".to_string()),
				Value::float8(2.0),
				Value::none(),
			],
		),
		(
			"test::m",
			"region == 'eu' and tier == $1",
			vec![
				Value::Utf8("gold".to_string()),
				Value::Utf8("free".to_string()),
				Value::Utf8("x".to_string()),
			],
		),
	];
	for (source, filter, values) in cases {
		for value in values {
			let pruned = run(
				&t,
				&format!(
					"FROM {source} | filter {{ {filter} }} | sort {{ id: ASC }} | map {{ v: id }}"
				),
				positional(vec![value.clone()]),
			);
			let plain = run(
				&t,
				&format!(
					"FROM {source} | filter {{ ({filter}) == true }} | sort {{ id: ASC }} | map {{ v: id }}"
				),
				positional(vec![value.clone()]),
			);
			assert_eq!(pruned, plain, "{source} {filter} with {value:?}");
		}
	}
}

#[test]
fn a_partition_parameter_answers_the_rows_of_its_partition() {
	// A pruned scan must still read every row of the named partition, never a different partition.
	let t = engine();
	let rows = "FROM test::k | filter { k == $1 } | sort { id: ASC } | map { v: id }";
	assert_eq!(run(&t, rows, positional(vec![Value::Int8(2)])), ids(&["1", "3"]));
	assert_eq!(run(&t, rows, positional(vec![Value::Int4(3)])), ids(&["2"]));

	let view = "FROM test::v | filter { region == $1 } | sort { id: ASC } | map { v: id }";
	assert_eq!(run(&t, view, positional(vec![Value::Utf8("eu".to_string())])), ids(&["1", "3"]));

	let tiers = "FROM test::m | filter { region == 'eu' and tier == $1 } | sort { id: ASC } | map { v: id }";
	assert_eq!(run(&t, tiers, positional(vec![Value::Utf8("gold".to_string())])), ids(&["1"]));
}

#[derive(Debug, PartialEq)]
enum Answer {
	Rows(Vec<String>),
	Error(String, String),
}

fn positional(values: Vec<Value>) -> Params {
	Params::Positional(Arc::new(values))
}

fn ids(values: &[&str]) -> Answer {
	Answer::Rows(values.iter().map(|v| v.to_string()).collect())
}

fn run(t: &TestEngine, rql: &str, params: Params) -> Answer {
	let result = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(err) = result.error {
		let diagnostic = err.diagnostic();
		return Answer::Error(diagnostic.code.clone(), diagnostic.message.clone());
	}
	let column = result.frames.last().expect("a frame").column("v").unwrap().expect("column v");
	Answer::Rows((0..column.len()).map(|i| column.get_value(i).to_string()).collect())
}
