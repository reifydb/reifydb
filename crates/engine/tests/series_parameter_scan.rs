// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, datetime::DateTime},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE SERIES test::s { ts: int8, v: int4 } WITH { key: ts }");
	t.admin("CREATE SERIES test::u { k: uint8, v: int4 } WITH { key: k }");
	t.admin("CREATE SERIES test::d { ts: datetime, v: int4 } WITH { key: ts }");
	t.admin(
		"CREATE SERIES test::p { ts: int8, region: utf8, v: int4 } WITH { key: ts, partition: { by: { region } } }",
	);
	t.command("INSERT test::s [{ ts: 1, v: 1 }, { ts: 2, v: 2 }, { ts: 3, v: 3 }, { ts: 4, v: 4 }, { ts: 5, v: 5 }, { ts: 6, v: 6 }]");
	t.command(
		"INSERT test::u [{ k: 1, v: 1 }, { k: 18446744073709551614, v: 2 }, { k: 18446744073709551615, v: 3 }]",
	);
	t.command(
		"INSERT test::p [{ ts: 1, region: 'eu', v: 1 }, { ts: 2, region: 'us', v: 2 }, { ts: 3, region: 'eu', v: 3 }, { ts: 4, region: 'eu', v: 4 }]",
	);
	for (v, nanos) in [(1, 1_000_000), (2, 1_700_000), (3, 2_000_000), (4, 3_000_000)] {
		let params = positional(vec![Value::DateTime(DateTime::from_nanos(nanos)), Value::Int4(v)]);
		let result = t.inner().command_as(TestEngine::identity(), "INSERT test::d [{ ts: $1, v: $2 }]", params);
		assert!(result.error.is_none(), "{:?}", result.error);
	}
	t
}

fn at(nanos: i64) -> Value {
	Value::DateTime(DateTime::from_nanos(nanos))
}

#[test]
fn a_series_parameter_bound_answers_exactly_what_the_plain_filter_answers() {
	// The wrapped filter never narrows the scan, so any difference means a narrowed range dropped or added a row.
	let t = engine();
	let integers = || {
		vec![
			vec![Value::Int4(3)],
			vec![Value::Int8(0)],
			vec![Value::Uint8(6)],
			vec![Value::Int4(-1)],
			vec![Value::Uint8(u64::MAX)],
			vec![Value::Utf8("3".to_string())],
			vec![Value::none()],
			vec![Value::float8(3.5)],
		]
	};
	let cases: Vec<(&str, &str, Vec<Vec<Value>>)> = vec![
		("test::s", "ts > $1", integers()),
		("test::s", "ts >= $1", integers()),
		("test::s", "ts < $1", integers()),
		("test::s", "ts <= $1", integers()),
		("test::s", "$1 < ts", integers()),
		("test::s", "ts between $1 and 4", integers()),
		("test::s", "ts > 2 and ts < $1", integers()),
		("test::u", "k >= $1", vec![vec![Value::Uint8(u64::MAX)], vec![Value::Uint8(u64::MAX - 1)]]),
		("test::u", "k <= $1", vec![vec![Value::Uint8(u64::MAX)], vec![Value::Uint8(u64::MAX - 1)]]),
		(
			"test::d",
			"ts > $1",
			vec![vec![at(1_500_000)], vec![at(1_000_000)], vec![at(2_000_000)], vec![Value::Int4(1)]],
		),
		("test::d", "ts <= $1", vec![vec![at(1_500_000)], vec![at(1_700_000)], vec![at(2_999_999)]]),
		(
			"test::p",
			"region == $1 and ts > $2",
			vec![
				vec![Value::Utf8("eu".to_string()), Value::Int4(1)],
				vec![Value::Utf8("us".to_string()), Value::Int4(0)],
				vec![Value::Utf8("eu".to_string()), Value::Utf8("x".to_string())],
				vec![Value::Int4(3), Value::Int4(1)],
			],
		),
		("test::p", "region == $1 and ts < 4", vec![vec![Value::Utf8("eu".to_string())], vec![Value::none()]]),
	];
	for (source, filter, values) in cases {
		for value in values {
			let narrowed = run(
				&t,
				&format!(
					"FROM {source} | filter {{ {filter} }} | sort {{ v: ASC }} | map {{ out: v }}"
				),
				positional(value.clone()),
			);
			let plain = run(
				&t,
				&format!(
					"FROM {source} | filter {{ ({filter}) == true }} | sort {{ v: ASC }} | map {{ out: v }}"
				),
				positional(value.clone()),
			);
			assert_eq!(narrowed, plain, "{source} {filter} with {value:?}");
		}
	}
}

#[test]
fn a_series_parameter_bound_answers_the_rows_in_its_range() {
	// A narrowed range must still hold every row the bound admits, including rows sharing the floored key.
	let t = engine();
	let before = "FROM test::d | filter { ts < $1 } | sort { v: ASC } | map { out: v }";
	assert_eq!(run(&t, before, positional(vec![at(1_500_000)])), rows(&["1", "2"]));

	let window = "FROM test::s | filter { ts >= $1 and ts < $2 } | sort { v: ASC } | map { out: v }";
	assert_eq!(run(&t, window, positional(vec![Value::Int4(2), Value::Int4(5)])), rows(&["2", "3", "4"]));

	let regional = "FROM test::p | filter { region == $1 and ts > $2 } | sort { v: ASC } | map { out: v }";
	assert_eq!(
		run(&t, regional, positional(vec![Value::Utf8("eu".to_string()), Value::Int4(1)])),
		rows(&["3", "4"])
	);
}

#[test]
fn a_constant_bound_at_the_largest_key_keeps_its_exact_answer() {
	// A bound whose exclusive end would overflow must stay in the filter, never saturate onto the largest key.
	let t = engine();
	let above = "FROM test::u | filter { k > 18446744073709551615 } | sort { v: ASC } | map { out: v }";
	let through = "FROM test::u | filter { k <= 18446744073709551615 } | sort { v: ASC } | map { out: v }";
	let between =
		"FROM test::u | filter { k between 2 and 18446744073709551615 } | sort { v: ASC } | map { out: v }";
	assert_eq!(run(&t, above, Params::None), rows(&[]));
	assert_eq!(run(&t, through, Params::None), rows(&["1", "2", "3"]));
	assert_eq!(run(&t, between, Params::None), rows(&["2", "3"]));
}

#[test]
fn a_series_range_with_no_room_answers_no_rows() {
	// Bounds that exclude each other must answer no rows, never abort the scan on an inverted range.
	let t = engine();
	let constants = "FROM test::s | filter { ts > 5 and ts < 2 } | map { out: v }";
	let parameters = "FROM test::s | filter { ts > 2 and ts < $1 } | map { out: v }";
	assert_eq!(run(&t, constants, Params::None), rows(&[]));
	assert_eq!(run(&t, parameters, positional(vec![Value::Int8(0)])), rows(&[]));
}

#[test]
fn a_series_parameter_bound_plans_the_run_time_series_scan() {
	// Without the run-time node a parameter bound plans a full series scan and the fast path is silently gone.
	let t = engine();
	let plain = explain(&t, "from test::s filter {ts > $1}");
	let regional = explain(&t, "from test::p filter {region == $1 and ts > $2}");

	assert_eq!(
		plain.iter().map(|(kind, _)| kind.as_str()).collect::<Vec<_>>(),
		["Filter", "SeriesParameterScan", "SeriesScan"]
	);
	assert_eq!(plain[1].1, "source=s start=[$1]");
	assert_eq!(
		regional.iter().map(|(kind, _)| kind.as_str()).collect::<Vec<_>>(),
		["Filter", "SeriesParameterScan", "SeriesScan"]
	);
	assert_eq!(regional[1].1, "source=p start=[$2] partition=[region=$1]");
}

fn explain(t: &TestEngine, rql: &str) -> Vec<(String, String)> {
	let result = t.inner().query_as(TestEngine::identity(), &format!("CALL rql::explain(\"{rql}\")"), Params::None);
	assert!(result.error.is_none(), "{:?}", result.error);
	let frame = result.frames.last().expect("a frame");
	let kind = frame.column("kind").unwrap().expect("column kind");
	let detail = frame.column("detail").unwrap().expect("column detail");
	(0..kind.len()).map(|i| (kind.get_value(i).to_string(), detail.get_value(i).to_string())).collect()
}

#[derive(Debug, PartialEq)]
enum Answer {
	Rows(Vec<String>),
	Error(String, String),
}

fn positional(values: Vec<Value>) -> Params {
	Params::Positional(Arc::new(values))
}

fn rows(values: &[&str]) -> Answer {
	Answer::Rows(values.iter().map(|v| v.to_string()).collect())
}

fn run(t: &TestEngine, rql: &str, params: Params) -> Answer {
	let result = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(err) = result.error {
		let diagnostic = err.diagnostic();
		return Answer::Error(diagnostic.code.clone(), diagnostic.message.clone());
	}
	let column = result.frames.last().expect("a frame").column("out").unwrap().expect("column out");
	Answer::Rows((0..column.len()).map(|i| column.get_value(i).to_string()).collect())
}
