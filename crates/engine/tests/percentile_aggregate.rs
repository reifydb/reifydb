// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{common::CommitVersion, interface::catalog::config::ConfigKey, metrics::heap::HeapSize};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{Value, digest::Digest, duration::Duration, frame::frame::Frame, value_type::ValueType};

struct Row {
	g: i32,
	f: Option<f64>,
	i: Option<i32>,
	d: Option<(&'static str, i64)>,
	a: i32,
	b: Option<i32>,
	c: Option<i64>,
}

const ROWS: &[Row] = &[
	Row {
		g: 1,
		f: Some(1.0),
		i: Some(3),
		d: Some(("5ms", 5)),
		a: 10,
		b: Some(3),
		c: Some(7),
	},
	Row {
		g: 1,
		f: Some(1.05),
		i: Some(30),
		d: Some(("250ms", 250)),
		a: 100,
		b: Some(1),
		c: Some(99),
	},
	Row {
		g: 1,
		f: Some(1.1),
		i: Some(300),
		d: Some(("25h", 90_000_000)),
		a: 5,
		b: Some(8),
		c: Some(-3),
	},
	Row {
		g: 1,
		f: Some(5.0),
		i: None,
		d: None,
		a: 6,
		b: None,
		c: None,
	},
	Row {
		g: 1,
		f: Some(100.0),
		i: Some(-40),
		d: Some(("1500ms", 1_500)),
		a: 4,
		b: Some(1),
		c: Some(3),
	},
	Row {
		g: 2,
		f: Some(2.5),
		i: Some(0),
		d: Some(("2s", 2_000)),
		a: 1000,
		b: Some(1),
		c: Some(999),
	},
	Row {
		g: 2,
		f: Some(2.6),
		i: Some(7),
		d: Some(("90m", 5_400_000)),
		a: -50,
		b: Some(50),
		c: Some(-100),
	},
	Row {
		g: 2,
		f: Some(40.0),
		i: Some(8),
		d: Some(("2100ms", 2_100)),
		a: 17,
		b: Some(2),
		c: Some(15),
	},
	Row {
		g: 3,
		f: None,
		i: None,
		d: None,
		a: 1,
		b: None,
		c: None,
	},
];

fn text<T: ToString>(value: Option<T>) -> String {
	value.map_or_else(|| "none".to_string(), |v| v.to_string())
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE TABLE test::t { g: int4, f: Option(float8), i: Option(int4), d: Option(duration), a: int4, b: Option(int4), c: Option(int8) }",
	);
	let rows: Vec<String> = ROWS
		.iter()
		.map(|row| {
			format!(
				"{{ g: {}, f: {}, i: {}, d: {}, a: {}, b: {}, c: {} }}",
				row.g,
				text(row.f),
				text(row.i),
				text(row.d.map(|(literal, _)| literal)),
				row.a,
				text(row.b),
				text(row.c)
			)
		})
		.collect();
	t.command(&format!("INSERT test::t [{}]", rows.join(", ")));
	t
}

fn in_group(row: &Row, group: Option<i32>) -> bool {
	group.is_none_or(|g| row.g == g)
}

fn float_values(group: Option<i32>) -> Vec<Value> {
	ROWS.iter().filter(|row| in_group(row, group)).filter_map(|row| row.f.map(Value::float8)).collect()
}

fn int_values(group: Option<i32>) -> Vec<Value> {
	ROWS.iter().filter(|row| in_group(row, group)).filter_map(|row| row.i.map(Value::Int4)).collect()
}

fn duration_values(group: Option<i32>) -> Vec<Value> {
	ROWS.iter()
		.filter(|row| in_group(row, group))
		.filter_map(|row| row.d.map(|(_, ms)| Value::Duration(Duration::from_milliseconds(ms).unwrap())))
		.collect()
}

fn difference_values(group: Option<i32>) -> Vec<Value> {
	ROWS.iter().filter(|row| in_group(row, group)).filter_map(|row| row.c.map(Value::Int8)).collect()
}

fn oracle(inner: ValueType, accuracy: u32, values: &[Value]) -> Option<Digest> {
	if values.is_empty() {
		return None;
	}
	let mut digest = Digest::new(inner, accuracy).unwrap();
	for value in values {
		digest.add_value(value).unwrap();
	}
	Some(digest)
}

fn expected_read(inner: ValueType, accuracy: u32, values: &[Value], p: f64) -> Option<Value> {
	oracle(inner, accuracy, values).map(|digest| digest.percentile_value(p).unwrap())
}

fn column_names(frames: &[Frame]) -> Vec<String> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	frames[0].columns.iter().map(|c| c.name.clone()).collect()
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn by_group(frames: &[Frame], name: &str) -> BTreeMap<i32, Value> {
	let groups = column_values(frames, "g");
	let values = column_values(frames, name);
	groups.into_iter()
		.zip(values)
		.map(|(group, value)| match group {
			Value::Int4(g) => (g, value),
			other => panic!("group key {other:?} is not an int4"),
		})
		.collect()
}

fn assert_read(actual: &Value, expected: Option<Value>, what: &str) {
	match expected {
		Some(expected) => assert_eq!(actual, &expected, "{what}"),
		None => assert!(matches!(actual, Value::None { .. }), "{what} must be none, got {actual:?}"),
	}
}

fn assert_p50_and_p99_by_group_match_the_oracle(input: &str, inner: ValueType, values: fn(Option<i32>) -> Vec<Value>) {
	let t = engine();
	let frames = t.query(&format!(
		"FROM test::t | aggregate {{ p50: stats::approx_percentile({input}, 0.5, 0.01), p99: stats::approx_percentile({input}, 0.99, 0.01) }} by {{ g }}"
	));

	let p50 = by_group(&frames, "p50");
	let p99 = by_group(&frames, "p99");
	assert_eq!(p50.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3], "every group must come back");
	for group in [1, 2, 3] {
		let group_values = values(Some(group));
		assert_read(
			&p50[&group],
			expected_read(inner.clone(), 10_000, &group_values, 0.5),
			&format!("group {group} p50 of {input}"),
		);
		assert_read(
			&p99[&group],
			expected_read(inner.clone(), 10_000, &group_values, 0.99),
			&format!("group {group} p99 of {input}"),
		);
	}

	let all = t.query(&format!(
		"FROM test::t | aggregate {{ p50: stats::approx_percentile({input}, 0.5, 0.01), p99: stats::approx_percentile({input}, 0.99, 0.01) }}"
	));
	assert_eq!(column_values(&all, "p50"), vec![expected_read(inner.clone(), 10_000, &values(None), 0.5).unwrap()]);
	assert_eq!(column_values(&all, "p99"), vec![expected_read(inner, 10_000, &values(None), 0.99).unwrap()]);
}

#[test]
fn float8_percentiles_with_and_without_by_equal_the_oracle() {
	// A row routed to the wrong group or a none counted as a value moves a read away from the oracle.
	assert_p50_and_p99_by_group_match_the_oracle("f", ValueType::Float8, float_values);
}

#[test]
fn int4_percentiles_with_and_without_by_equal_the_oracle() {
	// Negative and zero ints must reach the digest as values, and the read must come back as float8.
	assert_p50_and_p99_by_group_match_the_oracle("i", ValueType::Int4, int_values);
	let t = engine();
	let frames = t.query("FROM test::t | aggregate { p: stats::approx_percentile(i, 0.5, 0.01) }");
	assert!(matches!(column_values(&frames, "p")[0], Value::Float8(_)), "an int read must be float8");
}

#[test]
fn duration_percentiles_with_and_without_by_read_back_as_duration() {
	// A duration read as float8 nanoseconds would lose its unit, so the oracle compare pins the type too.
	assert_p50_and_p99_by_group_match_the_oracle("d", ValueType::Duration, duration_values);
	let t = engine();
	let frames = t.query("FROM test::t | aggregate { p: stats::approx_percentile(d, 1, 0.01) }");
	assert!(matches!(column_values(&frames, "p")[0], Value::Duration(_)), "a duration read must be a duration");
}

#[test]
fn p_and_accuracy_each_change_the_answer() {
	// Every pair of columns must differ, so a dropped or defaulted p or accuracy fails this test.
	let t = engine();

	let frames = t.query(
		"FROM test::t | filter { g == 1 } | aggregate { fine_p40: stats::approx_percentile(f, 0.4, 0.01), fine_p60: stats::approx_percentile(f, 0.6, 0.01), coarse_p40: stats::approx_percentile(f, 0.4, 0.1) }",
	);

	let values = float_values(Some(1));
	let fine_p40 = expected_read(ValueType::Float8, 10_000, &values, 0.4).unwrap();
	let fine_p60 = expected_read(ValueType::Float8, 10_000, &values, 0.6).unwrap();
	let coarse_p40 = expected_read(ValueType::Float8, 100_000, &values, 0.4).unwrap();
	assert_ne!(fine_p40, fine_p60, "the chosen p values must separate the answers");
	assert_ne!(fine_p40, coarse_p40, "the chosen accuracies must separate the answers");
	assert_eq!(column_values(&frames, "fine_p40"), vec![fine_p40]);
	assert_eq!(column_values(&frames, "fine_p60"), vec![fine_p60]);
	assert_eq!(column_values(&frames, "coarse_p40"), vec![coarse_p40]);
}

#[test]
fn a_percentile_over_an_expression_equals_one_over_a_column_holding_it() {
	// c holds a - b, so any row the expression input drops or misreads shows up as a different read.
	let t = engine();

	let frames = t.query(
		"FROM test::t | aggregate { x: stats::approx_percentile(a - b, 0.9, 0.01), y: stats::approx_percentile(c, 0.9, 0.01) } by { g }",
	);

	let x = by_group(&frames, "x");
	let y = by_group(&frames, "y");
	assert_eq!(x, y, "the read over a - b must equal the read over c in every group");
	for group in [1, 2] {
		assert_eq!(
			x[&group],
			expected_read(ValueType::Int8, 10_000, &difference_values(Some(group)), 0.9).unwrap(),
			"group {group}"
		);
	}
	assert!(matches!(x[&3], Value::None { .. }), "a group with b none in every row must read none");
}

#[test]
fn the_merge_form_over_a_prior_aggregates_digests_equals_the_oracle() {
	// Merging per-group digests must count every value once, so its reads equal one digest over all rows.
	let t = engine();

	let merged = t.query(
		"FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | aggregate { p50: stats::approx_percentile(lat, 0.5), p99: stats::approx_percentile(lat, 0.99) }",
	);
	let regrouped = t.query(
		"FROM test::t | aggregate { lat: stats::digest(d, 0.01) } by { g } | aggregate { p50: stats::approx_percentile(lat, 0.5), p99: stats::approx_percentile(lat, 0.99) } by { g }",
	);

	let all = duration_values(None);
	assert_eq!(column_values(&merged, "p50"), vec![expected_read(ValueType::Duration, 10_000, &all, 0.5).unwrap()]);
	assert_eq!(
		column_values(&merged, "p99"),
		vec![expected_read(ValueType::Duration, 10_000, &all, 0.99).unwrap()]
	);
	let p99 = by_group(&regrouped, "p99");
	for group in [1, 2, 3] {
		assert_read(
			&p99[&group],
			expected_read(ValueType::Duration, 10_000, &duration_values(Some(group)), 0.99),
			&format!("merged group {group} p99"),
		);
	}
}

#[test]
fn a_percentile_read_combines_with_other_aggregates_in_one_map_entry() {
	// The read must be an ordinary expression over the slot, so arithmetic around it keeps working per group.
	let t = engine();

	let frames = t.query(
		"FROM test::t | aggregate { spread: stats::approx_percentile(i, 1, 0.01) - stats::approx_percentile(i, 0, 0.01), n: math::count(i) } by { g }",
	);

	let spread = by_group(&frames, "spread");
	let count = by_group(&frames, "n");
	let values = int_values(Some(1));
	let high = match expected_read(ValueType::Int4, 10_000, &values, 1.0).unwrap() {
		Value::Float8(v) => v.value(),
		other => panic!("expected a float8 read, got {other:?}"),
	};
	let low = match expected_read(ValueType::Int4, 10_000, &values, 0.0).unwrap() {
		Value::Float8(v) => v.value(),
		other => panic!("expected a float8 read, got {other:?}"),
	};
	assert_eq!(spread[&1], Value::float8(high - low));
	assert_eq!(count[&1], Value::Int8(4), "the count next to the reads must still count group 1");
}

#[test]
fn slot_columns_never_reach_the_output_and_never_clash_with_user_columns() {
	// User columns and aliases named like slots must read their own data, and no slot column may leak out.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::s { __aggregate0: float8, __aggregate1: float8 }");
	t.command(
		"INSERT test::s [{ __aggregate0: 1.0, __aggregate1: 10.0 }, { __aggregate0: 2.0, __aggregate1: 20.0 }, { __aggregate0: 4.0, __aggregate1: 40.0 }]",
	);

	let frames = t.query(
		"FROM test::s | aggregate { __aggregate0: stats::approx_percentile(__aggregate1, 0.99, 0.01), __aggregate1: math::sum(__aggregate0), p50: stats::approx_percentile(__aggregate1, 0.5, 0.01) }",
	);

	let tens = [10.0, 20.0, 40.0].map(Value::float8);
	assert_eq!(column_names(&frames), vec!["__aggregate0", "__aggregate1", "p50"]);
	assert_eq!(
		column_values(&frames, "__aggregate0"),
		vec![expected_read(ValueType::Float8, 10_000, &tens, 0.99).unwrap()]
	);
	assert_eq!(column_values(&frames, "__aggregate1"), vec![Value::float8(7.0)]);
	assert_eq!(column_values(&frames, "p50"), vec![expected_read(ValueType::Float8, 10_000, &tens, 0.5).unwrap()]);

	let grouped = engine().query(
		"FROM test::t | aggregate { p50: stats::approx_percentile(f, 0.5, 0.01), p99: stats::approx_percentile(f, 0.99, 0.01), total: math::sum(a) } by { g }",
	);
	assert_eq!(column_names(&grouped), vec!["g", "p50", "p99", "total"]);
}

fn spread_values() -> Vec<i64> {
	(0..4000).map(|k| (1000.0 * 1.005f64.powi(k)) as i64).collect()
}

fn wide_engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::w { v: int8 }");
	let rows: Vec<String> = spread_values().iter().map(|v| format!("{{ v: {v} }}")).collect();
	t.command(&format!("INSERT test::w [{}]", rows.join(", ")));
	t
}

fn set_query_memory_limit(t: &TestEngine, bytes: usize) {
	t.inner()
		.catalog()
		.cache()
		.set_config(ConfigKey::QueryMemoryLimit, CommitVersion(1), Value::Uint8(bytes as u64))
		.expect("failed to set QUERY_MEMORY_LIMIT");
}

fn one_digest_heap_size() -> usize {
	let values: Vec<Value> = spread_values().into_iter().map(Value::Int8).collect();
	let digest = oracle(ValueType::Int8, 1_000, &values).unwrap();
	assert_eq!(digest.bucket_count(), 4000, "every value must occupy its own bucket to make the state large");
	digest.heap_size()
}

fn wide_reads(values: &[i64]) -> (Value, Value) {
	let values: Vec<Value> = values.iter().copied().map(Value::Int8).collect();
	(
		expected_read(ValueType::Int8, 1_000, &values, 0.5).unwrap(),
		expected_read(ValueType::Int8, 1_000, &values, 0.99).unwrap(),
	)
}

#[test]
fn p50_and_p99_on_one_column_charge_query_memory_for_one_digest() {
	// Answers cannot tell one shared digest from two equal ones, so the charged state must fit only one digest.
	let t = wide_engine();
	let one = one_digest_heap_size();
	set_query_memory_limit(&t, one + one / 2);

	let frames = t.query(
		"FROM test::w | aggregate { p50: stats::approx_percentile(v, 0.5, 0.001), p99: stats::approx_percentile(v, 0.99, 0.001) }",
	);

	let (p50, p99) = wide_reads(&spread_values());
	assert_eq!(column_values(&frames, "p50"), vec![p50]);
	assert_eq!(column_values(&frames, "p99"), vec![p99]);
	let err = t.query_err(
		"FROM test::w | aggregate { p50: stats::approx_percentile(v, 0.5, 0.001), p99: stats::approx_percentile(v, 0.99, 0.002) }",
	);
	assert!(err.contains("QUERY_006"), "two accuracies on one column must build two digests, got: {err}");
}

#[test]
fn p50_and_p99_on_one_expression_charge_query_memory_for_two_digests() {
	// An expression input is never matched, so the same pair over v * 1 must need room for two digests.
	let t = wide_engine();
	let one = one_digest_heap_size();
	let pair = "FROM test::w | aggregate { p50: stats::approx_percentile(v * 1, 0.5, 0.001), p99: stats::approx_percentile(v * 1, 0.99, 0.001) }";

	set_query_memory_limit(&t, one + one / 2);
	let err = t.query_err(pair);
	assert!(err.contains("QUERY_006"), "two digests must not fit the room of one and a half, got: {err}");

	set_query_memory_limit(&t, 2 * one + one / 2);
	let frames = t.query(pair);
	let (p50, p99) = wide_reads(&spread_values());
	assert_eq!(column_values(&frames, "p50"), vec![p50]);
	assert_eq!(column_values(&frames, "p99"), vec![p99]);
}
