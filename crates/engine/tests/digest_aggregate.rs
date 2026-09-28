// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_core::interface::catalog::config::ConfigKey;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, digest::Digest, duration::Duration, frame::frame::Frame, value_type::ValueType},
};

struct Row {
	g: i32,
	f: Option<f64>,
	i: Option<i32>,
	d: Option<(&'static str, i64)>,
	a: i32,
	b: Option<i32>,
	c: Option<i32>,
}

const ROWS: &[Row] = &[
	Row {
		g: 1,
		f: Some(1.5),
		i: Some(3),
		d: Some(("5ms", 5)),
		a: 10,
		b: Some(3),
		c: Some(7),
	},
	Row {
		g: 1,
		f: Some(20.25),
		i: Some(30),
		d: Some(("250ms", 250)),
		a: 100,
		b: Some(1),
		c: Some(99),
	},
	Row {
		g: 1,
		f: Some(300.0),
		i: Some(300),
		d: Some(("25h", 90_000_000)),
		a: 5,
		b: Some(8),
		c: Some(-3),
	},
	Row {
		g: 1,
		f: None,
		i: None,
		d: None,
		a: 6,
		b: None,
		c: None,
	},
	Row {
		g: 2,
		f: Some(7.0),
		i: Some(-40),
		d: Some(("2s", 2_000)),
		a: 4,
		b: Some(1),
		c: Some(3),
	},
	Row {
		g: 2,
		f: Some(7.5),
		i: Some(0),
		d: Some(("1500ms", 1_500)),
		a: 1000,
		b: Some(1),
		c: Some(999),
	},
	Row {
		g: 2,
		f: Some(9000.0),
		i: Some(7),
		d: Some(("90m", 5_400_000)),
		a: -50,
		b: Some(50),
		c: Some(-100),
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

fn create_table(t: &TestEngine) {
	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE TABLE test::t { g: int4, f: Option(float8), i: Option(int4), d: Option(duration), a: int4, b: Option(int4), c: Option(int8) }",
	);
}

fn engine() -> TestEngine {
	let t = TestEngine::new();
	create_table(&t);
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

fn one_row_batches() -> TestEngine {
	let t = engine();
	t.set_config(ConfigKey::QueryRowBatchSize, Value::Uint2(1));
	t
}

fn empty_engine() -> TestEngine {
	let t = TestEngine::new();
	create_table(&t);
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

fn query(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	query_with(t, rql, Params::None)
}

fn query_with(t: &TestEngine, rql: &str, params: Params) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn column_type(frames: &[Frame], name: &str) -> ValueType {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}")).data.get_type()
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

fn assert_groups_match_oracle(rql: &str, inner: ValueType, values: fn(Option<i32>) -> Vec<Value>) {
	let t = engine();
	let frames = query(&t, rql).unwrap();

	let digests = by_group(&frames, "d");
	let reads = by_group(&frames, "p");
	assert_eq!(digests.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3], "every group must come back");
	for (group, digest) in &digests {
		match oracle(inner.clone(), 10_000, &values(Some(*group))) {
			Some(expected) => {
				assert_eq!(reads[group], expected.percentile_value(0.99).unwrap(), "group {group} p99");
				assert_eq!(digest, &Value::Digest(Box::new(expected)), "group {group} digest");
			}
			None => {
				assert!(
					matches!(digest, Value::None { .. }),
					"all-none group {group} must be none, got {digest:?}"
				);
				assert!(
					matches!(reads[group], Value::None { .. }),
					"all-none group {group} must read none"
				);
			}
		}
	}
}

fn with_digests(pairs: &[(&str, Digest)]) -> Params {
	let map: HashMap<String, Value> = pairs
		.iter()
		.map(|(name, digest)| (name.to_string(), Value::Digest(Box::new(digest.clone()))))
		.collect();
	Params::from(map)
}

#[test]
fn a_digest_without_by_equals_the_oracle_and_renders_its_count() {
	// One group over all rows must count every present value once and skip the two none rows.
	let t = engine();

	let frames = query(&t, "FROM test::t | aggregate { d: stats::digest(f, 0.01) }").unwrap();

	let expected = oracle(ValueType::Float8, 10_000, &float_values(None)).unwrap();
	assert_eq!(column_values(&frames, "d"), vec![Value::Digest(Box::new(expected))]);
	assert!(frames[0].to_string().contains("digest(n: 6)"), "rendered:\n{}", frames[0]);
}

#[test]
fn float8_digests_by_group_read_like_the_oracle() {
	// A value routed to the wrong group or a none counted as zero moves p99 or the digest away from the oracle.
	assert_groups_match_oracle(
		"FROM test::t | aggregate { d: stats::digest(f, 0.01) } by { g } | map { g, d, p: stats::approx_percentile(d, 0.99) }",
		ValueType::Float8,
		float_values,
	);
}

#[test]
fn int4_digests_by_group_read_like_the_oracle() {
	// Negative and zero ints must land in the negative store and the zero count, not be dropped or mirrored.
	assert_groups_match_oracle(
		"FROM test::t | aggregate { d: stats::digest(i, 0.01) } by { g } | map { g, d, p: stats::approx_percentile(d, 0.99) }",
		ValueType::Int4,
		int_values,
	);
}

#[test]
fn duration_digests_by_group_read_back_as_duration_including_a_value_over_a_day() {
	// 25h must count as 90,000 s; a days part lost or rejected changes group 1's p99 or fails the query.
	assert_groups_match_oracle(
		"FROM test::t | aggregate { d: stats::digest(d, 0.01) } by { g } | map { g, d, p: stats::approx_percentile(d, 0.99) }",
		ValueType::Duration,
		duration_values,
	);
	let t = engine();
	let frames = query(
		&t,
		"FROM test::t | filter { g == 1 } | aggregate { d: stats::digest(d, 0.01) } | map { p: stats::approx_percentile(d, 1.0) }",
	)
	.unwrap();
	let p = column_values(&frames, "p");
	assert!(matches!(p[0], Value::Duration(_)), "a duration digest must read back as a duration, got {p:?}");
	let hours = Duration::from_hours(25).unwrap().as_nanos().unwrap() as f64;
	let read = match &p[0] {
		Value::Duration(read) => read.as_nanos().unwrap() as f64,
		other => panic!("expected a duration, got {other:?}"),
	};
	assert!((read - hours).abs() <= hours * 0.01, "p100 of group 1 must be within 1% of 25h, got {read} ns");
}

#[test]
fn merging_group_digests_equals_one_digest_built_from_every_raw_value() {
	// Merge must add every bucket of every group, including the none group, without double counting.
	let t = engine();

	let frames = query(
		&t,
		"FROM test::t | aggregate { d: stats::digest(f, 0.01) } by { g } | aggregate { m: stats::digest(d) }",
	)
	.unwrap();

	let expected = oracle(ValueType::Float8, 10_000, &float_values(None)).unwrap();
	assert_eq!(column_values(&frames, "m"), vec![Value::Digest(Box::new(expected))]);
}

#[test]
fn a_digest_over_an_expression_equals_a_digest_over_a_column_holding_it() {
	// c must equal a - b as int8, otherwise the digests differ by input type instead of by the rows read.
	let t = engine();

	let frames = query(
		&t,
		"FROM test::t | aggregate { x: stats::digest(a - b, 0.01), y: stats::digest(c, 0.01) } by { g }",
	)
	.unwrap();

	let x = by_group(&frames, "x");
	let y = by_group(&frames, "y");
	assert_eq!(x, y, "the digest over a - b must equal the digest over c in every group");
	assert!(matches!(&x[&1], Value::Digest(d) if d.count() == 3), "group 1 must count 3 values, got {:?}", x[&1]);
	assert!(matches!(x[&3], Value::None { .. }), "group 3 has b none in every row, so it must be none");
}

#[test]
fn two_accuracies_give_different_answers_that_each_match_their_own_oracle() {
	// Both columns must differ, otherwise the accuracy argument was dropped or defaulted.
	let t = engine();

	let frames = query(
		&t,
		"FROM test::t | aggregate { fine: stats::digest(f, 0.001), coarse: stats::digest(f, 0.1) } | map { fine: stats::approx_percentile(fine, 0.5), coarse: stats::approx_percentile(coarse, 0.5) }",
	)
	.unwrap();

	let fine = oracle(ValueType::Float8, 1_000, &float_values(None)).unwrap().percentile_value(0.5).unwrap();
	let coarse = oracle(ValueType::Float8, 100_000, &float_values(None)).unwrap().percentile_value(0.5).unwrap();
	assert_ne!(fine, coarse, "the chosen values must separate the two accuracies");
	assert_eq!(column_values(&frames, "fine"), vec![fine]);
	assert_eq!(column_values(&frames, "coarse"), vec![coarse]);
}

#[test]
fn an_accuracy_with_a_trailing_zero_builds_the_same_digest_type() {
	// Accuracy is parsed from text, so 0.010 must give 10,000 ppm exactly like 0.01.
	let t = engine();

	let frames = query(&t, "FROM test::t | aggregate { x: stats::digest(f, 0.01), y: stats::digest(f, 0.010) }")
		.unwrap();

	assert_eq!(column_values(&frames, "x"), column_values(&frames, "y"));
	assert_eq!(
		column_type(&frames, "y"),
		ValueType::Digest {
			inner: Box::new(ValueType::Float8),
			accuracy: 10_000
		}
	);
}

#[test]
fn an_accuracy_out_of_range_fails_before_any_row_is_read() {
	// On an empty table no row is ever read, so the error must come from the literal alone.
	let t = empty_engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f, 0.5) } by { g }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_011", "got: {err:?}");
	assert_eq!(err.message, "accuracy must be between 0.001 and 0.1");
	assert_eq!(err.fragment.text(), "0.5", "the error must point at the accuracy literal");
}

#[test]
fn an_accuracy_that_is_not_a_whole_ppm_fails_before_any_row_is_read() {
	// 0.0000005 must never round to a whole ppm and silently build a digest at an accuracy nobody wrote.
	let t = empty_engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f, 0.0000005) } by { g }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_012", "got: {err:?}");
	assert_eq!(err.message, "accuracy must be a whole number of parts per million");
}

#[test]
fn a_text_accuracy_fails_before_any_row_is_read() {
	// A quoted '0.01' must not be parsed as a number the user never wrote as one.
	let t = empty_engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f, '0.01') } by { g }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_013", "got: {err:?}");
	assert_eq!(err.message, "accuracy must be a number");
}

#[test]
fn a_column_accuracy_fails_before_any_row_is_read() {
	// A column accuracy must fail, since a per-row accuracy can never become one digest type.
	let t = empty_engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f, a) } by { g }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_010", "got: {err:?}");
}

#[test]
fn a_negative_accuracy_reports_the_range_error_instead_of_the_literal_error() {
	// -0.5 must report the range error, never the not-a-literal error, since it is written as a literal.
	let t = empty_engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f, -0.5) } by { g }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_011", "got: {err:?}");
	assert_eq!(err.fragment.text(), "-0.5", "the error must point at the whole negated literal");
}

#[test]
fn a_third_argument_fails_before_any_row_is_read() {
	// An extra literal must fail, otherwise a call written for approx_percentile would run silently.
	let t = empty_engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f, 0.5, 0.01) } by { g }").unwrap_err();

	assert_eq!(err.code, "FUNCTION_003", "got: {err:?}");
}

#[test]
fn a_datetime_input_fails_on_the_first_value() {
	// A datetime must fail, since it has no natural zero and a relative error over it has no meaning.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::e { ts: datetime }");
	t.command("INSERT test::e [{ ts: '2024-01-01T00:00:00Z' }]");

	let err = query(&t, "FROM test::e | aggregate { d: stats::digest(ts, 0.01) }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_016", "got: {err:?}");
	assert!(err.message.contains("stats::digest not supported for DateTime"), "got: {err:?}");
}

#[test]
fn a_raw_input_without_accuracy_fails_on_the_first_value() {
	// Without an accuracy there is no digest type to build, so a raw value must not pick a default.
	let t = engine();

	let err = query(&t, "FROM test::t | aggregate { d: stats::digest(f) } by { g }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_014", "got: {err:?}");
	assert_eq!(err.message, "accuracy is required for a non-digest input");
}

#[test]
fn a_digest_input_with_accuracy_fails_on_the_first_value() {
	// A written accuracy must fail for a digest input, since the digest type already fixes the accuracy.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | aggregate { d: stats::digest(f, 0.01) } by { g } | aggregate { m: stats::digest(d, 0.01) }",
	)
	.unwrap_err();

	assert_eq!(err.code, "AGGREGATE_015", "got: {err:?}");
	assert_eq!(err.message, "accuracy comes from the digest type, remove the argument");
}

#[test]
fn merging_digests_with_different_accuracy_fails() {
	// One column can never hold two digest types, so each one-row batch must carry its own.
	let t = one_row_batches();
	let fine = oracle(ValueType::Float8, 10_000, &float_values(Some(1))).unwrap();
	let coarse = oracle(ValueType::Float8, 50_000, &float_values(Some(2))).unwrap();

	let err = query_with(
		&t,
		"FROM test::t | filter { g != 3 } | extend { x: if g == 1 { $fine } else { $coarse } } | aggregate { m: stats::digest(x) }",
		with_digests(&[("fine", fine), ("coarse", coarse)]),
	)
	.unwrap_err();

	assert_eq!(err.code, "AGGREGATE_017", "got: {err:?}");
	assert!(
		[
			"cannot merge Digest(Float8, 0.01) with Digest(Float8, 0.05)",
			"cannot merge Digest(Float8, 0.05) with Digest(Float8, 0.01)"
		]
		.contains(&err.message.as_str()),
		"the message must name both digest types, got: {}",
		err.message
	);
}

#[test]
fn merging_digests_with_different_input_types_fails() {
	// An int4 digest and a float8 digest must not merge, or the output column would hold two digest types.
	let t = one_row_batches();
	let ints = oracle(ValueType::Int4, 10_000, &int_values(Some(1))).unwrap();
	let floats = oracle(ValueType::Float8, 10_000, &float_values(Some(2))).unwrap();

	let err = query_with(
		&t,
		"FROM test::t | filter { g != 3 } | extend { x: if g == 1 { $ints } else { $floats } } | aggregate { m: stats::digest(x) }",
		with_digests(&[("ints", ints), ("floats", floats)]),
	)
	.unwrap_err();

	assert_eq!(err.code, "AGGREGATE_017", "got: {err:?}");
	assert!(
		[
			"cannot merge Digest(Int4, 0.01) with Digest(Float8, 0.01)",
			"cannot merge Digest(Float8, 0.01) with Digest(Int4, 0.01)"
		]
		.contains(&err.message.as_str()),
		"the message must name both digest types, got: {}",
		err.message
	);
}

#[test]
fn a_duration_with_a_month_part_fails_the_query() {
	// A month part must fail, since a month has no fixed length and converting it would change every percentile.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::m { d: duration }");
	t.command("INSERT test::m [{ d: 1d }, { d: 1mo }]");

	let err = query(&t, "FROM test::m | aggregate { x: stats::digest(d, 0.01) }").unwrap_err();

	assert_eq!(err.code, "AGGREGATE_019", "got: {err:?}");
	assert_eq!(err.message, "digest input cannot have a month part");
}

#[test]
fn an_empty_raw_input_gives_a_float8_digest_column() {
	// With no value seen the input type is unknown, so the column must still be a digest with a float8 inner type.
	let t = engine();
	let empty = empty_engine();

	let frames = query(&t, "FROM test::t | filter { g == 3 } | aggregate { d: stats::digest(d, 0.05) }").unwrap();
	let no_rows = query(&empty, "FROM test::t | aggregate { d: stats::digest(d, 0.05) }").unwrap();

	assert_eq!(column_values(&frames, "d").len(), 1, "one group of none values must give one row");
	assert!(matches!(column_values(&frames, "d")[0], Value::None { .. }), "an all-none group must give none");
	let expected = ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 50_000,
	};
	for (ty, input) in [(column_type(&frames, "d"), "an all-none group"), (column_type(&no_rows, "d"), "no rows")] {
		assert!(
			ty == expected || ty == ValueType::Option(Box::new(expected.clone())),
			"{input} must give a digest of float8 at the written accuracy, got {ty:?}"
		);
	}
}

#[test]
fn a_digest_input_with_no_values_gives_none_that_reads_as_none() {
	// An all-none merge must read as none, never fail.
	let t = engine();

	let frames = query(
		&t,
		"FROM test::t | aggregate { d: stats::digest(f, 0.01) } by { g } | filter { g == 3 } | aggregate { m: stats::digest(d) } | map { m, p: stats::approx_percentile(m, 0.5) }",
	)
	.unwrap();

	assert_eq!(column_values(&frames, "m").len(), 1, "one group must come back");
	assert!(matches!(column_values(&frames, "m")[0], Value::None { .. }), "an all-none merge must give none");
	assert!(matches!(column_values(&frames, "p")[0], Value::None { .. }), "reading none must give none");
}

#[test]
fn a_merge_over_an_all_none_group_keeps_the_input_digest_type_and_reads_as_none() {
	// The input column already names the digest type, so dropping it to untyped none would lose the inner type.
	let t = engine();
	let merge = "FROM test::t | aggregate { x: stats::digest(i, 0.02) } by { g } | filter { g == 3 } | aggregate { m: stats::digest(x) }";

	let frames = query(&t, merge).unwrap();
	let read = query(&t, &format!("{merge} | map {{ p: stats::approx_percentile(m, 0.5) }}")).unwrap();

	let expected = ValueType::Digest {
		inner: Box::new(ValueType::Int4),
		accuracy: 20_000,
	};
	let ty = column_type(&frames, "m");
	assert!(
		ty == expected || ty == ValueType::Option(Box::new(expected.clone())),
		"an all-none merge must keep the input digest type, got {ty:?}"
	);
	assert_eq!(column_values(&frames, "m").len(), 1, "the one group must come back");
	assert!(matches!(column_values(&frames, "m")[0], Value::None { .. }), "an all-none merge must give none");
	assert_eq!(column_values(&read, "p").len(), 1, "the one group must be read");
	assert!(matches!(column_values(&read, "p")[0], Value::None { .. }), "reading an empty digest must give none");
}

#[test]
fn a_merge_after_a_filter_that_removes_every_row_keeps_the_input_digest_type() {
	// No row reaches the merge, so the output type must come from the input column, never a default.
	let t = engine();
	let merge = "FROM test::t | aggregate { x: stats::digest(d, 0.03) } by { g } | filter { g == 99 } | aggregate { m: stats::digest(x) }";

	let frames = query(&t, merge).unwrap();
	let read = query(&t, &format!("{merge} | map {{ p: stats::approx_percentile(m, 0.5) }}")).unwrap();

	let expected = ValueType::Digest {
		inner: Box::new(ValueType::Duration),
		accuracy: 30_000,
	};
	let ty = column_type(&frames, "m");
	assert!(
		ty == expected || ty == ValueType::Option(Box::new(expected.clone())),
		"a merge over no rows must keep the input digest type, got {ty:?}"
	);
	assert!(column_values(&frames, "m").is_empty(), "a merge without by over no rows must give no row");
	assert!(column_values(&read, "p").is_empty(), "a read over no digest rows must give no row, not fail");
}
