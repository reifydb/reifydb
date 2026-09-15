// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{
		Value, blob::Blob, date::Date, datetime::DateTime, digest::Digest, duration::Duration,
		frame::frame::Frame, identity::IdentityId, time::Time, uuid::Uuid4, value_type::ValueType,
	},
};

fn query(rql: &str, params: Params) -> Vec<Frame> {
	let t = TestEngine::new();
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(e) = r.error {
		panic!("query failed: {e:?}\nrql: {rql}")
	}
	r.frames
}

fn query_err(rql: &str, params: Params) -> Diagnostic {
	let t = TestEngine::new();
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got columns {:?}\nrql: {rql}", r.frames[0].columns),
	}
}

fn column(frames: &[Frame], name: &str) -> (ValueType, Vec<Value>) {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(column.data.get_type(), (0..column.data.len()).map(|row| column.data.get_value(row)).collect())
}

fn assert_values_between_none_rows_keep_type(first: Value, second: Value, expected: ValueType) {
	// Rows are value, none, value so a type lost on either value or on the none row shows up.
	let params = Params::from(HashMap::from([("a".to_string(), first.clone()), ("b".to_string(), second.clone())]));

	let frames = query("from [{ v: $a }, { v: none }, { v: $b }]", params);

	let (ty, values) = column(&frames, "v");
	assert_eq!(ty, ValueType::Option(Box::new(expected.clone())));
	assert_eq!(values, vec![first, Value::none_of(expected), second]);
}

fn digest_of(values: &[f64]) -> Value {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

#[test]
fn duration_literal_reads_back_as_a_duration_column() {
	// The inferred column must not fall back to a none Boolean for a type outside the numeric and text set.
	let frames = query("from [{ d: duration::hours(25) }]", Params::None);

	let (ty, values) = column(&frames, "d");
	assert_eq!(ty, ValueType::Duration);
	assert_eq!(values, vec![Value::Duration(Duration::from_hours(25).unwrap())]);
}

#[test]
fn datetime_literal_reads_back_as_a_datetime_column() {
	// A temporal literal must keep its instant, not become none.
	let frames = query("from [{ ts: cast('2024-01-02T03:04:05Z', datetime) }]", Params::None);

	let (ty, values) = column(&frames, "ts");
	assert_eq!(ty, ValueType::DateTime);
	assert_eq!(values, vec![Value::DateTime(DateTime::new(2024, 1, 2, 3, 4, 5, 0).unwrap())]);
}

#[test]
fn leading_none_row_does_not_erase_a_later_duration() {
	// The first non-none row decides the type, so a leading none must not turn the column into Boolean.
	let frames = query("from [{ d: none }, { d: duration::hours(1) }]", Params::None);

	let (ty, values) = column(&frames, "d");
	assert_eq!(ty, ValueType::Option(Box::new(ValueType::Duration)));
	assert_eq!(
		values,
		vec![Value::none_of(ValueType::Duration), Value::Duration(Duration::from_hours(1).unwrap())]
	);
}

#[test]
fn inline_duration_feeds_arithmetic_in_a_later_step() {
	// A dropped value would reach the map as none and the sum would silently be none too.
	let frames = query("from [{ d: duration::hours(1) }] map { w: d + duration::hours(1) }", Params::None);

	let (ty, values) = column(&frames, "w");
	assert_eq!(ty, ValueType::Duration);
	assert_eq!(values, vec![Value::Duration(Duration::from_hours(2).unwrap())]);
}

#[test]
fn duration_values_mixed_with_none_keep_their_type() {
	// Durations on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::Duration(Duration::from_minutes(90).unwrap()),
		Value::Duration(Duration::new(1, 2, 3).unwrap()),
		ValueType::Duration,
	);
}

#[test]
fn datetime_values_mixed_with_none_keep_their_type() {
	// Instants on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::DateTime(DateTime::new(2024, 2, 29, 23, 59, 59, 999_999_999).unwrap()),
		Value::DateTime(DateTime::new(1970, 1, 1, 0, 0, 0, 0).unwrap()),
		ValueType::DateTime,
	);
}

#[test]
fn date_values_mixed_with_none_keep_their_type() {
	// Dates on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::Date(Date::new(2024, 2, 29).unwrap()),
		Value::Date(Date::new(1999, 12, 31).unwrap()),
		ValueType::Date,
	);
}

#[test]
fn time_values_mixed_with_none_keep_their_type() {
	// Times on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::Time(Time::new(3, 4, 5, 6).unwrap()),
		Value::Time(Time::new(23, 59, 59, 0).unwrap()),
		ValueType::Time,
	);
}

#[test]
fn uuid4_values_mixed_with_none_keep_their_type() {
	// Uuids on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::Uuid4(Uuid4::generate()),
		Value::Uuid4(Uuid4::generate()),
		ValueType::Uuid4,
	);
}

#[test]
fn uuid7_values_mixed_with_none_keep_their_type() {
	// Uuids on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::Uuid7(IdentityId::root().value()),
		Value::Uuid7(IdentityId::anonymous().value()),
		ValueType::Uuid7,
	);
}

#[test]
fn identity_id_values_mixed_with_none_keep_their_type() {
	// Identities on both sides of a none row must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::IdentityId(IdentityId::root()),
		Value::IdentityId(IdentityId::system()),
		ValueType::IdentityId,
	);
}

#[test]
fn blob_values_mixed_with_none_keep_their_type() {
	// Blobs on both sides of a none row, the empty one included, must not read back as none.
	assert_values_between_none_rows_keep_type(
		Value::Blob(Blob::new(vec![0xde, 0xad, 0xbe, 0xef])),
		Value::Blob(Blob::new(vec![])),
		ValueType::Blob,
	);
}

#[test]
fn digest_values_mixed_with_none_keep_their_type() {
	// The column type must carry the digest inner type and accuracy, not a none Boolean.
	let first = digest_of(&[1.0, 2.0, 3.0]);
	let second = digest_of(&[100.0]);
	let expected = first.get_type();
	assert_eq!(expected, second.get_type());

	assert_values_between_none_rows_keep_type(first, second, expected);
}

#[test]
fn list_values_mixed_with_none_keep_their_type() {
	// A list arrives as an Any value and must stay one instead of becoming none.
	assert_values_between_none_rows_keep_type(
		Value::Any(Box::new(Value::List(vec![Value::int4(1), Value::int4(2)]))),
		Value::Any(Box::new(Value::List(vec![]))),
		ValueType::Any,
	);
}

#[test]
fn typed_none_only_column_keeps_its_declared_type() {
	// A column of typed nones must report that type, not the Boolean used for untyped nones.
	let frames = query("from [{ d: cast(none, duration) }, { d: none }]", Params::None);

	let (ty, values) = column(&frames, "d");
	assert_eq!(ty, ValueType::Option(Box::new(ValueType::Duration)));
	assert_eq!(values, vec![Value::none_of(ValueType::Duration), Value::none_of(ValueType::Duration)]);
}

#[test]
fn untyped_none_only_column_stays_boolean() {
	// With no typed value or typed none to go on, the column keeps the Boolean none convention.
	let frames = query("from [{ v: none }, { v: none }]", Params::None);

	let (ty, values) = column(&frames, "v");
	assert_eq!(ty, ValueType::Option(Box::new(ValueType::Boolean)));
	assert_eq!(values, vec![Value::none_of(ValueType::Boolean), Value::none_of(ValueType::Boolean)]);
}

#[test]
fn integer_column_with_an_uncastable_text_row_is_an_error_with_its_fragment() {
	// A row that cannot take the column type must fail loud instead of reading back as none.
	let err = query_err("from [{ v: 1 }, { v: 'abc' }]", Params::None);

	assert_eq!(err.fragment.text(), "abc", "got: {err:?}");
}

#[test]
fn uuid_column_with_an_uncastable_text_row_is_an_error_with_its_fragment() {
	// A kept uuid type must not turn a text row it cannot parse into a silent none.
	let err = query_err("from [{ v: uuid::v4() }, { v: 'not-a-uuid' }]", Params::None);

	assert_eq!(err.fragment.text(), "not-a-uuid", "got: {err:?}");
}

#[test]
fn integer_column_with_an_int_row_too_large_for_int16_is_an_error_with_its_fragment() {
	// An arbitrary precision int beyond the widest fixed integer must not vanish into a none.
	let err =
		query_err("from [{ v: 1 }, { v: cast('170141183460469231731687303715884105728', int) }]", Params::None);

	assert!(err.fragment.text().contains("170141183460469231731687303715884105728"), "got: {err:?}");
}

#[test]
fn digest_rows_with_different_accuracy_are_an_error_with_the_second_fragment() {
	// Two digests of different accuracy cannot share a column, so the second must fail loud, not become none.
	let mut other = Digest::new(ValueType::Float8, 20_000).unwrap();
	other.add_value(&Value::float8(2.0)).unwrap();
	let params = Params::from(HashMap::from([
		("a".to_string(), digest_of(&[1.0])),
		("b".to_string(), Value::Digest(Box::new(other))),
	]));

	let err = query_err("from [{ v: $a }, { v: $b }]", params);

	assert_eq!(err.fragment.text(), "$b", "got: {err:?}");
}

#[test]
fn integer_rows_that_do_not_fit_a_narrow_type_keep_the_wide_type() {
	// Narrowing picks the smallest type holding every row, so a large row must keep a wide type and its value.
	let frames = query("from [{ v: 1 }, { v: 100000 }]", Params::None);
	let (ty, values) = column(&frames, "v");
	assert_eq!(ty, ValueType::Int4);
	assert_eq!(values, vec![Value::Int4(1), Value::Int4(100_000)]);

	let params = Params::from(HashMap::from([
		("a".to_string(), Value::Int1(1)),
		("b".to_string(), Value::Int16(i128::MAX)),
	]));
	let frames = query("from [{ v: $a }, { v: $b }]", params);
	let (ty, values) = column(&frames, "v");
	assert_eq!(ty, ValueType::Int16);
	assert_eq!(values, vec![Value::Int16(1), Value::Int16(i128::MAX)]);
}

