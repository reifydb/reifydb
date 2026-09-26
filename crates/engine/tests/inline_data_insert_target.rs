// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE ENUM s::status { Active, Inactive }");
	t.admin("CREATE TABLE s::t { a: int4 }");
	t.admin("CREATE RINGBUFFER s::r { a: int4 } WITH { capacity: 10 }");
	t.admin("CREATE SERIES s::x { ts: datetime, val: float8 } WITH { key: ts }");
	t.admin("CREATE SERIES s::m { ts: datetime, val: int4 } WITH { key: ts, tag: s::status }");
	t
}

fn command_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
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

fn assert_column_not_found(err: &Diagnostic, name: &str) {
	assert_eq!(err.code, "QUERY_001", "got: {err:?}");
	assert_eq!(err.fragment.text(), name, "got: {err:?}");
}

#[test]
fn series_insert_with_an_integer_in_a_column_not_in_the_series_is_column_not_found() {
	// A value for a column the series does not have must fail loud instead of being dropped.
	let t = engine();

	let err = command_err(&t, "INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: 5 }]");

	assert_column_not_found(&err, "extra");
	assert!(t.query("FROM s::x").iter().all(|f| f.columns.iter().all(|c| c.data.is_empty())));
}

#[test]
fn series_insert_with_a_duration_in_a_column_not_in_the_series_is_column_not_found() {
	// A non integer value in an unknown column must be the same error, never a panic in the column buffer.
	let t = engine();

	let err = command_err(
		&t,
		"INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: duration::hours(1) }]",
	);

	assert_column_not_found(&err, "extra");
}

#[test]
fn series_insert_with_text_in_a_column_not_in_the_series_is_column_not_found() {
	// Text in an unknown column must be the same error, never a panic in the column buffer.
	let t = engine();

	let err =
		command_err(&t, "INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: 'abc' }]");

	assert_column_not_found(&err, "extra");
}

#[test]
fn tagged_series_insert_with_a_numeric_tag_stores_that_tag() {
	// The tag pseudo column is the one name outside the schema a tagged series must accept.
	let t = engine();

	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1 }]");

	assert_eq!(column_values(&t.query("FROM s::m"), "tag"), vec![Value::Uint1(1)]);
}

#[test]
fn tagged_series_insert_with_a_text_tag_is_an_error_with_its_fragment() {
	// A tag that is not a number must fail loud at the value, never panic in the column buffer.
	let t = engine();

	let err = command_err(&t, "INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 'abc' }]");

	assert_eq!(err.fragment.text(), "abc", "got: {err:?}");
}

#[test]
fn tagged_series_insert_with_a_variant_name_stores_that_variant_tag() {
	// Inactive is the second variant, so writing the default tag 0 silently files the row under Active.
	let t = engine();

	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: Inactive }]");

	assert_eq!(column_values(&t.query("FROM s::m"), "tag"), vec![Value::Uint1(1)]);
}

#[test]
fn tagged_series_insert_with_a_tag_that_is_no_variant_is_an_error() {
	// The enum has tags 0 and 1, so 5, 200 and -1 must fail loud instead of storing 5, 0 and 255.
	let t = engine();

	for tag in ["5", "200", "-1"] {
		command_err(
			&t,
			&format!("INSERT s::m [{{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: {tag} }}]"),
		);
	}
}

#[test]
fn table_insert_with_a_column_not_in_the_table_is_column_not_found() {
	// A value for a column the table does not have must fail loud instead of being dropped.
	let t = engine();

	let err = command_err(&t, "INSERT s::t [{ a: 1, extra: 5 }]");

	assert_column_not_found(&err, "extra");
}

#[test]
fn ringbuffer_insert_with_a_column_not_in_the_ringbuffer_is_column_not_found() {
	// A value for a column the ring buffer does not have must fail loud instead of being dropped.
	let t = engine();

	let err = command_err(&t, "INSERT s::r [{ a: 1, extra: 5 }]");

	assert_column_not_found(&err, "extra");
}

fn command_ok(t: &TestEngine, rql: &str) -> Vec<Frame> {
	let r = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	if let Some(e) = r.error {
		panic!("command failed: {:?}\nrql: {rql}", e.diagnostic())
	}
	r.frames
}

fn row_count(t: &TestEngine, rql: &str) -> usize {
	t.query(rql).iter().map(|f| f.columns.first().map_or(0, |c| c.data.len())).sum()
}

fn assert_diagnostic(err: &Diagnostic, code: &str, fragment: &str) {
	assert_eq!(err.code, code, "got: {err:?}");
	assert_eq!(err.fragment.text(), fragment, "got: {err:?}");
}

#[test]
fn queue_insert_with_a_column_not_in_the_queue_is_column_not_found() {
	// A value for a column the queue does not have must fail loud instead of being dropped.
	let t = engine();
	t.admin("CREATE QUEUE s::q { a: int4 } WITH { fifo: {} }");

	let err = command_err(&t, "INSERT s::q [{ a: 1, extra: 5 }]");

	assert_column_not_found(&err, "extra");
}

#[test]
fn table_insert_from_a_query_with_a_column_not_in_the_table_is_column_not_found() {
	// A query source is not inline data, so its extra column needs the same check at the write target.
	let t = engine();
	t.admin("CREATE TABLE s::src { a: int4, extra: int4 }");
	t.command("INSERT s::src [{ a: 1, extra: 5 }]");

	let err = command_err(&t, "INSERT s::t FROM s::src");

	assert_column_not_found(&err, "extra");
	assert_eq!(row_count(&t, "FROM s::t"), 0);
}

#[test]
fn ringbuffer_insert_from_a_query_with_a_column_not_in_the_ringbuffer_is_column_not_found() {
	// A query source extra column must fail loud at the ring buffer instead of being dropped.
	let t = engine();
	t.admin("CREATE TABLE s::src { a: int4, extra: int4 }");
	t.command("INSERT s::src [{ a: 1, extra: 5 }]");

	let err = command_err(&t, "INSERT s::r FROM s::src");

	assert_column_not_found(&err, "extra");
	assert_eq!(row_count(&t, "FROM s::r"), 0);
}

#[test]
fn queue_insert_from_a_query_with_a_column_not_in_the_queue_is_column_not_found() {
	// A query source extra column must fail loud at the queue instead of being dropped.
	let t = engine();
	t.admin("CREATE QUEUE s::q { a: int4 } WITH { fifo: {} }");
	t.admin("CREATE TABLE s::src { a: int4, extra: int4 }");
	t.command("INSERT s::src [{ a: 1, extra: 5 }]");

	let err = command_err(&t, "INSERT s::q FROM s::src");

	assert_column_not_found(&err, "extra");
}

#[test]
fn series_insert_from_a_query_with_a_column_not_in_the_series_is_column_not_found() {
	// A query source extra column must fail loud at the series instead of being dropped.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: datetime, val: float8, extra: int4 }");
	t.command("INSERT s::src [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0, extra: 5 }]");

	let err = command_err(&t, "INSERT s::x FROM s::src");

	assert_column_not_found(&err, "extra");
	assert_eq!(row_count(&t, "FROM s::x"), 0);
}

#[test]
fn untagged_series_insert_from_a_tagged_series_is_column_not_found() {
	// The tag pseudo column is only valid for a tagged series, so copying it into an untagged one must fail loud.
	let t = engine();
	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1 }]");

	let err = command_err(&t, "INSERT s::x FROM s::m");

	assert_column_not_found(&err, "tag");
}

#[test]
fn view_insert_is_an_error_with_the_view_fragment() {
	// A view has no storage of its own, so an insert must fail loud instead of reporting rows inserted.
	let t = engine();
	t.admin("CREATE TABLE s::src { a: int4 }");
	t.admin("CREATE DEFERRED VIEW s::v { a: int4 } AS { FROM s::src | map { a } }");

	let err = command_err(&t, "INSERT s::v [{ a: 1 }]");

	assert_diagnostic(&err, "CA_004", "v");
}

#[test]
fn table_update_with_a_column_not_in_the_table_is_column_not_found() {
	// An update of a column the table does not have must fail loud instead of reporting the row updated.
	let t = engine();
	t.command("INSERT s::t [{ a: 1 }]");

	let err = command_err(&t, "UPDATE s::t { extra: 5 } FILTER { a == 1 }");

	assert_column_not_found(&err, "extra");
}

#[test]
fn ringbuffer_update_with_a_column_not_in_the_ringbuffer_is_column_not_found() {
	// An update of a column the ring buffer does not have must fail loud instead of reporting the row updated.
	let t = engine();
	t.command("INSERT s::r [{ a: 1 }]");

	let err = command_err(&t, "UPDATE s::r { extra: 5 } FILTER { a == 1 }");

	assert_column_not_found(&err, "extra");
}

#[test]
fn series_update_with_a_column_not_in_the_series_is_column_not_found() {
	// An update of a column the series does not have must fail loud instead of reporting the row updated.
	let t = engine();
	t.command("INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1.0 }]");

	let err = command_err(&t, "UPDATE s::x { extra: 5 } FILTER { val == 1.0 }");

	assert_column_not_found(&err, "extra");
}

#[test]
fn tagged_series_insert_with_an_unknown_variant_name_is_an_error_with_its_fragment() {
	// A name that is no variant must fail loud instead of falling back to the default tag 0.
	let t = engine();

	let err = command_err(&t, "INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: Nope }]");

	assert_diagnostic(&err, "CA_100", "Nope");
	assert_eq!(row_count(&t, "FROM s::m"), 0);
}

#[test]
fn tagged_series_insert_with_a_numeric_tag_that_is_no_variant_reports_that_tag() {
	// 5, 200 and -1 are no declared tag, and the error must point at the value, not only fail.
	let t = engine();

	for tag in ["5", "200", "-1"] {
		let err = command_err(
			&t,
			&format!("INSERT s::m [{{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: {tag} }}]"),
		);
		assert_eq!(err.code, "CA_100", "tag {tag}, got: {err:?}");
		assert!(err.fragment.text().contains(tag), "tag {tag}, got: {err:?}");
	}
	assert_eq!(row_count(&t, "FROM s::m"), 0);
}

#[test]
fn tagged_series_insert_with_a_qualified_variant_stores_that_variant_tag() {
	// The qualified form must resolve like the bare name, never panic or store the default tag 0.
	let t = engine();

	command_ok(
		&t,
		"INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: s::status::Inactive }]",
	);

	assert_eq!(column_values(&t.query("FROM s::m"), "tag"), vec![Value::Uint1(1)]);
}

#[test]
fn tagged_series_insert_with_a_variant_of_another_enum_is_an_error() {
	// s::other::Y has tag 1 like Inactive, so accepting it would file the row under the wrong enum.
	let t = engine();
	t.admin("CREATE ENUM s::other { X, Y }");

	let err = command_err(
		&t,
		"INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: s::other::Y }]",
	);

	assert_diagnostic(&err, "CA_100", "s::other::Y");
}

#[test]
fn tagged_series_insert_with_a_variant_of_an_unknown_namespace_is_an_error() {
	// A qualified variant must resolve its namespace, not match on the enum and variant name alone.
	let t = engine();

	let err = command_err(
		&t,
		"INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: nope::status::Inactive }]",
	);

	assert_diagnostic(&err, "CA_002", "nope");
}

#[test]
fn tagged_series_insert_with_a_variant_name_in_a_data_column_is_column_not_found() {
	// Only the tag resolves variant names, so a variant name in val must not be stored as its tag number.
	let t = engine();

	let err = command_err(&t, "INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: Inactive }]");

	assert_column_not_found(&err, "Inactive");
}

#[test]
fn tagged_series_insert_from_a_query_with_a_tag_that_is_no_variant_is_an_error() {
	// A tag read from a query must pass the same variant check as an inline tag, or 7 is stored as is.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: datetime, val: int4, tag: uint1 }");
	t.command("INSERT s::src [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 7 }]");

	let err = command_err(&t, "INSERT s::m FROM s::src");

	assert_diagnostic(&err, "CA_100", "7");
	assert_eq!(row_count(&t, "FROM s::m"), 0);
}

#[test]
fn tag_filter_beyond_the_tag_range_matches_no_row() {
	// 257 and -255 must not wrap to tag 1 when the filter is pushed down to the tag byte.
	let t = engine();
	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1 }]");

	assert_eq!(row_count(&t, "FROM s::m FILTER { tag == 1 }"), 1);
	assert_eq!(row_count(&t, "FROM s::m FILTER { tag == 257 }"), 0);
	assert_eq!(row_count(&t, "FROM s::m FILTER { tag == -255 }"), 0);
}

#[test]
fn series_update_of_the_tag_does_not_silently_update_nothing() {
	// The tag is part of the row key, so an update of it must move the row or fail loud, never update nothing.
	let t = engine();
	t.command("INSERT s::m [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 1, tag: 1 }]");

	let err = command_err(&t, "UPDATE s::m { tag: 0 } FILTER { val == 1 }");

	assert_diagnostic(&err, "UPDATE_004", "0");
}

#[test]
fn series_insert_without_a_value_for_a_non_optional_column_is_an_error() {
	// val is not optional, so a missing value must fail loud as for tables instead of storing 0.
	let t = engine();

	let err = command_err(&t, "INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime) }]");

	assert_eq!(err.code, "CONSTRAINT_007", "got: {err:?}");
	assert_eq!(row_count(&t, "FROM s::x"), 0);
}

#[test]
fn table_insert_with_an_unknown_unit_variant_is_an_error_with_its_fragment() {
	// A name that is no variant of the column enum must fail loud and name the value.
	let t = engine();
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");

	let err = command_err(&t, "INSERT s::e [{ id: 1, status: Nope }]");

	assert_diagnostic(&err, "CA_100", "Nope");
}

#[test]
fn table_insert_with_an_unknown_qualified_variant_is_an_error_with_its_fragment() {
	// A qualified variant that the enum does not declare must fail loud, never panic.
	let t = engine();
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");

	let err = command_err(&t, "INSERT s::e [{ id: 1, status: s::status::Nope }]");

	assert_diagnostic(&err, "CA_100", "Nope");
}

#[test]
fn table_insert_with_a_variant_of_another_enum_is_an_error() {
	// s::other::Y has tag 1 like Inactive, so accepting it would store a variant of the wrong enum.
	let t = engine();
	t.admin("CREATE ENUM s::other { X, Y }");
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");

	let err = command_err(&t, "INSERT s::e [{ id: 1, status: s::other::Y }]");

	assert_diagnostic(&err, "CA_100", "s::other::Y");
	assert_eq!(row_count(&t, "FROM s::e"), 0);
}

#[test]
fn table_insert_with_a_variant_of_an_unknown_namespace_is_namespace_not_found() {
	// An unknown namespace in a qualified variant must fail loud, never panic.
	let t = engine();
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");

	let err = command_err(&t, "INSERT s::e [{ id: 1, status: nope::status::Active }]");

	assert_diagnostic(&err, "CA_002", "nope");
}

#[test]
fn table_insert_with_a_variant_of_an_unknown_enum_is_type_not_found() {
	// An unknown enum in a qualified variant must fail loud, never panic.
	let t = engine();
	t.admin("CREATE TABLE s::e { id: int4, status: s::status }");

	let err = command_err(&t, "INSERT s::e [{ id: 1, status: s::nope::Active }]");

	assert_diagnostic(&err, "CA_002", "nope");
}

#[test]
fn table_insert_with_a_field_the_variant_does_not_declare_is_column_not_found() {
	// side belongs to Square, so a Circle carrying it must fail loud instead of losing the value.
	let t = engine();
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Square { side: float8 } }");
	t.admin("CREATE TABLE s::g { id: int4, shape: s::shape }");

	let err = command_err(&t, "INSERT s::g [{ id: 1, shape: s::shape::Circle { side: 1.0 } }]");

	assert_column_not_found(&err, "side");
}

#[test]
fn table_insert_with_a_constructor_in_a_column_that_is_no_enum_is_an_error() {
	// a is int4, so a constructor for it has no enum to resolve against and must fail loud, never panic.
	let t = engine();

	let err = command_err(&t, "INSERT s::t [{ a: Foo { x: 1 } }]");

	assert_diagnostic(&err, "CA_101", "Foo");
}

#[test]
fn table_insert_with_a_qualified_variant_in_a_column_that_is_no_enum_is_an_error() {
	// A qualified variant for an int4 column must fail loud at the value instead of at a hidden tag column.
	let t = engine();

	let err = command_err(&t, "INSERT s::t [{ a: s::status::Active }]");

	assert_diagnostic(&err, "CA_101", "Active");
}

#[test]
fn series_insert_with_a_constructor_in_a_data_column_is_an_error() {
	// A series data column is never an enum, so a constructor for it must fail loud, never panic.
	let t = engine();

	let err = command_err(&t, "INSERT s::x [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: Foo { x: 1 } }]");

	assert_diagnostic(&err, "CA_101", "Foo");
}

#[test]
fn series_insert_from_a_query_with_an_int4_value_for_a_float8_column_stores_it_as_float8() {
	// Tables coerce a query value to the column type, so a series must store 7.0 instead of panicking on int4.
	let t = engine();
	t.admin("CREATE TABLE s::src { ts: datetime, val: int4 }");
	t.command("INSERT s::src [{ ts: cast('2024-01-01T00:00:00Z', datetime), val: 7 }]");

	command_ok(&t, "INSERT s::x FROM s::src");

	assert_eq!(column_values(&t.query("FROM s::x"), "val"), vec![Value::float8(7.0)]);
}

#[test]
fn table_insert_with_a_duplicate_column_in_a_row_is_an_error() {
	// Two values for a in one row are ambiguous, so the first must not be dropped in favour of the last.
	let t = engine();

	let err = command_err(&t, "INSERT s::t [{ a: 1, a: 2 }]");

	assert_eq!(err.fragment.text(), "a", "got: {err:?}");
	assert_eq!(row_count(&t, "FROM s::t"), 0);
}

#[test]
fn table_insert_with_a_duplicate_constructor_field_is_an_error() {
	// Two values for radius are ambiguous, so the first must not be dropped in favour of the last.
	let t = engine();
	t.admin("CREATE ENUM s::shape { Circle { radius: float8 }, Square { side: float8 } }");
	t.admin("CREATE TABLE s::g { id: int4, shape: s::shape }");

	let err = command_err(&t, "INSERT s::g [{ id: 1, shape: s::shape::Circle { radius: 1.0, radius: 2.0 } }]");

	assert_eq!(err.fragment.text(), "radius", "got: {err:?}");
	assert_eq!(row_count(&t, "FROM s::g"), 0);
}
