// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	fragment::{Fragment, StatementColumn, StatementLine},
	params::Params,
	value::{Value, frame::frame::Frame},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.admin("CREATE TABLE test::u { c: int4, e: int4 }");
	t.command("INSERT test::t [{ a: 1, b: 10 }, { a: 2, b: 20 }]");
	t.command("INSERT test::u [{ c: 2, e: 200 }, { c: 3, e: 300 }]");
	t
}

fn rows(frames: &[Frame], names: &[&str]) -> Vec<Vec<Value>> {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	let frame = &frames[0];
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
	(0..row_count).map(|row| columns.iter().map(|c| c.data.get_value(row)).collect()).collect()
}

fn empty_tables(t: &TestEngine) {
	t.admin("CREATE TABLE test::et { a: int4, b: int4 }");
	t.admin("CREATE TABLE test::eu { c: int4, e: int4 }");
}

fn error_of(t: &TestEngine, rql: &str) -> Diagnostic {
	let result = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match result.error {
		Some(error) => error.diagnostic(),
		None => panic!("{rql} must be an error, got {} frames: {:?}", result.frames.len(), result.frames),
	}
}

fn assert_no_shared_column(diagnostic: &Diagnostic, left: &str, right: &str) {
	assert_eq!(diagnostic.code, "JOIN_002", "{diagnostic:?}");
	let words: Vec<&str> = diagnostic.message.split_whitespace().collect();
	assert!(words.contains(&left), "the message must name the left side {left}: {}", diagnostic.message);
	assert!(words.contains(&right), "the message must name the right side {right}: {}", diagnostic.message);
	assert!(diagnostic.message.contains("no shared column"), "{}", diagnostic.message);
}

#[test]
fn natural_join_on_a_column_written_in_two_places_matches_the_equivalent_using_join() {
	// Each extend names k at its own position, so matching names by position would find no common column.
	let t = engine();
	let names = ["a", "b", "k", "s_c", "s_e"];

	let natural = t.query("FROM test::t | extend { k: a } NATURAL JOIN { FROM test::u | extend { k: c } } AS s");
	let using = t.query(
		"FROM test::t | extend { k: a } INNER JOIN { FROM test::u | extend { k: c } } AS s USING (k, s.k)",
	);

	let expected = vec![vec![Value::Int4(2), Value::Int4(20), Value::Int4(2), Value::Int4(2), Value::Int4(200)]];
	assert_eq!(rows(&using, &names), expected, "using join:\n{}", using[0]);
	assert_eq!(rows(&natural, &names), expected, "natural join:\n{}", natural[0]);
	assert!(
		natural[0].columns.iter().all(|c| c.name != "s_k"),
		"a natural join must emit its common column once:\n{}",
		natural[0]
	);
}

#[test]
fn natural_join_without_a_common_column_is_an_error_naming_both_sides_at_the_natural_keyword() {
	// With no shared name there is no key, and an empty frame would read as a valid empty answer.
	let t = engine();

	let diagnostic = error_of(&t, "FROM test::t NATURAL JOIN { FROM test::u } AS s");

	assert_no_shared_column(&diagnostic, "t", "s");
	match &diagnostic.fragment {
		Fragment::Statement {
			text,
			line,
			column,
		} => {
			assert_eq!(&**text, "NATURAL", "{diagnostic:?}");
			assert_eq!((*line, *column), (StatementLine(1), StatementColumn(14)), "{diagnostic:?}");
		}
		other => panic!("the fragment must point at the NATURAL JOIN in the statement, got {other:?}"),
	}
}

#[test]
fn natural_left_join_without_a_common_column_is_an_error() {
	// A left join keeps every left row, so an empty frame would silently drop all of them.
	let t = engine();

	let diagnostic = error_of(&t, "FROM test::t NATURAL LEFT JOIN { FROM test::u } AS s");

	assert_no_shared_column(&diagnostic, "t", "s");
}

#[test]
fn natural_join_without_a_common_column_is_an_error_even_when_a_side_is_empty() {
	// The missing key is a property of the shapes, so it must not depend on whether rows exist.
	let t = engine();
	empty_tables(&t);

	assert_no_shared_column(&error_of(&t, "FROM test::et NATURAL JOIN { FROM test::u } AS s"), "et", "s");
	assert_no_shared_column(&error_of(&t, "FROM test::t NATURAL JOIN { FROM test::eu } AS s"), "t", "s");
	assert_no_shared_column(&error_of(&t, "FROM test::et NATURAL JOIN { FROM test::eu } AS s"), "et", "s");
}

#[test]
fn natural_join_with_an_empty_side_that_shares_a_column_is_not_an_error() {
	// An empty input still carries its columns, so a shared name on an empty side must join to no rows.
	let t = engine();
	empty_tables(&t);

	let left_empty = t.query("FROM test::et | extend { c: a } NATURAL JOIN { FROM test::u } AS s");
	let right_empty = t.query("FROM test::t NATURAL JOIN { FROM test::et | filter { a > 5 } } AS s");

	assert!(rows(&left_empty, &["a", "b", "c", "s_e"]).is_empty(), "{}", left_empty[0]);
	assert!(rows(&right_empty, &["a", "b"]).is_empty(), "{}", right_empty[0]);
}
