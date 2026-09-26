// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_value::value::{Value, duration::Duration, frame::frame::Frame};

const TIMEOUT: Duration = Duration::from_seconds_const(10);

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("the {name} column is missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE s");
	db.admin("CREATE ENUM s::status { Active, Inactive }");
	db.admin("CREATE TABLE s::t { a: int4 }");
	db.admin("CREATE TABLE s::e { id: int4, status: s::status }");
	db
}

#[test]
fn a_deferred_view_with_a_variant_inside_an_expression_is_rejected_when_it_is_created() {
	// The flow compiles the view on its first row, so an accepted variant aborts the flow worker.
	for view in [
		"CREATE DEFERRED VIEW s::v { a: int4 } AS { FROM s::t FILTER { a == 1 + s::status::Active } }",
		"CREATE DEFERRED VIEW s::v { a: int4 } AS { FROM s::t | map { a: a + s::status::Active } }",
		"CREATE DEFERRED VIEW s::v { a: int4 } AS { FROM s::t | extend { x: 1 + s::status::Active } | map { a } }",
	] {
		let db = setup();

		let Err(err) = db.try_admin(view) else {
			panic!("{view}: the flow cannot evaluate the variant, so creating the view must fail");
		};
		let diagnostic = err.diagnostic();
		assert_eq!(
			diagnostic.fragment.text(),
			"Active",
			"{view}: the error must point at the variant: {diagnostic:?}"
		);
	}
}

#[test]
fn a_deferred_view_filtering_an_enum_column_by_variant_equality_serves_that_variants_rows() {
	// A query keeps only the Active row for this filter, so the view over the same query must serve that row too.
	let db = setup();
	let view = "CREATE DEFERRED VIEW s::v { id: int4 } AS { FROM s::e | filter { status == s::status::Active } | map { id } }";

	if let Err(err) = db.try_admin(view) {
		let diagnostic = err.diagnostic();
		assert!(
			diagnostic.fragment.text().contains("Active"),
			"the error must point at the variant: {diagnostic:?}"
		);
		return;
	}
	db.command("INSERT s::e [{ id: 1, status: Active }, { id: 2, status: Inactive }]");

	assert_eq!(db.await_row_count("FROM s::v", 1, TIMEOUT), 1, "the Active row must reach the view");
}

#[test]
fn a_deferred_view_filtering_by_variant_equality_is_created_and_serves_only_that_variants_rows() {
	// The query runs this filter, so refusing the view or letting an Inactive row through breaks the view.
	let db = setup();
	db.admin(
		"CREATE DEFERRED VIEW s::v { id: int4 } AS { FROM s::e | filter { status == s::status::Active } | map { id } }",
	);
	db.command("INSERT s::e [{ id: 1, status: Active }, { id: 2, status: Inactive }, { id: 3, status: Active }]");

	assert_eq!(db.await_row_count("FROM s::v", 2, TIMEOUT), 2, "exactly the two Active rows must reach the view");
	assert_eq!(
		column_values(&db.query("FROM s::v | sort { id: ASC }"), "id"),
		vec![Value::Int4(1), Value::Int4(3)],
		"only the Active rows may reach the view"
	);
}

#[test]
fn a_deferred_view_testing_a_variant_with_is_in_a_map_serves_the_result_for_each_row() {
	// The flow map never resolves an IS tag itself, so the view must carry the tag from create.
	let db = setup();
	db.admin(
		"CREATE DEFERRED VIEW s::v { id: int4, active: bool } AS { FROM s::e | map { id, active: status IS s::status::Active } }",
	);
	db.command("INSERT s::e [{ id: 1, status: Active }, { id: 2, status: Inactive }]");

	assert_eq!(db.await_row_count("FROM s::v", 2, TIMEOUT), 2, "both rows must reach the view");
	assert_eq!(
		column_values(&db.query("FROM s::v | sort { id: ASC }"), "active"),
		vec![Value::Boolean(true), Value::Boolean(false)],
		"each row must say whether it holds the Active variant"
	);
}

#[test]
fn a_deferred_view_naming_an_unknown_variant_is_rejected_with_the_error_its_query_reports() {
	// A view and its query must resolve a variant the same way, or the view error points at the wrong fix.
	for (query, view) in [
		(
			"FROM s::e | filter { status == s::status::Missing } | map { id }",
			"CREATE DEFERRED VIEW s::v { id: int4 } AS { FROM s::e | filter { status == s::status::Missing } | map { id } }",
		),
		(
			"FROM s::e | map { id, active: status IS s::status::Missing }",
			"CREATE DEFERRED VIEW s::v { id: int4, active: bool } AS { FROM s::e | map { id, active: status IS s::status::Missing } }",
		),
		(
			"FROM s::t | map { a, x: s::status::Missing }",
			"CREATE DEFERRED VIEW s::v { a: int4 } AS { FROM s::t | map { a, x: s::status::Missing } }",
		),
	] {
		let db = setup();
		db.command("INSERT s::e [{ id: 1, status: Active }]");
		db.command("INSERT s::t [{ a: 1 }]");

		let Err(query_error) = db.try_query(query) else {
			panic!("{query}: the query must refuse the unknown variant");
		};
		let query_error = query_error.diagnostic();
		let Err(view_error) = db.try_admin(view) else {
			panic!("{view}: creating the view must refuse the unknown variant");
		};
		let view_error = view_error.diagnostic();
		assert_eq!(
			(view_error.code.as_str(), view_error.fragment.text()),
			(query_error.code.as_str(), query_error.fragment.text()),
			"{view}: the view must report the query's error: view {view_error:?} query {query_error:?}"
		);
	}
}
