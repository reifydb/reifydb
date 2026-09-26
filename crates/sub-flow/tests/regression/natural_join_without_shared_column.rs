// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_value::value::duration::Duration;

const SETTLE: Duration = Duration::from_seconds_const(5);

fn make_db() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { a: int4, b: int4 }");
	db.admin("CREATE TABLE app::u { c: int4, e: int4 }");
	db.admin("CREATE TABLE app::w { a: int4, e: int4 }");
	db
}

fn insert_matching_rows(db: &TestDb) {
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
	db.command("INSERT app::u [{ c: 2, e: 200 }]");
	db.command("INSERT app::w [{ a: 2, e: 200 }]");
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_shared_table_column_fills_in() {
	// A column both tables declare must carry joined rows into the view, so view syntax and shape are valid.
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::w } AS s }",
	);

	insert_matching_rows(&db);

	assert_eq!(db.await_row_count("FROM app::v", 1, SETTLE), 1, "the shared column a must join the two rows");
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_column_added_by_extend_fills_in() -> Result<(), String> {
	// Key names read from a schema without the extended column find no key, so every row is dropped.
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::u | extend { a: c } } AS s }",
	);

	insert_matching_rows(&db);

	match db.await_row_count("FROM app::v", 1, SETTLE) {
		1 => Ok(()),
		rows => {
			Err(format!("the extended column a must join the two rows like a batch query, got {rows} rows"))
		}
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_without_a_shared_column_is_an_error() -> Result<(), String> {
	// The flow join drops every row when no column is shared, so the view would stay empty forever.
	let db = make_db();

	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_c: int4, s_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::u } AS s }",
	);

	match created {
		Err(err) if err.0.code == "JOIN_002" => Ok(()),
		Err(err) => Err(format!("creating the view must report JOIN_002, got {err:?}")),
		Ok(_) => {
			insert_matching_rows(&db);
			db.await_all_flows(SETTLE);
			Err(format!(
				"creating the view must report JOIN_002, got Ok and the view holds {} rows",
				db.row_count("FROM app::v")
			))
		}
	}
}
