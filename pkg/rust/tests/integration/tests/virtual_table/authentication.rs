// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{RuntimeConfig, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::Value;

fn new_db() -> TestDb {
	TestDb::from(embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"))
}

fn single(db: &TestDb, rql: &str, column: &str) -> Value {
	let frames = db.query(rql);
	let values = column_values(frames.first().expect("query returned no frame"), column);
	match values.as_slice() {
		[value] => value.clone(),
		other => panic!("expected exactly one {column} for `{rql}`, found {other:?}"),
	}
}

#[test]
fn methods_lists_each_declared_method_with_its_settings() {
	// A wrong ttl here would hide a session-lifetime bug from every app that reads this table.
	let db = TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(0))
			.with_auth(|a| a.github_client_secret("test-secret"))
			.build()
			.expect("build"),
	);
	db.admin(
		"CREATE AUTHENTICATION METHOD github WITH { ttl: 30d, promote_guest: true, client_id: 'abc', redirect_uri: 'x' }",
	);
	db.admin("CREATE AUTHENTICATION METHOD token WITH { ttl: forever }");

	let github = "FROM system::authentication::methods FILTER { name == \"github\" }";
	let token = "FROM system::authentication::methods FILTER { name == \"token\" }";
	assert_eq!(db.row_count("FROM system::authentication::methods"), 2);
	assert_eq!(single(&db, github, "ttl"), Value::Utf8("30d".to_string()));
	assert_eq!(single(&db, github, "promote_guest"), Value::Boolean(true));
	assert_eq!(
		single(&db, github, "properties"),
		Value::Utf8(r#"{"client_id":"abc","redirect_uri":"x"}"#.to_string())
	);
	assert_eq!(single(&db, token, "ttl"), Value::Utf8("forever".to_string()));
	assert_eq!(single(&db, token, "promote_guest"), Value::Boolean(false));
}

#[test]
fn credentials_moved_under_the_authentication_namespace() {
	let db = new_db();
	db.admin("CREATE USER alice");
	db.admin("CREATE AUTHENTICATION METHOD password WITH { ttl: forever }");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'secret' }");

	assert_eq!(
		single(&db, "FROM system::authentication::credentials", "method"),
		Value::Utf8("password".to_string())
	);
}

#[test]
fn the_old_credentials_name_no_longer_resolves() {
	// Without an alias a stale script must fail loud rather than read an empty table.
	let db = new_db();
	assert!(db.try_query("FROM system::authentications").is_err());
}

#[test]
fn both_tables_are_listed_in_the_authentication_namespace() {
	let db = new_db();
	let listed = "FROM system::virtual_tables FILTER { namespace_id == 58 }";
	let names = column_values(db.query(listed).first().expect("no frame"), "name");
	let ids = column_values(db.query(listed).first().expect("no frame"), "id");

	assert!(names.contains(&Value::Utf8("credentials".to_string())), "{names:?}");
	assert!(names.contains(&Value::Utf8("methods".to_string())), "{names:?}");
	assert!(ids.contains(&Value::Uint8(44)) && ids.contains(&Value::Uint8(66)), "{ids:?}");
}
