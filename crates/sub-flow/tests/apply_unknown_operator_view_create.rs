// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_value::fragment::Fragment;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn a_deferred_view_applying_an_unknown_operator_is_rejected_at_create_and_leaves_no_view() {
	// An accepted view with an unknown operator never fills, so create must refuse it and store nothing.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4 }");

	let created = db.try_admin("CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t | apply no_such_op {} }");

	let Err(err) = created else {
		panic!(
			"no_such_op is not a registered operator, so creating the view must fail, but the create succeeded"
		);
	};
	let diagnostic = err.diagnostic();
	assert!(diagnostic.message.contains("no_such_op"), "the error must name the unknown operator: {diagnostic:?}");
	assert!(
		matches!(&diagnostic.fragment, Fragment::Statement { text, .. } if &**text == "no_such_op"),
		"the fragment must be the operator name in the statement: {diagnostic:?}"
	);
	assert_eq!(
		db.row_count("FROM system::views FILTER { name == 'v' }"),
		0,
		"a refused create must not leave a view in the catalog"
	);
	assert!(db.try_query("FROM app::v").is_err(), "a refused create must not leave a readable view");
}
