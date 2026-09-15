// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn a_deferred_view_calling_a_script_udf_is_rejected_when_it_is_created() {
	// The flow evaluates without the script's udfs, so an accepted view aborts the flow worker on its first row.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, a: int4 }");

	let created = db.try_admin(
		"UDF twice ($x: int4): int2 { RETURN $x * 2 }; CREATE DEFERRED VIEW app::v { g: int4, x: int2 } AS { FROM app::t | map { g, x: twice(a) } }",
	);

	let Err(err) = created else {
		panic!(
			"the flow cannot call twice, so creating the view must fail instead of crashing on the first insert"
		);
	};
	let diagnostic = err.diagnostic();
	assert!(
		diagnostic.message.contains("twice") || diagnostic.fragment.text() == "twice",
		"the error must name the udf the view cannot call, got: {diagnostic:?}"
	);
}
