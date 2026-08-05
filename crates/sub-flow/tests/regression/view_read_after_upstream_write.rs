// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Reading any view inside the transaction that already wrote something transitively upstream of it
// must fail with TXN_015: a view still holds its pre-request contents during the transaction, so
// such a read is silently stale. Reading before writing, and separate requests, stay legal.

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

fn make_db() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::orders { id: int4, total: int8 }");
	db.admin(
		"CREATE DEFERRED VIEW app::revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
	);
	db
}

#[test]
fn read_after_upstream_write_in_one_command_fails_with_txn_015() {
	let db = make_db();

	let err = db.try_command("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::revenue").unwrap_err();

	assert_eq!(
		err.0.code, "TXN_015",
		"reading a view after writing its source in the same command must fail; got: {err:?}"
	);

	// The failed request must not have committed anything.
	let frames = db.query("FROM app::orders");
	let row_count = frames.first().and_then(|f| f.columns.first()).map(|c| c.data.len()).unwrap_or(0);
	assert_eq!(row_count, 0, "the rejected transaction must have rolled back its insert");
}

#[test]
fn read_after_upstream_write_in_one_admin_fails_with_txn_015() {
	let db = make_db();

	let err = db.try_admin("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::revenue").unwrap_err();

	assert_eq!(err.0.code, "TXN_015", "the admin transaction path must be guarded too; got: {err:?}");
}

#[test]
fn view_on_view_transitive_read_fails_with_txn_015() {
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::revenue_squared { squared: int8 } AS { FROM app::revenue MAP { squared: revenue * revenue } }",
	);

	let err = db.try_command("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::revenue_squared").unwrap_err();

	assert_eq!(
		err.0.code, "TXN_015",
		"the upstream walk must be transitive: orders -> revenue -> revenue_squared; got: {err:?}"
	);
}

#[test]
fn read_before_write_is_allowed() {
	let db = make_db();

	db.command("FROM app::revenue; INSERT app::orders [{ id: 1, total: 40 }]");
}

#[test]
fn deferred_view_read_after_write_fails_with_txn_015() {
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
	);

	let err = db.try_command("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::deferred_revenue").unwrap_err();

	assert_eq!(err.0.code, "TXN_015", "deferred views are guarded by the same uniform rule; got: {err:?}");
	assert!(
		err.0.message.starts_with("Deferred view"),
		"the message must name the view kind; got: {}",
		err.0.message
	);
	assert!(
		err.0.help.as_deref().unwrap_or("").contains("subscription"),
		"the deferred help must point at subscriptions, not at splitting requests; got: {:?}",
		err.0.help
	);
}

#[test]
fn deferred_over_deferred_chain_fails_with_txn_015() {
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
	);
	db.admin(
		"CREATE DEFERRED VIEW app::deferred_doubled { doubled: int8 } AS { FROM app::deferred_revenue MAP { doubled: revenue * 2 } }",
	);

	let err = db.try_command("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::deferred_doubled").unwrap_err();

	assert_eq!(err.0.code, "TXN_015", "the walk crosses deferred-over-deferred chains; got: {err:?}");
}

#[test]
fn read_before_write_on_deferred_is_allowed() {
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
	);

	db.command("FROM app::deferred_revenue; INSERT app::orders [{ id: 1, total: 40 }]");
}

#[test]
fn deferred_view_read_in_separate_request_is_allowed() {
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
	);

	db.command("INSERT app::orders [{ id: 1, total: 40 }]");
	// Cross-request staleness is the deferred contract, so the contents are deliberately not
	// asserted - only that the read is legal.
	db.query("FROM app::deferred_revenue");
}

#[test]
fn freshly_created_deferred_view_is_guarded_in_next_request() {
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
	);

	// The lineage snapshot is published by the CDC-driven supervisor, which lags the CREATE, so
	// this exercises the catalog fallback until the snapshot catches up.
	let err = db.try_command("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::deferred_revenue").unwrap_err();
	assert_eq!(err.0.code, "TXN_015", "lineage must cover a deferred view immediately after CREATE; got: {err:?}");
}

#[test]
fn deferred_view_created_and_written_in_one_request_is_guarded() {
	let db = make_db();

	// Lineage only learns of a flow at post-commit, so the snapshot cannot know a view this request
	// created. The guard has to fall back to the catalog, which already holds the uncommitted
	// CREATE, rather than read a snapshot miss as "this view has no upstreams".
	let err = db
		.try_admin(
			"CREATE DEFERRED VIEW app::fresh { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} };
			 INSERT app::orders [{ id: 1, total: 40 }];
			 FROM app::fresh",
		)
		.unwrap_err();

	assert_eq!(
		err.0.code, "TXN_015",
		"a view created, written, and read in one request must be guarded via the catalog fallback; got: {err:?}"
	);

	let frames = db.query("FROM app::orders");
	let row_count = frames.first().and_then(|f| f.columns.first()).map(|c| c.data.len()).unwrap_or(0);
	assert_eq!(row_count, 0, "the rejected transaction must have rolled back its insert");
}

#[test]
fn view_created_in_one_request_without_reading_it_is_allowed() {
	let db = make_db();

	// The fallback must not over-fire: creating a view and writing its upstream in one request is
	// legal so long as the view is never read, or every bootstrap script breaks.
	db.admin(
		"CREATE DEFERRED VIEW app::fresh { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} };
		 INSERT app::orders [{ id: 1, total: 40 }]",
	);
}
