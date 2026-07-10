// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Contract: reading ANY view - transactional or deferred - inside the same command/admin
// transaction that already wrote to any shape transitively upstream of it (through any mix of view
// kinds, no async boundaries) must fail the transaction with TXN_015. You never read your own
// uncommitted writes through a view.
//
// WHY this matters: transactional views are maintained by the pre-commit interceptor and deferred
// views asynchronously after commit, so during the transaction the view still holds its pre-request
// contents. Before this guard, such a read silently returned stale data. The guard turns the silent
// staleness into a loud failure, while leaving every legitimate pattern untouched: reading before
// writing, separate requests (deferred views may legitimately lag there - that is their contract),
// and RUN TESTS bodies (which maintain views inline and are exempt via the Test transaction
// variant).

use reifydb::{Database, Params, WithSubsystem, embedded};
use reifydb_value::value::Value;

fn make_db() -> Database {
	let db = embedded::memory().with_flow(|f| f).build().expect("build memory db with flow");
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::orders { id: int4, total: int8 }", Params::None).expect("create table");
	db.admin_as_root(
		"CREATE TRANSACTIONAL VIEW app::revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create transactional view");
	db
}

#[test]
fn read_after_upstream_write_in_one_command_fails_with_txn_015() {
	let db = make_db();

	let err = db
		.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::revenue", Params::None)
		.unwrap_err();

	assert_eq!(
		err.0.code, "TXN_015",
		"reading a transactional view after writing its source in the same command must fail; got: {err:?}"
	);

	// The failed request must not have committed anything.
	let frames = db.query_as_root("FROM app::orders", Params::None).expect("query source table");
	let row_count = frames.first().and_then(|f| f.columns.first()).map(|c| c.data.len()).unwrap_or(0);
	assert_eq!(row_count, 0, "the rejected transaction must have rolled back its insert");
}

#[test]
fn read_after_upstream_write_in_one_admin_fails_with_txn_015() {
	let db = make_db();

	let err = db
		.admin_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::revenue", Params::None)
		.unwrap_err();

	assert_eq!(err.0.code, "TXN_015", "the admin transaction path must be guarded too; got: {err:?}");
}

#[test]
fn view_on_view_transitive_read_fails_with_txn_015() {
	let db = make_db();
	db.admin_as_root(
		"CREATE TRANSACTIONAL VIEW app::revenue_squared { squared: int8 } AS { FROM app::revenue MAP { squared: revenue * revenue } }",
		Params::None,
	)
	.expect("create view over view");

	let err = db
		.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::revenue_squared", Params::None)
		.unwrap_err();

	assert_eq!(
		err.0.code, "TXN_015",
		"the upstream walk must be transitive: orders -> revenue -> revenue_squared; got: {err:?}"
	);
}

#[test]
fn read_before_write_is_allowed() {
	let db = make_db();

	db.command_as_root("FROM app::revenue; INSERT app::orders [{ id: 1, total: 40 }]", Params::None)
		.expect("reading the view before writing upstream must stay legal");
}

#[test]
fn deferred_view_read_after_write_fails_with_txn_015() {
	let db = make_db();
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create deferred view");

	let err = db
		.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::deferred_revenue", Params::None)
		.unwrap_err();

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
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create deferred view");
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_doubled { doubled: int8 } AS { FROM app::deferred_revenue MAP { doubled: revenue * 2 } }",
		Params::None,
	)
	.expect("create deferred view over deferred view");

	let err = db
		.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::deferred_doubled", Params::None)
		.unwrap_err();

	assert_eq!(err.0.code, "TXN_015", "the walk crosses deferred-over-deferred chains; got: {err:?}");
}

#[test]
fn read_before_write_on_deferred_is_allowed() {
	let db = make_db();
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create deferred view");

	db.command_as_root("FROM app::deferred_revenue; INSERT app::orders [{ id: 1, total: 40 }]", Params::None)
		.expect("reading the deferred view before writing upstream must stay legal");
}

#[test]
fn deferred_view_read_in_separate_request_is_allowed() {
	let db = make_db();
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create deferred view");

	db.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]", Params::None).expect("insert");
	// Cross-request staleness is the accepted deferred contract: the read is
	// legal and its contents are deliberately not asserted here.
	db.query_as_root("FROM app::deferred_revenue", Params::None)
		.expect("reading a deferred view in a separate request must stay legal");
}

#[test]
fn freshly_created_deferred_view_is_guarded_in_next_request() {
	let db = make_db();
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create deferred view");

	// The very next request must already be guarded: lineage publishes
	// synchronously at post-commit of the CREATE, not via the CDC-driven
	// deferred supervisor (which lags). This test pins that design choice.
	let err = db
		.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::deferred_revenue", Params::None)
		.unwrap_err();
	assert_eq!(err.0.code, "TXN_015", "lineage must cover a deferred view immediately after CREATE; got: {err:?}");
}

#[test]
fn deferred_view_created_and_written_in_one_request_is_guarded() {
	let db = make_db();

	// Lineage only learns of a flow at post-commit, so the published snapshot cannot know a view
	// this very request created. The guard must fall back to the catalog - which already holds
	// the uncommitted CREATE - rather than read a snapshot miss as "this view has no upstreams".
	// Failing open here returned an empty frame for a deferred view that then backfills from its
	// creation version to revenue=40: a silently stale read, exactly what TXN_015 forbids.
	let err = db
		.admin_as_root(
			"CREATE DEFERRED VIEW app::fresh { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} };
			 INSERT app::orders [{ id: 1, total: 40 }];
			 FROM app::fresh",
			Params::None,
		)
		.unwrap_err();

	assert_eq!(
		err.0.code, "TXN_015",
		"a view created, written, and read in one request must be guarded via the catalog fallback; got: {err:?}"
	);

	let frames = db.query_as_root("FROM app::orders", Params::None).expect("query source table");
	let row_count = frames.first().and_then(|f| f.columns.first()).map(|c| c.data.len()).unwrap_or(0);
	assert_eq!(row_count, 0, "the rejected transaction must have rolled back its insert");
}

#[test]
fn transactional_view_created_and_written_in_one_request_is_guarded() {
	let db = make_db();

	// Same snapshot miss, transactional sink. The stale read is less visible here (transactional
	// views never backfill, so the view stays empty forever) but that is precisely the point: the
	// empty frame concealed that the write would never reach the view at all.
	let err = db
		.admin_as_root(
			"CREATE TRANSACTIONAL VIEW app::fresh { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} };
			 INSERT app::orders [{ id: 1, total: 40 }];
			 FROM app::fresh",
			Params::None,
		)
		.unwrap_err();

	assert_eq!(err.0.code, "TXN_015", "the catalog fallback must guard transactional sinks too; got: {err:?}");
	assert!(
		err.0.message.starts_with("Transactional view"),
		"the scanned view's own kind decides the message; got: {}",
		err.0.message
	);
}

#[test]
fn view_created_in_one_request_without_reading_it_is_allowed() {
	let db = make_db();

	// The fallback must not over-fire: creating a view and writing its upstream in one request is
	// legal as long as the view is never read. Guarding this would break every bootstrap script.
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::fresh { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} };
		 INSERT app::orders [{ id: 1, total: 40 }]",
		Params::None,
	)
	.expect("create + write with no view read must stay legal");
}

#[test]
fn separate_requests_are_allowed_and_view_is_current() {
	let db = make_db();

	db.command_as_root("INSERT app::orders [{ id: 1, total: 40 }, { id: 2, total: 25 }]", Params::None)
		.expect("insert");
	let frames = db.query_as_root("FROM app::revenue", Params::None).expect("read view in next request");

	let frame = frames.first().expect("view read returns a frame");
	let col = frame.columns.iter().find(|c| c.name == "revenue").expect("revenue column");
	assert_eq!(
		col.data.get_value(0),
		Value::Int8(65),
		"the recommended pattern (write, then read in the next request) must see the maintained view"
	);
}

#[test]
fn chain_through_deferred_boundary_fails_with_txn_015() {
	let db = make_db();
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::deferred_revenue { revenue: int8 } AS { FROM app::orders AGGREGATE { revenue: math::sum(total) } BY {} }",
		Params::None,
	)
	.expect("create deferred view");
	db.admin_as_root(
		"CREATE TRANSACTIONAL VIEW app::over_deferred { doubled: int8 } AS { FROM app::deferred_revenue MAP { doubled: revenue * 2 } }",
		Params::None,
	)
	.expect("create transactional view over the deferred view");

	// The uniform rule has no async boundaries: orders feeds over_deferred
	// through the deferred view, so a same-request write+read still counts
	// as reading your own uncommitted writes through a view.
	let err = db
		.command_as_root("INSERT app::orders [{ id: 1, total: 40 }]; FROM app::over_deferred", Params::None)
		.unwrap_err();
	assert_eq!(err.0.code, "TXN_015", "the walk must cross the deferred boundary; got: {err:?}");
	assert!(
		err.0.message.starts_with("Transactional view"),
		"the scanned view's own kind decides the message; got: {}",
		err.0.message
	);
}

#[test]
fn run_tests_body_writing_then_reading_view_is_exempt() {
	let db = make_db();
	db.admin_as_root(
		"CREATE TEST app::write_then_read_view {
			INSERT app::orders [{ id: 1, total: 40 }];
			FROM app::revenue | ASSERT { revenue == 40 }
		}",
		Params::None,
	)
	.expect("create test");

	let frames = db.admin_as_root("RUN TESTS app", Params::None).expect("RUN TESTS must not hit TXN_015");
	let frame = frames.first().expect("test results frame");
	let outcome = frame.columns.iter().find(|c| c.name == "outcome").expect("outcome column");
	for i in 0..outcome.data.len() {
		assert_eq!(
			outcome.data.get_value(i),
			Value::Utf8("pass".to_string()),
			"RUN TESTS bodies maintain views inline (Test transactions are exempt from the guard); \
			 message: {:?}",
			frame.columns.iter().find(|c| c.name == "message").map(|c| c.data.get_value(i))
		);
	}
}
