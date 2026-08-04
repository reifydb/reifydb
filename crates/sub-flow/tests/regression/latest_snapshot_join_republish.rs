// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{Value, WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;

const SETTLE: Duration = Duration::from_secs(5);

fn make_db() -> TestDb {
	// The aggregate sits in the same pipeline as the join, with no view between them. That placement
	// is the whole point: a materialised view recomputes its own delta from the row it already holds,
	// so a join that retracts against the wrong right value is silently corrected at the view
	// boundary and nothing downstream can see it. The aggregate applies -pre +post from the join's
	// own diffs, so a wrong pre shows up as a drifted total.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::bar { k: utf8, n: int4 }");
	db.admin("CREATE TABLE app::price { k: utf8, p: int8 }");
	db.admin("CREATE DEFERRED VIEW app::total { total: int8 } AS { \
		 FROM app::bar \
		 INNER JOIN { FROM app::price } AS pr USING (k, pr.k) WITH { snapshot: true, latest: true } \
		 AGGREGATE { total: math::sum(pr_p) } BY {} }");
	db
}

fn total(db: &TestDb) -> i64 {
	let frames = db.query("FROM app::total");
	let column = frames.first().and_then(|f| f.columns.first()).expect("the total view must have a column");
	assert_eq!(column.data.len(), 1, "the ungrouped aggregate must hold exactly one row");
	match column.data.get_value(0) {
		Value::Int8(v) => v,
		other => panic!("total must be an int8, got {other:?}"),
	}
}

#[test]
fn a_left_update_against_an_unchanged_slot_emits_the_same_pair_as_the_ledger_round_trip() {
	// The shortcut skips the withdraw/publish round trip when the slot still holds the version the
	// left row published against. Skipping it must not change what is emitted: the pre still has to
	// carry the right value the previous emission used, which here is the unchanged price.
	let db = make_db();
	db.command("INSERT app::price [{ k: 'a', p: 10 }]");
	db.command("INSERT app::bar [{ k: 'a', n: 1 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(total(&db), 10, "the left row must join against the price that was there when it arrived");

	db.command("UPDATE app::bar { n: 2 } FILTER { k == 'a' }");
	db.await_all_flows(SETTLE);

	assert_eq!(
		total(&db),
		10,
		"an update that touches no priced column must leave the total alone; a drift here means the \
		 retraction and the emission disagreed about the right value"
	);
}

#[test]
fn a_left_update_after_the_slot_moved_still_retracts_against_the_version_it_published() {
	// The guard on the shortcut. Once the slot has moved the ledger is the only record of what the
	// left row was emitted against, so the round trip has to run. Taking the shortcut here would
	// retract 20 against an emission of 10 and pin the total at 10 forever.
	let db = make_db();
	db.command("INSERT app::price [{ k: 'a', p: 10 }]");
	db.command("INSERT app::bar [{ k: 'a', n: 1 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(total(&db), 10);

	db.command("UPDATE app::price { p: 20 } FILTER { k == 'a' }");
	db.await_all_flows(SETTLE);
	assert_eq!(
		total(&db),
		10,
		"a latest+snapshot join must not re-emit when the right side changes; the new price waits for \
		 the next left row"
	);

	db.command("UPDATE app::bar { n: 2 } FILTER { k == 'a' }");
	db.await_all_flows(SETTLE);

	assert_eq!(
		total(&db),
		20,
		"the left update must withdraw 10 (the version it published against) and publish 20; \
		 retracting against 20 would leave the total at 10"
	);
}
