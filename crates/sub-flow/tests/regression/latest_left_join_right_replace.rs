// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Value, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_value::value::{column_view::ColumnView, duration::Duration, system_columns::user_columns};

const SETTLE: Duration = Duration::from_seconds_const(5);

fn make_db(partitioned: bool) -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::bar { k: utf8, n: int4 }");
	db.admin("CREATE TABLE app::price { k: utf8, p: int8 }");
	let partition = if partitioned {
		", partition: { by: { k } }"
	} else {
		""
	};
	db.admin(&format!(
		"CREATE DEFERRED RINGBUFFER VIEW app::current {{ k: utf8, p: int8 }} WITH {{ capacity: 1{partition} }} AS {{ FROM app::price }}"
	));
	db.admin("CREATE DEFERRED VIEW app::total { total: Option(int8), rows: int8, priced: int8 } AS { \
		 FROM app::bar \
		 LEFT JOIN { FROM app::current } AS c USING (k, c.k) WITH { latest: true } \
		 AGGREGATE { total: math::sum(c_p), rows: math::count(n), priced: math::count(c_p) } BY {} }");
	db
}

fn int8(value: Value) -> Option<i64> {
	match value {
		Value::Int8(v) => Some(v),
		Value::None {
			..
		} => None,
		other => panic!("expected an int8 or none, got {other:?}"),
	}
}

fn totals(db: &TestDb) -> (Option<i64>, i64, i64) {
	let frames = db.query("FROM app::total");
	let batch = &frames.first().expect("the total view must return a frame").batch;
	let values: Vec<Value> = user_columns(batch)
		.map(|(field, array)| {
			let column = ColumnView::try_from((array, field.as_ref())).expect("a total column reads");
			assert_eq!(column.len(), 1, "the ungrouped aggregate must hold exactly one row");
			column.get_value(0)
		})
		.collect();
	let [total, rows, priced] = <[Value; 3]>::try_from(values).expect("the total view has three columns");
	(int8(total), int8(rows).expect("rows is never none"), int8(priced).expect("priced is never none"))
}

#[test]
fn a_right_replace_on_the_same_key_withdraws_the_old_price_from_every_left_row() {
	// A pair coalesced into one republish must withdraw the old price exactly once per left row, otherwise rows or
	// total drift.
	let db = make_db(true);
	db.command("INSERT app::bar [{ k: 'a', n: 1 }, { k: 'a', n: 2 }, { k: 'a', n: 3 }, { k: 'b', n: 4 }]");
	db.command("INSERT app::price [{ k: 'a', p: 10 }, { k: 'b', p: 100 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(130), 4, 4));

	db.command("INSERT app::price [{ k: 'a', p: 20 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(160), 4, 4), "every a row must move from 10 to 20 and b must stay at 100");

	db.command("INSERT app::price [{ k: 'a', p: 30 }]");
	db.command("INSERT app::price [{ k: 'a', p: 40 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(
		totals(&db),
		(Some(220), 4, 4),
		"chained replaces must each withdraw the price the previous one published"
	);
}

#[test]
fn a_right_replace_inside_one_command_ends_on_the_newest_price() {
	// Two prices for one key in one command must leave every left row on the price the ring keeps, never on both or
	// neither.
	let db = make_db(true);
	db.command("INSERT app::bar [{ k: 'a', n: 1 }, { k: 'a', n: 2 }]");
	db.command("INSERT app::price [{ k: 'a', p: 10 }]");
	db.await_all_flows(SETTLE);
	db.command("INSERT app::price [{ k: 'a', p: 20 }, { k: 'a', p: 30 }]");
	db.await_all_flows(SETTLE);

	let kept = db.query("FROM app::current");
	let batch = &kept.first().expect("the ring view must return a frame").batch;
	let (field, array) = user_columns(batch).nth(1).expect("the ring view has a price column");
	let column = ColumnView::try_from((array, field.as_ref())).expect("the price column reads");
	assert_eq!(column.len(), 1, "a capacity one ring keeps exactly one price per key");
	let newest = int8(column.get_value(0)).expect("the kept price is never none");
	assert_eq!(totals(&db), (Some(2 * newest), 2, 2));
}

#[test]
fn a_right_replace_that_moves_to_another_key_unprices_the_old_key_and_prices_the_new_one() {
	// An unpartitioned capacity one ring evicts key a when key b arrives, so the pair carries two keys and must not
	// be joined as one.
	let db = make_db(false);
	db.command("INSERT app::bar [{ k: 'a', n: 1 }, { k: 'a', n: 2 }, { k: 'b', n: 3 }]");
	db.command("INSERT app::price [{ k: 'a', p: 10 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(20), 3, 2));

	db.command("INSERT app::price [{ k: 'b', p: 7 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(7), 3, 1), "the a rows must lose their price and the b row must gain one");

	db.command("INSERT app::price [{ k: 'a', p: 5 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(10), 3, 2), "moving back must price both a rows again and unprice b");
}

#[test]
fn a_right_replace_with_no_left_rows_then_a_left_insert_joins_the_newest_price() {
	// The replace must still move the slot when no left row exists, otherwise a later left row joins a stale price.
	let db = make_db(true);
	db.command("INSERT app::price [{ k: 'a', p: 10 }]");
	db.await_all_flows(SETTLE);
	db.command("INSERT app::price [{ k: 'a', p: 20 }]");
	db.await_all_flows(SETTLE);
	db.command("INSERT app::bar [{ k: 'a', n: 1 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(20), 1, 1));
}

fn make_picked_db() -> TestDb {
	// The right side must be a plain table, so one UPDATE changes several rows of one key in one change.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::bar { k: utf8, n: int4 }");
	db.admin("CREATE TABLE app::quote { id: int4, k: utf8, p: int8 }");
	db.admin("CREATE DEFERRED VIEW app::total { total: Option(int8), rows: int8, priced: int8 } AS { \
		 FROM app::bar \
		 LEFT JOIN { FROM app::quote } AS c USING (k, c.k) WITH { latest: { c.p: desc } } \
		 AGGREGATE { total: math::sum(c_p), rows: math::count(n), priced: math::count(c_p) } BY {} }");
	db
}

#[test]
fn a_right_replace_where_another_row_of_the_key_takes_the_pick_frees_the_old_winner() {
	// When another row takes the pick, the old winner must be deleted, otherwise it serves a stale price.
	let db = make_picked_db();
	db.command("INSERT app::bar [{ k: 'a', n: 1 }, { k: 'a', n: 2 }]");
	db.command("INSERT app::quote [{ id: 1, k: 'a', p: 10 }, { id: 2, k: 'a', p: 5 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(20), 2, 2), "the highest quote 10 must win for both bar rows");

	db.command("UPDATE app::quote { id, k, p: if id == 1 { 1 } else { 20 } } FILTER { k == 'a' }");
	db.await_all_flows(SETTLE);
	assert_eq!(
		totals(&db),
		(Some(40), 2, 2),
		"quote 2 now holds 20 and must replace the old winner for every bar row"
	);

	db.command("UPDATE app::quote { p: 30 } FILTER { id == 2 }");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(60), 2, 2), "a later replace must start from exactly one held winner");
}

#[test]
fn a_ring_eviction_that_moves_the_winner_position_to_another_key_leaves_one_fresh_winner() {
	// An evicted winner must be freed and never stay a candidate when its ring position moves to another key.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::bar { k: utf8, n: int4 }");
	db.admin("CREATE TABLE app::price { k: utf8, p: int8 }");
	db.admin(
		"CREATE DEFERRED RINGBUFFER VIEW app::current { k: utf8, p: int8 } WITH { capacity: 2 } AS { FROM app::price }",
	);
	db.admin("CREATE DEFERRED VIEW app::total { total: Option(int8), rows: int8, priced: int8 } AS { \
		 FROM app::bar \
		 LEFT JOIN { FROM app::current } AS c USING (k, c.k) WITH { latest: { c.p: desc } } \
		 AGGREGATE { total: math::sum(c_p), rows: math::count(n), priced: math::count(c_p) } BY {} }");
	db.command("INSERT app::bar [{ k: 'a', n: 1 }, { k: 'b', n: 2 }]");
	db.command("INSERT app::price [{ k: 'a', p: 10 }, { k: 'b', p: 1 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(11), 2, 2));

	db.command("INSERT app::price [{ k: 'b', p: 2 }, { k: 'a', p: 8 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(10), 2, 2), "both old prices were evicted, so a must join 8 and b must join 2");

	db.command("INSERT app::bar [{ k: 'a', n: 3 }]");
	db.await_all_flows(SETTLE);
	assert_eq!(totals(&db), (Some(18), 3, 3), "a new bar row must read exactly the one fresh winner of its key");
}
