// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use arrow_array::RecordBatch;
use reifydb_cdc::rebuild::rebuild_changes;
use reifydb_core::interface::{
	catalog::object::ObjectId,
	cdc::Cdc,
	change::{ChangeOrigin, Diff},
};
use reifydb_store_cdc::storage::CdcStorage;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

fn setup() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::src | filter { id > 1 } }");
	t
}

fn ids(frames: &[Frame]) -> Vec<i32> {
	let mut ids: Vec<i32> = frames[0].rows().map(|row| row.get::<i32>("id").unwrap().unwrap()).collect();
	ids.sort();
	ids
}

#[test]
fn an_insert_shows_in_the_view_through_the_filter() {
	// A view that lags or skips the filter would show 1 or nothing here.
	let t = setup();
	t.command("INSERT ns::src [{ id: 1 }, { id: 2 }, { id: 3 }]");
	assert_eq!(ids(&t.query("FROM ns::v")), vec![2, 3]);
}

#[test]
fn an_update_moves_a_row_in_and_out_of_the_view() {
	// An update must retract the old row, or the view keeps a row the filter now drops.
	let t = setup();
	t.command("INSERT ns::src [{ id: 2 }]");
	t.command("UPDATE ns::src { id: 5 } FILTER { id == 2 }");
	assert_eq!(ids(&t.query("FROM ns::v")), vec![5]);
	t.command("UPDATE ns::src { id: 0 } FILTER { id == 5 }");
	assert!(ids(&t.query("FROM ns::v")).is_empty());
}

#[test]
fn a_delete_removes_the_row_from_the_view() {
	// Without the retraction the deleted row stays visible in the view forever.
	let t = setup();
	t.command("INSERT ns::src [{ id: 2 }, { id: 3 }]");
	t.command("DELETE ns::src FILTER { id == 2 }");
	assert_eq!(ids(&t.query("FROM ns::v")), vec![3]);
}

#[test]
fn a_read_later_in_the_same_txn_sees_the_view_updated() {
	// This is the whole point of a transactional view: no lag, not even inside the writing txn.
	let t = setup();
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("INSERT ns::src [{ id: 2 }]", Params::None);
	assert!(r.error.is_none(), "insert failed: {:?}", r.error);
	let r = txn.rql("FROM ns::v", Params::None);
	assert!(r.error.is_none(), "view read failed: {:?}", r.error);
	assert_eq!(ids(&r.frames), vec![2]);
	txn.commit().unwrap();
	assert_eq!(ids(&t.query("FROM ns::v")), vec![2]);
}

#[test]
fn a_dictionary_column_in_the_source_reaches_the_view_as_its_value() {
	// The view must resolve the dictionary id, or it stores a number instead of the text.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE DICTIONARY ns::syms FOR utf8 AS uint4");
	t.admin("CREATE TABLE ns::src { id: int4, sym: utf8 with { dictionary: ns::syms } }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::v { id: int4, sym: utf8 } AS { FROM ns::src }");
	t.command("INSERT ns::src [{ id: 1, sym: 'red' }]");
	let frames = t.query("FROM ns::v");
	let syms: Vec<String> = frames[0].rows().map(|row| row.get::<String>("sym").unwrap().unwrap()).collect();
	assert_eq!(syms, vec!["red".to_string()]);
}

#[test]
fn run_tests_reads_the_view_its_own_body_updated_and_leaves_nothing_behind() {
	// A stale view would make the count 0; a leaked test write would show after RUN TESTS.
	let t = setup();
	t.admin(
		"CREATE TEST ns::sees_view { INSERT ns::src [{ id: 2 }]; FROM ns::v | aggregate { n: math::count(id) } | ASSERT { n == 1 } }",
	);
	let result = t.inner().admin_as(TestEngine::identity(), "RUN TESTS ns", Params::None);
	assert!(result.error.is_none(), "RUN TESTS itself must not fail, got: {:?}", result.error);
	let rows: Vec<_> = result.frames[0].rows().collect();
	assert_eq!(rows.len(), 1, "exactly the one test must run, got: {}", result.frames[0]);
	let outcome = rows[0].get::<String>("outcome").unwrap().unwrap();
	let message = rows[0].get::<String>("message").unwrap().unwrap();
	assert_eq!(outcome, "pass", "the view must hold the test body's row, got message: {message}");
	assert!(ids(&t.query("FROM ns::v")).is_empty());
}

fn cdc_entries(t: &TestEngine) -> Vec<Cdc> {
	t.await_cdc();
	t.cdc_store().read_range(Bound::Unbounded, Bound::Unbounded, 10_000).expect("cdc read").items
}

fn object_origins(t: &TestEngine, cdc: &Cdc) -> Vec<ObjectId> {
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	let mut origins = Vec::new();
	for change in rebuild_changes(cdc, &t.catalog(), &mut Transaction::Query(&mut query)).expect("rebuild") {
		if let ChangeOrigin::Object(object) = change.origin {
			origins.push(object);
		}
	}
	origins
}

#[test]
fn a_rollback_leaves_the_view_unchanged() {
	// View rows live in the user txn's write set, so a rollback must drop them with the source rows.
	let t = setup();
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("INSERT ns::src [{ id: 2 }]", Params::None);
	assert!(r.error.is_none(), "insert failed: {:?}", r.error);
	assert_eq!(ids(&txn.rql("FROM ns::v", Params::None).frames), vec![2]);
	txn.rollback().unwrap();
	assert!(ids(&t.query("FROM ns::v")).is_empty());
}

#[test]
fn the_commit_cdc_carries_the_view_rows_next_to_the_source_rows() {
	// A deferred reader of this view (P4) can only see it through CDC.
	let t = setup();
	t.command("INSERT ns::src [{ id: 2 }]");
	let origins: Vec<ObjectId> = cdc_entries(&t).iter().flat_map(|cdc| object_origins(&t, cdc)).collect();
	assert!(origins.iter().any(|o| matches!(o, ObjectId::Table(_))), "no table change in CDC: {origins:?}");
	assert!(origins.iter().any(|o| matches!(o, ObjectId::View(_))), "no view change in CDC: {origins:?}");
}

#[test]
fn a_view_created_after_an_earlier_load_is_maintained() {
	// A flow set reused from before the create would skip view w.
	let t = setup();
	t.command("INSERT ns::src [{ id: 2 }]");
	t.admin("CREATE TRANSACTIONAL VIEW ns::w { id: int4 } AS { FROM ns::src }");
	t.command("INSERT ns::src [{ id: 3 }]");
	assert_eq!(ids(&t.query("FROM ns::w")), vec![3]);
	assert_eq!(ids(&t.query("FROM ns::v")), vec![2, 3]);
}

#[test]
fn a_dropped_view_is_no_longer_maintained() {
	// Running the flow of a dropped view would write rows for a view that no longer exists.
	let t = setup();
	t.command("INSERT ns::src [{ id: 2 }]");
	t.admin("DROP VIEW ns::v");
	t.command("INSERT ns::src [{ id: 3 }]");
	assert_eq!(TestEngine::row_count(&t.query("FROM ns::src")), 2);
}

#[test]
fn a_view_created_and_written_in_one_admin_txn_is_maintained() {
	// The flow created one statement earlier in the same txn must be found, or the view stays empty.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::src }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("INSERT ns::src [{ id: 2 }]", Params::None);
	assert!(r.error.is_none(), "insert failed: {:?}", r.error);
	assert_eq!(ids(&txn.rql("FROM ns::v", Params::None).frames), vec![2]);
	txn.commit().unwrap();
	assert_eq!(ids(&t.query("FROM ns::v")), vec![2]);
}

#[test]
fn a_view_row_changed_then_removed_in_one_txn_is_gone_after_commit() {
	// The set then remove on a committed view row must not cancel out, or commit brings the old row back.
	let t = setup();
	t.command("INSERT ns::src [{ id: 2 }]");
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("UPDATE ns::src { id: 3 } FILTER { id == 2 }", Params::None);
	assert!(r.error.is_none(), "update failed: {:?}", r.error);
	let r = txn.rql("DELETE ns::src FILTER { id == 3 }", Params::None);
	assert!(r.error.is_none(), "delete failed: {:?}", r.error);
	assert!(ids(&txn.rql("FROM ns::v", Params::None).frames).is_empty());
	txn.commit().unwrap();
	assert!(ids(&t.query("FROM ns::v")).is_empty());
}

#[test]
fn a_row_that_entered_the_view_earlier_in_the_txn_leaves_it_on_delete() {
	// The delete must retract the row as the txn last saw it, not as committed, or the filter drops the retraction.
	let t = setup();
	t.command("INSERT ns::src [{ id: 1 }]");
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("UPDATE ns::src { id: 5 } FILTER { id == 1 }", Params::None);
	assert!(r.error.is_none(), "update failed: {:?}", r.error);
	assert_eq!(ids(&txn.rql("FROM ns::v", Params::None).frames), vec![5]);
	let r = txn.rql("DELETE ns::src FILTER { id == 5 }", Params::None);
	assert!(r.error.is_none(), "delete failed: {:?}", r.error);
	assert!(ids(&txn.rql("FROM ns::v", Params::None).frames).is_empty());
	txn.commit().unwrap();
	assert!(ids(&t.query("FROM ns::v")).is_empty());
}

#[test]
fn a_sort_value_change_reaches_the_view_cdc_as_one_update_of_its_row() {
	// The sort value is in the view key, so an insert and a remove of one row number replay in the wrong order.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::v { id: int4, v: int4 } AS { FROM ns::src | sort { v } }");
	t.command("INSERT ns::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");
	t.command("UPDATE ns::src { v: 90 } FILTER { id == 1 }");
	let last = cdc_entries(&t).pop().expect("the update must write a cdc record");
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	let changes = rebuild_changes(&last, &t.catalog(), &mut Transaction::Query(&mut query)).expect("rebuild");
	let view = changes
		.iter()
		.find(|change| matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_))))
		.expect("the update must change the view");
	let v = |batch: &RecordBatch| -> Vec<i32> {
		Frame::from(batch.clone()).rows().map(|row| row.get::<i32>("v").unwrap().unwrap()).collect()
	};
	match view.diffs.as_slice() {
		[
			Diff::Update {
				pre,
				post,
				..
			},
		] => {
			assert_eq!(v(pre), vec![10]);
			assert_eq!(v(post), vec![90]);
		}
		other => panic!("expected one update of the moved row, got {other:?}"),
	}
}
