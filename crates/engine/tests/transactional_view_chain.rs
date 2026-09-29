// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, ops::Bound};

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
	value::{frame::frame::Frame, identity::IdentityId, system_columns::row_numbers},
};

fn chain() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::a { id: int4, v: int4 } AS { FROM ns::src | filter { v > 10 } }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::b { id: int4, v: int4 } AS { FROM ns::a | filter { v < 90 } }");
	t
}

fn diamond() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::lo { id: int4, v: int4 } AS { FROM ns::src | filter { v < 50 } }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::hi { id: int4, v: int4 } AS { FROM ns::src | filter { v > 20 } }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::both { id: int4, v: int4 } AS { FROM ns::lo | append { FROM ns::hi } }");
	t
}

fn dictionary_chain() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE DICTIONARY ns::syms FOR utf8 AS uint4");
	t.admin("CREATE TABLE ns::src { id: int4, sym: utf8 with { dictionary: ns::syms } }");
	t.admin(
		"CREATE TRANSACTIONAL VIEW ns::a { id: int4, sym: utf8 with { dictionary: ns::syms } } AS { FROM ns::src }",
	);
	t.admin("CREATE TRANSACTIONAL VIEW ns::b { id: int4, sym: utf8 } AS { FROM ns::a }");
	t.admin(
		"CREATE TRANSACTIONAL VIEW ns::c { id: int4, sym: utf8 with { dictionary: ns::syms } } AS { FROM ns::a }",
	);
	t
}

fn syms(frames: &[Frame]) -> Vec<String> {
	let mut syms: Vec<String> = frames[0].rows().map(|row| row.get::<String>("sym").unwrap().unwrap()).collect();
	syms.sort();
	syms
}

fn follows_the_dictionary_source(view: &str) {
	let t = dictionary_chain();
	for (rql, want) in [
		("INSERT ns::src [{ id: 1, sym: 'red' }]", vec!["red"]),
		("UPDATE ns::src { sym: 'blue' } FILTER { id == 1 }", vec!["blue"]),
		("DELETE ns::src FILTER { id == 1 }", vec![]),
	] {
		t.command(rql);
		assert_eq!(syms(&t.query(&format!("FROM {view}"))), want, "{view} after: {rql}");
	}
}

fn ids(frames: &[Frame]) -> Vec<i32> {
	let mut ids: Vec<i32> = frames[0].rows().map(|row| row.get::<i32>("id").unwrap().unwrap()).collect();
	ids.sort();
	ids
}

fn pairs(frames: &[Frame]) -> Vec<(i32, i32)> {
	let mut pairs: Vec<(i32, i32)> = frames[0]
		.rows()
		.map(|row| (row.get::<i32>("id").unwrap().unwrap(), row.get::<i32>("v").unwrap().unwrap()))
		.collect();
	pairs.sort();
	pairs
}

fn last_cdc(t: &TestEngine) -> Cdc {
	t.await_cdc();
	let mut entries = t.cdc_store().read_range(Bound::Unbounded, Bound::Unbounded, 10_000).expect("cdc read").items;
	entries.pop().expect("the commit must write a cdc record")
}

fn view_origins(t: &TestEngine, cdc: &Cdc) -> BTreeSet<ObjectId> {
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	rebuild_changes(cdc, &t.catalog(), &mut Transaction::Query(&mut query))
		.expect("rebuild")
		.into_iter()
		.filter_map(|change| match change.origin {
			ChangeOrigin::Object(object @ ObjectId::View(_)) => Some(object),
			_ => None,
		})
		.collect()
}

fn view_inserted_rows(t: &TestEngine, cdc: &Cdc) -> Vec<usize> {
	let mut query = t.begin_query(IdentityId::system()).expect("query transaction");
	let mut counts: Vec<usize> = rebuild_changes(cdc, &t.catalog(), &mut Transaction::Query(&mut query))
		.expect("rebuild")
		.into_iter()
		.filter(|change| matches!(change.origin, ChangeOrigin::Object(ObjectId::View(_))))
		.map(|change| {
			change.diffs
				.iter()
				.map(|diff| match diff {
					Diff::Insert {
						post,
						..
					} => post.num_rows(),
					other => panic!(
						"an insert-only commit rebuilt a non-insert view diff: {other:?}"
					),
				})
				.sum()
		})
		.collect();
	counts.sort();
	counts
}

#[test]
fn an_insert_reaches_the_second_view_through_both_filters() {
	// The second hop must run on the first hop's output, or b stays empty or skips a's filter.
	let t = chain();
	t.command("INSERT ns::src [{ id: 1, v: 5 }, { id: 2, v: 50 }, { id: 3, v: 95 }]");
	assert_eq!(ids(&t.query("FROM ns::a")), vec![2, 3]);
	assert_eq!(ids(&t.query("FROM ns::b")), vec![2]);
}

#[test]
fn an_update_leaves_the_second_view_but_stays_in_the_first() {
	// The update must reach b as a retraction, or b keeps a row its filter now drops.
	let t = chain();
	t.command("INSERT ns::src [{ id: 2, v: 50 }]");
	t.command("UPDATE ns::src { v: 95 } FILTER { id == 2 }");
	assert_eq!(pairs(&t.query("FROM ns::a")), vec![(2, 95)]);
	assert!(ids(&t.query("FROM ns::b")).is_empty());
}

#[test]
fn a_delete_leaves_both_views() {
	// A delete that stops at the first hop leaves the row in b forever.
	let t = chain();
	t.command("INSERT ns::src [{ id: 2, v: 50 }]");
	t.command("DELETE ns::src FILTER { id == 2 }");
	assert!(ids(&t.query("FROM ns::a")).is_empty());
	assert!(ids(&t.query("FROM ns::b")).is_empty());
}

#[test]
fn a_read_later_in_the_same_txn_sees_both_hops() {
	// The second hop must also run inside the writing txn, not only at commit.
	let t = chain();
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("INSERT ns::src [{ id: 2, v: 50 }]", Params::None);
	assert!(r.error.is_none(), "insert failed: {:?}", r.error);
	assert_eq!(ids(&txn.rql("FROM ns::b", Params::None).frames), vec![2]);
	txn.commit().unwrap();
	assert_eq!(ids(&t.query("FROM ns::b")), vec![2]);
}

#[test]
fn a_rollback_leaves_both_views_unchanged() {
	// Rows of both hops live in the txn's write set, so a rollback must drop them all.
	let t = chain();
	let mut txn = t.begin_command(IdentityId::system()).unwrap();
	let r = txn.rql("INSERT ns::src [{ id: 2, v: 50 }]", Params::None);
	assert!(r.error.is_none(), "insert failed: {:?}", r.error);
	assert_eq!(ids(&txn.rql("FROM ns::b", Params::None).frames), vec![2]);
	txn.rollback().unwrap();
	assert!(ids(&t.query("FROM ns::a")).is_empty());
	assert!(ids(&t.query("FROM ns::b")).is_empty());
}

#[test]
fn the_commit_cdc_carries_a_change_for_each_view() {
	// A deferred reader of b sees b only through CDC, so the second hop must reach it too.
	let t = chain();
	t.command("INSERT ns::src [{ id: 2, v: 50 }]");
	let last = last_cdc(&t);
	assert_eq!(view_origins(&t, &last).len(), 2, "expected a change for a and for b in the insert's CDC");
}

#[test]
fn a_chain_created_and_written_in_one_admin_txn_is_maintained() {
	// Both flows created earlier in the same txn must be found and ordered, or b stays empty.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	for rql in [
		"CREATE TRANSACTIONAL VIEW ns::a { id: int4, v: int4 } AS { FROM ns::src | filter { v > 10 } }",
		"CREATE TRANSACTIONAL VIEW ns::b { id: int4, v: int4 } AS { FROM ns::a | filter { v < 90 } }",
		"INSERT ns::src [{ id: 2, v: 50 }]",
	] {
		let r = txn.rql(rql, Params::None);
		assert!(r.error.is_none(), "{rql} failed: {:?}", r.error);
	}
	assert_eq!(ids(&txn.rql("FROM ns::b", Params::None).frames), vec![2]);
	txn.commit().unwrap();
	assert_eq!(ids(&t.query("FROM ns::b")), vec![2]);
}

#[test]
fn a_sort_value_change_upstream_reaches_the_downstream_view_as_one_row() {
	// The sort value moves a's key; b must end with the new row and not keep the old one.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4, v: int4 }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::a { id: int4, v: int4 } AS { FROM ns::src | sort { v } }");
	t.admin("CREATE TRANSACTIONAL VIEW ns::b { id: int4, v: int4 } AS { FROM ns::a }");
	t.command("INSERT ns::src [{ id: 1, v: 10 }, { id: 2, v: 20 }]");
	t.command("UPDATE ns::src { v: 90 } FILTER { id == 1 }");
	assert_eq!(pairs(&t.query("FROM ns::a")), vec![(1, 90), (2, 20)]);
	assert_eq!(pairs(&t.query("FROM ns::b")), vec![(1, 90), (2, 20)]);
}

#[test]
fn a_diamond_holds_each_branch_row_once() {
	// both must run after lo and hi settle; running early misses a branch, running twice doubles one.
	let t = diamond();
	t.command("INSERT ns::src [{ id: 1, v: 10 }, { id: 2, v: 30 }, { id: 3, v: 70 }]");
	let frames = t.query("FROM ns::both");
	assert_eq!(ids(&frames), vec![1, 2, 2, 3]);
	let rownums: BTreeSet<u64> = row_numbers(&frames[0].batch).expect("row numbers").iter().map(|r| r.0).collect();
	assert_eq!(rownums.len(), 4, "the two branch rows of id 2 must not share a row number: {rownums:?}");
}

#[test]
fn a_diamond_follows_an_update_that_leaves_one_branch() {
	// The retraction must come through lo only; retracting through hi too would drop id 2 from both.
	let t = diamond();
	t.command("INSERT ns::src [{ id: 1, v: 10 }, { id: 2, v: 30 }, { id: 3, v: 70 }]");
	t.command("UPDATE ns::src { v: 80 } FILTER { id == 2 }");
	assert_eq!(pairs(&t.query("FROM ns::both")), vec![(1, 10), (2, 80), (3, 70)]);
}

#[test]
fn a_diamond_follows_a_delete_in_both_branches() {
	// A delete seen through one branch only would leave the other branch's row in both.
	let t = diamond();
	t.command("INSERT ns::src [{ id: 1, v: 10 }, { id: 2, v: 30 }, { id: 3, v: 70 }]");
	t.command("DELETE ns::src FILTER { id == 2 }");
	assert_eq!(ids(&t.query("FROM ns::both")), vec![1, 3]);
}

#[test]
fn a_diamond_commit_emits_each_branch_row_once() {
	// lo and hi carry one row each and both carries exactly two; a double feed shows as more.
	let t = diamond();
	t.command("INSERT ns::src [{ id: 2, v: 30 }]");
	let last = last_cdc(&t);
	assert_eq!(view_inserted_rows(&t, &last), vec![1, 1, 2]);
}

#[test]
fn a_dictionary_view_feeds_its_values_to_a_plain_column_downstream() {
	// b must get the text; a dictionary id read as a value would show a number or none instead.
	follows_the_dictionary_source("ns::b");
}

#[test]
fn a_dictionary_view_feeds_its_values_to_a_dictionary_column_downstream() {
	// c re-interns what a hands on; the delete also takes the lookup path, which fails on a stray id.
	follows_the_dictionary_source("ns::c");
}
