// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// A deferred view column declared `utf8 with { dictionary: ns::d }` must be a full peer of a table
// dictionary column: the materialization sink interns the value (assigning a dictionary id exactly as
// table DML does), stores the compact id, and a query over the view transparently decodes it back to
// the original string. The source table here carries NO dictionary, so the values are interned ONLY by
// the view sink - if the sink merely resolved existing ids (instead of interning) these rows would
// never materialize, and if the view scan did not decode, the query would return integer ids instead
// of strings. The duplicate `usdc` proves dedup: both rows decode to the same string from one id.

use std::{thread, time::Instant};

use reifydb::{WithSubsystem, embedded};
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::{Value, duration::Duration};

fn make_db() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE DICTIONARY app::syms FOR utf8 AS uint4");
	db.admin("CREATE TABLE app::src { id: int4, sym: utf8 }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { id: int4, sym: utf8 with { dictionary: app::syms } } AS { FROM app::src | map { id, sym } }",
	);
	db
}

fn read_syms(db: &TestDb) -> Vec<(i32, String)> {
	let Ok(frames) = db.try_query("FROM app::v") else {
		return vec![];
	};
	let Some(frame) = frames.first() else {
		return vec![];
	};
	let Some(id_col) = frame.columns.iter().find(|c| c.name == "id") else {
		return vec![];
	};
	let Some(sym_col) = frame.columns.iter().find(|c| c.name == "sym") else {
		return vec![];
	};

	let mut out = Vec::new();
	for i in 0..id_col.data.len() {
		let id = match id_col.data.get_value(i) {
			Value::Int4(v) => v,
			other => panic!("expected Int4 id, got {:?}", other),
		};
		let sym = match sym_col.data.get_value(i) {
			Value::Utf8(s) => s.to_string(),
			other => panic!(
				"view dictionary column must decode the stored id back to its Utf8 value; got {:?}",
				other
			),
		};
		out.push((id, sym));
	}
	out.sort();
	out
}

fn await_syms(db: &TestDb, expected_rows: usize) -> Vec<(i32, String)> {
	let deadline = Instant::now() + Duration::from_seconds(10).unwrap().to_std();
	loop {
		let rows = read_syms(db);
		if rows.len() >= expected_rows {
			return rows;
		}
		if Instant::now() >= deadline {
			return rows;
		}
		thread::sleep(Duration::from_milliseconds(20).unwrap().to_std());
	}
}

#[test]
fn deferred_view_dictionary_column_interns_and_decodes() {
	let db = make_db();

	db.command("INSERT app::src [{ id: 1, sym: 'sol' }, { id: 2, sym: 'usdc' }, { id: 3, sym: 'usdc' }]");

	let rows = await_syms(&db, 3);
	assert_eq!(
		rows,
		vec![(1, "sol".to_string()), (2, "usdc".to_string()), (3, "usdc".to_string())],
		"deferred view with a `utf8 with {{ dictionary }}` column must materialize all rows (sink interns \
		 the values) and decode the stored ids back to strings on query; got {:?}",
		rows
	);
}

#[test]
fn view_interned_value_shares_id_with_table_on_same_dictionary() {
	let db = make_db();

	db.command("INSERT app::src [{ id: 1, sym: 'usdc' }]");
	let view_rows = await_syms(&db, 1);
	assert_eq!(
		view_rows,
		vec![(1, "usdc".to_string())],
		"view must materialize the interned value; got {:?}",
		view_rows
	);

	// A table column on the SAME dictionary must reuse the id the view already assigned for 'usdc'
	// (shared, deduped id space across every column referencing the dictionary).
	db.admin("CREATE TABLE app::t { sym: utf8 with { dictionary: app::syms } }");
	db.command("INSERT app::t [{ sym: 'usdc' }]");

	let frames = db.query("FROM app::t");
	let frame = frames.first().expect("table frame");
	let sym_col = frame.columns.iter().find(|c| c.name == "sym").expect("sym column");
	assert_eq!(
		sym_col.data.get_value(0),
		Value::Utf8("usdc".into()),
		"a table dictionary column must decode to the same string the view stored, proving the view and \
		 table share one interned id space"
	);
}

// The interceptor-free commit, end to end. A TRANSACTIONAL view materializes inline, inside the
// committing transaction's pre-commit interceptor, which runs its flows in a rayon scope on the
// compute pool. Interning from there opens and commits the dictionary entry's own transaction. If
// that commit ran the interceptor chain, rayon would re-enter the transactional flow interceptor
// inline on this very thread, a sibling sink would call intern on the same dictionary, and it would
// re-acquire the same non-reentrant allocation lock: hard self-deadlock. This test hangs if the
// dictionary commit is ever given interceptors.
#[test]
fn transactional_view_interning_a_dictionary_column_does_not_deadlock() {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE DICTIONARY app::syms FOR utf8 AS uint4");
	db.admin("CREATE TABLE app::src { id: int4, sym: utf8 }");
	db.admin(
		"CREATE TRANSACTIONAL VIEW app::tv { id: int4, sym: utf8 with { dictionary: app::syms } } AS { FROM app::src | map { id, sym } }",
	);

	db.command("INSERT app::src [{ id: 1, sym: 'sol' }, { id: 2, sym: 'usdc' }, { id: 3, sym: 'usdc' }]");

	let frames = db.query("FROM app::tv");
	let frame = frames.first().expect("view frame");
	let sym_col = frame.columns.iter().find(|c| c.name == "sym").expect("sym column");

	let mut syms: Vec<String> = (0..sym_col.data.len())
		.map(|i| match sym_col.data.get_value(i) {
			Value::Utf8(s) => s.to_string(),
			other => panic!("dictionary column must decode to Utf8, got {:?}", other),
		})
		.collect();
	syms.sort();
	assert_eq!(
		syms,
		vec!["sol".to_string(), "usdc".to_string(), "usdc".to_string()],
		"a transactional view must intern and decode its dictionary column inline"
	);
}

// Two transactional views over the SAME dictionary materialize as rayon-parallel siblings inside one
// commit, each with its own pending writes but one shared registry. A first-seen value reaching both
// at once is the easiest way to fork one value into two ids - which would silently split every
// group-by and operator state keyed on that id, with no error anywhere. The allocation lock plus the
// committed re-read under it must collapse them onto one id, and the two views must agree.
#[test]
fn parallel_transactional_views_on_one_dictionary_agree_on_one_id() {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE DICTIONARY app::syms FOR utf8 AS uint4");
	db.admin("CREATE TABLE app::src { id: int4, sym: utf8 }");
	db.admin(
		"CREATE TRANSACTIONAL VIEW app::a { id: int4, sym: utf8 with { dictionary: app::syms } } AS { FROM app::src | map { id, sym } }",
	);
	db.admin(
		"CREATE TRANSACTIONAL VIEW app::b { id: int4, sym: utf8 with { dictionary: app::syms } } AS { FROM app::src | map { id, sym } }",
	);

	db.command("INSERT app::src [{ id: 1, sym: 'wsol' }]");

	let decode = |view: &str| -> String {
		let frames = db.query(&format!("FROM app::{view}"));
		let frame = frames.first().expect("view frame");
		let sym_col = frame.columns.iter().find(|c| c.name == "sym").expect("sym column");
		match sym_col.data.get_value(0) {
			Value::Utf8(s) => s.to_string(),
			other => panic!("expected Utf8, got {:?}", other),
		}
	};

	assert_eq!(decode("a"), "wsol");
	assert_eq!(decode("b"), "wsol", "both sibling views must decode the one id the dictionary assigned");
}
