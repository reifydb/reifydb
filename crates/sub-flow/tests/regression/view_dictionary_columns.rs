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

use reifydb::{Database, Params, WithSubsystem, embedded};
use reifydb_value::value::{Value, duration::Duration};

fn make_db() -> Database {
	let db = embedded::memory().with_flow(|f| f).build().expect("build memory db");
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE DICTIONARY app::syms FOR utf8 AS uint4", Params::None).expect("create dictionary");
	db.admin_as_root("CREATE TABLE app::src { id: int4, sym: utf8 }", Params::None).expect("create table");
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::v { id: int4, sym: utf8 with { dictionary: app::syms } } AS { FROM app::src | map { id, sym } }",
		Params::None,
	)
	.expect("create deferred view with dictionary column");
	db
}

fn read_syms(db: &Database) -> Vec<(i32, String)> {
	let Ok(frames) = db.query_as_root("FROM app::v", Params::None) else {
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

fn await_syms(db: &Database, expected_rows: usize) -> Vec<(i32, String)> {
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

	db.command_as_root(
		"INSERT app::src [{ id: 1, sym: 'sol' }, { id: 2, sym: 'usdc' }, { id: 3, sym: 'usdc' }]",
		Params::None,
	)
	.expect("insert source rows");

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

	db.command_as_root("INSERT app::src [{ id: 1, sym: 'usdc' }]", Params::None).expect("insert source row");
	let view_rows = await_syms(&db, 1);
	assert_eq!(
		view_rows,
		vec![(1, "usdc".to_string())],
		"view must materialize the interned value; got {:?}",
		view_rows
	);

	// A table column on the SAME dictionary must reuse the id the view already assigned for 'usdc'
	// (shared, deduped id space across every column referencing the dictionary).
	db.admin_as_root("CREATE TABLE app::t { sym: utf8 with { dictionary: app::syms } }", Params::None)
		.expect("create table on same dictionary");
	db.command_as_root("INSERT app::t [{ sym: 'usdc' }]", Params::None).expect("insert table row");

	let frames = db.query_as_root("FROM app::t", Params::None).expect("query table");
	let frame = frames.first().expect("table frame");
	let sym_col = frame.columns.iter().find(|c| c.name == "sym").expect("sym column");
	assert_eq!(
		sym_col.data.get_value(0),
		Value::Utf8("usdc".into()),
		"a table dictionary column must decode to the same string the view stored, proving the view and \
		 table share one interned id space"
	);
}
