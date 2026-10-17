// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread;

use reifydb::{
	Frame, SqliteConfig, Value, WithSubsystem, embedded,
	runtime::context::clock::Clock,
	testing::db::{TempDbPath, TestDb},
	value::value::duration::Duration,
};
use reifydb_test_harness::assert::column_values;

fn sorted_syms(frames: &[Frame]) -> Vec<Value> {
	let mut values = column_values(&frames[0], "sym");
	values.sort_by_key(|v| format!("{v:?}"));
	values
}

fn usdc_and_wsol() -> Vec<Value> {
	vec![Value::Utf8("usdc".into()), Value::Utf8("wsol".into())]
}

#[test]
fn dictionary_entries_survive_a_reopen() {
	// A dictionary id in a durable row is worthless if the entry that decodes it is not durable
	// too. A memory store cannot observe that, since the entry lives as long as the process; only
	// a real sqlite store, stopped and reopened, can.
	let path = TempDbPath::new("dict_reopen");

	{
		let mut db = TestDb::sqlite_at(&path);
		db.admin("create namespace app");
		db.admin("create dictionary app::syms for utf8 as uint4");
		db.admin("create table app::t { sym: utf8 with { dictionary: app::syms } }");
		db.command("insert app::t [{ sym: 'wsol' }, { sym: 'usdc' }]");

		assert_eq!(
			sorted_syms(&db.query("from app::t")),
			usdc_and_wsol(),
			"precondition: both values decode before the restart"
		);

		db.stop();
	}

	let mut db = TestDb::sqlite_at(&path);
	assert_eq!(
		sorted_syms(&db.query("from app::t")),
		usdc_and_wsol(),
		"after a reopen the rows' dictionary ids must still decode to their values: the entries must have \
		 reached the persistent tier, not just the in-memory commit buffer"
	);
	db.stop();
}

#[test]
fn dictionary_entries_interned_by_a_deferred_flow_sink_survive_a_reopen() {
	// Here the only interner is a deferred view's sink on a flow worker, not a table insert. If
	// that entry misses the persistent tier, the view rows survive a restart holding ids nothing
	// can decode.
	let path = TempDbPath::new("dict_flow_reopen");

	{
		let mut db = TestDb::from(embedded::sqlite(SqliteConfig::new(&path)).with_flow(|f| f).build().unwrap());
		db.admin("create namespace app");
		db.admin("create dictionary app::syms for utf8 as uint4");
		db.admin("create table app::src { id: int4, sym: utf8 }");
		db.admin(
			"create deferred view app::v { id: int4, sym: utf8 with { dictionary: app::syms } } as { from app::src | map { id, sym } }",
		);

		db.command("insert app::src [{ id: 1, sym: 'wsol' }, { id: 2, sym: 'usdc' }]");

		let deadline = Clock::Real.instant() + Duration::from_seconds_const(10);
		loop {
			let frames = db.query("from app::v");
			let n = frames.first().map_or(0, |f| f.row_count());
			if n >= 2 || Clock::Real.instant() >= deadline {
				assert_eq!(n, 2, "precondition: the deferred view must materialize both rows");
				break;
			}
			thread::sleep(Duration::from_milliseconds_const(20).to_std());
		}

		db.stop();
	}

	let mut db = TestDb::from(embedded::sqlite(SqliteConfig::new(&path)).with_flow(|f| f).build().unwrap());
	assert_eq!(
		sorted_syms(&db.query("from app::v")),
		usdc_and_wsol(),
		"a value interned by a deferred flow sink must have a durable dictionary entry: after a reopen the \
		 view's stored ids must still decode to their strings, not to none"
	);
	db.stop();
}
