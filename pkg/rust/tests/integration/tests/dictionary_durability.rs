// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread,
	time::{Duration, Instant},
};

use reifydb::{Frame, SqliteConfig, Value, WithSubsystem, embedded};
use reifydb_test_harness::{
	assert::column_values,
	db::{TempDbPath, TestDb},
};

fn sorted_syms(frames: &[Frame]) -> Vec<Value> {
	let mut values = column_values(&frames[0], "sym");
	values.sort_by_key(|v| format!("{v:?}"));
	values
}

fn usdc_and_wsol() -> Vec<Value> {
	vec![Value::Utf8("usdc".into()), Value::Utf8("wsol".into())]
}

// A dictionary id embedded in a durable row is worthless if the entry that decodes it is not durable
// too. Interning writes the entry through the single store; if it never reaches the persistent tier,
// every id in every surviving row is a dangling reference, and the first read after a restart cannot
// resolve any of them.
//
// The whole suite around interning runs on a memory store, which cannot observe this: the entry is
// "durable" for as long as the process lives. Only a real sqlite store, stopped and reopened, does.
#[test]
fn dictionary_entries_survive_a_reopen() {
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

// The same durability question for the path raptor actually uses: the value is interned by a DEFERRED
// VIEW's sink, not by a table insert. The source table carries no dictionary, so the only interner is
// the flow sink, running on a flow worker inside a slice. If that intern's entry does not reach the
// persistent tier, the view rows survive a restart holding ids that nothing can decode - and an
// operator that resolves a value to its id on replay finds nothing.
#[test]
fn dictionary_entries_interned_by_a_deferred_flow_sink_survive_a_reopen() {
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

		let deadline = Instant::now() + Duration::from_secs(10);
		loop {
			let frames = db.query("from app::v");
			let n = frames.first().and_then(|f| f.columns.first()).map_or(0, |c| c.data.len());
			if n >= 2 || Instant::now() >= deadline {
				assert_eq!(n, 2, "precondition: the deferred view must materialize both rows");
				break;
			}
			thread::sleep(Duration::from_millis(20));
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

// Durability WITHOUT a graceful stop. A crash, an abort, or a SIGKILL after docker's grace period
// expires all skip stop(), so the only thing that can have persisted a dictionary entry is the
// single store's periodic flush. Dictionary entries live in the single store (like sequences); this
// pins that the flush actor moves them to the persistent tier without a shutdown, and that they no
// longer land in the multi store at all.
#[test]
fn dictionary_entries_reach_disk_without_a_graceful_stop() {
	let path = TempDbPath::new("dict_nostop");

	let db = TestDb::sqlite_at(&path);
	db.admin("create namespace app");
	db.admin("create dictionary app::syms for utf8 as uint4");
	db.admin("create table app::t { sym: utf8 with { dictionary: app::syms } }");
	db.command("insert app::t [{ sym: 'wsol' }]");

	// Give the periodic flush (5s interval) ample time to reach the persistent tier.
	thread::sleep(Duration::from_secs(12));

	// Skip Drop, which would run the graceful shutdown flush. This is the crash case.
	std::mem::forget(db);

	let count_dictionary_entries = |db_file: &str, table: &str| -> i64 {
		let file = path.with_extension("").join(db_file);
		let out = std::process::Command::new("sqlite3")
			.arg(&file)
			.arg(format!("SELECT COUNT(*) FROM {table} WHERE hex(substr(key,1,1))='DE';"))
			.output()
			.expect("sqlite3 must be available");
		String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(-1)
	};

	let single_entries = count_dictionary_entries("single.db", "entries");
	assert!(
		single_entries > 0,
		"a dictionary entry must reach the single store's persistent tier via the periodic flush, not \
		 only via the shutdown flush: found {single_entries} DictionaryEntry rows in single.db after an \
		 ungraceful exit"
	);

	let multi_entries = count_dictionary_entries("multi.db", "multi__current");
	assert_eq!(
		multi_entries, 0,
		"dictionary entries must no longer be written to the multi store: found {multi_entries} \
		 DictionaryEntry rows in multi.db"
	);
}
