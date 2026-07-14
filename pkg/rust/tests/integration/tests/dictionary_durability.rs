// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fs, thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use reifydb::{Params, SqliteConfig, Value, WithSubsystem, embedded};

fn unique_db_path(tag: &str) -> std::path::PathBuf {
	let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	std::env::temp_dir().join(format!("reifydb_dict_{tag}_{}_{}.reifydb", std::process::id(), nanos))
}

// A dictionary id embedded in a durable row is worthless if the entry that decodes it is not durable
// too. Interning writes the entry through the multi store; if it never reaches the persistent tier,
// every id in every surviving row is a dangling reference, and the first read after a restart cannot
// resolve any of them.
//
// The whole suite around interning runs on a memory store, which cannot observe this: the entry is
// "durable" for as long as the process lives. Only a real sqlite store, stopped and reopened, does.
#[test]
fn dictionary_entries_survive_a_reopen() {
	let path = unique_db_path("reopen");
	let _ = fs::remove_dir_all(&path);

	{
		let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
		db.admin_as_root("create namespace app", Params::None).unwrap();
		db.admin_as_root("create dictionary app::syms for utf8 as uint4", Params::None).unwrap();
		db.admin_as_root("create table app::t { sym: utf8 with { dictionary: app::syms } }", Params::None)
			.unwrap();
		db.command_as_root("insert app::t [{ sym: 'wsol' }, { sym: 'usdc' }]", Params::None).unwrap();

		let frames = db.query_as_root("from app::t", Params::None).unwrap();
		let col = frames[0].columns.iter().find(|c| c.name == "sym").expect("sym column");
		let mut before: Vec<Value> = (0..col.data.len()).map(|i| col.data.get_value(i)).collect();
		before.sort_by_key(|v| format!("{v:?}"));
		assert_eq!(
			before,
			vec![Value::Utf8("usdc".into()), Value::Utf8("wsol".into())],
			"precondition: both values decode before the restart"
		);

		db.stop().unwrap();
	}

	let mut db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
	let frames = db.query_as_root("from app::t", Params::None).unwrap();
	let col = frames[0].columns.iter().find(|c| c.name == "sym").expect("sym column");

	let mut decoded: Vec<Value> = (0..col.data.len()).map(|i| col.data.get_value(i)).collect();
	decoded.sort_by_key(|v| format!("{v:?}"));

	assert_eq!(
		decoded,
		vec![Value::Utf8("usdc".into()), Value::Utf8("wsol".into())],
		"after a reopen the rows' dictionary ids must still decode to their values: the entries must have \
		 reached the persistent tier, not just the in-memory commit buffer"
	);

	db.stop().unwrap();
	let _ = fs::remove_dir_all(&path);
}

// The same durability question for the path raptor actually uses: the value is interned by a DEFERRED
// VIEW's sink, not by a table insert. The source table carries no dictionary, so the only interner is
// the flow sink, running on a flow worker inside a slice. If that intern's entry does not reach the
// persistent tier, the view rows survive a restart holding ids that nothing can decode - and an
// operator that resolves a value to its id on replay finds nothing.
#[test]
fn dictionary_entries_interned_by_a_deferred_flow_sink_survive_a_reopen() {
	let path = unique_db_path("flow_reopen");
	let _ = fs::remove_dir_all(&path);

	{
		let mut db = embedded::sqlite(SqliteConfig::new(&path)).with_flow(|f| f).build().unwrap();
		db.admin_as_root("create namespace app", Params::None).unwrap();
		db.admin_as_root("create dictionary app::syms for utf8 as uint4", Params::None).unwrap();
		db.admin_as_root("create table app::src { id: int4, sym: utf8 }", Params::None).unwrap();
		db.admin_as_root(
			"create deferred view app::v { id: int4, sym: utf8 with { dictionary: app::syms } } as { from app::src | map { id, sym } }",
			Params::None,
		)
		.unwrap();

		db.command_as_root("insert app::src [{ id: 1, sym: 'wsol' }, { id: 2, sym: 'usdc' }]", Params::None)
			.unwrap();

		let deadline = Instant::now() + Duration::from_secs(10);
		loop {
			let frames = db.query_as_root("from app::v", Params::None).unwrap();
			let n = frames.first().and_then(|f| f.columns.first()).map_or(0, |c| c.data.len());
			if n >= 2 || Instant::now() >= deadline {
				assert_eq!(n, 2, "precondition: the deferred view must materialize both rows");
				break;
			}
			thread::sleep(Duration::from_millis(20));
		}

		db.stop().unwrap();
	}

	let mut db = embedded::sqlite(SqliteConfig::new(&path)).with_flow(|f| f).build().unwrap();
	let frames = db.query_as_root("from app::v", Params::None).unwrap();
	let col = frames[0].columns.iter().find(|c| c.name == "sym").expect("sym column");

	let mut decoded: Vec<Value> = (0..col.data.len()).map(|i| col.data.get_value(i)).collect();
	decoded.sort_by_key(|v| format!("{v:?}"));

	assert_eq!(
		decoded,
		vec![Value::Utf8("usdc".into()), Value::Utf8("wsol".into())],
		"a value interned by a deferred flow sink must have a durable dictionary entry: after a reopen the \
		 view's stored ids must still decode to their strings, not to none"
	);

	db.stop().unwrap();
	let _ = fs::remove_dir_all(&path);
}

// Durability WITHOUT a graceful stop. A crash, an abort, or a SIGKILL after docker's grace period
// expires all skip stop(), so the only thing that can have persisted a dictionary entry is the
// periodic sweep. Source rows reach the persistent tier that way; this pins that dictionary entries
// do too. If they do not, every id in every surviving row is a dangling reference the moment the
// process dies uncleanly, and a restart cannot decode any of them.
#[test]
fn dictionary_entries_reach_disk_without_a_graceful_stop() {
	let path = unique_db_path("nostop");
	let _ = fs::remove_dir_all(&path);

	let db = embedded::sqlite(SqliteConfig::new(&path)).build().unwrap();
	db.admin_as_root("create namespace app", Params::None).unwrap();
	db.admin_as_root("create dictionary app::syms for utf8 as uint4", Params::None).unwrap();
	db.admin_as_root("create table app::t { sym: utf8 with { dictionary: app::syms } }", Params::None).unwrap();
	db.command_as_root("insert app::t [{ sym: 'wsol' }]", Params::None).unwrap();

	// Give the periodic sweep ample time to reach the persistent tier.
	thread::sleep(Duration::from_secs(12));

	// Skip Drop, which would run the graceful shutdown flush. This is the crash case.
	std::mem::forget(db);

	let multi = path.with_extension("").join("multi.db");
	let out = std::process::Command::new("sqlite3")
		.arg(&multi)
		.arg("SELECT COUNT(*) FROM multi__current WHERE hex(substr(key,1,1))='DE';")
		.output()
		.expect("sqlite3 must be available");
	let entries: i64 = String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(-1);

	assert!(
		entries > 0,
		"a dictionary entry must reach the persistent tier via the periodic sweep, not only via the \
		 shutdown flush: found {entries} DictionaryEntry rows in multi.db after an ungraceful exit"
	);
}
