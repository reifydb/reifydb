// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread,
	time::{Duration, Instant},
};

use reifydb::{
	Frame, SqliteConfig, Value, WithSubsystem,
	core::key::kind::KeyKind,
	embedded,
	testing::db::{TempDbPath, TestDb},
};
use reifydb_codec::key::serializer::KeySerializer;
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

#[test]
fn dictionary_entries_reach_disk_without_a_graceful_stop() {
	// A crash or SIGKILL skips stop(), so only the single store's periodic flush can have
	// persisted the entry. Pins that the flush actor reaches the persistent tier without a
	// shutdown, and that entries no longer land in the multi store at all.
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

	// Derived, never hardcoded: KeyKind discriminants get renumbered when a kind is dropped, and a stale
	// literal here would silently turn both assertions below into vacuous ones - "found 0 entries" reads
	// as a durability bug on one side and as a pass on the other, when it only means the prefix moved.
	let entry_prefix = {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(KeyKind::DictionaryEntry as u8);
		format!("{:02X}", serializer.to_encoded_key()[0])
	};

	let count_dictionary_entries = |db_file: &str, table: &str| -> i64 {
		let file = path.with_extension("").join(db_file);
		let out = std::process::Command::new("sqlite3")
			.arg(&file)
			.arg(format!("SELECT COUNT(*) FROM {table} WHERE hex(substr(key,1,1))='{entry_prefix}';"))
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
