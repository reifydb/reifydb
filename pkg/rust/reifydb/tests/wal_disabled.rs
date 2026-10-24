// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fs,
	panic::{AssertUnwindSafe, catch_unwind},
	path::Path,
};

use reifydb::{
	Database, Params, SqliteConfig, embedded,
	runtime::{
		RuntimeConfig,
		fatal::{FatalConfig, disarm},
	},
};
use reifydb_core::durability::Durability;
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::value::{frame::frame::Frame, identity::IdentityId};

fn boot(path: &Path, enabled: bool) -> Database {
	embedded::sqlite(SqliteConfig::new(path.join("db")))
		.with_runtime_config(disarmed())
		.with_fast_shutdown()
		.with_wal(move |wal| wal.enabled(enabled))
		.build()
		.expect("sqlite database must build")
}

fn disarmed() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn seed(db: &Database) {
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::t { id: int4 }", Params::None).expect("create table");
}

fn insert(db: &Database, id: i32) {
	db.command_as_root(&format!("INSERT app::t [{{ id: {id} }}]"), Params::None).expect("insert");
}

fn row_count(frames: &[Frame]) -> usize {
	frames.iter().map(|frame| frame.row_count()).sum()
}

fn panic_message(build: impl FnOnce() -> Database) -> String {
	// The boot panics before its own fatal config installs, so an armed hook would abort the whole test binary.
	disarm();
	let payload = match catch_unwind(AssertUnwindSafe(build)) {
		Ok(_) => panic!("boot must refuse, but it built a database"),
		Err(payload) => payload,
	};
	payload.downcast_ref::<String>()
		.cloned()
		.or_else(|| payload.downcast_ref::<&str>().map(|message| message.to_string()))
		.expect("boot panic carries a message")
}

#[test]
fn default_builder_creates_no_wal_dir() {
	// The wal is off unless asked for; a wal dir here means the default flipped back to the real log.
	temp_dir(|path| {
		{
			let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
				.with_runtime_config(disarmed())
				.build()
				.expect("fresh database must build");
			seed(&db);
			insert(&db, 1);
		}
		assert!(path.join("db").exists(), "the database dir must exist, or this test proves nothing");
		assert!(!path.join("db").join("wal").exists(), "a default boot must not write a commit log");
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}

#[test]
fn disabled_boot_refuses_a_leftover_wal_dir() {
	// Booting on the nop over a real log would silently drop every commit the log holds past the stores.
	temp_dir(|path| {
		{
			let db = boot(path, true);
			seed(&db);
			insert(&db, 1);
		}
		assert!(
			path.join("db").join("wal").exists(),
			"the first run must leave a commit log, or this proves nothing"
		);

		let message = panic_message(|| boot(path, false));
		assert!(message.contains("wal is disabled"), "unexpected boot panic: {message}");
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}

#[test]
fn enabled_boot_refuses_a_saved_lsn_past_the_wal_head() {
	// A fresh log restarts lsns at 1, so replay from a stale saved lsn would skip the single records it holds.
	temp_dir(|path| {
		{
			let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
				.with_runtime_config(disarmed())
				.with_wal(|wal| wal.enabled(true))
				.build()
				.expect("fresh database must build");
			seed(&db);
			insert(&db, 1);
		}
		fs::remove_dir_all(path.join("db").join("wal"))?;

		let message = panic_message(|| boot(path, true));
		assert!(message.contains("single.db is saved through lsn"), "unexpected boot panic: {message}");
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}

#[test]
fn enabled_wal_replays_single_records_after_restart() {
	// Row numbers come from single store sequences; without replay the next insert reuses one and overwrites a row.
	temp_dir(|path| {
		{
			let db = boot(path, true);
			seed(&db);
			insert(&db, 1);
			insert(&db, 2);
		}
		let db = boot(path, true);
		insert(&db, 3);
		let frames = db.query_as_root("FROM app::t", Params::None).expect("query rows");
		assert_eq!(row_count(&frames), 3, "a row was overwritten, so the sequence was not replayed");
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}

#[test]
fn fsync_commit_returns_with_the_wal_disabled() {
	// With the nop, fsync must not block on or panic over a log that never syncs.
	temp_dir(|path| {
		let db = boot(path, false);
		seed(&db);
		let mut txn =
			db.engine().begin_command(IdentityId::root(), Durability::Fsync).expect("begin fsync command");
		let inserted = txn.rql("INSERT app::t [{ id: 1 }]", Params::None);
		assert!(inserted.error.is_none(), "insert in fsync command failed: {:?}", inserted.error);
		txn.commit().expect("fsync commit returns");
		let frames = db.query_as_root("FROM app::t", Params::None).expect("query rows");
		assert_eq!(row_count(&frames), 1, "the fsync commit must be visible");
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}
