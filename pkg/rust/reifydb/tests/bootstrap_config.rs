// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, io, path::Path};

use reifydb::{Params, SqliteConfig, WalMode, embedded, multi_storage::MultiStore};
use reifydb_testing::tempdir::temp_dir;

fn copy_dir(from: &Path, to: &Path) -> io::Result<()> {
	fs::create_dir_all(to)?;
	for entry in fs::read_dir(from)? {
		let entry = entry?;
		let target = to.join(entry.file_name());
		if entry.file_type()?.is_dir() {
			copy_dir(&entry.path(), &target)?;
		} else {
			fs::copy(entry.path(), &target)?;
		}
	}
	Ok(())
}

fn range_shards(path: &Path) -> usize {
	let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
		.with_wal(|wal| wal.mode(WalMode::default()))
		.build()
		.expect("sqlite database must reopen");
	let store = db.engine().ioc().try_resolve::<MultiStore>().expect("MultiStore must be registered");
	store.range_shard_metrics().len()
}

#[test]
fn a_startup_config_set_before_a_crash_shapes_the_next_boot() {
	// Configs are read before the runtime exists, so replay must run first, otherwise a crash reverts them.
	temp_dir(|path| {
		{
			let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
				.with_wal(|wal| wal.mode(WalMode::default()))
				.with_fast_shutdown()
				.build()
				.expect("fresh sqlite database must build");
			db.admin_as_root(r#"CALL system::config::set("MULTI_RANGE_BUFFER_SHARDS", 5)"#, Params::None)
				.expect("the config set succeeds");
		}

		let control = path.join("control");
		copy_dir(&path.join("db"), &control.join("db"))?;
		fs::remove_dir_all(control.join("db").join("wal"))?;
		assert_ne!(range_shards(&control), 5, "the set must live only in the log, or this test proves nothing");

		assert_eq!(range_shards(path), 5, "the next boot must size the range buffer from the replayed config");
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}
