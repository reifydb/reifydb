// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fs, io, path::Path};

use reifydb::{
	ConfigKey, GetConfig, Params, SqliteConfig,
	cdc::consume::backlog::FlowBacklog,
	cdc_storage::{storage::CdcStorage, store::CdcStore},
	embedded,
	multi_storage::MultiStore,
	value::value::Value,
};
use reifydb_testing::tempdir::temp_dir;

#[test]
fn bootstrap_config_overrides_apply_on_first_boot() {
	// The override must not just land in the catalog: the component constructed from it during
	// bootstrap must observe the overridden value, not the default. The flow backlog is built
	// from FLOW_BACKLOG_MEMORY_LIMIT at boot, so its limit is the observable.
	temp_dir(|path| {
		let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
			.with_config(ConfigKey::FlowBacklogMemoryLimit, Value::Uint8(12_345_678))
			.build()
			.expect("fresh sqlite database must build");

		assert_eq!(
			db.engine().catalog().get_config(ConfigKey::FlowBacklogMemoryLimit),
			Value::Uint8(12_345_678)
		);
		let backlog = db.engine().ioc().try_resolve::<FlowBacklog>().expect("FlowBacklog must be registered");
		assert_eq!(backlog.limit().as_bytes(), 12_345_678);

		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}

#[test]
fn cdc_read_buffer_override_reaches_the_cdc_store() {
	// CDC_READ_BUFFER_BYTES requires_restart(), so the observable has to be the budget the read buffer actually
	// built, summed across its shards, not the catalog value.
	temp_dir(|path| {
		let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
			.with_config(ConfigKey::CdcReadBufferBytes, Value::Uint8(8 * 1024 * 1024))
			.build()
			.expect("fresh sqlite database must build");

		assert_eq!(
			db.engine().catalog().get_config(ConfigKey::CdcReadBufferBytes),
			Value::Uint8(8 * 1024 * 1024)
		);

		let cdc_store = db.engine().ioc().try_resolve::<CdcStore>().expect("CdcStore must be registered");
		let shards = cdc_store.read_buffer_shard_metrics();
		assert!(!shards.is_empty(), "a configured read buffer must exist, not be disabled");
		assert_eq!(
			shards.iter().map(|shard| shard.limit.as_bytes()).sum::<u64>(),
			8 * 1024 * 1024,
			"the read buffer must be sized from the configured bytes, not the compiled-in default"
		);

		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}

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
	let db = embedded::sqlite(SqliteConfig::new(path.join("db"))).build().expect("sqlite database must reopen");
	let store = db.engine().ioc().try_resolve::<MultiStore>().expect("MultiStore must be registered");
	store.range_shard_metrics().len()
}

#[test]
fn a_startup_config_set_before_a_crash_shapes_the_next_boot() {
	// Configs are read before the runtime exists, so replay must run first, otherwise a crash reverts them.
	temp_dir(|path| {
		{
			let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
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

#[test]
fn a_boot_that_lost_its_log_never_reissues_a_version_cdc_already_holds() {
	// A power cut can leave cdc.db ahead of the log; reusing those versions makes the CDC producer refuse every
	// commit.
	temp_dir(|path| {
		{
			let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
				.with_fast_shutdown()
				.build()
				.expect("fresh sqlite database must build");
			db.admin_as_root(r#"CALL system::config::set("MULTI_RANGE_BUFFER_SHARDS", 5)"#, Params::None)
				.expect("the config set succeeds");
		}
		fs::remove_dir_all(path.join("db").join("wal"))?;

		let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
			.build()
			.expect("sqlite database must reopen");
		let cdc_store = db.engine().ioc().try_resolve::<CdcStore>().expect("CdcStore must be registered");
		let cdc_head = CdcStorage::max_version(&cdc_store)
			.expect("the cdc head is readable")
			.expect("cdc.db must hold versions from the first run, or this test proves nothing");
		let current = db.engine().multi().version().expect("the current version is readable");
		assert!(
			current >= cdc_head,
			"boot resumed at {current} below cdc's {cdc_head}, so the next commit reuses one"
		);
		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}
