// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	ConfigKey, GetConfig, SqliteConfig, cdc::consume::backlog::FlowBacklog, cdc_storage::store::CdcStore, embedded,
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
