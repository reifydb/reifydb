// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	ConfigKey, GetConfig, SqliteConfig,
	cdc::{consume::backlog::FlowBacklog, storage::CdcStore},
	embedded,
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
fn cdc_block_cache_capacity_override_reaches_the_sqlite_cdc_store() {
	// CDC_COMPACT_BLOCK_CACHE_CAPACITY requires_restart(), so the only moment it can take effect is
	// when the CDC store is constructed during build(). Asserting on the catalog value alone would
	// pass even while the store keeps BlockCache::DEFAULT_CAPACITY, which is exactly how this key
	// stayed inert; the observable has to be the cache the store actually built.
	temp_dir(|path| {
		let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
			.with_config(ConfigKey::CdcCompactBlockCacheCapacity, Value::Uint8(37))
			.build()
			.expect("fresh sqlite database must build");

		assert_eq!(db.engine().catalog().get_config(ConfigKey::CdcCompactBlockCacheCapacity), Value::Uint8(37));

		let cdc_store = db.engine().ioc().try_resolve::<CdcStore>().expect("CdcStore must be registered");
		let CdcStore::Sqlite(storage) = cdc_store else {
			panic!("a sqlite-backed database must register a sqlite CDC store");
		};
		assert_eq!(
			storage.block_cache_capacity(),
			37,
			"the block cache must be sized from the configured capacity, not the compiled-in default"
		);

		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}
