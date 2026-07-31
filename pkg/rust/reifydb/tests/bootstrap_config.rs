// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{ConfigKey, GetConfig, SqliteConfig, cdc::consume::backlog::FlowBacklog, embedded, value::value::Value};
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
