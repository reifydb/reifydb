// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{ConfigKey, GetConfig, SqliteConfig, embedded, value::value::Value};
use reifydb_testing::tempdir::temp_dir;

#[test]
fn bootstrap_config_overrides_apply_on_first_boot() {
	temp_dir(|path| {
		let db = embedded::sqlite(SqliteConfig::new(path.join("db")))
			.with_config(ConfigKey::MultiReadBufferPages, Value::Uint8(512))
			.build()
			.expect("fresh sqlite database must build");

		assert_eq!(db.engine().catalog().get_config(ConfigKey::MultiReadBufferPages), Value::Uint8(512));

		Ok(())
	})
	.expect("temp dir lifecycle must succeed");
}
