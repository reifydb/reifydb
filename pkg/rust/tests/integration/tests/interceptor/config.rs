// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{ConfigKey, value::value::Value};
use reifydb_test_harness::db::TestDb;

#[test]
fn set_config_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("call system::config::set('OPERATOR_TTL_SCAN_INTERVAL', duration::seconds(30))");

	let value = db.catalog().cache().get_config(ConfigKey::OperatorTtlScanInterval);
	assert_eq!(value, Value::duration_seconds(30));
}
