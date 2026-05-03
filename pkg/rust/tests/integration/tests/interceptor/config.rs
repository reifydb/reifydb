// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::{
	ConfigKey,
	value::{Value, duration::Duration},
};

use super::common::{admin, fresh_db};

#[test]
fn set_config_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "call system::config::set('OPERATOR_TTL_SCAN_INTERVAL', duration::seconds(30))");

	let value = db.catalog().cache().get_config(ConfigKey::OperatorTtlScanInterval);
	assert_eq!(value, Value::Duration(Duration::from_seconds(30).unwrap()));
}
