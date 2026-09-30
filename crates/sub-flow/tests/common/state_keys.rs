// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{ConfigKey, WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::Value;

pub fn setup_with_metrics() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_flow(|f| f)
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow and metrics"),
	)
}

pub fn keyspace_keys(db: &TestDb, keyspace: &str) -> u64 {
	db.query(&format!("from system::metrics::flow::state::current filter {{ keyspace == '{keyspace}' }}"))
		.iter()
		.flat_map(|frame| column_values(frame, "keys"))
		.map(|value| match value {
			Value::Uint8(keys) => keys,
			other => panic!("the keys measure must be an unsigned count, found {other:?}"),
		})
		.sum()
}
