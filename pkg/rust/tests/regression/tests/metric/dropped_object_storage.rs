// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{ConfigKey, Value, embedded, testing::db::TestDb};

const TIMEOUT: Duration = Duration::from_secs(15);

const TABLES: &str = "from system::metrics::storage::current filter { object_kind == 'table' }";

fn setup() -> TestDb {
	// The sampler defaults to a 10s cadence, which would turn every wait below into a timeout.
	TestDb::from(
		embedded::memory()
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with a fast metrics cadence"),
	)
}

#[test]
fn a_dropped_table_stops_reporting_storage() {
	// A dropped table must never report live bytes: its census row is evicted and never re-emitted.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::gone { id: int4, v: int4 }");
	db.command(r#"INSERT app::gone [{ id: 1, v: 1 }, { id: 2, v: 2 }]"#);
	db.await_row_count(TABLES, 1, TIMEOUT);

	db.admin("DROP TABLE app::gone");
	db.admin("CREATE TABLE app::stays { id: int4, v: int4 }");
	db.command(r#"INSERT app::stays [{ id: 1, v: 1 }]"#);

	let settled = db.await_exact_row_count(TABLES, 1, TIMEOUT);
	assert_eq!(settled, 1, "only the surviving table may report storage; surface now: {:?}", db.query(TABLES));
}
