// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Every registered reporter must be readable with RQL through `system::metrics::instruments::current` in the
//! uniform ts/scope/metric/value/unit shape, histograms flattened to six scalar rows. Driven end to end: the
//! profiler subsystem registers the ProfilerInstruments into the shared registry, and with the instruments refresh
//! interval configured the metrics subsystem's RefreshActor reads them into the cache-backed `::current`, so a query
//! sees every instrument the registry holds.

use std::time::Duration;

use reifydb::{ConfigKey, Value, WithSubsystem, embedded as db_embedded};
use reifydb_profiler::category::ALL_CATEGORIES;
use reifydb_test_harness::db::TestDb;

const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn instruments_current_serves_every_registered_reporter() {
	let db = TestDb::from(
		db_embedded::memory()
			.with_profiler(|c| c)
			.with_config(ConfigKey::MetricsInstrumentsRefreshInterval, Value::duration_milliseconds(10))
			.build()
			.expect("build"),
	);

	let expected = ALL_CATEGORIES.len() * 6 + 3;
	let all = db.await_row_count("from system::metrics::instruments::current", expected, TIMEOUT);
	assert_eq!(all, expected, "every instrument must appear; a missing one silently vanishes from the surface");

	assert_eq!(
		db.row_count(
			"from system::metrics::instruments::current filter { scope == \"profiler.query.duration_us\" }"
		),
		6,
		"one histogram must serve exactly count/sum/p50/p95/p99/max rows",
	);
}
