// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Runtime metrics domain split, driven end to end through the wired subsystem.
//!
//! Pins the per-domain partitioning on the real path: mvcc metrics land under `mvcc`, never `memory`,
//! and an engine with no flow state exposes an empty `operators::current`. `::current` stays empty until a sampler
//! tick runs, so the positive assertions poll; the absence assertions hold regardless of timing.

use reifydb::{ConfigKey, Value, embedded as db_embedded, testing::db::TestDb};
use reifydb_value::value::duration::Duration;

const TIMEOUT: Duration = Duration::from_seconds_const(5);

fn db_with_runtime_refresh() -> TestDb {
	TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(10))
			.build()
			.expect("build"),
	)
}

#[test]
fn mvcc_metrics_live_in_the_mvcc_domain_not_memory() {
	let db = db_with_runtime_refresh();

	let query_done_until_lag = db.await_row_count(
		"from system::metric::runtime::mvcc::current filter { metric == \"query_done_until_lag\" }",
		1,
		TIMEOUT,
	);
	assert_eq!(query_done_until_lag, 1, "query_done_until_lag must appear in the mvcc domain");

	assert_eq!(
		db.row_count(
			"from system::metric::runtime::mvcc::current filter { metric == \"oracle_window_count\" }"
		),
		1,
		"oracle_window_count must be in the mvcc domain",
	);

	assert_eq!(
		db.row_count(
			"from system::metric::runtime::memory::current filter { metric == \"query_done_until_lag\" }"
		),
		0,
		"query_done_until_lag must not appear in the memory domain",
	);

	assert_eq!(
		db.row_count("from system::metric::runtime::operator::current"),
		0,
		"operators::current must be queryable and empty without any flow-operator state",
	);
}
