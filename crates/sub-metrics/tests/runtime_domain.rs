// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Runtime metrics domain split, driven end to end through the wired subsystem.
//!
//! Pins the per-domain partitioning on the real path: watermark metrics land under `watermarks`, never `memory`,
//! and an engine with no flow state exposes an empty `operators::current`. `::current` stays empty until a sampler
//! tick runs, so the positive assertions poll; the absence assertions hold regardless of timing.

use std::time::Duration;

use reifydb::testing::db::TestDb;
use reifydb::{ConfigKey, Value, embedded as db_embedded};

const TIMEOUT: Duration = Duration::from_secs(5);

fn db_with_runtime_refresh() -> TestDb {
	TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(10))
			.build()
			.expect("build"),
	)
}

#[test]
fn watermark_metrics_live_in_the_watermarks_domain_not_memory() {
	let db = db_with_runtime_refresh();

	let watermark_lag = db.await_row_count(
		"from system::metrics::runtime::watermarks::current filter { metric == \"watermark_lag\" }",
		1,
		TIMEOUT,
	);
	assert_eq!(watermark_lag, 1, "watermark_lag must appear in the watermarks domain");

	assert_eq!(
		db.row_count(
			"from system::metrics::runtime::watermarks::current filter { metric == \"oracle_window_count\" }"
		),
		1,
		"oracle_window_count must be in the watermarks domain",
	);

	assert_eq!(
		db.row_count("from system::metrics::runtime::memory::current filter { metric == \"watermark_lag\" }"),
		0,
		"watermark_lag must not appear in the memory domain",
	);

	assert_eq!(
		db.row_count("from system::metrics::runtime::operators::current"),
		0,
		"operators::current must be queryable and empty without any flow-operator state",
	);
}
