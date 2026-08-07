// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! `::snapshots` is the series of past `::current` readings, appended by the publisher on the snapshot
//! cadence. Driven end to end because the append path crosses the sampler, the bulk series insert and the
//! bootstrap-declared series schema; a column drift anywhere leaves the series silently unwritten.

use std::time::Duration;

use reifydb::testing::db::TestDb;
use reifydb::{ConfigKey, Value, embedded as db_embedded};

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn snapshots_append_readings_when_the_interval_is_set() {
	let db = TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.with_config(ConfigKey::MetricsSnapshotInterval, Value::duration_milliseconds(40))
			.build()
			.expect("build"),
	);

	let rows = db.await_row_count("from system::metrics::runtime::watermarks::snapshots", 1, TIMEOUT);
	assert!(rows >= 1, "the watermarks ::current reading never reached its ::snapshots series");

	// Lifecycle rows carry none-valued optional columns (binding, freelist), so this pins the
	// append path's none handling against the wide series schema.
	let lifecycle = db.await_row_count("from system::metrics::lifecycle::snapshots", 1, TIMEOUT);
	assert!(lifecycle >= 1, "the lifecycle ::current reading never reached its ::snapshots series");
}

#[test]
fn snapshots_stay_empty_when_the_interval_is_none() {
	// none means no snapshotting at all; the ordering trick makes this deterministic: once
	// ::current has published, the sampler has ticked, so an unwritten series is a decision,
	// not a race.
	let db = TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build"),
	);

	db.await_row_count("from system::metrics::runtime::watermarks::current", 1, TIMEOUT);
	assert_eq!(
		db.row_count("from system::metrics::runtime::watermarks::snapshots"),
		0,
		"with no snapshot interval configured, no series row may ever be written"
	);
}

#[test]
fn a_snapshot_interval_shorter_than_the_sample_interval_fails_at_boot() {
	// Between two rolls the published reading does not change, so a shorter snapshot cadence
	// would append duplicates; failing at boot beats silently writing junk.
	let result = db_embedded::memory()
		.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(100))
		.with_config(ConfigKey::MetricsSnapshotInterval, Value::duration_milliseconds(20))
		.build();
	assert!(result.is_err(), "a snapshot interval shorter than the sample interval must be rejected at boot");
}
