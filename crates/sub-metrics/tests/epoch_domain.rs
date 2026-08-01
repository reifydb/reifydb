// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! `system::metrics::epoch::current` must report the durable log's own size, which no metrics code can read: the
//! keyspace belongs to the lifecycle subsystem, so the figure crosses on the event bus. Driven end to end because a
//! missing emit, an unregistered listener and an unwired gauge all leave the column reading zero.

use std::time::Duration;

use reifydb::{ConfigKey, Value, embedded as db_embedded};
use reifydb_test_harness::db::TestDb;

const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn epoch_current_reports_the_durable_sample_count_over_the_event_bus() {
	let db = TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::EpochBucketInterval, Value::duration_seconds(1))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build"),
	);

	db.admin("create namespace test");
	db.admin("create table test::t { id: int4 }");
	db.command("insert test::t [{ id: 1 }]");

	let rows =
		db.await_row_count("from system::metrics::epoch::current filter { durable_samples > 0 }", 1, TIMEOUT);

	assert_eq!(
		rows, 1,
		"durable_samples never became non-zero: the epoch log persisted a sample but the count did not reach \
		 the metrics domain"
	);
}

#[test]
fn epoch_current_reports_coverage_the_epoch_can_actually_resolve() {
	// guaranteed_coverage is how far back the map promises to resolve. It is read live from the epoch rather
	// than arriving over the bus, so it needs its own assertion.
	let db = TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build"),
	);

	let rows = db.await_row_count("from system::metrics::epoch::current", 1, TIMEOUT);
	assert_eq!(rows, 1, "the epoch domain must serve exactly one row");

	let frames = db.query("from system::metrics::epoch::current");
	let frame = frames.first().expect("one frame");
	let guaranteed = frame.columns.iter().find(|c| c.name == "guaranteed_coverage").expect("column");

	assert_ne!(
		guaranteed.data.as_string(0),
		"PT0S",
		"guaranteed coverage must be a real span, or every declared ttl reads as unenforceable"
	);
}
