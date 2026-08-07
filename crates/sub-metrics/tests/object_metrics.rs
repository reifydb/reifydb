// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The merged `system::metrics::storage::current` and `system::metrics::cdc::current` surfaces, driven end to
//! end: the flush actor scans the metrics KV store, resolves object ids through the catalog, and pushes wide
//! rows to the sampler, which publishes them. A break anywhere in that chain leaves the surface empty.

use std::time::Duration;

use reifydb::{ConfigKey, Value, embedded as db_embedded, testing::db::TestDb};

const TIMEOUT: Duration = Duration::from_secs(10);

fn db_with_fast_metrics() -> TestDb {
	TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build"),
	)
}

#[test]
fn storage_current_reports_a_written_table_under_its_object_kind() {
	// The nine per-object tables merged into one; object_kind is now a column, so a written
	// table must surface as a 'table' row with live bytes, resolved to its catalog id.
	let db = db_with_fast_metrics();
	db.admin("create namespace test");
	db.admin("create table test::t { id: int4 }");
	db.command("insert test::t [{ id: 1 }]");

	let rows = db.await_row_count(
		"from system::metrics::storage::current filter { object_kind == 'table' and live_count > 0 }",
		1,
		TIMEOUT,
	);
	assert!(rows >= 1, "a written table never surfaced in the merged storage table with object_kind 'table'");
}

#[test]
fn cdc_current_reports_retained_cdc_bytes_per_object_kind() {
	// CDC accounting rides the same push chain but has no tier dimension; a written table must
	// surface its retained cdc bytes as a 'table' row.
	let db = db_with_fast_metrics();
	db.admin("create namespace test");
	db.admin("create table test::t { id: int4 }");
	db.command("insert test::t [{ id: 1 }]");

	let rows = db.await_row_count(
		"from system::metrics::cdc::current filter { object_kind == 'table' and total_bytes > 0 }",
		1,
		TIMEOUT,
	);
	assert!(rows >= 1, "a written table never surfaced in the merged cdc table with object_kind 'table'");
}
