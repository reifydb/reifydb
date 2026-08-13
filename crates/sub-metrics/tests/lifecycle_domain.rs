// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
//! `system::metrics::lifecycle::current` exposes the retention plane's per-class ledger.
//!
//! Without it the ledger is in-process only, so an operator cannot tell a class that is keeping up from one that
//! has never run - both read as "no complaints". Driven end to end because an unregistered vtable, an unresolved
//! IoC handle and a sampler that never spawns all leave the surface empty.

use std::time::Duration;

use reifydb::{ConfigKey, Value, embedded as db_embedded, testing::db::TestDb};
use reifydb_core::lifecycle::class::RetentionClass;

const TIMEOUT: Duration = Duration::from_secs(10);

fn db_with_refresh() -> TestDb {
	TestDb::from(
		db_embedded::memory()
			// The default cadence is 10s. A short one keeps the assertions well inside TIMEOUT.
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build"),
	)
}

#[test]
fn lifecycle_current_reports_every_retention_class_before_any_of_them_has_run() {
	// A class that only appears once it has done work is invisible exactly when it matters, because "never
	// registered" and "nothing to do" produce the same empty surface.
	let db = db_with_refresh();
	let want = RetentionClass::all().len();

	let rows = db.await_row_count("from system::metrics::lifecycle::current", want, TIMEOUT);

	assert_eq!(
		rows, want,
		"the domain must serve one row per retention class ({want}) from the first refresh; a class missing here \
		 is a class an operator cannot tell is dead"
	);
}

#[test]
fn lifecycle_current_names_each_class_so_a_row_can_be_attributed() {
	// Row count alone would pass if every row carried the same class name. The class column is the join key an
	// operator filters on, so each row must name a distinct, real class.
	let db = db_with_refresh();
	let want = RetentionClass::all().len();
	db.await_row_count("from system::metrics::lifecycle::current", want, TIMEOUT);

	for class in RetentionClass::all() {
		let rql = format!("from system::metrics::lifecycle::current filter {{ class == '{}' }}", class.name());
		assert_eq!(db.row_count(&rql), 1, "class {} must be addressable by name in the surface", class.name());
	}
}

#[test]
fn lifecycle_current_populates_without_per_domain_opt_in() {
	// Sampling is always on: the old none-is-off contract let a domain go silently unsampled,
	// which is the failure the sampler redesign removed. Only the cadence is configured here,
	// to keep the poll inside TIMEOUT; there is no per-domain switch left to forget.
	let db = TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build"),
	);

	let want = RetentionClass::all().len();
	let rows = db.await_row_count("from system::metrics::lifecycle::current", want, TIMEOUT);
	assert_eq!(rows, want, "the sampler must publish the lifecycle ledger without any per-domain opt-in");
}
