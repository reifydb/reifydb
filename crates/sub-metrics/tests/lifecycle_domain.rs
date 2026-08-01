// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
//! `system::metrics::lifecycle::current` exposes the retention plane's per-class ledger.
//!
//! Without it the ledger is in-process only, so an operator cannot tell a class that is keeping up from one that
//! has never run - both read as "no complaints". Driven end to end because an unregistered vtable, an unresolved
//! IoC handle and a refresh actor that never spawns all leave the surface empty.

use std::time::Duration;

use reifydb::{ConfigKey, SqliteConfig, Value, embedded as db_embedded};
use reifydb_core::lifecycle::class::RetentionClass;
use reifydb_test_harness::db::TestDb;
use reifydb_testing::tempdir::temp_dir;

const TIMEOUT: Duration = Duration::from_secs(10);

fn db_with_refresh() -> TestDb {
	TestDb::from(
		db_embedded::memory()
			// The default is none (domain off). A short cadence keeps the assertions well inside TIMEOUT.
			.with_config(ConfigKey::MetricsLifecycleRefreshInterval, Value::duration_milliseconds(20))
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
fn lifecycle_current_stays_empty_when_the_refresh_interval_is_none() {
	// A none interval means the domain is never refreshed; populating regardless of config would make the setting
	// a lie and run a collector nobody asked for on every tick.
	let db = TestDb::from(db_embedded::memory().build().expect("build"));

	assert_eq!(
		db.row_count("from system::metrics::lifecycle::current"),
		0,
		"with the refresh interval unset the vtable must exist but stay empty; a populated row means the \
		 none-is-off contract is not honoured"
	);
}

#[test]
fn lifecycle_current_reports_liveness_so_a_lane_that_stopped_ticking_is_visible() {
	// slices is the heartbeat, incrementing once per slice whether or not it found work; a class pinned at zero
	// while the process is up has stopped being scheduled, which work_done alone cannot distinguish from idle.
	let db = db_with_refresh();

	let rows = db.await_row_count("from system::metrics::lifecycle::current filter { slices > 0 }", 1, TIMEOUT);

	assert!(
		rows > 0,
		"no retention class ever reported a slice: either the lane is not running or liveness is not reaching the surface"
	);
}

#[test]
fn lifecycle_current_leaves_the_freelist_gauge_none_for_classes_that_never_observe_it() {
	// freelist_pages and page_count belong to the persistent tier, so on a memory-only database they must read as
	// none: zero is a legitimate freelist reading and would claim an observation that never happened.
	let db = db_with_refresh();
	let want = RetentionClass::all().len();
	db.await_row_count("from system::metrics::lifecycle::current", want, TIMEOUT);

	assert_eq!(
		db.row_count("from system::metrics::lifecycle::current filter { is::some(freelist_pages) }"),
		0,
		"a memory-only database has no persistent tier to measure, so no row may report a freelist reading"
	);
}

#[test]
fn lifecycle_current_reports_the_freelist_gauge_once_the_vacuum_class_has_measured_it() {
	// The positive control: without it a filter matching nothing, or a gauge never recorded, would let the
	// none-case assertion above pass vacuously. The freelist is read on every slice, including the ones that
	// find no work, so the reading must surface even while work_done stays zero.
	temp_dir(|dir| {
		let db = TestDb::from(
			db_embedded::sqlite(SqliteConfig::new(dir.join("metrics.db")))
				.with_config(
					ConfigKey::MetricsLifecycleRefreshInterval,
					Value::duration_milliseconds(20),
				)
				.with_config(ConfigKey::VacuumInterval, Value::duration_milliseconds(20))
				.build()
				.expect("build"),
		);

		let rows = db.await_row_count(
			"from system::metrics::lifecycle::current filter { class == 'vacuum-budget' and is::some(page_count) }",
			1,
			TIMEOUT,
		);

		assert_eq!(
			rows, 1,
			"the vacuum class measured the persistent freelist but the reading never reached the surface; the \
			 none-case assertion above would then pass for the wrong reason"
		);
		Ok(())
	})
	.expect("temp dir");
}
