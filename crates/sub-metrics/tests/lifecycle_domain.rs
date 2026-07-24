// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
//! `system::metrics::lifecycle::current` exposes the retention plane's per-class ledger.
//!
//! Until this domain existed the ledger was in-process only: `RetentionPlane::snapshot` was reachable from
//! sub-lifecycle's own tests and nowhere else, so an operator could not tell a class that was keeping up from one that
//! had never run. Both read as "no complaints". Driven end to end because every link is one nobody would notice
//! failing - an unregistered vtable, an unresolved IoC handle, or a refresh actor that never spawns all leave the
//! surface empty, which is indistinguishable from an idle system.

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
	// The defect family this whole plan targets is maintenance that is silently absent. A class that only appears
	// once it has done work is invisible exactly when it matters, because "never registered" and "nothing to do"
	// produce the same empty surface. Every class must have a row from the first refresh, zeros and all.
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
	// The single-optional-Duration contract: none means the domain is never refreshed. This is the half that is
	// easy to get wrong in the other direction - a domain that populates regardless of config would make the
	// setting a lie, and would run a collector nobody asked for on every tick.
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
	// slices is the heartbeat: it increments once per slice regardless of whether the slice found work. A class
	// pinned at zero slices while the process is up has stopped being scheduled, which is the failure the ledger
	// exists to make visible and which work_done alone cannot distinguish from an idle class.
	let db = db_with_refresh();

	let rows = db.await_row_count("from system::metrics::lifecycle::current filter { slices > 0 }", 1, TIMEOUT);

	assert!(
		rows > 0,
		"no retention class ever reported a slice: either the lane is not running or liveness is not reaching the surface"
	);
}

#[test]
fn lifecycle_current_leaves_the_freelist_gauge_none_for_classes_that_never_observe_it() {
	// freelist_pages and page_count are properties of the persistent tier, populated only by the vacuum class. On a
	// memory-only database nothing observes them, and they must read as none rather than zero: zero is a legitimate
	// freelist reading and would claim an observation that never happened.
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
	// The positive control for the test above. Without it, a filter that silently matched nothing - or a gauge that
	// was never recorded at all - would let the none-case assertion pass vacuously. vacuum.rs reads the freelist on
	// every slice, including the common one where the ratio is under threshold and it does no work, so the reading
	// must reach the surface even when work_done stays zero.
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
