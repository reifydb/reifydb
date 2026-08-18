// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The `/proc` metrics tree, driven end to end through the wired subsystem.
//!
//! Pins where kernel-sourced readings live: everything read from `/proc` or the cgroup filesystem answers under
//! `system::metrics::proc`, and `system::metrics::runtime` keeps only what the process computes about itself.
//! `::current` stays empty until a sampler tick runs, so positive assertions poll; absence assertions hold regardless
//! of timing.

use std::time::Duration;

use reifydb::{ConfigKey, Value, embedded as db_embedded, testing::db::TestDb};

const TIMEOUT: Duration = Duration::from_secs(5);

fn db_with_fast_sampler() -> TestDb {
	TestDb::from(
		db_embedded::memory()
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(10))
			.build()
			.expect("build"),
	)
}

#[test]
fn every_proc_current_table_is_queryable() {
	// A namespace that bootstraps without its virtual table would fail only when someone queries it in production.
	let db = db_with_fast_sampler();

	for path in [
		"system::metrics::proc::process::io::current",
		"system::metrics::proc::process::memory::current",
		"system::metrics::proc::process::sched::current",
		"system::metrics::proc::cgroup::io::current",
		"system::metrics::proc::cgroup::memory::current",
		"system::metrics::proc::cgroup::cpu::current",
		"system::metrics::proc::cgroup::pressure::current",
	] {
		db.query(&format!("from {path}"));
	}
}

#[cfg(target_os = "linux")]
#[test]
fn process_io_publishes_one_row_carrying_the_write_amplification_inputs() {
	// wchar and write_bytes are the two halves of the amplification ratio, so either one missing makes it
	// uncomputable.
	let db = db_with_fast_sampler();

	assert_eq!(
		db.await_row_count("from system::metrics::proc::process::io::current", 1, TIMEOUT),
		1,
		"process io must publish exactly one row for the process"
	);
	assert_eq!(
		db.row_count("from system::metrics::proc::process::io::current filter { rchar > 0 }"),
		1,
		"a running database has read bytes, so rchar must be populated"
	);
	db.query("from system::metrics::proc::process::io::current map { wchar, write_bytes, cancelled_write_bytes }");
}

#[cfg(target_os = "linux")]
#[test]
fn process_memory_carries_the_resident_set_that_left_the_runtime_domain() {
	// The move is only complete if the reading survives it; a bootstrapped but unfed table is the silent failure.
	let db = db_with_fast_sampler();

	assert_eq!(
		db.await_row_count(
			"from system::metrics::proc::process::memory::current filter { rss_total > 0 }",
			1,
			TIMEOUT
		),
		1,
		"resident set size must be published under proc::process::memory"
	);
}

#[cfg(target_os = "linux")]
#[test]
fn process_sched_reports_the_open_file_count_against_its_limit() {
	// An fd leak is only visible as a fraction of the limit, so the ceiling must travel with the count.
	let db = db_with_fast_sampler();

	assert_eq!(
		db.await_row_count(
			"from system::metrics::proc::process::sched::current filter { open_files > 0 and max_open_files > 0 }",
			1,
			TIMEOUT
		),
		1,
		"open file count and its soft limit must both be published"
	);
}

#[test]
fn no_proc_reading_is_left_behind_in_the_runtime_memory_domain() {
	// runtime holds what the process computes about itself; a kernel-sourced row there means the split leaked.
	let db = db_with_fast_sampler();

	db.await_row_count("from system::metrics::runtime::memory::current", 1, TIMEOUT);

	for metric in ["rss_total_bytes", "rss_anon_bytes", "rss_file_bytes", "pss_bytes", "uss_bytes", "thread_count"]
	{
		assert_eq!(
			db.row_count(&format!(
				"from system::metrics::runtime::memory::current filter {{ metric == \"{metric}\" }}"
			)),
			0,
			"{metric} is read from /proc and must not be published under runtime"
		);
	}
}

#[test]
fn the_allocator_readings_stay_in_the_runtime_memory_domain() {
	// The split moves the kernel's numbers out, not the process's own; losing these would gut the memory domain.
	let db = db_with_fast_sampler();

	assert!(
		db.await_row_count(
			"from system::metrics::runtime::memory::current filter { scope == \"allocator\" }",
			1,
			TIMEOUT
		) > 0,
		"allocator readings are not sourced from /proc and must stay under runtime"
	);
}
