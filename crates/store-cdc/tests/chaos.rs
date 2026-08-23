// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

//! Every configuration must answer identically except across a retention cutoff, which the block cut size alone
//! decides.

#[path = "chaos/fixtures.rs"]
mod fixtures;
#[path = "chaos/oracle.rs"]
mod oracle;
#[path = "chaos/restart.rs"]
mod restart;
#[path = "chaos/snapshot.rs"]
mod snapshot;
#[path = "chaos/workload.rs"]
mod workload;

use reifydb_testing_macro::chaos_test;

use crate::workload::{Params, drive};

chaos_test!(cdc_store_chaos, |seed| {
	// Flushes stay rare, so a read must merge a sealed prefix with a buffered tail.
	drive(
		seed,
		Params {
			min_steps: 140,
			max_steps: 280,
			write_pct: 46,
			flush_pct: 10,
			drop_pct: 8,
			unbounded_drop_pct: 12,
			reopen_pct: 3,
			duplicate_pct: 18,
			max_changes: 3,
			max_value_bytes: 16,
			max_gap: 3,
			tables: 3,
			rows: 12,
			max_batch: 6,
			max_limit: 4,
			timestamp_span: 1_000_000,
		},
	);
});

chaos_test!(cdc_store_flush_heavy_chaos, |seed| {
	// A flush after almost every write leaves single-record blocks, so a retention cutoff can land between blocks
	// instead of inside one.
	drive(
		seed,
		Params {
			min_steps: 140,
			max_steps: 280,
			write_pct: 40,
			flush_pct: 34,
			drop_pct: 8,
			unbounded_drop_pct: 12,
			reopen_pct: 2,
			duplicate_pct: 12,
			max_changes: 2,
			max_value_bytes: 8,
			max_gap: 2,
			tables: 2,
			rows: 8,
			max_batch: 4,
			max_limit: 3,
			timestamp_span: 1_000_000,
		},
	);
});

chaos_test!(cdc_store_drop_heavy_chaos, |seed| {
	// Retention runs constantly under a tight limit, so a read must step over the hole instead of stalling on it.
	drive(
		seed,
		Params {
			min_steps: 160,
			max_steps: 320,
			write_pct: 40,
			flush_pct: 16,
			drop_pct: 22,
			unbounded_drop_pct: 20,
			reopen_pct: 2,
			duplicate_pct: 10,
			max_changes: 4,
			max_value_bytes: 24,
			max_gap: 4,
			tables: 4,
			rows: 16,
			max_batch: 8,
			max_limit: 2,
			timestamp_span: 1_000_000,
		},
	);
});

chaos_test!(cdc_store_reopen_chaos, |seed| {
	// A boot discards the commit tier, so a version lost with it is writable again but one a block carries must
	// still be refused.
	drive(
		seed,
		Params {
			min_steps: 140,
			max_steps: 280,
			write_pct: 44,
			flush_pct: 12,
			drop_pct: 8,
			unbounded_drop_pct: 12,
			reopen_pct: 14,
			duplicate_pct: 16,
			max_changes: 2,
			max_value_bytes: 12,
			max_gap: 2,
			tables: 3,
			rows: 8,
			max_batch: 5,
			max_limit: 4,
			timestamp_span: 1_000_000,
		},
	);
});

chaos_test!(cdc_store_snapshot_chaos, |seed| {
	// A drained range must hold its exact snapshot while writes and flushes land between pulls.
	snapshot::drive(
		seed,
		snapshot::Params {
			frozen: 64,
			mutable: 96,
			min_batch: 1,
			max_batch: 7,
			interleave_pct: 80,
			flush_pct: 45,
			max_interleaved: 5,
		},
	);
});

chaos_test!(cdc_store_restart_chaos, |seed| {
	// A reopened store must show exactly the blocks the last flush wrote, and a persisted truncation floor must
	// survive the boot.
	restart::drive(
		seed,
		Params {
			min_steps: 100,
			max_steps: 200,
			write_pct: 0,
			flush_pct: 18,
			drop_pct: 10,
			unbounded_drop_pct: 15,
			reopen_pct: 0,
			duplicate_pct: 0,
			max_changes: 3,
			max_value_bytes: 16,
			max_gap: 3,
			tables: 3,
			rows: 12,
			max_batch: 6,
			max_limit: 3,
			timestamp_span: 1_000_000,
		},
	);
});

chaos_test!(cdc_store_random_chaos, |seed| {
	// A failure here must pin the RESOLVED parameters, never the master seed, which stops meaning the same.
	workload::drive_random(seed);
});
