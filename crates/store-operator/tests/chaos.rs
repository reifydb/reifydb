// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "chaos")]

//! Model-based chaos for store-operator: a seeded op sequence runs against a full-state oracle and three store
//! configurations, and every read is differentially checked. The store is single-version, so unlike store-multi
//! there is no watermark below which the oracle stops being trusted; the model is exact truth at every step.
//! `checkpoint_floor` is the one deliberate exception, because the layered tier reports the flushed minimum so
//! retention never reaps a version a restart would send a flow back to.

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

chaos_test!(operator_store_chaos, |seed| {
	// Flushes stay rare, so the resident state keeps real depth over sqlite and reads must merge both layers.
	drive(
		seed,
		Params {
			operators: 3,
			groups: 3,
			keyspaces: 4,
			suffixes: 96,
			flows: 4,
			anchor_rows: 24,
			sides: 2,
			expiry_span: 32,
			min_steps: 120,
			max_steps: 260,
			write_pct: 34,
			anchor_pct: 18,
			checkpoint_pct: 8,
			flush_pct: 10,
			drop_pct: 3,
			max_writes: 6,
			max_batch: 16,
			max_limit: 12,
		},
	);
});

chaos_test!(operator_store_flush_heavy_chaos, |seed| {
	// Frequent flushes over a narrow keyspace drag the same keys across the buffer/sqlite boundary repeatedly.
	drive(
		seed,
		Params {
			operators: 2,
			groups: 2,
			keyspaces: 2,
			suffixes: 24,
			flows: 3,
			anchor_rows: 12,
			sides: 2,
			expiry_span: 8,
			min_steps: 140,
			max_steps: 300,
			write_pct: 30,
			anchor_pct: 20,
			checkpoint_pct: 10,
			flush_pct: 28,
			drop_pct: 2,
			max_writes: 4,
			max_batch: 8,
			max_limit: 8,
		},
	);
});

chaos_test!(operator_store_drop_heavy_chaos, |seed| {
	// A drop is only a buffer marker, so every other operator must stay byte-for-byte intact across one.
	drive(
		seed,
		Params {
			operators: 5,
			groups: 4,
			keyspaces: 4,
			suffixes: 48,
			flows: 6,
			anchor_rows: 16,
			sides: 2,
			expiry_span: 16,
			min_steps: 120,
			max_steps: 240,
			write_pct: 32,
			anchor_pct: 24,
			checkpoint_pct: 6,
			flush_pct: 12,
			drop_pct: 12,
			max_writes: 5,
			max_batch: 12,
			max_limit: 10,
		},
	);
});

chaos_test!(operator_store_snapshot_chaos, |seed| {
	// A drained range must hold its exact snapshot while writes outside it and flushes land between pulls.
	snapshot::drive(
		seed,
		snapshot::Params {
			frozen: 64,
			mutable: 48,
			min_batch: 1,
			max_batch: 9,
			interleave_pct: 80,
			flush_pct: 45,
			max_interleaved: 6,
		},
	);
});

chaos_test!(operator_store_restart_chaos, |seed| {
	// A reopened store must show exactly the model as of the last completed flush, never a buffered write.
	restart::drive(
		seed,
		Params {
			operators: 3,
			groups: 2,
			keyspaces: 3,
			suffixes: 40,
			flows: 4,
			anchor_rows: 12,
			sides: 2,
			expiry_span: 16,
			min_steps: 80,
			max_steps: 180,
			write_pct: 0,
			anchor_pct: 0,
			checkpoint_pct: 0,
			flush_pct: 14,
			drop_pct: 0,
			max_writes: 4,
			max_batch: 10,
			max_limit: 8,
		},
	);
});

chaos_test!(operator_store_random_chaos, |seed| {
	// A failure here must pin the RESOLVED parameters, never the master seed, which stops meaning the same.
	workload::drive_random(seed);
});
