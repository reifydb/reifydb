// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Determinism guarantees. The harness's primary contract for failure
//! triage is "give me the seed and I can reproduce". These tests pin
//! that contract:
//!
//!   1. Same seed across runs -> same materialized-table output.
//!   2. Different seeds -> different event sequences (probabilistically).
//!
//! If (1) breaks, the harness has non-determinism somewhere - typically a
//! HashMap iteration order leak. If (2) breaks, the seed isn't actually
//! threading through to the RNG stream.

use reifydb_sdk::testing::chaos::{
	ChaosHarness,
	config::{BatchSizeDist, ChaosConfig, SupportedOps},
	runner::ChaosOutcome,
	schema::KeyStrategy,
	strategy::samplers,
};

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn build_and_run(seed: u64) -> ChaosOutcome {
	ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(ChaosConfig {
			num_ops: 100,
			max_live_rows: 30,
			duplicate_update_burst: 0.3,
			update_as_remove_insert: 0.2,
			batch_size: BatchSizeDist::Geometric(0.4),
			supported_ops: SupportedOps::all(),
		})
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run()
}

#[test]
fn same_seed_produces_identical_materialized_tables() {
	let a = build_and_run(42);
	let b = build_and_run(42);
	a.assert_matches();
	b.assert_matches();
	assert_eq!(a.operator_table, b.operator_table, "same seed must produce identical operator tables");
	assert_eq!(a.oracle_table, b.oracle_table, "same seed must produce identical oracle tables");
	assert_eq!(a.ops_count(), b.ops_count(), "same seed must produce identical event-log lengths");
}

#[test]
fn different_seeds_diverge_in_event_log() {
	let a = build_and_run(42);
	let b = build_and_run(43);
	a.assert_matches();
	b.assert_matches();
	// Event content (RowNumbers + values) should differ for different
	// seeds. We don't compare the full Vec<ChaosEvent> because ChaosEvent
	// doesn't impl PartialEq directly (it carries Row); compare the
	// per-event RowNumber sequence instead, which is cheap and stable.
	let rns_a: Vec<_> = a.events().map(|e| e.row_number()).collect();
	let rns_b: Vec<_> = b.events().map(|e| e.row_number()).collect();
	assert_ne!(rns_a, rns_b, "different seeds must produce different event sequences");
}

#[test]
fn same_seed_produces_identical_operator_history_lengths() {
	// Indirect determinism check: the number of Changes the harness drove
	// through OperatorTestHarness::apply must match across runs.
	let a = build_and_run(7);
	let b = build_and_run(7);
	a.assert_matches();
	b.assert_matches();
	assert_eq!(
		a.operator_table.rows.len(),
		b.operator_table.rows.len(),
		"materialized row count must agree across same-seed runs"
	);
}
