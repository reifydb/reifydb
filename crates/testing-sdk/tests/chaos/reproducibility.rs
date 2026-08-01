// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The harness's triage contract is "give me the seed and I can reproduce". A same-seed
//! mismatch means non-determinism somewhere, typically a HashMap iteration-order leak; a
//! different-seed match means the seed is not reaching the RNG stream.

use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario, SupportedOps};
use reifydb_testing_sdk::chaos::{ChaosHarness, runner::ChaosOutcome, schema::KeyStrategy, strategy::samplers};

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn build_and_run(seed: u64) -> ChaosOutcome {
	ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(
			Scenario::mixed(100)
				.with_ops(SupportedOps::all())
				.with_max_live(30)
				.with_batch(BatchSize::Geometric {
					p: 0.4,
					max: 8,
				})
				.with_duplicate_update_burst(0.3)
				.with_update_as_remove_insert(0.2),
		)
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
	// ChaosEvent has no PartialEq, so the RowNumber sequence stands in for the event log.
	let rns_a: Vec<_> = a.events().map(|e| e.row_number()).collect();
	let rns_b: Vec<_> = b.events().map(|e| e.row_number()).collect();
	assert_ne!(rns_a, rns_b, "different seeds must produce different event sequences");
}

#[test]
fn same_seed_produces_identical_operator_history_lengths() {
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

#[test]
fn the_corpus_fingerprint_identifies_the_sequence_a_seed_produced() {
	// A pinned seed only means something relative to the generator that consumes it: widen a
	// sampler and the pin silently points at a different sequence while staying green. The
	// fingerprint only catches that if it is stable per seed and distinct across seeds.
	let first = build_and_run(1234);
	let again = build_and_run(1234);
	let other = build_and_run(5678);

	assert_eq!(
		first.fingerprint(),
		again.fingerprint(),
		"the same seed must fingerprint identically, or a pin could never be recorded"
	);
	assert_ne!(
		first.fingerprint(),
		other.fingerprint(),
		"different seeds must fingerprint differently, or the fingerprint records nothing"
	);
	assert!(first.corpus.steps() > 0, "a fingerprint over an empty sequence would pin nothing");

	first.assert_pinned(again.fingerprint());

	let stale = std::panic::catch_unwind(|| build_and_run(1234).assert_pinned(other.fingerprint()));
	assert!(stale.is_err(), "a fingerprint that no longer matches its pin must fail loudly");
}
