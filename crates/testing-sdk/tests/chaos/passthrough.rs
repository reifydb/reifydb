// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A passthrough operator paired with the identity oracle has to agree on the materialized
//! output table for every valid `Scenario`. A failure here is a harness bug: tighten the
//! harness, do not loosen the test.
//!
//! A failure reports its seed; replay with `make test-chaos SEED=... FILTER=...`.

use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario, SupportedOps};
use reifydb_testing_macro::chaos_test;
use reifydb_testing_sdk::chaos::{ChaosHarness, schema::KeyStrategy, strategy::samplers};

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn baseline_chaos(steps: u32, supported_ops: SupportedOps) -> Scenario {
	Scenario::mixed(steps)
		.with_ops(supported_ops)
		.with_max_live(50)
		.with_batch(BatchSize::Constant(1))
		.with_duplicate_update_burst(0.0)
		.with_update_as_remove_insert(0.0)
}

chaos_test!(passthrough_matches_under_default_config, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

chaos_test!(passthrough_matches_under_insert_only, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(baseline_chaos(100, SupportedOps::insert_only()))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	assert!(outcome.events().all(|e| e.is_insert()), "non-insert under insert_only");
});

chaos_test!(passthrough_matches_under_no_remove, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(baseline_chaos(150, SupportedOps::no_remove()))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	assert!(!outcome.events().any(|e| e.is_remove()), "Remove emitted under no_remove");
});

chaos_test!(passthrough_matches_under_no_update, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(baseline_chaos(150, SupportedOps::no_update()))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	assert!(!outcome.events().any(|e| e.is_update()), "Update emitted under no_update");
});

chaos_test!(passthrough_matches_with_chaos_primitives_at_high_probability, |seed| {
	// Both primitives high enough that most Updates get rewritten or duplicated; both are
	// equivalent at the materialized-table level, so the oracle must not move.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(
			Scenario::mixed(200)
				.with_ops(SupportedOps::all())
				.with_max_live(40)
				.with_batch(BatchSize::Constant(1))
				.with_duplicate_update_burst(0.6)
				.with_update_as_remove_insert(0.4),
		)
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

chaos_test!(passthrough_matches_at_zero_ops, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(baseline_chaos(0, SupportedOps::all()))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	assert_eq!(outcome.ops_count(), 0);
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
});
