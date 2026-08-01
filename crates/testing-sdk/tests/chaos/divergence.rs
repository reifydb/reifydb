// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end proof that the harness catches a buggy operator and names the seed in its
//! panic. The `#[should_panic(expected = "...")]` annotations are the assertion: no panic,
//! or a panic without the seed, fails the test.

use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario, SupportedOps};
use reifydb_testing_macro::chaos_test;
use reifydb_testing_sdk::chaos::{ChaosHarness, schema::KeyStrategy, strategy::samplers};

use super::common::{DoubleInsertOperator, SwallowsRemoveOperator, passthrough_oracle, simple_kv_shape};

#[test]
#[should_panic(expected = "seed: 42")]
fn swallows_remove_operator_panics_with_seed() {
	let outcome = ChaosHarness::<SwallowsRemoveOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(
			Scenario::mixed(200)
				.with_ops(SupportedOps::all())
				.with_max_live(30)
				.with_batch(BatchSize::Constant(1))
				.with_duplicate_update_burst(0.0)
				.with_update_as_remove_insert(0.0),
		)
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(42)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}

#[test]
#[should_panic(expected = "chaos divergence")]
fn swallows_remove_operator_panic_message_mentions_divergence() {
	// Authors grep for the literal "chaos divergence" header when triaging, so it has to stay
	// in the panic message.
	let outcome = ChaosHarness::<SwallowsRemoveOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(
			Scenario::mixed(150)
				.with_ops(SupportedOps::all())
				.with_max_live(25)
				.with_batch(BatchSize::Constant(1))
				.with_duplicate_update_burst(0.0)
				.with_update_as_remove_insert(0.0),
		)
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(99)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}

chaos_test!(swallows_remove_operator_does_not_diverge_under_no_remove, |seed| {
	// With the buggy path unreachable the run must stay green, or divergence reporting is
	// firing on runs that never exercised the defect.
	let outcome = ChaosHarness::<SwallowsRemoveOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(
			Scenario::mixed(200)
				.with_ops(SupportedOps::no_remove())
				.with_max_live(100)
				.with_batch(BatchSize::Constant(1))
				.with_duplicate_update_burst(0.0)
				.with_update_as_remove_insert(0.0),
		)
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

#[test]
#[should_panic(expected = "unfoldable diff stream")]
fn a_row_published_twice_is_caught_even_though_every_value_matches() {
	// Two identical inserts under one row number leave the materialized table indistinguishable
	// from the oracle's, so only the coherence fold can see the row inserted over itself.
	let outcome = ChaosHarness::<DoubleInsertOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(
			Scenario::mixed(40)
				.with_ops(SupportedOps::insert_only())
				.with_max_live(20)
				.with_batch(BatchSize::Constant(1))
				.with_duplicate_update_burst(0.0)
				.with_update_as_remove_insert(0.0),
		)
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(7)
		.build()
		.expect("build")
		.run();

	assert!(
		outcome.comparison.is_match(),
		"the values must agree, or this test would be proving the wrong thing: it exists to show the \
		 coherence check catching what the value comparison cannot"
	);
	outcome.assert_matches();
}
