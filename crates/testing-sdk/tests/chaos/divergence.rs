// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end demonstration that the harness catches a buggy operator and
//! reports the seed in the panic message. `SwallowsRemoveOperator` drops
//! every Remove diff, which under chaos with `SupportedOps::all()` produces
//! a deterministic divergence: rows the oracle removed remain in the
//! operator's materialized table.
//!
//! The `#[should_panic(expected = "...")]` annotation does the assertion -
//! if the harness fails to panic at all, the test fails. If it panics
//! without the seed in the message, the test fails. That is the contract
//! the chaindex chaos tests will rely on when they reproduce the OHLCV bug.

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
	// This must panic - the operator drops Removes, the oracle does not.
	outcome.assert_matches();
}

#[test]
#[should_panic(expected = "chaos divergence")]
fn swallows_remove_operator_panic_message_mentions_divergence() {
	// Same scenario, different seed, different assertion: confirm the
	// panic message also contains the literal "chaos divergence" header.
	// Authors will grep for this when triaging.
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
	// Sanity: under SupportedOps::no_remove(), no Remove ops are generated,
	// so the operator's bug is unreachable. assert_matches must succeed for
	// every seed. This guards against false positives - the divergence
	// reporting must only fire when actual Removes happen.
	let outcome = ChaosHarness::<SwallowsRemoveOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
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
	// The defect class the value comparison structurally cannot see. DoubleInsertOperator emits each
	// Insert twice under the same row number; because both copies are identical and the table keys on
	// output columns, the materialized comparison agrees with the identity oracle exactly. A consumer
	// folding the real diff stream would see a row inserted over itself.
	// Mutation: drop the View fold from RunnableChaos::run and this stops panicking.
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
