// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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

use reifydb_sdk::testing::chaos::{
	ChaosHarness,
	config::{BatchSizeDist, ChaosConfig, SupportedOps},
	schema::KeyStrategy,
	strategy::samplers,
};

use super::common::{SwallowsRemoveOperator, passthrough_oracle, simple_kv_shape};

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
		.with_chaos(ChaosConfig {
			num_ops: 200,
			max_live_rows: 30,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: SupportedOps::all(),
		})
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
		.with_chaos(ChaosConfig {
			num_ops: 150,
			max_live_rows: 25,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: SupportedOps::all(),
		})
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(99)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}

#[test]
fn swallows_remove_operator_does_not_diverge_under_no_remove() {
	// Sanity: under SupportedOps::no_remove(), no Remove ops are generated,
	// so the operator's bug is unreachable. assert_matches must succeed.
	// This guards against false positives - the divergence reporting must
	// only fire when actual Removes happen.
	let outcome = ChaosHarness::<SwallowsRemoveOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(ChaosConfig {
			num_ops: 200,
			max_live_rows: 100,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: SupportedOps::no_remove(),
		})
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(42)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}
