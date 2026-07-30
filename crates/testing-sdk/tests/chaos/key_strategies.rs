// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! All three `KeyStrategy` variants must drive the operator end-to-end.
//! HashOf with a small key range is particularly load-bearing because
//! collisions trigger the generator's Insert -> Update rewrite path, which
//! is the closest analog to the production OHLCV pattern (re-emission of
//! the same per-slot row as Update).
//!
//! Each `chaos_test!` expands to N separate `#[test]` cases (`make test-chaos
//! N=`, default 32), one per index; each draws a fresh random seed per run
//! unless `SEED` pins it. A failure reports its seed for replay (`make
//! test-chaos SEED=... FILTER=...`).

use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario, SupportedOps};
use reifydb_testing_macro::chaos_test;
use reifydb_testing_sdk::chaos::{
	ChaosHarness,
	schema::KeyStrategy,
	strategy::{RowContent, samplers},
};
use reifydb_value::value::row_number::RowNumber;

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn cfg(steps: u32) -> Scenario {
	Scenario::mixed(steps)
		.with_ops(SupportedOps::all())
		.with_max_live(30)
		.with_batch(BatchSize::Constant(1))
		.with_duplicate_update_burst(0.0)
		.with_update_as_remove_insert(0.0)
}

chaos_test!(sequential_keys_drive_passthrough, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(150))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

chaos_test!(hashof_keys_drive_passthrough_with_collisions, |seed| {
	// k_range is tiny so collisions are frequent. Each collision converts
	// what would have been an Insert into an Update-against-the-existing-
	// live-row inside the generator. Passthrough must still agree with
	// the identity oracle because the events are valid Insert/Update flows.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::hash_of(["k"]))
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..6))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(150))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	// Sanity: with k in [1, 5] and 150 ops, collisions must have happened.
	let updates: usize = outcome.events().filter(|e| e.is_update()).count();
	assert!(updates > 10, "expected many Updates from HashOf collisions; got {updates}");
});

chaos_test!(custom_keys_drive_passthrough, |seed| {
	// Custom RowNumber derivation: use the `k` column directly.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Custom(Box::new(|content: &RowContent| {
			RowNumber(content.u64("k").unwrap_or(0))
		})))
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..30))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(100))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});
