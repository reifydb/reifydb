// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! All three `KeyStrategy` variants must drive the operator end-to-end.
//! HashOf with a small key range is particularly load-bearing because
//! collisions trigger the generator's Insert -> Update rewrite path, which
//! is the closest analog to the production OHLCV pattern (re-emission of
//! the same per-slot row as Update).
//!
//! Each `chaos_test!` runs `CHAOS_ITERATIONS` randomized seeds via the shared
//! chaos runner; a failure reports the base seed for replay.

use reifydb_sdk::testing::chaos::{
	ChaosHarness,
	config::{BatchSizeDist, ChaosConfig, SupportedOps},
	schema::KeyStrategy,
	strategy::{RowContent, samplers},
};
use reifydb_testing::chaos_test;
use reifydb_value::value::row_number::RowNumber;

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn cfg(num_ops: usize) -> ChaosConfig {
	ChaosConfig {
		num_ops,
		max_live_rows: 30,
		duplicate_update_burst: 0.0,
		update_as_remove_insert: 0.0,
		batch_size: BatchSizeDist::Constant(1),
		supported_ops: SupportedOps::all(),
	}
}

chaos_test!(sequential_keys_drive_passthrough, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(150))
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
		.with_chaos(cfg(150))
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
		.with_chaos(cfg(100))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});
