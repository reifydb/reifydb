// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! All three `BatchSize` variants must drive the operator end-to-end
//! without breaking the materialized-table contract. Operators that batch
//! input rows internally (block-trade, normalized-block) have batch-size
//! sensitivity in production; this suite verifies the harness drives them
//! with valid Changes regardless of batching shape.
//!
//! Each `chaos_test!` expands to N separate `#[test]` cases (`make test-chaos
//! N=`, default 32), one per index; each draws a fresh random seed per run
//! unless `SEED` pins it. A failure reports its seed for replay (`make
//! test-chaos SEED=... FILTER=...`).

use reifydb_sdk::testing::chaos::{ChaosHarness, schema::KeyStrategy, strategy::samplers};
use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario, SupportedOps};
use reifydb_testing_macro::chaos_test;

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn cfg(batch: BatchSize) -> Scenario {
	Scenario::mixed(200)
		.with_ops(SupportedOps::all())
		.with_max_live(50)
		.with_batch(batch)
		.with_duplicate_update_burst(0.0)
		.with_update_as_remove_insert(0.0)
}

chaos_test!(constant_batch_size_one_drives_passthrough, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(BatchSize::Constant(1)))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

chaos_test!(uniform_batch_size_range_drives_passthrough, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(BatchSize::Uniform {
			min: 5,
			max: 20,
		}))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

chaos_test!(geometric_batch_size_drives_passthrough, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(BatchSize::Geometric {
			p: 0.4,
			max: 8,
		}))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});
