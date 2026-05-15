// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! All three `BatchSizeDist` variants must drive the operator end-to-end
//! without breaking the materialized-table contract. Operators that batch
//! input rows internally (block-trade, normalized-block) have batch-size
//! sensitivity in production; this suite verifies the harness drives them
//! with valid Changes regardless of batching shape.

use reifydb_sdk::testing::chaos::{
	ChaosHarness,
	config::{BatchSizeDist, ChaosConfig, SupportedOps},
	schema::KeyStrategy,
	strategy::samplers,
};

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn cfg(batch_size: BatchSizeDist) -> ChaosConfig {
	ChaosConfig {
		num_ops: 200,
		max_live_rows: 50,
		duplicate_update_burst: 0.0,
		update_as_remove_insert: 0.0,
		batch_size,
		supported_ops: SupportedOps::all(),
	}
}

#[test]
fn constant_batch_size_one_drives_passthrough() {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(BatchSizeDist::Constant(1)))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(42)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}

#[test]
fn uniform_batch_size_range_drives_passthrough() {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(BatchSizeDist::Uniform {
			min: 5,
			max: 20,
		}))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(99)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}

#[test]
fn geometric_batch_size_drives_passthrough() {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(BatchSizeDist::Geometric(0.4)))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(7)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}
