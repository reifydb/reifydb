// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Must-match scenarios. A correct (passthrough) operator paired with the
//! identity oracle has to agree on the materialized output table for every
//! valid `ChaosConfig`. If any of these fail, the harness has a bug -
//! tighten the harness, do not loosen the test.
//!
//! Each `chaos_test!` runs `CHAOS_ITERATIONS` randomized seeds via the shared
//! chaos runner; a failure reports the base seed for replay (`make test-chaos
//! SEED=... N=...`).

use reifydb_sdk::testing::chaos::{
	ChaosHarness,
	config::{BatchSizeDist, ChaosConfig, SupportedOps},
	schema::KeyStrategy,
	strategy::samplers,
};
use reifydb_testing::chaos_test;

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn baseline_chaos(num_ops: usize, supported_ops: SupportedOps) -> ChaosConfig {
	ChaosConfig {
		num_ops,
		max_live_rows: 50,
		duplicate_update_burst: 0.0,
		update_as_remove_insert: 0.0,
		batch_size: BatchSizeDist::Constant(1),
		supported_ops,
	}
}

chaos_test!(passthrough_matches_under_default_config, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(ChaosConfig::default())
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
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(baseline_chaos(100, SupportedOps::insert_only()))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	// Sanity: every event under insert_only must be Insert.
	assert!(outcome.events().all(|e| e.is_insert()), "non-insert under insert_only");
});

chaos_test!(passthrough_matches_under_no_remove, |seed| {
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(baseline_chaos(150, SupportedOps::no_remove()))
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
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(baseline_chaos(150, SupportedOps::no_update()))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	assert!(!outcome.events().any(|e| e.is_update()), "Update emitted under no_update");
});

chaos_test!(passthrough_matches_with_chaos_primitives_at_high_probability, |seed| {
	// duplicate-burst at 0.6 + rewrite at 0.4: most Updates get rewritten or
	// duplicated. Passthrough must still match the identity oracle because
	// both rewrites are equivalent at the materialized-table level.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(ChaosConfig {
			num_ops: 200,
			max_live_rows: 40,
			duplicate_update_burst: 0.6,
			update_as_remove_insert: 0.4,
			batch_size: BatchSizeDist::Constant(1),
			supported_ops: SupportedOps::all(),
		})
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
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(baseline_chaos(0, SupportedOps::all()))
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
