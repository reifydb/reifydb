// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! `duplicate_update_burst` and `update_as_remove_insert` at extreme
//! probabilities. A correct operator (passthrough) must remain consistent
//! with the identity oracle because both rewrites are equivalent at the
//! materialized-table level. If any test in this file fails, the operator
//! is responding to chaos primitives in a way that produces a different
//! visible state - a real bug class the harness exists to expose.

use reifydb_sdk::testing::chaos::{
	ChaosHarness,
	config::{BatchSizeDist, ChaosConfig, SupportedOps},
	schema::KeyStrategy,
	strategy::samplers,
};

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn cfg(duplicate_update_burst: f64, update_as_remove_insert: f64) -> ChaosConfig {
	ChaosConfig {
		num_ops: 200,
		max_live_rows: 40,
		duplicate_update_burst,
		update_as_remove_insert,
		batch_size: BatchSizeDist::Constant(1),
		supported_ops: SupportedOps::all(),
	}
}

#[test]
fn no_chaos_primitives_passthrough_matches() {
	// Baseline: both probabilities at 0.0. If this fails, base passthrough
	// is broken and every other test is meaningless.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(0.0, 0.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(42)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
}

#[test]
fn duplicate_burst_at_one_passthrough_matches() {
	// Every Update spawns one identical no-op Update (pre = post).
	// Passthrough handles correctly because re-applying the same post
	// to the same row is idempotent at the materialized-table level.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(1.0, 0.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(99)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	// Sanity: at p=1.0, many Updates should be duplicate no-ops.
	let updates: usize = outcome.events.iter().filter(|e| e.is_update()).count();
	assert!(updates > 50, "expected duplicate-burst to inflate Update count; got {updates}");
}

#[test]
fn rewrite_at_one_passthrough_matches() {
	// Every Update is rewritten to Remove(pre) + Insert(post). Passthrough
	// must agree with the identity oracle because removing-then-inserting
	// the same key with the new value lands in the same materialized state
	// as one Update.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(0.0, 1.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(7)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	// Sanity: at rewrite p=1.0, no Updates should appear in the output stream.
	let updates: usize = outcome.events.iter().filter(|e| e.is_update()).count();
	assert_eq!(updates, 0, "rewrite at p=1.0 must eliminate all Updates");
}

#[test]
fn both_chaos_primitives_at_one_passthrough_matches() {
	// Rewrite takes precedence over duplicate-burst (per generator's
	// documented rule). With rewrite at 1.0 every Update becomes
	// Remove+Insert; duplicate-burst never fires because there is no
	// surviving Update to duplicate. Passthrough still matches.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_chaos(cfg(1.0, 1.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(11)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	let updates: usize = outcome.events.iter().filter(|e| e.is_update()).count();
	assert_eq!(updates, 0, "rewrite precedence: no Updates should reach the operator");
}
