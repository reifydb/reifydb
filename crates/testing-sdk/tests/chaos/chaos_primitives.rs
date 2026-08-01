// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! `duplicate_update_burst` and `update_as_remove_insert` at extreme probabilities. Both
//! rewrites are equivalent at the materialized-table level, so a passthrough operator must
//! stay consistent with the identity oracle.
//!
//! A failure reports its seed; replay with `make test-chaos SEED=... FILTER=...`.

use reifydb_testing_chaos::operator::{
	event::ChaosEvent,
	scenario::{BatchSize, Scenario, SupportedOps},
};
use reifydb_testing_macro::chaos_test;
use reifydb_testing_sdk::chaos::{ChaosHarness, schema::KeyStrategy, strategy::samplers};

use super::common::{PassthroughOperator, passthrough_oracle, simple_kv_shape};

fn cfg(duplicate_update_burst: f64, update_as_remove_insert: f64) -> Scenario {
	Scenario::mixed(200)
		.with_ops(SupportedOps::all())
		.with_max_live(40)
		.with_batch(BatchSize::Constant(1))
		.with_duplicate_update_burst(duplicate_update_burst)
		.with_update_as_remove_insert(update_as_remove_insert)
}

chaos_test!(no_chaos_primitives_passthrough_matches, |seed| {
	// Baseline with both primitives off; if this fails every other test in the file is
	// meaningless.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(0.0, 0.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
});

chaos_test!(duplicate_burst_at_one_passthrough_matches, |seed| {
	// Re-applying the same post to the same row is idempotent at the materialized-table level,
	// so a no-op duplicate per Update must not move the output.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(1.0, 0.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	// Equality rather than a magnitude threshold: the number of Update decisions in a 200-op
	// stream is seed-dependent and its tail can dip to any value.
	let (noop, real) = outcome.events().fold((0usize, 0usize), |(noop, real), e| match e {
		ChaosEvent::Update {
			pre,
			post,
			..
		} if pre.encoded == post.encoded => (noop + 1, real),
		ChaosEvent::Update {
			..
		} => (noop, real + 1),
		_ => (noop, real),
	});
	assert_eq!(
		noop, real,
		"duplicate-burst at p=1.0 must spawn one no-op duplicate per real Update; got {noop} no-ops vs {real} reals"
	);
});

chaos_test!(rewrite_at_one_passthrough_matches, |seed| {
	// Removing then re-inserting the same key with the new value lands in the same materialized
	// state as one Update, so the rewrite must be invisible to the oracle.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(0.0, 1.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	let updates: usize = outcome.events().filter(|e| e.is_update()).count();
	assert_eq!(updates, 0, "rewrite at p=1.0 must eliminate all Updates");
});

chaos_test!(both_chaos_primitives_at_one_passthrough_matches, |seed| {
	// Rewrite takes precedence, so duplicate-burst never fires: there is no surviving Update
	// left to duplicate.
	let outcome = ChaosHarness::<PassthroughOperator>::builder()
		.with_input_shape(simple_kv_shape())
		.with_output_shape(simple_kv_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["k"])
		.with_column("k", samplers::u64_range(1..1_000_000_000_000))
		.with_column("v", samplers::f64_range(0.0..100.0))
		.with_scenario(cfg(1.0, 1.0))
		.with_oracle(passthrough_oracle(vec!["k".into()]))
		.seed(seed)
		.build()
		.expect("build")
		.run();
	outcome.assert_matches();
	let updates: usize = outcome.events().filter(|e| e.is_update()).count();
	assert_eq!(updates, 0, "rewrite precedence: no Updates should reach the operator");
});
