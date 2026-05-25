// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the rolling-incremental driver
//! (`VelocityIncremental`). The driver maintains a `Running` accumulator
//! incrementally as windows are added, updated, and evicted; the oracle
//! rebuilds `Running` from scratch over the whole buffer at each snapshot.
//! Any drift in the incremental maintenance (a missed eviction-remove, a
//! double-count, a stale running sum) shows up as a divergence here.
//!
//! `recent` and `windows` are compared exactly. `baseline` carries an absolute
//! tolerance because the incrementally-maintained running sum and the
//! recomputed sum are different floating-point operation sequences for the
//! same mathematical value; the divergence we care about is structural, not
//! the last ULP.

use reifydb_sdk::{
	operator::{FFIOperatorAdapter, windowed::rolling_incremental::RollingIncrementalDriver},
	testing::chaos::{
		ChaosHarness,
		accumulator_oracle::rolling_incremental_accumulator_oracle,
		config::{ChaosConfig, SupportedOps},
		runner::ChaosOutcome,
		schema::KeyStrategy,
		strategy::{ColumnSampler, samplers},
	},
};

use super::common::{self, VelocityIncremental};

const BASELINE_TOL: f64 = 1e-6;

fn group_key() -> Vec<String> {
	vec!["group".to_string()]
}

fn value_sampler(none_values: bool) -> ColumnSampler {
	if none_values {
		common::maybe_none_f64(-50.0, 50.0)
	} else {
		samplers::f64_range(-50.0..50.0)
	}
}

fn run(none_values: bool, cfg: ChaosConfig, seed: u64) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<RollingIncrementalDriver<VelocityIncremental>>>::builder()
		.with_input_shape(common::rolling_shape())
		.with_output_shape(common::velocity_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("window_start", samplers::u64_range(0..10))
		.with_column("value", value_sampler(none_values))
		.with_tolerance("baseline", BASELINE_TOL)
		.with_chaos(cfg)
		.with_oracle(|ctx, batches| {
			rolling_incremental_accumulator_oracle(
				&common::velocity_incremental(),
				ctx,
				batches,
				&group_key(),
			)
		})
		.seed(seed)
		.build()
		.expect("build incremental harness")
		.run()
}

#[test]
fn velocity_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		run(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
		run(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
		run(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
		run(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
		run(false, common::full_chaos(250), seed).assert_matches();
	}
}

#[test]
fn velocity_handles_none_inputs() {
	for &seed in &common::SEEDS {
		run(true, common::full_chaos(200), seed).assert_matches();
	}
}

#[test]
fn velocity_exercises_eviction_and_running_maintenance() {
	// 10 distinct windows, capacity 3, full churn: the running accumulator
	// is repeatedly added-to, updated, and evicted-from. The oracle's
	// from-scratch recompute must still agree.
	let outcome = run(false, common::full_chaos(400), 12_345);
	outcome.assert_matches();
	assert!(!outcome.oracle_table.is_empty(), "expected velocity output rows");
}

#[test]
fn velocity_empty_stream_is_empty() {
	let outcome = run(false, common::baseline(0, SupportedOps::all()), 0);
	outcome.assert_matches();
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}
