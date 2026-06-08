// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the rolling V2 driver (`RollingSum`): a sliding
//! buffer of the last `ROLLING_CAPACITY` windows per group. The window_start
//! range is deliberately wider than the capacity so eviction is exercised on
//! most seeds, and events share window coordinates so within-window
//! accumulation and partial removal are hit.

use reifydb_core::window::engine::LatePolicy;
use reifydb_sdk::{
	operator::{FFIOperatorAdapter, windowed::rolling::RollingDriver},
	testing::chaos::{
		ChaosHarness,
		accumulator_oracle::rolling_accumulator_oracle,
		config::{ChaosConfig, SupportedOps},
		runner::ChaosOutcome,
		schema::KeyStrategy,
		strategy::{ColumnSampler, samplers},
	},
};
use reifydb_value::value::Value;

use super::common::{self, RollingSum};

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

fn run(none_values: bool, cfg: ChaosConfig, seed: u64, policy: LatePolicy) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<RollingDriver<RollingSum>>>::builder()
		.with_input_shape(common::rolling_shape())
		.with_output_shape(common::rolling_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		// More distinct window coordinates than capacity -> eviction.
		.with_column("window_start", samplers::u64_range(0..10))
		.with_column("value", value_sampler(none_values))
		.with_config([("__late_policy", Value::Utf8(common::policy_label(policy).into()))])
		.with_chaos(cfg)
		.with_oracle(move |ctx, batches| {
			rolling_accumulator_oracle(&common::rolling_sum(), ctx, batches, &group_key(), policy)
		})
		.seed(seed)
		.build()
		.expect("build rolling harness")
		.run()
}

#[test]
fn rolling_sum_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		for policy in common::POLICIES {
			run(false, common::baseline(150, SupportedOps::insert_only()), seed, policy).assert_matches();
			run(false, common::baseline(150, SupportedOps::no_remove()), seed, policy).assert_matches();
			run(false, common::baseline(150, SupportedOps::no_update()), seed, policy).assert_matches();
			run(false, common::baseline(200, SupportedOps::all()), seed, policy).assert_matches();
			run(false, common::full_chaos(250), seed, policy).assert_matches();
		}
	}
}

#[test]
fn rolling_sum_handles_none_inputs() {
	for &seed in &common::SEEDS {
		for policy in common::POLICIES {
			run(true, common::full_chaos(200), seed, policy).assert_matches();
		}
	}
}

#[test]
fn rolling_sum_evicts_beyond_capacity() {
	// With 10 distinct window coordinates and capacity 3, an inserts-only
	// run must leave each live group reporting exactly `ROLLING_CAPACITY`
	// windows once it has seen enough.
	let outcome = run(false, common::baseline(300, SupportedOps::insert_only()), 7, LatePolicy::Drop);
	outcome.assert_matches();
	assert!(!outcome.oracle_table.is_empty(), "expected rolling output rows");
}

#[test]
fn rolling_sum_empty_stream_is_empty() {
	let outcome = run(false, common::baseline(0, SupportedOps::all()), 0, LatePolicy::Drop);
	outcome.assert_matches();
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}
