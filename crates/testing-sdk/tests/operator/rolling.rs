// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the rolling driver. The ts range floors to more buckets than the
//! buffer capacity so eviction is reached on most seeds, and many event times land in the same
//! bucket so within-bucket accumulation and partial removal are reached too.

use reifydb_sdk::flow::operator::{
	extern_c::binding::operator::ExternCOperatorAdapter, windowed::rolling::RollingDriver,
};
use reifydb_testing_chaos::operator::scenario::{Scenario, SupportedOps};
use reifydb_testing_sdk::chaos::{
	ChaosHarness,
	accumulator_oracle::rolling_accumulator_oracle,
	runner::ChaosOutcome,
	schema::KeyStrategy,
	strategy::{ColumnSampler, samplers},
};

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

fn run(none_values: bool, scenario: Scenario, seed: u64) -> ChaosOutcome {
	ChaosHarness::<ExternCOperatorAdapter<RollingDriver<RollingSum>>>::builder()
		.with_input_shape(common::rolling_shape())
		.with_output_shape(common::rolling_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group"])
		.with_time_column("ts")
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		// Floors to more distinct buckets than capacity, so eviction is reachable.
		.with_column("ts", samplers::u64_range(0..100))
		.with_column("value", value_sampler(none_values))
		.with_scenario(scenario)
		.with_oracle(move |ctx, batches| {
			rolling_accumulator_oracle(&common::rolling_sum(), ctx, batches, &group_key())
		})
		.seed(seed)
		.build()
		.expect("build rolling harness")
		.run()
}

#[test]
fn rolling_sum_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		{
			run(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
			run(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
			run(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
			run(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
			run(false, common::full_chaos(250), seed).assert_matches();
		}
	}
}

#[test]
fn rolling_sum_handles_none_inputs() {
	for &seed in &common::SEEDS {
		{
			run(true, common::full_chaos(200), seed).assert_matches();
		}
	}
}

#[test]
fn rolling_sum_evicts_beyond_capacity() {
	// Guards against a trivially-matching run: with far more coordinates than capacity, an
	// inserts-only stream must still produce output.
	let outcome = run(false, common::baseline(300, SupportedOps::insert_only()), 7);
	outcome.assert_matches();
	assert!(!outcome.oracle_table.is_empty(), "expected rolling output rows");
}

#[test]
fn rolling_sum_empty_stream_is_empty() {
	let outcome = run(false, common::baseline(0, SupportedOps::all()), 0);
	outcome.assert_matches();
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}
