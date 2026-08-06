// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the rolling-incremental driver. The driver keeps running moments and
//! never rebuilds the buffer; the oracle recomputes from the whole buffer through the naive
//! `combine`. Any drift between the incrementally maintained running state and a from-scratch
//! recomputation is a mismatch here, which is the bug class the incremental path exists to risk.

use reifydb_sdk::operator::{FFIOperatorAdapter, windowed::rolling_incremental::RollingIncrementalDriver};
use reifydb_testing_chaos::operator::scenario::{Scenario, SupportedOps};
use reifydb_testing_sdk::chaos::{
	ChaosHarness,
	accumulator_oracle::rolling_accumulator_oracle,
	runner::ChaosOutcome,
	schema::KeyStrategy,
	strategy::{ColumnSampler, samplers},
};

use reifydb_value::value::{Value, value_type::ValueType};

use super::common::{self, VelocityIncremental};

fn group_key() -> Vec<String> {
	vec!["group".to_string()]
}

fn value_sampler(none_values: bool) -> ColumnSampler {
	// Integer-valued on purpose. The driver maintains its sum incrementally (adding on arrival,
	// subtracting on eviction) while the oracle folds the buffer from scratch, so with arbitrary
	// reals the two rounding paths land 1-2 ulp apart and the comparison would need a tolerance.
	// f64 addition of integers below 2^53 is exact in any order, so both sides reach the same
	// sum bit-for-bit and this differential stays strict. A real logic error moves the mean by
	// O(1) and is still caught.
	let mut choices: Vec<Value> = (-10..=10).map(|n| Value::float8((n * 5) as f64)).collect();
	if none_values {
		choices.push(Value::none_of(ValueType::Float8));
	}
	samplers::select(&choices)
}

fn run(none_values: bool, scenario: Scenario, seed: u64) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<RollingIncrementalDriver<VelocityIncremental>>>::builder()
		.with_input_shape(common::rolling_shape())
		.with_output_shape(common::velocity_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group"])
		.with_time_column("ts")
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		// Floors to more distinct buckets than capacity, so eviction is reachable.
		.with_column("ts", samplers::u64_range(0..100))
		.with_column("value", value_sampler(none_values))
		.with_scenario(scenario)
		.with_oracle(move |ctx, batches| {
			rolling_accumulator_oracle(&common::velocity_incremental(), ctx, batches, &group_key())
		})
		.seed(seed)
		.build()
		.expect("build rolling incremental harness")
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
fn velocity_evicts_beyond_capacity() {
	// Guards against a trivially-matching run: with far more buckets than capacity, an
	// inserts-only stream must still produce output, so the running state has survived
	// eviction rather than the whole comparison being empty-vs-empty.
	let outcome = run(false, common::baseline(300, SupportedOps::insert_only()), 7);
	outcome.assert_matches();
	assert!(!outcome.oracle_table.is_empty(), "expected rolling incremental output rows");
}
