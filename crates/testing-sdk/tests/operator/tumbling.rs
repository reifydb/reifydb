// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the tumbling driver: every randomized stream is replayed through
//! the real operator and through `tumbling_accumulator_oracle`, and the materialized tables
//! must agree. Covers an invertible sum, a removal-safe multiset min, and sealing OHLCV.

use reifydb_sdk::flow::operator::{
	extern_c::binding::operator::ExternCOperatorAdapter, windowed::tumbling::TumblingDriver,
};
use reifydb_testing_chaos::operator::scenario::{Scenario, SupportedOps};
use reifydb_testing_sdk::chaos::{
	ChaosHarness,
	accumulator_oracle::tumbling_accumulator_oracle,
	runner::ChaosOutcome,
	schema::KeyStrategy,
	strategy::{ColumnSampler, samplers},
};

use super::common::{self, MinTumbling, OhlcvSealingTumbling, VolumeTumbling};

fn window_key() -> Vec<String> {
	vec!["group".to_string(), "window_start".to_string()]
}

fn size_sampler(none_values: bool) -> ColumnSampler {
	if none_values {
		common::maybe_none_f64(1.0, 100.0)
	} else {
		samplers::f64_range(1.0..100.0)
	}
}

fn run_volume(none_values: bool, scenario: Scenario, seed: u64) -> ChaosOutcome {
	ChaosHarness::<ExternCOperatorAdapter<TumblingDriver<VolumeTumbling>>>::builder()
		.with_input_shape(common::tumbling_shape())
		.with_output_shape(common::volume_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_time_column("slot")
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("slot", samplers::u64_range(0..300))
		.with_column("size", size_sampler(none_values))
		.with_scenario(scenario)
		.with_oracle(move |ctx, batches| {
			tumbling_accumulator_oracle(&VolumeTumbling, ctx, batches, &window_key())
		})
		.seed(seed)
		.build()
		.expect("build volume harness")
		.run()
}

fn run_min(none_values: bool, scenario: Scenario, seed: u64) -> ChaosOutcome {
	ChaosHarness::<ExternCOperatorAdapter<TumblingDriver<MinTumbling>>>::builder()
		.with_input_shape(common::tumbling_shape())
		.with_output_shape(common::min_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_time_column("slot")
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("slot", samplers::u64_range(0..300))
		// Tight value set so duplicate minima exercise multiset removal.
		.with_column("size", size_sampler(none_values))
		.with_scenario(scenario)
		.with_oracle(move |ctx, batches| {
			tumbling_accumulator_oracle(&MinTumbling, ctx, batches, &window_key())
		})
		.seed(seed)
		.build()
		.expect("build min harness")
		.run()
}

fn run_ohlcv(none_values: bool, scenario: Scenario, seed: u64) -> ChaosOutcome {
	let price = if none_values {
		common::maybe_none_f64(10.0, 500.0)
	} else {
		samplers::f64_range(10.0..500.0)
	};
	ChaosHarness::<ExternCOperatorAdapter<TumblingDriver<OhlcvSealingTumbling>>>::builder()
		.with_input_shape(common::ohlcv_shape())
		.with_output_shape(common::ohlcv_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_time_column("slot")
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH"]))
		// Slots span more than WINDOW so some events age past OHLCV_GRACE and reach the
		// sealing path.
		.with_column("slot", samplers::u64_range(0..180))
		.with_column("price", price)
		.with_scenario(scenario)
		.with_oracle(move |ctx, batches| {
			tumbling_accumulator_oracle(&OhlcvSealingTumbling, ctx, batches, &window_key())
		})
		.seed(seed)
		.build()
		.expect("build ohlcv harness")
		.run()
}

#[test]
fn volume_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		{
			run_volume(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
			run_volume(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
			run_volume(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
			run_volume(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
			run_volume(false, common::full_chaos(200), seed).assert_matches();
		}
	}
}

#[test]
fn volume_handles_none_inputs() {
	for &seed in &common::SEEDS {
		{
			let outcome = run_volume(true, common::full_chaos(200), seed);
			outcome.assert_matches();
		}
	}
}

#[test]
fn volume_empty_stream_is_empty() {
	let outcome = run_volume(false, common::baseline(0, SupportedOps::all()), 0);
	outcome.assert_matches();
	assert_eq!(outcome.ops_count(), 0);
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}

#[test]
fn min_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		{
			run_min(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
			run_min(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
			run_min(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
			run_min(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
			run_min(false, common::full_chaos(200), seed).assert_matches();
		}
	}
}

#[test]
fn min_handles_none_inputs() {
	for &seed in &common::SEEDS {
		{
			run_min(true, common::full_chaos(200), seed).assert_matches();
		}
	}
}

#[test]
fn ohlcv_sealing_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		{
			run_ohlcv(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
			run_ohlcv(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
			run_ohlcv(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
			run_ohlcv(false, common::full_chaos(250), seed).assert_matches();
		}
	}
}

#[test]
fn ohlcv_sealing_produces_nonempty_output() {
	// A fixture that emits nothing matches the oracle trivially, so emptiness has to fail.
	let outcome = run_ohlcv(false, common::baseline(200, SupportedOps::insert_only()), 42);
	outcome.assert_matches();
	assert!(
		!outcome.oracle_table.is_empty(),
		"sealing OHLCV produced no rows; fixture is not exercising the driver"
	);
}
