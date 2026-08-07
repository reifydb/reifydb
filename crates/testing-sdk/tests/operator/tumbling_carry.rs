// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the tumbling carry-forward driver. Output depends on the prior
//! window's carried close, so the carry must rotate exactly once per boundary crossing and
//! survive Updates and Removes inside the current window.

use reifydb_sdk::operator::{FFIOperatorAdapter, windowed::tumbling_carry::TumblingCarryDriver};
use reifydb_testing_chaos::operator::scenario::{Scenario, SupportedOps};
use reifydb_testing_sdk::chaos::{
	ChaosHarness,
	accumulator_oracle::tumbling_carry_accumulator_oracle,
	runner::ChaosOutcome,
	schema::KeyStrategy,
	strategy::{ColumnSampler, samplers},
};
use reifydb_value::{factory::time::millis, value::Value};

use super::common::{self, TwapCarry};

fn window_key() -> Vec<String> {
	vec!["group".to_string(), "window_start".to_string()]
}

fn price_sampler(none_values: bool) -> ColumnSampler {
	if none_values {
		common::maybe_none_f64(10.0, 500.0)
	} else {
		samplers::f64_range(10.0..500.0)
	}
}

fn run(none_values: bool, scenario: Scenario, seed: u64, retention: Option<u64>) -> ChaosOutcome {
	let mut config: Vec<(&str, Value)> = vec![];
	if let Some(l) = retention {
		config.push(("__retention", Value::Uint8(l)));
	}
	ChaosHarness::<FFIOperatorAdapter<TumblingCarryDriver<TwapCarry>>>::builder()
		.with_input_shape(common::carry_shape())
		.with_output_shape(common::carry_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_time_column("ts")
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("ts", samplers::u64_range(0..300))
		.with_column("price", price_sampler(none_values))
		.with_config(config)
		.with_scenario(scenario)
		.with_oracle(move |ctx, batches| {
			tumbling_carry_accumulator_oracle(
				&common::twap_carry(retention),
				ctx,
				batches,
				&window_key(),
				retention.map(millis),
			)
		})
		.seed(seed)
		.build()
		.expect("build carry harness")
		.run()
}

#[test]
fn carry_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		{
			for retention in [None, Some(90)] {
				run(false, common::baseline(150, SupportedOps::insert_only()), seed, retention)
					.assert_matches();
				run(false, common::baseline(150, SupportedOps::no_remove()), seed, retention)
					.assert_matches();
				run(false, common::baseline(150, SupportedOps::no_update()), seed, retention)
					.assert_matches();
				run(false, common::baseline(200, SupportedOps::all()), seed, retention)
					.assert_matches();
				run(false, common::full_chaos(250), seed, retention).assert_matches();
			}
		}
	}
}

#[test]
fn carry_handles_none_inputs() {
	for &seed in &common::SEEDS {
		{
			for retention in [None, Some(90)] {
				run(true, common::full_chaos(200), seed, retention).assert_matches();
			}
		}
	}
}

#[test]
fn carry_chains_across_windows() {
	// A run where nothing ever carries would match the oracle trivially without proving the
	// chain works.
	let outcome = run(false, common::baseline(250, SupportedOps::insert_only()), 42, None);
	outcome.assert_matches();
	let carried = outcome
		.oracle_table
		.rows
		.values()
		.filter(|r| matches!(r.columns.get("has_carry"), Some(Value::Boolean(true))))
		.count();
	assert!(carried > 0, "expected at least one window to carry a prior close");
}

#[test]
fn carry_empty_stream_is_empty() {
	let outcome = run(false, common::baseline(0, SupportedOps::all()), 0, None);
	outcome.assert_matches();
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}
