// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the tumbling carry-forward driver (`TwapCarry`).
//! The window output depends on the prior window's carried-forward close, so
//! correctness hinges on the carry rotating exactly once per window-boundary
//! crossing and surviving Updates/Removes inside the current window. The ts
//! range spans several windows so the carry chains across boundaries.

use reifydb_sdk::{
	operator::{FFIOperatorAdapter, windowed::tumbling_carry::TumblingCarryDriver},
	testing::chaos::{
		ChaosHarness,
		accumulator_oracle::tumbling_carry_accumulator_oracle,
		config::{ChaosConfig, SupportedOps},
		runner::ChaosOutcome,
		schema::KeyStrategy,
		strategy::{ColumnSampler, samplers},
	},
};
use reifydb_type::value::Value;

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

fn run(none_values: bool, cfg: ChaosConfig, seed: u64) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<TumblingCarryDriver<TwapCarry>>>::builder()
		.with_input_shape(common::carry_shape())
		.with_output_shape(common::carry_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("ts", samplers::u64_range(0..300))
		.with_column("price", price_sampler(none_values))
		.with_chaos(cfg)
		.with_oracle(|ctx, batches| tumbling_carry_accumulator_oracle(&TwapCarry, ctx, batches, &window_key()))
		.seed(seed)
		.build()
		.expect("build carry harness")
		.run()
}

#[test]
fn carry_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		run(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
		run(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
		run(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
		run(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
		run(false, common::full_chaos(250), seed).assert_matches();
	}
}

#[test]
fn carry_handles_none_inputs() {
	for &seed in &common::SEEDS {
		run(true, common::full_chaos(200), seed).assert_matches();
	}
}

#[test]
fn carry_chains_across_windows() {
	// With many timestamps spanning several windows and inserts only, at
	// least one emitted window must carry a prior close (has_carry = true).
	let outcome = run(false, common::baseline(250, SupportedOps::insert_only()), 42);
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
	let outcome = run(false, common::baseline(0, SupportedOps::all()), 0);
	outcome.assert_matches();
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}
