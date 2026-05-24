// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the multi-rolling V2 driver
//! (`TopVolumeMultiRolling`): a rolling buffer that emits multiple rows per
//! group keyed by rank, with Insert/Update/**Remove** diffing as the top-K set
//! churns. A small trader space and wide window space make ranks appear,
//! change, and vanish across batches, exercising the per-secondary-key
//! emission and the high-water-driven Remove path.

use reifydb_sdk::{
	operator::{FFIOperatorAdapter, windowed::multi_rolling_v2::MultiRollingDriverV2},
	testing::chaos::{
		ChaosHarness,
		accumulator_oracle::multi_rolling_accumulator_oracle,
		config::{ChaosConfig, SupportedOps},
		runner::ChaosOutcome,
		schema::KeyStrategy,
		strategy::{ColumnSampler, samplers},
	},
};

use super::common::{self, TopVolumeMultiRolling};

fn rank_key() -> Vec<String> {
	vec!["group".to_string(), "rank".to_string()]
}

fn volume_sampler(none_values: bool) -> ColumnSampler {
	if none_values {
		common::maybe_none_f64(1.0, 100.0)
	} else {
		samplers::f64_range(1.0..100.0)
	}
}

fn run(none_values: bool, cfg: ChaosConfig, seed: u64) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<MultiRollingDriverV2<TopVolumeMultiRolling>>>::builder()
		.with_input_shape(common::multi_rolling_shape())
		.with_output_shape(common::top_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "rank"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH"]))
		.with_column("window_start", samplers::u64_range(0..10))
		// Small trader space so the top-2 set churns and ranks vanish.
		.with_column("trader", samplers::u64_range(0..5))
		.with_column("volume", volume_sampler(none_values))
		.with_chaos(cfg)
		.with_oracle(|batches| multi_rolling_accumulator_oracle(&TopVolumeMultiRolling, batches, &rank_key()))
		.seed(seed)
		.build()
		.expect("build multi-rolling harness")
		.run()
}

#[test]
fn top_volume_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		run(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
		run(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
		run(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
		run(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
		run(false, common::full_chaos(250), seed).assert_matches();
	}
}

#[test]
fn top_volume_handles_none_inputs() {
	for &seed in &common::SEEDS {
		run(true, common::full_chaos(200), seed).assert_matches();
	}
}

#[test]
fn top_volume_emits_multiple_ranks() {
	// Two ranks must materialize at least once; otherwise the secondary-key
	// emission path is not being exercised.
	let outcome = run(false, common::baseline(200, SupportedOps::insert_only()), 99);
	outcome.assert_matches();
	let ranks = outcome.oracle_table.rows.keys().count();
	assert!(ranks >= 2, "expected at least two (group, rank) rows, got {ranks}");
}

#[test]
fn top_volume_empty_stream_is_empty() {
	let outcome = run(false, common::baseline(0, SupportedOps::all()), 0);
	outcome.assert_matches();
	assert!(outcome.operator_table.is_empty());
	assert!(outcome.oracle_table.is_empty());
}
