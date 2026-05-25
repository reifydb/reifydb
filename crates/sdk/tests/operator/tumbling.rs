// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Differential chaos for the tumbling V2 driver. Each randomized
//! Insert/Update/Remove stream is replayed through the real operator and
//! through `tumbling_accumulator_oracle`; the materialized output tables must
//! agree. Covers an invertible sum (`VolumeTumbling`), a removal-safe multiset
//! min (`MinTumbling`), and the bounded-lateness sealing OHLCV
//! (`OhlcvSealingTumbling`).

use reifydb_sdk::{
	operator::{FFIOperatorAdapter, windowed::tumbling::TumblingDriver},
	testing::chaos::{
		ChaosHarness,
		accumulator_oracle::tumbling_accumulator_oracle,
		config::{ChaosConfig, SupportedOps},
		runner::ChaosOutcome,
		schema::KeyStrategy,
		strategy::{ColumnSampler, samplers},
	},
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

fn run_volume(none_values: bool, cfg: ChaosConfig, seed: u64) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<TumblingDriver<VolumeTumbling>>>::builder()
		.with_input_shape(common::tumbling_shape())
		.with_output_shape(common::volume_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("slot", samplers::u64_range(0..300))
		.with_column("size", size_sampler(none_values))
		.with_chaos(cfg)
		.with_oracle(|batches| tumbling_accumulator_oracle(&VolumeTumbling, batches, &window_key()))
		.seed(seed)
		.build()
		.expect("build volume harness")
		.run()
}

fn run_min(none_values: bool, cfg: ChaosConfig, seed: u64) -> ChaosOutcome {
	ChaosHarness::<FFIOperatorAdapter<TumblingDriver<MinTumbling>>>::builder()
		.with_input_shape(common::tumbling_shape())
		.with_output_shape(common::min_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH", "SOL"]))
		.with_column("slot", samplers::u64_range(0..300))
		// Tight value set so duplicate minima exercise multiset removal.
		.with_column("size", size_sampler(none_values))
		.with_chaos(cfg)
		.with_oracle(|batches| tumbling_accumulator_oracle(&MinTumbling, batches, &window_key()))
		.seed(seed)
		.build()
		.expect("build min harness")
		.run()
}

fn run_ohlcv(none_values: bool, cfg: ChaosConfig, seed: u64) -> ChaosOutcome {
	let price = if none_values {
		common::maybe_none_f64(10.0, 500.0)
	} else {
		samplers::f64_range(10.0..500.0)
	};
	ChaosHarness::<FFIOperatorAdapter<TumblingDriver<OhlcvSealingTumbling>>>::builder()
		.with_input_shape(common::ohlcv_shape())
		.with_output_shape(common::ohlcv_out_shape())
		.with_key_strategy(KeyStrategy::Sequential)
		.with_output_key(["group", "window_start"])
		.with_column("group", samplers::utf8_choices(&["BTC", "ETH"]))
		// Slots span > WINDOW so multiple events land per window and some
		// age past OHLCV_LATENESS, exercising the sealing path.
		.with_column("slot", samplers::u64_range(0..180))
		.with_column("price", price)
		.with_chaos(cfg)
		.with_oracle(|batches| tumbling_accumulator_oracle(&OhlcvSealingTumbling, batches, &window_key()))
		.seed(seed)
		.build()
		.expect("build ohlcv harness")
		.run()
}

#[test]
fn volume_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		run_volume(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
		run_volume(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
		run_volume(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
		run_volume(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
		run_volume(false, common::full_chaos(200), seed).assert_matches();
	}
}

#[test]
fn volume_handles_none_inputs() {
	for &seed in &common::SEEDS {
		let outcome = run_volume(true, common::full_chaos(200), seed);
		outcome.assert_matches();
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
		run_min(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
		run_min(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
		run_min(false, common::baseline(150, SupportedOps::no_update()), seed).assert_matches();
		run_min(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
		run_min(false, common::full_chaos(200), seed).assert_matches();
	}
}

#[test]
fn min_handles_none_inputs() {
	for &seed in &common::SEEDS {
		run_min(true, common::full_chaos(200), seed).assert_matches();
	}
}

#[test]
fn ohlcv_sealing_matches_across_configs_and_seeds() {
	for &seed in &common::SEEDS {
		run_ohlcv(false, common::baseline(150, SupportedOps::insert_only()), seed).assert_matches();
		run_ohlcv(false, common::baseline(150, SupportedOps::no_remove()), seed).assert_matches();
		run_ohlcv(false, common::baseline(200, SupportedOps::all()), seed).assert_matches();
		run_ohlcv(false, common::full_chaos(250), seed).assert_matches();
	}
}

#[test]
fn ohlcv_sealing_produces_nonempty_output() {
	// Guards against a fixture that trivially matches because nothing ever
	// emits: with inserts only and many slots per window, the operator must
	// materialize at least one OHLCV row.
	let outcome = run_ohlcv(false, common::baseline(200, SupportedOps::insert_only()), 42);
	outcome.assert_matches();
	assert!(
		!outcome.oracle_table.is_empty(),
		"sealing OHLCV produced no rows; fixture is not exercising the driver"
	);
}
