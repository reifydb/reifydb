// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use rand::RngExt;
use reifydb_core::value::column::columns::Columns;
use reifydb_flow::operator::take::TakeOperator;
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{run_reported, split},
	operator::{
		drive as driver,
		scenario::{BatchSize, Scenario},
	},
};

use crate::{
	framework::harness::Harness,
	operators::take::{
		oracle::TakeOracle,
		workload::{TAKE_OPERATOR, TakeWorkload},
	},
};

/// The widest live set an exact oracle stays valid for at a given limit. Beyond it the operator's
/// candidate buffer prunes and a pruned row can never be promoted back. See `TakeOracle`.
pub const fn exact_oracle_ceiling(limit: usize) -> usize {
	limit * 5
}

pub fn build(limit: usize) -> TakeOperator {
	TakeOperator::new(Some(Columns::empty()), TAKE_OPERATOR, limit)
}

#[derive(Debug, Clone)]
pub struct Params {
	pub limit: usize,

	pub value_ceiling: i64,

	pub steps: u32,
	pub max_batch: u32,

	/// Must not exceed `exact_oracle_ceiling(limit)`; the oracle asserts it rather than trusting it.
	pub max_live: usize,

	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(|_| build(params.limit));
	let workload = TakeWorkload {
		value_ceiling: params.value_ceiling,
	};
	let mut model = TakeOracle::new(params.limit);

	driver::drive(
		seed,
		Scenario::mixed(params.steps)
			.with_batch(BatchSize::Geometric {
				p: 0.45,
				max: params.max_batch,
			})
			.with_mix(params.remove_pct, params.update_pct, 0)
			.with_max_live(params.max_live),
		&mut harness,
		&workload,
		&mut model,
	)
	.assert_clean()
	.corpus
}

pub fn random_params(seed: u64) -> (u64, Params) {
	let (mut rng, sequence_seed) = split(seed);
	let max_batch = rng.random_range(1..=8u32);
	// Reaches 1: a limit of one means every arrival evicts the incumbent and every departure promotes
	// a candidate, which is the eviction and promotion paths on every single step.
	let limit = rng.random_range(1..=12usize);
	let params = Params {
		limit,
		value_ceiling: rng.random_range(2..=40i64),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		// Deliberately allowed to sit below the limit as well as above it: a live set smaller than the
		// limit never evicts at all, which is the path where take must behave as a pass-through.
		max_live: rng.random_range(2..=exact_oracle_ceiling(limit)),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(5..=40u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("take_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
