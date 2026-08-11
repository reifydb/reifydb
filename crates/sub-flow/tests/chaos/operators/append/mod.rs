// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use rand::{RngExt, rngs::StdRng};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_flow::{
	operator::{OperatorCell, append::AppendOperator, scan::series::SourceSeriesOperator},
	transaction::deferred::DeferredTransaction,
};
use reifydb_testing_chaos::{
	corpus::Corpus,
	fuzz::{run_reported, split},
	operator::{
		drive as driver,
		scenario::{BatchSize, Scenario},
	},
};
use reifydb_value::value::duration::Duration;

use crate::{
	framework::harness::Harness,
	operators::append::{
		oracle::AppendOracle,
		workload::{APPEND_OPERATOR, AppendWorkload, input},
	},
};

pub fn build(inputs: usize) -> AppendOperator<DeferredTransaction> {
	build_with_ttl(inputs, None)
}

pub fn build_with_ttl(inputs: usize, ttl: Option<Duration>) -> AppendOperator<DeferredTransaction> {
	let operators: Vec<OperatorId> = (0..inputs).map(input).collect();
	let parents =
		operators.iter().map(|operator| OperatorCell::new(SourceSeriesOperator::new(*operator))).collect();
	AppendOperator::new(APPEND_OPERATOR, parents, operators, ttl)
}

#[derive(Debug, Clone)]
pub struct Params {
	pub inputs: usize,

	/// How many distinct row numbers each input draws from. Narrow enough that the inputs collide on
	/// the same number, which is what makes the input index in the group key load-bearing.
	pub row_space: u64,

	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(|_| build(params.inputs));
	let workload = AppendWorkload {
		inputs: params.inputs,
		row_space: params.row_space,
	};
	let mut model = AppendOracle::new();

	driver::drive(
		seed,
		// Append has no clock and nothing in flight, so there is no tick share and no drain horizon.
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
	let params = Params {
		inputs: rng.random_range(2..=4usize),
		row_space: row_space(&mut rng),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=60usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(5..=40u32),
	};
	(sequence_seed, params)
}

/// Deliberately reaches 1: every input writing the same single row number is the sharpest form of
/// the collision the group key exists to separate.
fn row_space(rng: &mut StdRng) -> u64 {
	rng.random_range(1..=24u64)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("append_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
