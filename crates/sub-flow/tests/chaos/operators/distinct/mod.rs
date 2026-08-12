// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use std::sync::Arc;

use rand::{RngExt, rngs::StdRng};
use reifydb_core::value::column::columns::Columns;
use reifydb_flow::{context::FlowContext, operator::distinct::operator::DistinctOperator};
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::RuntimeContext;
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
	operators::{
		distinct::{
			oracle::DistinctOracle,
			workload::{DISTINCT_OPERATOR, DistinctWorkload, KEY_COLUMN},
		},
		routines,
	},
};

pub fn build(runtime: RuntimeContext) -> DistinctOperator {
	DistinctOperator::new(
		Some(Columns::empty()),
		DISTINCT_OPERATOR,
		parse_expression(KEY_COLUMN).expect("the distinct key parses"),
		routines(),
		runtime,
		Arc::new(FlowContext::default()),
		None,
	)
}

#[derive(Debug, Clone)]
pub struct Params {
	/// Narrow is the interesting direction: with one key every row collides, which is the sharpest
	/// form of the contention the operator's per-key row map exists to resolve.
	pub groups: i32,

	pub value_ceiling: i64,

	pub regroup_pct: u32,

	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(build);
	let workload = DistinctWorkload {
		groups: params.groups,
		value_ceiling: params.value_ceiling,
		regroup_pct: params.regroup_pct,
	};
	let mut model = DistinctOracle::new();

	driver::drive(
		seed,
		// No clock and nothing in flight, so no tick share and no drain horizon; the drain runs only to
		// prove that ticking past every horizon changes nothing.
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
		groups: groups(&mut rng),
		value_ceiling: rng.random_range(2..=20i64),
		regroup_pct: rng.random_range(0..=60u32),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=60usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(5..=40u32),
	};
	(sequence_seed, params)
}

/// Deliberately reaches 1: every row in the corpus sharing one distinct key means every arrival either
/// displaces the visible row or is suppressed by it, and every departure either promotes a successor or
/// empties the key.
fn groups(rng: &mut StdRng) -> i32 {
	rng.random_range(1..=8i32)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("distinct_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
