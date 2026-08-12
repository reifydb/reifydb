// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use std::sync::Arc;

use rand::RngExt;
use reifydb_core::value::column::columns::Columns;
use reifydb_flow::{context::FlowContext, operator::gate::GateOperator};
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
		gate::{
			oracle::GateOracle,
			workload::{GATE_OPERATOR, GateWorkload, PAYLOAD_COLUMN},
		},
		routines,
	},
};

pub fn condition(threshold: i64) -> String {
	format!("{PAYLOAD_COLUMN} > {threshold}")
}

pub fn build(threshold: i64, runtime: RuntimeContext) -> GateOperator {
	GateOperator::new(
		Some(Columns::empty()),
		GATE_OPERATOR,
		parse_expression(&condition(threshold)).expect("the gate condition parses"),
		routines(),
		runtime,
		Arc::new(FlowContext::default()),
	)
}

#[derive(Debug, Clone)]
pub struct Params {
	/// A row is admitted the first time its payload exceeds this. Where it sits relative to
	/// `value_ceiling` sets how many rows ever get in at all.
	pub threshold: i64,

	pub value_ceiling: i64,

	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(|runtime| build(params.threshold, runtime));
	let workload = GateWorkload {
		value_ceiling: params.value_ceiling,
	};
	let mut model = GateOracle::new(params.threshold);

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
	let value_ceiling = rng.random_range(4..=100i64);
	let params = Params {
		// Spans the whole range including both ends: a threshold at zero admits everything on first
		// sight and never exercises a late admission, while one at the ceiling admits almost nothing
		// and makes every update a chance to cross upwards for the first time.
		threshold: rng.random_range(0..=value_ceiling),
		value_ceiling,
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=60usize),
		remove_pct: rng.random_range(5..=35u32),
		// Updates are what make a gate a gate: they are the only way a row crosses the threshold after
		// arrival, and the only way a visible row can fall back below it while staying visible.
		update_pct: rng.random_range(20..=50u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("gate_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
