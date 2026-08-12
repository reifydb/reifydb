// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Two operators driven as one subject, so the corpus crosses a boundary rather than stopping at it.
//!
//! Every other suite drives a single operator and compares the view it publishes. That cannot see
//! anything about how one operator's output is consumed - most sharply, it cannot see the `pre` half
//! of an Update at all, because a folded view keeps only the post. A downstream aggregate does read
//! `pre`: it retracts the old contribution and admits the new one, so a wrong `pre` shows up as a
//! total that drifts. Chaining is what turns an invisible defect into a visible one.
//!
//! The composition is deliberately literal - `second.apply(txn, first.apply(txn, change)?)` - because
//! that is what a real flow does with an operator's output change.

pub mod oracle;

use std::sync::Arc;

use rand::RngExt;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	value::column::columns::Columns,
};
use reifydb_flow::{
	context::FlowContext,
	operator::{
		Operator, aggregation::operator::AggregateOperator, bridge::Bridge, filter::FilterOperator,
		gate::GateOperator, map::MapOperator,
	},
};
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
		aggregate::workload::{AggregateRow, AggregateWorkload},
		pipeline::oracle::PipelineOracle,
		routines,
	},
};

const SOURCE_OPERATOR: OperatorId = OperatorId(0);

/// The first stage. Distinct from the terminal aggregate's id so the two cannot share state.
const STAGE_OPERATOR: OperatorId = OperatorId(1);

const TERMINAL_OPERATOR: OperatorId = OperatorId(2);

const GROUP_COLUMN: &str = "g";
const PAYLOAD_COLUMN: &str = "v";

/// The column map projects the payload into, and what the terminal aggregate then sums.
const MAPPED_COLUMN: &str = "scaled";

const FACTOR: i64 = 2;

const TOTAL_COLUMN: &str = "total";

/// Which upstream operator feeds the aggregate. Every variant ends in the same
/// `total: math::sum(...) by g`, so a divergence is attributable to the stage rather than to the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
	/// Membership changes upstream, so the aggregate sees inserts and removes where the corpus only
	/// did updates. Getting the transition wrong makes a row contribute twice or not at all.
	Filter {
		threshold: i64,
	},

	/// Content changes upstream. The aggregate sums the projected column, so a wrong projection or a
	/// wrong `pre` on an update both land as a wrong total.
	Map,

	/// A latch upstream. A row that falls back below the threshold stays in the aggregate, which is
	/// the shape a filter-shaped gate would silently get wrong.
	Gate {
		threshold: i64,
	},
}

impl Chain {
	pub fn label(self) -> &'static str {
		match self {
			Chain::Filter {
				..
			} => "filter_aggregate",
			Chain::Map => "map_aggregate",
			Chain::Gate {
				..
			} => "gate_aggregate",
		}
	}

	fn stage_rql(self) -> Vec<String> {
		match self {
			Chain::Filter {
				threshold,
			}
			| Chain::Gate {
				threshold,
			} => vec![format!("{PAYLOAD_COLUMN} > {threshold}")],
			Chain::Map => {
				vec![GROUP_COLUMN.to_string(), format!("{MAPPED_COLUMN}: {PAYLOAD_COLUMN} * {FACTOR}")]
			}
		}
	}

	fn summed_column(self) -> &'static str {
		match self {
			Chain::Map => MAPPED_COLUMN,
			_ => PAYLOAD_COLUMN,
		}
	}

	/// What one admitted row contributes to its group's total, stated in Rust rather than derived
	/// through the same expression the operator evaluates.
	pub fn contribution(self, row: &AggregateRow) -> i128 {
		match self {
			Chain::Map => (row.value * FACTOR) as i128,
			_ => row.value as i128,
		}
	}

	/// Whether a row passes the first stage on its own current value. Gate's latch is not expressed
	/// here - the oracle holds that, because it depends on history rather than on the row.
	pub fn passes(self, row: &AggregateRow) -> bool {
		match self {
			Chain::Filter {
				threshold,
			}
			| Chain::Gate {
				threshold,
			} => row.value > threshold,
			Chain::Map => true,
		}
	}

	pub fn latches(self) -> bool {
		matches!(self, Chain::Gate { .. })
	}
}

pub const MATRIX: [Chain; 3] = [
	Chain::Filter {
		threshold: 50,
	},
	Chain::Map,
	Chain::Gate {
		threshold: 50,
	},
];

/// Two operators as one subject. The harness drives a single `Operator`, and a real flow feeds each
/// operator's output change to the next, so composing them inside one `apply` is both the smallest
/// change and the faithful one.
pub struct Pipeline {
	stage: Box<dyn Operator + Send>,
	terminal: AggregateOperator,
}

impl Operator for Pipeline {
	fn id(&self) -> OperatorId {
		Operator::id(&self.terminal)
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		Operator::capabilities(&self.terminal)
	}

	fn apply(&mut self, bridge: &mut dyn Bridge, change: Change) -> Result<Change, reifydb_value::error::Error> {
		let staged = self.stage.apply(bridge, change)?;
		self.terminal.apply(bridge, staged)
	}

	fn output_schema(&self) -> Option<Columns> {
		Operator::output_schema(&self.terminal)
	}
}

pub fn build(chain: Chain, runtime: RuntimeContext) -> Pipeline {
	let source_schema = Some(Columns::empty());
	let expressions: Vec<_> = chain
		.stage_rql()
		.iter()
		.flat_map(|text| parse_expression(text).expect("the stage expression parses"))
		.collect();
	let ctx = Arc::new(FlowContext::default());

	let stage: Box<dyn Operator + Send> = match chain {
		Chain::Filter {
			..
		} => Box::new(FilterOperator::new(
			source_schema.clone(),
			STAGE_OPERATOR,
			expressions,
			routines(),
			runtime.clone(),
			ctx.clone(),
		)),
		Chain::Map => Box::new(MapOperator::new(
			source_schema.clone(),
			STAGE_OPERATOR,
			expressions,
			routines(),
			runtime.clone(),
			ctx.clone(),
		)),
		Chain::Gate {
			..
		} => Box::new(GateOperator::new(
			source_schema.clone(),
			STAGE_OPERATOR,
			expressions,
			routines(),
			runtime.clone(),
			ctx.clone(),
		)),
	};

	let terminal = AggregateOperator::new(
		source_schema,
		TERMINAL_OPERATOR,
		parse_expression(GROUP_COLUMN).expect("group_by parses"),
		parse_expression(&format!("{TOTAL_COLUMN}: math::sum({})", chain.summed_column()))
			.expect("the aggregation parses"),
		routines(),
		runtime,
		None,
	);

	Pipeline {
		stage,
		terminal,
	}
}

#[derive(Debug, Clone)]
pub struct Params {
	pub chain: Chain,
	pub groups: i32,
	pub value_ceiling: i64,
	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(|runtime| build(params.chain, runtime));
	let workload = AggregateWorkload {
		groups: params.groups,
		value_ceiling: params.value_ceiling,
	};
	let mut model = PipelineOracle::new(params.chain);

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
	let chain = match rng.random_range(0..3u32) {
		0 => Chain::Filter {
			threshold: rng.random_range(0..=value_ceiling),
		},
		1 => Chain::Map,
		_ => Chain::Gate {
			threshold: rng.random_range(0..=value_ceiling),
		},
	};
	let params = Params {
		chain,
		groups: rng.random_range(1..=5i32),
		value_ceiling,
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=50usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(20..=50u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("pipeline_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
