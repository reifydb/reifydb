// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use rand::{RngExt, rngs::StdRng};
use reifydb_flow::operator::{
	OperatorCell, aggregation::operator::AggregateOperator, scan::series::SourceSeriesOperator,
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
use reifydb_value::value::Value;

use crate::{
	framework::harness::Harness,
	operators::{
		aggregate::{
			oracle::AggregateOracle,
			workload::{AGGREGATE_OPERATOR, AggregateWorkload, SOURCE_OPERATOR},
		},
		routines,
	},
};

pub const GROUP_COLUMN: &str = "g";

/// The aggregate column's name, shared by every shape so the oracle and the operator agree on where to
/// look without each `Agg` restating it.
const OUTPUT_COLUMN: &str = "total";

/// One aggregate shape: the RQL the operator is built from, and the independent Rust fold the oracle
/// answers with. Adding a monoid means adding a cell here, which is what stops one being covered by
/// the sweeps while another is silently left out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
	Sum,
	Count,
	Min,
	Max,
}

/// Every shape, so a new monoid cannot be added without a sweep noticing it has no cell.
pub const MATRIX: [Agg; 4] = [Agg::Sum, Agg::Count, Agg::Min, Agg::Max];

impl Agg {
	pub fn label(self) -> &'static str {
		match self {
			Agg::Sum => "sum",
			Agg::Count => "count",
			Agg::Min => "min",
			Agg::Max => "max",
		}
	}

	pub fn column(self) -> &'static str {
		OUTPUT_COLUMN
	}

	fn expression(self) -> &'static str {
		match self {
			Agg::Sum => "total: math::sum(v)",
			Agg::Count => "total: math::count(v)",
			Agg::Min => "total: math::min(v)",
			Agg::Max => "total: math::max(v)",
		}
	}

	/// The answer, stated without the monoid registry.
	///
	/// The widths are measured from what the operator emits, not read off `Monoid::state_type` - the
	/// aggregate engine routes representable shapes through `SlotKind` and never consults the monoid,
	/// so `state_type` has no callers and says `Uint8` for count where the engine emits `Int8`. A
	/// width mismatch surfaces as a divergent value whose two sides print identically, which is
	/// unreadable, so the widths are pinned by name in a test of their own.
	pub fn fold(self, values: &[i64]) -> Value {
		assert!(!values.is_empty(), "a group with no live rows has no fold; the oracle must not publish one");
		match self {
			Agg::Sum => Value::Int16(values.iter().map(|v| *v as i128).sum()),
			Agg::Count => Value::Int8(values.len() as i64),
			Agg::Min => Value::Int8(*values.iter().min().expect("non-empty")),
			Agg::Max => Value::Int8(*values.iter().max().expect("non-empty")),
		}
	}
}

pub fn build(agg: Agg, runtime: RuntimeContext) -> AggregateOperator {
	let parent = OperatorCell::new(SourceSeriesOperator::new(SOURCE_OPERATOR));
	AggregateOperator::new(
		parent,
		AGGREGATE_OPERATOR,
		parse_expression(GROUP_COLUMN).expect("group_by parses"),
		parse_expression(agg.expression()).expect("aggregation parses"),
		routines(),
		runtime,
		None,
	)
}

#[derive(Debug, Clone)]
pub struct Params {
	pub agg: Agg,

	pub groups: i32,

	/// Narrow on purpose: `min` and `max` can only invert a retraction whose value is not the one the
	/// group currently reports, so a small ceiling is what drives the full-recompute path.
	pub value_ceiling: i64,

	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(|runtime| build(params.agg, runtime));
	let workload = AggregateWorkload {
		groups: params.groups,
		value_ceiling: params.value_ceiling,
	};
	let mut model = AggregateOracle::new(params.agg);

	driver::drive(
		seed,
		// An aggregate has no clock and holds nothing in flight, so there is no tick share and no drain
		// horizon; the drain exists only to prove ticking changes nothing.
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
		agg: MATRIX[rng.random_range(0..MATRIX.len())],
		groups: rng.random_range(1..=6i32),
		value_ceiling: value_ceiling(&mut rng),
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=60usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(5..=40u32),
	};
	(sequence_seed, params)
}

/// Deliberately reaches 1: every row in a group carrying the same value is the sharpest form of the
/// tie that `min` and `max` cannot invert through.
fn value_ceiling(rng: &mut StdRng) -> i64 {
	rng.random_range(1..=20i64)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("aggregate_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
