// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod oracle;
pub mod workload;

use std::sync::Arc;

use rand::RngExt;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	value::column::columns::Columns,
};
use reifydb_flow::{
	context::FlowContext,
	operator::{Operator, bridge::Bridge, extend::ExtendOperator, filter::FilterOperator, map::MapOperator},
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
use reifydb_value::{Result, value::Value};

use crate::{
	framework::harness::Harness,
	operators::{
		routines,
		rowwise::{
			oracle::RowwiseOracle,
			workload::{IDENTITY_COLUMN, PAYLOAD_COLUMN, ROWWISE_OPERATOR, RowwiseRow, RowwiseWorkload},
		},
	},
};

/// The name the derived column is published under by map and extend.
pub const DERIVED_COLUMN: &str = "doubled";

/// The multiplier the derived column applies. Named so the RQL the operator is built from and the
/// arithmetic the oracle answers with cannot drift apart by a typo in one of them.
const FACTOR: i64 = 2;

/// One row-wise operator, as the pair of things that must agree: the RQL it is built from, and the
/// answer stated independently in Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
	/// Membership changes with the value, so an update can become an insert or a remove.
	Filter {
		threshold: i64,
	},

	/// Replaces the row with a projection, dropping the payload column.
	Map,

	/// Keeps every source column and appends a derived one.
	Extend,
}

impl Shape {
	pub fn label(self) -> &'static str {
		match self {
			Shape::Filter {
				..
			} => "filter",
			Shape::Map => "map",
			Shape::Extend => "extend",
		}
	}

	/// The expressions the operator is built from. A list rather than one string because
	/// `parse_expression` parses a single expression: a comma-separated projection is two parses
	/// concatenated, not one parse of a longer string.
	fn rql(self) -> Vec<String> {
		match self {
			Shape::Filter {
				threshold,
			} => vec![format!("{PAYLOAD_COLUMN} > {threshold}")],
			Shape::Map => vec![
				IDENTITY_COLUMN.to_string(),
				format!("{DERIVED_COLUMN}: {PAYLOAD_COLUMN} * {FACTOR}"),
			],
			Shape::Extend => vec![format!("{DERIVED_COLUMN}: {PAYLOAD_COLUMN} * {FACTOR}")],
		}
	}

	/// The output column positions the driver compares. Extend appends a column, so it has one more
	/// than the other two; a projection that named only the first two would compare extend's output
	/// without ever looking at the column extend exists to add.
	pub fn projection(self) -> &'static [usize] {
		match self {
			Shape::Filter {
				..
			}
			| Shape::Map => &[0, 1],
			Shape::Extend => &[0, 1, 2],
		}
	}

	/// Whether a live row reaches the output at all. Only filter ever says no.
	pub fn admits(self, row: &RowwiseRow) -> bool {
		match self {
			Shape::Filter {
				threshold,
			} => row.value > threshold,
			Shape::Map | Shape::Extend => true,
		}
	}

	/// The output row, column for column, in the order the operator publishes them.
	///
	/// The widths are measured from the operator rather than assumed, and the derived column is where
	/// that matters: arithmetic promotes, so an int8 payload times a literal comes out int16 - the same
	/// promotion `math::sum` applies. A column that merely passes through keeps its width.
	/// `the_rowwise_operators_emit_what_their_oracles_render` is what pins this.
	pub fn render(self, row: &RowwiseRow) -> Vec<Value> {
		let derived = Value::Int16((row.value * FACTOR) as i128);
		match self {
			Shape::Filter {
				..
			} => vec![Value::Int4(row.identity()), Value::Int8(row.value)],
			Shape::Map => vec![Value::Int4(row.identity()), derived],
			Shape::Extend => vec![Value::Int4(row.identity()), Value::Int8(row.value), derived],
		}
	}
}

pub const MATRIX: [Shape; 3] = [
	Shape::Filter {
		threshold: 50,
	},
	Shape::Map,
	Shape::Extend,
];

/// One of the three, as a concrete type. The harness needs a sized subject; an enum that delegates is the
/// smallest thing that gives all three shapes one driver.
pub enum Rowwise {
	Filter(FilterOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
}

impl Operator for Rowwise {
	fn id(&self) -> OperatorId {
		match self {
			Rowwise::Filter(op) => Operator::id(op),
			Rowwise::Map(op) => Operator::id(op),
			Rowwise::Extend(op) => Operator::id(op),
		}
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		match self {
			Rowwise::Filter(op) => Operator::capabilities(op),
			Rowwise::Map(op) => Operator::capabilities(op),
			Rowwise::Extend(op) => Operator::capabilities(op),
		}
	}

	fn apply(&mut self, bridge: &mut dyn Bridge, change: Change) -> Result<Change> {
		match self {
			Rowwise::Filter(op) => op.apply(bridge, change),
			Rowwise::Map(op) => op.apply(bridge, change),
			Rowwise::Extend(op) => op.apply(bridge, change),
		}
	}

	fn output_schema(&self) -> Option<Columns> {
		match self {
			Rowwise::Filter(op) => Operator::output_schema(op),
			Rowwise::Map(op) => Operator::output_schema(op),
			Rowwise::Extend(op) => Operator::output_schema(op),
		}
	}
}

pub fn build(shape: Shape, runtime: RuntimeContext) -> Rowwise {
	let parent_schema = Some(Columns::empty());
	let expressions: Vec<_> = shape
		.rql()
		.iter()
		.flat_map(|text| parse_expression(text).expect("the shape's expression parses"))
		.collect();
	let ctx = Arc::new(FlowContext::default());
	match shape {
		Shape::Filter {
			..
		} => Rowwise::Filter(FilterOperator::new(
			parent_schema,
			ROWWISE_OPERATOR,
			expressions,
			routines(),
			runtime,
			ctx,
		)),
		Shape::Map => Rowwise::Map(MapOperator::new(
			parent_schema,
			ROWWISE_OPERATOR,
			expressions,
			routines(),
			runtime,
			ctx,
		)),
		Shape::Extend => Rowwise::Extend(ExtendOperator::new(
			parent_schema,
			ROWWISE_OPERATOR,
			expressions,
			routines(),
			runtime,
			ctx,
		)),
	}
}

#[derive(Debug, Clone)]
pub struct Params {
	pub shape: Shape,
	pub value_ceiling: i64,
	pub steps: u32,
	pub max_batch: u32,
	pub max_live: usize,
	pub remove_pct: u32,
	pub update_pct: u32,
}

pub fn drive(seed: u64, params: Params) -> Corpus {
	let mut harness = Harness::new(|runtime| build(params.shape, runtime));
	let workload = RowwiseWorkload {
		value_ceiling: params.value_ceiling,
		shape: params.shape,
	};
	let mut model = RowwiseOracle::new(params.shape);

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
	let shape = match rng.random_range(0..3u32) {
		0 => Shape::Filter {
			// Spans the range so the sweep reaches both the almost-everything-passes and the
			// almost-nothing-passes ends, where an update crossing the predicate is common or rare.
			threshold: rng.random_range(0..=value_ceiling),
		},
		1 => Shape::Map,
		_ => Shape::Extend,
	};
	let params = Params {
		shape,
		value_ceiling,
		max_batch,
		steps: rng.random_range(30..=90u32).min((320 / max_batch).max(30)),
		max_live: rng.random_range(8..=60usize),
		remove_pct: rng.random_range(5..=35u32),
		update_pct: rng.random_range(20..=50u32),
	};
	(sequence_seed, params)
}

pub fn drive_random(seed: u64) {
	let (sequence_seed, params) = random_params(seed);
	let run = params.clone();
	run_reported("rowwise_random_chaos", sequence_seed, &params, || {
		drive(sequence_seed, run);
	});
}
