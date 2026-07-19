// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
	window::{
		engine::{config::WindowEngineConfig, tumbling::TumblingBuckets},
		span::WindowSpan,
	},
};
use reifydb_engine::flow::aggregate::AggregateContext;
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, duration::Duration},
};

use super::{
	accumulator::WindowSlotKey,
	aggregation::Aggregation,
	tumbling::{finish_tumbling_engine, route_into_buckets},
};
use crate::{
	operator::{Operator, OperatorCell, stateful::row::RowNumberProvider},
	transaction::FlowTransaction,
};

type EngineBuckets = TumblingBuckets<Hash128, u64, (WindowSlotKey, Vec<Option<Value>>)>;

pub struct AggregateOperator {
	core: Aggregation,
	row_number_provider: RowNumberProvider,
}

impl AggregateOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		by: Vec<Expression>,
		map: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Self {
		Self {
			core: Aggregation::new(
				node,
				parent,
				by,
				map,
				routines,
				runtime_context,
				AggregateContext::Grouped,
			),
			row_number_provider: RowNumberProvider::new(node),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.core.parent.output_schema()
	}
}

impl Operator for AggregateOperator {
	fn id(&self) -> FlowNodeId {
		self.core.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD_WITH_SAMPLE
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		apply_aggregate_engine(&self.core, txn, &self.row_number_provider, change)
	}

	fn sample(&self) -> Option<OperatorSample> {
		let base = match self.core.tumbling_engine_slot().as_ref() {
			Some(engine) => OperatorSample::with_memory(engine.approximate_memory()),
			None => OperatorSample::default(),
		};
		Some(base.with_row_number_cache(self.row_number_provider.memory()))
	}
}

pub fn apply_aggregate_engine(
	core: &Aggregation,
	txn: &mut FlowTransaction,
	row_numbers: &RowNumberProvider,
	change: Change,
) -> Result<Change> {
	let kinds = core.slot_kinds.clone().expect("aggregate requires representable slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64> = HashMap::new();

	let degenerate_span = |_row_idx: usize| (WindowSpan::new(0u64, 1u64), 0u64);

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_into_buckets(
				core,
				post,
				true,
				degenerate_span,
				&mut buckets,
				&mut group_values,
				&mut arrival,
				&mut window_max_ts,
			)?,
			Diff::Remove {
				pre,
				..
			} => route_into_buckets(
				core,
				pre,
				false,
				degenerate_span,
				&mut buckets,
				&mut group_values,
				&mut arrival,
				&mut window_max_ts,
			)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				route_into_buckets(
					core,
					pre,
					false,
					degenerate_span,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?;
				route_into_buckets(
					core,
					post,
					true,
					degenerate_span,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?;
			}
		}
	}

	let diffs = finish_tumbling_engine(
		core,
		txn,
		row_numbers,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		WindowEngineConfig::builder().build(),
		Duration::default(),
		false,
	)?;
	Ok(Change::from_flow(core.node, change.version, diffs, change.changed_at))
}
