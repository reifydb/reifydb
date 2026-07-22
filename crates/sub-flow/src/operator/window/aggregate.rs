// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
};

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
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
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
use crate::{context::FlowContext, operator::OperatorCell};

type EngineBuckets = TumblingBuckets<Hash128, u64, (WindowSlotKey, Vec<Option<Value>>)>;

pub struct AggregateOperator {
	core: Aggregation,
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
				Arc::new(FlowContext::default()),
			),
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
		OperatorCapability::STANDARD
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		apply_aggregate_engine(&self.core, txn, change)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.core.tumbling_engine_slot().as_ref().map(|engine| {
			OperatorSample::with_memory(engine.approximate_memory())
				.with_dirty_memory(engine.dirty_memory())
				.with_membership(engine.membership_memory())
				.with_completeness(engine.completeness())
		})
	}
}

pub fn apply_aggregate_engine(core: &Aggregation, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
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

	let engine_config = WindowEngineConfig::builder(txn.state_budget()).build();

	let diffs = finish_tumbling_engine(
		core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		engine_config,
		Duration::default(),
		false,
	)?;
	Ok(Change::from_flow(core.node, change.version, diffs, change.changed_at))
}
