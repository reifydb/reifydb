// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reifydb_core::{
	expression::Expression,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	metrics::heap::OperatorSample,
};
use reifydb_flow::{aggregate::AggregateContext, context::FlowContext, operator::source::decode_change};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{Result, util::hash::Hash128, value::datetime::DateTime};

use super::{
	core::Aggregation,
	engine::{
		Routed, Routing, Stamp, distinct_row_runs, finish_tumbling_engine, intern_partition_groups,
		route_into_buckets,
	},
};
use crate::{
	operator::{HostOperator, host::HostContext, row_times, state::seal::coord::Coord},
	window::{
		engine::{ExpiryAnchor, config::WindowEngineConfig},
		span::WindowSpan,
	},
};

pub struct AggregateOperator {
	core: Aggregation,
}

impl AggregateOperator {
	pub fn new(
		parent_schema: Option<SchemaRef>,
		operator: OperatorId,
		by: Vec<Expression>,
		map: Vec<Expression>,
		routines: Routines,
		runtime_context: RuntimeContext,
	) -> Result<Self> {
		Ok(Self {
			core: Aggregation::new(
				operator,
				parent_schema,
				by,
				map,
				routines,
				runtime_context,
				AggregateContext::Grouped,
				Arc::new(FlowContext::default()),
			)?,
		})
	}

	pub(crate) fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.core.output_schema.clone())
	}
}

impl HostOperator for AggregateOperator {
	fn id(&self) -> OperatorId {
		self.core.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let change = decode_change(host, change)?;
		apply_aggregate_engine(&mut self.core, host, change)
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}

pub fn apply_aggregate_engine(core: &mut Aggregation, host: &mut dyn HostContext, change: Change) -> Result<Change> {
	let kinds = core.slot_kinds.clone().expect("aggregate requires representable slot kinds");

	let mut routing = Routing::default();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_grouped(core, post, true, &mut routing)?,
			Diff::Remove {
				pre,
				..
			} => route_grouped(core, pre, false, &mut routing)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				for (pre, post) in distinct_row_runs(pre, post)? {
					route_grouped(core, &pre, false, &mut routing)?;
					route_grouped(core, &post, true, &mut routing)?;
				}
			}
		}
	}

	let Routed {
		buckets,
		group_values,
		arrival,
		window_max_ts,
		..
	} = routing.finish();
	let engine_config = WindowEngineConfig::builder().build();

	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	let groups = intern_partition_groups(&windows);

	let diffs = finish_tumbling_engine(
		core,
		host,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		engine_config,
		None,
		ExpiryAnchor::Unindexed,
		false,
		Stamp::Latest,
	)?;
	Ok(Change::from_flow(core.operator, change.version, diffs, change.changed_at))
}

fn route_grouped(core: &Aggregation, batch: &RecordBatch, is_add: bool, routing: &mut Routing) -> Result<()> {
	let times = row_times(batch)?;
	let degenerate = WindowSpan::new(<DateTime as Coord>::from_order(0), <DateTime as Coord>::from_order(1));
	route_into_buckets(
		core,
		batch,
		is_add,
		|row_idx| (degenerate, times.get(row_idx).copied().flatten().unwrap_or_default()),
		routing,
	)
}
