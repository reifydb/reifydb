// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use reifydb_core::{
	expression::Expression,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	internal_err,
	metrics::heap::OperatorSample,
};
use reifydb_flow::{
	aggregate::{AggregateContext, SlotKind},
	context::FlowContext,
	operator::source::decode_change,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime},
};

use super::{
	accumulator::RowAccumulator,
	core::{Aggregation, EmitRow},
	engine::{
		EngineBuckets, Routed, Routing, WindowGroups, distinct_row_runs, group_of, intern_partition_groups,
		route_into_buckets,
	},
};
use crate::{
	operator::{
		HostOperator,
		host::HostContext,
		row_times,
		state::{seal::coord::Coord, store},
	},
	window::{
		coord::DateTimeCoord,
		engine::{EmitKind, config::WindowEngineConfig, tumbling::TumblingEngine},
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
		..
	} = routing.finish();

	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	let groups = intern_partition_groups(&windows);

	let diffs = finish_aggregate_engine(
		core,
		host,
		buckets,
		&group_values,
		arrival,
		&groups,
		&kinds,
		change.changed_at,
	)?;
	Ok(Change::from_flow(core.operator, change.version, diffs, change.changed_at))
}

#[allow(clippy::too_many_arguments)]
fn finish_aggregate_engine(
	core: &mut Aggregation,
	host: &mut dyn HostContext,
	buckets: EngineBuckets,
	group_values: &HashMap<Hash128, Vec<Value>>,
	arrival: Vec<(Hash128, WindowSpan<DateTimeCoord>)>,
	groups: &WindowGroups,
	kinds: &[SlotKind],
	changed_at: DateTime,
) -> Result<Vec<Diff>> {
	let mut engine = core.tumbling_engine_slot().take().unwrap_or_else(|| {
		Box::new(TumblingEngine::<Hash128, DateTimeCoord, RowAccumulator>::new(
			WindowEngineConfig::builder().build(),
		))
	});
	let results = engine.apply(
		host,
		buckets,
		&arrival,
		|hash, window_start| (group_of(groups, *hash, window_start.to_order()), store::empty_key()),
		|| RowAccumulator::new(kinds, None),
	)?;
	*core.tumbling_engine_slot() = Some(engine);

	let mut inserts = Vec::new();
	let mut updates = Vec::new();
	let mut removes = Vec::new();
	for r in results {
		let Some(group_values) = group_values.get(&r.group).cloned() else {
			return internal_err!("aggregate result for group {:?} has no routed group values", r.group);
		};
		let emit = |mut slot_values: Vec<Value>| {
			let time = match slot_values.pop() {
				Some(Value::DateTime(time)) => time,
				other => panic!("aggregate group {:?}: the event time slot holds {other:?}", r.group),
			};
			EmitRow {
				group_values: group_values.clone(),
				slot_values,
				row_number: r.row_number,
				span: None,
				time: Some(time),
			}
		};
		let pre_values = r.prior.clone().unwrap_or_else(|| r.value.clone());
		match r.kind {
			EmitKind::Insert => inserts.push(emit(r.value)),
			EmitKind::Update => updates.push((emit(pre_values), emit(r.value))),
			EmitKind::Remove => removes.push(emit(pre_values)),
		}
	}
	core.emit_diffs(inserts, updates, removes, changed_at)
}

fn route_grouped(core: &Aggregation, batch: &RecordBatch, is_add: bool, routing: &mut Routing) -> Result<()> {
	let times = row_times(batch)?;
	let degenerate =
		WindowSpan::new(<DateTimeCoord as Coord>::from_order(0), <DateTimeCoord as Coord>::from_order(1));
	route_into_buckets(
		core,
		batch,
		is_add,
		|row_idx| (degenerate, DateTimeCoord(times.get(row_idx).copied().flatten().unwrap_or_default())),
		routing,
	)
}
